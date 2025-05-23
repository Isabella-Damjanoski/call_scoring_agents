import azure.functions as func
import logging
import json
import os
import uuid
from openai import AzureOpenAI
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey
from azure.storage.blob import BlobServiceClient
import azure.cognitiveservices.speech as speechsdk
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import tempfile
import threading
load_dotenv()

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name(name="blobberfunction")
@app.blob_trigger(arg_name="myblob",
                  path="blobber-container/{name}",
                  connection="AzureWebJobsStorage")
def blobberfunction(myblob: func.InputStream):
    logging.info(f"Blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    # Download audio blob to temp file
    filename = os.path.basename(myblob.name).replace(" ", "_")
    temp_dir = tempfile.gettempdir()
    temp_audio_path = os.path.join(temp_dir, filename)

    with open(temp_audio_path, "wb") as audio_file:
        audio_file.write(myblob.read())

    # Transcribe audio using Azure Speech-to-Text
    speech_key = os.getenv("AZURE_SPEECH_KEY")
    speech_region = os.getenv("AZURE_SPEECH_REGION")
    speech_config = speechsdk.SpeechConfig(subscription=speech_key, region=speech_region)
    
    # Enable speaker diarization
    speech_config.set_service_property(
        name="diarizationEnabled",
        value="true",
        channel=speechsdk.ServicePropertyChannel.UriQueryParameter
    )
    speech_config.set_service_property(
        name="diarizationSpeakerCount",
        value="2",
        channel=speechsdk.ServicePropertyChannel.UriQueryParameter
    )

    # Set up audio and transcriber
    audio_config = speechsdk.AudioConfig(filename=temp_audio_path)
    transcriber = speechsdk.transcription.ConversationTranscriber(
        speech_config=speech_config, audio_config=audio_config)

    transcript_lines = []
    done = threading.Event()

    def handle_transcribed(evt):
        speaker = evt.result.speaker_id
        label = f"Speaker {speaker}" if speaker else "Unknown"
        if evt.result.text:
            transcript_lines.append(f"{label}: {evt.result.text}")
            logging.info(f"{label}: {evt.result.text}")

    def handle_canceled(evt):
        logging.warning(f"Transcription canceled: {evt.reason}")
        if evt.reason == speechsdk.CancellationReason.Error:
            logging.error(f"Error details: {evt.error_details}")
        done.set()

    def handle_session_stopped(evt):
        logging.info("Transcription session stopped.")
        done.set()

    transcriber.transcribed.connect(handle_transcribed)
    transcriber.canceled.connect(handle_canceled)
    transcriber.session_stopped.connect(handle_session_stopped)

    # Start transcription and wait until complete
    transcriber.start_transcribing_async().get()
    done.wait()
    transcriber.stop_transcribing_async().get()

    # Compile final transcript
    transcript = "\n".join(transcript_lines)

    # Generate a random call_id
    call_id = str(uuid.uuid4())

    # Prepare transcript as JSON with call_id
    transcript_json = json.dumps({
        "call_id": call_id,
        "transcript": transcript
    })

    # Send transcript to Service Bus Topic
    servicebus_conn_str = os.getenv("ServiceBusConnection")
    topic_name = os.getenv("AZURE_SERVICEBUS_TOPIC_NAME", "transcripttopic")
    with ServiceBusClient.from_connection_string(servicebus_conn_str) as client:
        sender = client.get_topic_sender(topic_name=topic_name)
        with sender:
            message = ServiceBusMessage(transcript_json)
            sender.send_messages(message)
    
    logging.info("Transcript sent to Service Bus topic.")

@app.function_name(name="politeness_from_topic")
@app.service_bus_topic_trigger(
    arg_name="msg",
    topic_name="transcriptstopic", 
    subscription_name="politeness_from_topic_sub", 
    connection="ServiceBusConnection"
)
def politeness_from_topic(msg: func.ServiceBusMessage) -> None:
    logging.info('Politeness assessment function triggered by Service Bus topic.')

    try:
        data = json.loads(msg.get_body().decode('utf-8'))
        call_id = data.get("call_id")
        transcript = data.get("transcript")
        if not call_id or not transcript:
                logging.error("Missing call_id or transcript in message.")
                return
    except Exception as e:
        logging.error(f"Could not parse Service Bus message as JSON: {e}")
        return
    
    # Initialize the Azure OpenAI client
    client = AzureOpenAI(
        api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_KEY"),
    )

    # Define the prompt for the AI
    messages = [
        {
            "role": "system",
            "content": (
                "You are a call center manager evaluating the performance of your agents. "
                "Your goal is to ensure that agents are polite and treat customers well. "
                "You score agents on politeness using a scale from of 1 to 5 where 1 is very impolite and 5 is extremely polite."
                "Especially for agents who don't performe well, you will provide a detailed reasoning for the score. "
                "You will also provide a brief summary of the call. "
                "The summary should include the main points of the conversation and any issues that were raised. "
                "The summary should be concise and to the point. "
                "{\"politeness_score\": <score>, \"summary\": <summary>, \"reasoning\": <reasoning>} "
                "Use double quotes for all keys and string values. Do NOT include any explanations, markdown, or extra text."
                "Respond ONLY with a valid JSON object. Use double quotes on all keys and values. Do NOT include any explanations or markdown formatting like ```json."
            ),
        },
        {
            "role": "user",
            "content": f"Transcript:\n{transcript}\n\nPoliteness score and reasoning:",
        },
    ]

    # Call the Azure OpenAI API
    response = client.chat.completions.create(
        messages=messages,
        max_tokens=500,
        temperature=0.7,
        top_p=1.0,
        model=os.getenv("AZURE_DEPLOYMENT_NAME"),
    )

    assessment = response.choices[0].message.content.strip()
    try:
        assessment_data = json.loads(assessment)
    except Exception as e:
        logging.error(f"AI response was not valid JSON: {e}")
        return
    
    # Prepare Cosmos DB item
    item = {
        "id": str(uuid.uuid4()),
        "call_id": call_id,
        "assessment": assessment_data,
        "type": "politeness"
    }

    # Add the answer from chat into your Cosmos DB
    cosmos_client = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY"))
    database = cosmos_client.create_database_if_not_exists(id=os.getenv("COSMOS_DATABASE"))
    container = database.create_container_if_not_exists(
        id=os.getenv("COSMOS_CONTAINER2"),
        partition_key=PartitionKey(path="/id"),
        offer_throughput=400
    )
    
    container.create_item(item)
    logging.info(f"Politeness assessment written to Cosmos DB: {item}")
    
@app.function_name(name="empathy_from_topic")
@app.service_bus_topic_trigger(
    arg_name="msg",
    topic_name="transcriptstopic",  
    subscription_name="empathy_from_topic_sub", 
    connection="ServiceBusConnection"
)
def empathy_from_topic(msg: func.ServiceBusMessage) -> None:
    logging.info('Empathy assessment function triggered by Service Bus topic.')

    try:
        data = json.loads(msg.get_body().decode('utf-8'))
        call_id = data.get("call_id")
        transcript = data.get("transcript")
        if not call_id or not transcript:
            logging.error("Missing call_id or transcript in message.")
            return
    except Exception as e:
        logging.error(f"Could not parse Service Bus message as JSON: {e}")
        return

    # Initialize the Azure OpenAI client
    client = AzureOpenAI(
        api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_KEY"),
    )

    # Define the prompt for the AI
    messages = [
        {
            "role": "system",
            "content": (
                "You are a call center manager evaluating the performance of your agents. "
                "Your goal is to ensure agents express understanding, concern, and support toward customers. "
                "You score agents on empathy using a scale from 1 to 5, where 1 is lacking empathy and 5 is highly empathetic. "
                "Express score out of 5. "
                "Analyze the following call transcript and provide an empathy score with a brief reasoning."
                "You will also provide a brief summary of the call. "
                "The summary should include the main points of the conversation and any issues that were raised. "
                "The summary should be concise and to the point. "
                "{\"empathy_score\": <score>, \"summary\": <summary>, \"reasoning\": <reasoning>} "
                "Use double quotes for all keys and string values. Do NOT include any explanations, markdown, or extra text."
                "Respond ONLY with a valid JSON object. Use double quotes on all keys and values. Do NOT include any explanations or markdown formatting like ```json."            ),
        },
        {
            "role": "user",
            "content": f"Transcript:\n{transcript}\n\nEmpathy score and reasoning:",
        },
    ]

    # Call the Azure OpenAI API
    response = client.chat.completions.create(
        messages=messages,
        max_tokens=500,
        temperature=0.7,
        top_p=1.0,
        model=os.getenv("AZURE_DEPLOYMENT_NAME"),
    )

    assessment = response.choices[0].message.content.strip()

    try:
        assessment_data = json.loads(assessment)
    except Exception as e:
        logging.error(f"AI response was not valid JSON: {e}")
        return

    # Prepare Cosmos DB item
    item = {
        "id": str(uuid.uuid4()),
        "call_id": call_id,
        "assessment": assessment_data,
        "type": "empathy"
    }
    
    # Add the answer from chat into your Cosmos DB
    cosmos_client = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY"))
    database = cosmos_client.create_database_if_not_exists(id=os.getenv("COSMOS_DATABASE"))
    container = database.create_container_if_not_exists(
        id=os.getenv("COSMOS_CONTAINER2"),
        partition_key=PartitionKey(path="/id"),
        offer_throughput=400
    )

    container.create_item(item)
    logging.info(f"Empathy assessment written to Cosmos DB: {item}")

@app.function_name(name="professionalism_from_topic")
@app.service_bus_topic_trigger(
    arg_name="msg",
    topic_name="transcriptstopic",  
    subscription_name="professionalism_from_topic_sub",  
    connection="ServiceBusConnection"
)
def professionalism_from_topic(msg: func.ServiceBusMessage) -> None:
    logging.info('Professionalism assessment function triggered by Service Bus topic.')

    try:
        data = json.loads(msg.get_body().decode('utf-8'))
        call_id = data.get("call_id")
        transcript = data.get("transcript")
        if not call_id or not transcript:
            logging.error("Missing call_id or transcript in message.")
            return
    except Exception as e:
        logging.error(f"Could not parse Service Bus message as JSON: {e}")
        return

    # Initialize the Azure OpenAI client
    client = AzureOpenAI(
        api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_KEY"),
    )

    # Define the prompt for the AI
    messages = [
        {
            "role": "system",
            "content": (
                "You are a call center manager evaluating the performance of your agents."
                "Your goal is to assess the AI agent's level of professionalism."
                "Focus on whether the agent maintains a respectful and courteous tone, avoids inappropriate or dismissive language, and communicates in a clear and service-oriented manner."
                "Pay close attention to the agent's word choice, tone consistency, and ability to remain composed and professional throughout the interaction."
                "You score agents on professionalism using a scale from 1 to 5, where 1 is unprofessional and 5 is highly professional."
                "Express score out of 5."
                "You will also provide a brief summary of the call. "
                "The summary should include the main points of the conversation and any issues that were raised. "
                "The summary should be concise and to the point. "
                "{\"professionalism_score\": <score>, \"summary\": <summary>, \"reasoning\": <reasoning>} "
                "Use double quotes for all keys and string values. Do NOT include any explanations, markdown, or extra text."
                "Respond ONLY with a valid JSON object. Use double quotes on all keys and values. Do NOT include any explanations or markdown formatting like ```json."            ),
        },
        {
            "role": "user",
            "content": f"Transcript:\n{transcript}\n\nProfessionalism score and reasoning:",
        },
    ]

    # Call the Azure OpenAI API
    response = client.chat.completions.create(
        messages=messages,
        max_tokens=500,
        temperature=0.7,
        top_p=1.0,
        model=os.getenv("AZURE_DEPLOYMENT_NAME"),
    )

    assessment = response.choices[0].message.content.strip()
    try:
        assessment_data = json.loads(assessment)
    except Exception as e:
        logging.error(f"AI response was not valid JSON: {e}")
        return

    # Prepare Cosmos DB item
    item = {
        "id": str(uuid.uuid4()),
        "call_id": call_id,
        "assessment": assessment_data,
        "type": "professionalism"
    }

    # Add the answer from chat into your Cosmos DB
    cosmos_client = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY"))
    database = cosmos_client.create_database_if_not_exists(id=os.getenv("COSMOS_DATABASE"))
    container = database.create_container_if_not_exists(
        id=os.getenv("COSMOS_CONTAINER2"),
        partition_key=PartitionKey(path="/id"),
        offer_throughput=400
    )

    container.create_item(item)
    logging.info(f"Professionalism assessment written to Cosmos DB: {item}")

@app.function_name(name="topic_to_cosmos")
@app.service_bus_topic_trigger(
    arg_name="msg",
    topic_name="transcriptstopic",
    subscription_name="topic_to_cosmos_sub",
    connection="ServiceBusConnection"
)
def topic_to_cosmos(msg: func.ServiceBusMessage) -> None:
    logging.info('Queue trigger function processed a message.')

    logging.info(f"Raw message body: {msg.get_body()}")

    try:
        data = json.loads(msg.get_body().decode('utf-8'))  # Assumes the queue message is JSON
        call_id = data.get("call_id")
        transcript = data.get("transcript")
        if not call_id or not transcript:
            logging.error("Missing call_id or transcript in message.")
            return
    except Exception as e:
        logging.error(f"Could not parse Service Bus message as JSON: {e}")
        return

    # Prepare the item to store in Cosmos DB
    item = {
        "id": str(uuid.uuid4()),     
        "call_id": call_id,
        "transcript": transcript
    }

    # Write to Cosmos DB
    cosmos_client = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY"))
    database = cosmos_client.create_database_if_not_exists(id=os.getenv("COSMOS_DATABASE"))
    container = database.create_container_if_not_exists(
        id=os.getenv("COSMOS_CONTAINER1"),
        partition_key=PartitionKey(path="/id"),
        offer_throughput=400
    )

    container.create_item(item)
    logging.info(f"Item written to Cosmos DB transcript container: {item}")

@app.function_name(name="get_all_transcripts")
@app.route(route="transcripts", methods=["GET"])
def get_all_transcripts(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP trigger function to get all transcripts.')

    try:
        cosmos_client = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY"))
        database = cosmos_client.get_database_client(os.getenv("COSMOS_DATABASE"))
        container = database.get_container_client(os.getenv("COSMOS_CONTAINER1"))

        # Query all items (transcripts)
        query = "SELECT c.id, c.call_id, c.transcript FROM c"
        items = list(container.query_items(query=query, enable_cross_partition_query=True))

        return func.HttpResponse(
            json.dumps(items),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error fetching transcripts: {e}")
        return func.HttpResponse(
            "Error fetching transcripts.",
            status_code=500
        )