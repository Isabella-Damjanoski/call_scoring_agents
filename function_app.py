import azure.functions as func #handle Azure Function triggers
import logging #allows to log events
import json #work with JSON data
import os #environment variables and operating system functionality
import uuid #unique identifiers
from openai import AzureOpenAI #import OpenAI client from Azure
from dotenv import load_dotenv #loads environment variables
from azure.cosmos import CosmosClient, PartitionKey #interact with Cosmos DB
from azure.storage.blob import BlobServiceClient #interact with blob storage
import azure.cognitiveservices.speech as speechsdk #SDK for speech services
from azure.servicebus import ServiceBusClient, ServiceBusMessage #interact with service bus
import tempfile #create temporary files
import threading #concurrent execution using threads
load_dotenv() #loads env file to access environment variables

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION) #initialize Azure Function

@app.function_name(name="blobberfunction") #Azure function name 
@app.blob_trigger(arg_name="myblob", #name of variable to hold blob data, blob trigger
                  path="blobber-container/{name}", #blob path
                  connection="AzureWebJobsStorage") #storage account connection
def blobberfunction(myblob: func.InputStream): #function takes myblob input as input stream
    logging.info(f"Blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n" #log name of blob
                 f"Blob Size: {myblob.length} bytes") #log size of blob

    # Download audio blob to temp file
    filename = os.path.basename(myblob.name).replace(" ", "_") #extract filename from blob name
    temp_dir = tempfile.gettempdir() #get systems temporary path
    temp_audio_path = os.path.join(temp_dir, filename) #construct path to temporary audio file

    with open(temp_audio_path, "wb") as audio_file: #open temporary file in white-binary mode
        audio_file.write(myblob.read()) #read blob and write to file

    # Transcribe audio using Azure Speech-to-Text
    speech_key = os.getenv("AZURE_SPEECH_KEY") #get Azure speech key
    speech_region = os.getenv("AZURE_SPEECH_REGION") #get Azure speech region
    speech_config = speechsdk.SpeechConfig(subscription=speech_key, region=speech_region) #configure Azure speech
    
    # Enable speaker diarization (label different speakers)
    speech_config.set_service_property(
        name="diarizationEnabled",
        value="true",
        channel=speechsdk.ServicePropertyChannel.UriQueryParameter
    )
    # Specify the number of expected speakers (2)
    speech_config.set_service_property(
        name="diarizationSpeakerCount",
        value="2",
        channel=speechsdk.ServicePropertyChannel.UriQueryParameter
    )

    # Set up audio and transcriber
    audio_config = speechsdk.AudioConfig(filename=temp_audio_path) #audio source using saved file
    transcriber = speechsdk.transcription.ConversationTranscriber( #create conversation transcriber
        speech_config=speech_config, audio_config=audio_config)

    transcript_lines = [] #list to collect transcript lines
    done = threading.Event() #event to signal when transcription is done

    def handle_transcribed(evt): #handler for transcribed events
        speaker = evt.result.speaker_id #get speaker ID
        label = f"Speaker {speaker}" if speaker else "Unknown" #use speaker ID or unknown
        if evt.result.text: 
            transcript_lines.append(f"{label}: {evt.result.text}") #if there is text in result, append to transcritpt
            logging.info(f"{label}: {evt.result.text}") #log the transcrit line

    def handle_canceled(evt): #handler for canceled transcription
        logging.warning(f"Transcription canceled: {evt.reason}") #log reason
        if evt.reason == speechsdk.CancellationReason.Error: #log if theres an error
            logging.error(f"Error details: {evt.error_details}")
        done.set() #signal that transcription is done
    
    def handle_session_stopped(evt): #handler for stopped session
        logging.info("Transcription session stopped.")
        done.set() #signal that transcription is done

    # Connect the transcriber event handlers
    transcriber.transcribed.connect(handle_transcribed)
    transcriber.canceled.connect(handle_canceled)
    transcriber.session_stopped.connect(handle_session_stopped)

    # Start transcription and wait until complete
    transcriber.start_transcribing_async().get() #transcribe asynchronously
    done.wait() #wait for transcription to complete
    transcriber.stop_transcribing_async().get() #stop transcription asynchronously

    # Join transcript lines into a single string
    transcript = "\n".join(transcript_lines)

    # Generate a random call_id
    call_id = str(uuid.uuid4())

    # Prepare transcript as JSON with call_id
    transcript_json = json.dumps({ #dictionary of JSON object
        "call_id": call_id,
        "transcript": transcript
    })

    # Send transcript to Service Bus Topic
    servicebus_conn_str = os.getenv("ServiceBusConnection") #get service bus connection string
    topic_name = os.getenv("AZURE_SERVICEBUS_TOPIC_NAME", "transcripttopic") #get topic name or use default
    with ServiceBusClient.from_connection_string(servicebus_conn_str) as client: #connect to service bus as client
        sender = client.get_topic_sender(topic_name=topic_name) #get a sender for the topic
        with sender:
            message = ServiceBusMessage(transcript_json) #create a message with transcript JSON
            sender.send_messages(message) #send message to topic
    
    logging.info("Transcript sent to Service Bus topic.") #log that transcript was successful

@app.function_name(name="politeness_from_topic")  #Azure function name
@app.service_bus_topic_trigger(  #triggered by a message on the service bus
    arg_name="msg", #message will be passed as variable 
    topic_name="transcriptstopic", #the topic to listen to
    subscription_name="politeness_from_topic_sub", #subscription name
    connection="ServiceBusConnection"#service bus connection string
)
def politeness_from_topic(msg: func.ServiceBusMessage) -> None: #define function that will run when message received
    logging.info('Politeness assessment function triggered by Service Bus topic.') #log that function was triggered

    try:
        data = json.loads(msg.get_body().decode('utf-8')) #parse the message body as JSON
        call_id = data.get("call_id") #extract call_id
        transcript = data.get("transcript") #extract transcript
        if not call_id or not transcript: #check if field are missing
                logging.error("Missing call_id or transcript in message.")
                return
    except Exception as e: #log error if message could not be parsed
        logging.error(f"Could not parse Service Bus message as JSON: {e}")
        return
    
    # Initialize the Azure OpenAI client
    client = AzureOpenAI(
        api_version=os.getenv("AZURE_OPENAI_API_VERSION"), #openAI version
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"), #openAI endpoint
        api_key=os.getenv("AZURE_OPENAI_KEY"), #openAI key
    )

    # Define the prompt for the AI
    messages = [
        {
            "role": "system", #how the AI should behave
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
            "role": "user", #user message to the AI
            "content": f"Transcript:\n{transcript}\n\nPoliteness score and reasoning:",
        },
    ]

    # Call the Azure OpenAI API
    response = client.chat.completions.create(
        messages=messages, #pass full message list
        max_tokens=500, #limit response length
        temperature=0.7, #control sensitivity
        top_p=1.0, #use nucleus sampling
        model=os.getenv("AZURE_DEPLOYMENT_NAME"), #which deployment to use
    )

    assessment = response.choices[0].message.content.strip() #extract the AI response
    try:
        assessment_data = json.loads(assessment) #try to parse the assessment as a JSON
    except Exception as e: #log an error if the response is not valid JSON
        logging.error(f"AI response was not valid JSON: {e}")
        return
    
    # Prepare Cosmos DB item
    item = { #dictionary item
        "id": str(uuid.uuid4()), #unique ID
        "call_id": call_id, #unique call ID
        "assessment": assessment_data, #assessment from AI
        "type": "politeness" #type of assessment
    }

    # Add the answer from chat into your Cosmos DB
    cosmos_client = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY")) #cosmos db client
    database = cosmos_client.create_database_if_not_exists(id=os.getenv("COSMOS_DATABASE")) #create or get database
    container = database.create_container_if_not_exists( #database where assessment will be stored
        id=os.getenv("COSMOS_CONTAINER2"), #the container name
        partition_key=PartitionKey(path="/id"), #partition key
        offer_throughput=400 #set container throughput
    )
    
    container.create_item(item) #store item
    logging.info(f"Politeness assessment written to Cosmos DB: {item}") #log that item was stored
    
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

@app.function_name(name="topic_to_cosmos") #Azure function name
@app.service_bus_topic_trigger( #triggered from service bus topic
    arg_name="msg", #parameter that will hold the message
    topic_name="transcriptstopic", #the topic to listen to
    subscription_name="topic_to_cosmos_sub", #subscription name
    connection="ServiceBusConnection" #service bus connection string
)
def topic_to_cosmos(msg: func.ServiceBusMessage) -> None: #function that processes incoming message
    logging.info('Queue trigger function processed a message.') #log that function was triggered

    logging.info(f"Raw message body: {msg.get_body()}") #log the raw message

    try:
        data = json.loads(msg.get_body().decode('utf-8'))  #parse the message as JSON
        call_id = data.get("call_id") #get call_id
        transcript = data.get("transcript") #get transcript
        if not call_id or not transcript: #check if fields are present
            logging.error("Missing call_id or transcript in message.")
            return
    except Exception as e: #log error if message could not be parsed
        logging.error(f"Could not parse Service Bus message as JSON: {e}")
        return

    # Prepare the item to store in Cosmos DB
    item = { #dictionary item
        "id": str(uuid.uuid4()), #unique ID     
        "call_id": call_id, #unique call ID
        "transcript": transcript #transcript
    }

    # Write to Cosmos DB
    cosmos_client = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY")) #connect to cosmos db
    database = cosmos_client.create_database_if_not_exists(id=os.getenv("COSMOS_DATABASE")) #create or access database
    container = database.create_container_if_not_exists( #create or access container
        id=os.getenv("COSMOS_CONTAINER1"), #container name
        partition_key=PartitionKey(path="/id"), #partition key
        offer_throughput=400 #set throughput
    )

    container.create_item(item) #store item in container
    logging.info(f"Item written to Cosmos DB transcript container: {item}") #log that item was stored

@app.function_name(name="get_all_transcripts") #Azure function name
@app.route(route="transcripts", methods=["GET"]) #route /transcripts and allows GET requests
def get_all_transcripts(req: func.HttpRequest) -> func.HttpResponse: #HTTP trigger function
    logging.info('HTTP trigger function to get all transcripts.') #log function was triggered

    try:
        cosmos_client = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY")) #cosmos db client
        database = cosmos_client.get_database_client(os.getenv("COSMOS_DATABASE")) #access target database
        container = database.get_container_client(os.getenv("COSMOS_CONTAINER1")) #access target container

        # Query all items (transcripts)
        query = "SELECT c.id, c.call_id, c.transcript FROM c" #select only id, call_id, and transcript fields
        items = list(container.query_items(query=query, enable_cross_partition_query=True)) #execute query and convert into list

        return func.HttpResponse( #return query results as JSON HTTP response
            json.dumps(items), #convert list into JSON
            status_code=200, #200 OK
            mimetype="application/json" #response MIME type to JSON
        )
    except Exception as e: #error handling
        logging.error(f"Error fetching transcripts: {e}")
        return func.HttpResponse(
            "Error fetching transcripts.",
            status_code=500 #500 internal server error
        )