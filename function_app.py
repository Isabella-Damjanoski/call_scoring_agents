import azure.functions as func
import logging
import json
import os
import uuid
from openai import AzureOpenAI
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey
import uuid
load_dotenv()

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# @app.function_name(name="politeness_from_queue")
# @app.service_bus_queue_trigger(arg_name="msg", queue_name="test1", connection="ServiceBusConnection")
# def politeness_from_queue(msg: func.ServiceBusMessage) -> None:
#     logging.info('Python HTTP trigger function processed a request.')

#     try:
#         data = json.loads(msg.get_body().decode('utf-8'))
#         call_id = data.get("call_id")
#         transcript = data.get("transcript")
#         if not call_id or not transcript:
#                 logging.error("Missing call_id or transcript in message.")
#                 return
#     except Exception as e:
#         logging.error(f"Could not parse Service Bus message as JSON: {e}")
#         return
    
#     # Initialize the Azure OpenAI client
#     client = AzureOpenAI(
#         api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
#         azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
#         api_key=os.getenv("AZURE_OPENAI_KEY"),
#     )

#     # Define the prompt for the AI
#     messages = [
#         {
#             "role": "system",
#             "content": (
#                 "You are a call center manager evaluating the performance of your agents. "
#                 "Your goal is to ensure that agents are polite and treat customers well. "
#                 "You score agents on politeness using a scale from of 1 to 5 where 1 is very impolite and 5 is extremely polite."
#                 "Especially for agents who don't performe well, you will provide a detailed reasoning for the score. "
#                 "You will also provide a brief summary of the call. "
#                 "The summary should include the main points of the conversation and any issues that were raised. "
#                 "The summary should be concise and to the point. "
#                 "{\"politeness_score\": <score>, \"summary\": <summary>, \"reasoning\": <reasoning>} "
#                 "Use double quotes for all keys and string values. Do NOT include any explanations, markdown, or extra text."
#                 "Respond ONLY with a valid JSON object. Use double quotes on all keys and values. Do NOT include any explanations or markdown formatting like ```json."
#             ),
#         },
#         {
#             "role": "user",
#             "content": f"Transcript:\n{transcript}\n\nPoliteness score and reasoning:",
#         },
#     ]

#     # Call the Azure OpenAI API
#     response = client.chat.completions.create(
#         messages=messages,
#         max_tokens=500,
#         temperature=0.7,
#         top_p=1.0,
#         model=os.getenv("AZURE_DEPLOYMENT_NAME"),
#     )

#     assessment = response.choices[0].message.content.strip()
#     try:
#         assessment_data = json.loads(assessment)
#     except Exception as e:
#         logging.error(f"AI response was not valid JSON: {e}")
#         return
    
#     # Prepare Cosmos DB item
#     item = {
#         "id": str(uuid.uuid4()),
#         "call_id": call_id,
#         "assessment": assessment_data,
#         "type": "politeness"
#     }

#     # Add the answer from chat into your Cosmos DB
#     cosmos_client = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY"))
#     database = cosmos_client.create_database_if_not_exists(id=os.getenv("COSMOS_DATABASE"))
#     container = database.create_container_if_not_exists(
#         id=os.getenv("COSMOS_CONTAINER2"),
#         partition_key=PartitionKey(path="/id"),
#         offer_throughput=400
#     )
    
#     container.create_item(item)
#     logging.info(f"Politeness assessment written to Cosmos DB: {item}")
    
# @app.function_name(name="empathy_from_queue")
# @app.service_bus_queue_trigger(arg_name="msg", queue_name="test1", connection="ServiceBusConnection")
# def empathy_from_queue(msg: func.ServiceBusMessage) -> None:
#     logging.info('Python HTTP trigger function processed a request.')

#     # Initialize the Azure OpenAI client
#     client = AzureOpenAI(
#         api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
#         azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
#         api_key=os.getenv("AZURE_OPENAI_KEY"),
#     )

#     try:
#         data = req.get_json()
#         call_transcript = data.get("transcript")
#         if not call_transcript:
#             return func.HttpResponse(
#                 "Missing 'transcript' in request body.",
#                 status_code=400
#             )
#     except ValueError:
#         return func.HttpResponse(
#             "Invalid JSON in request body.",
#             status_code=400
#         )

#     # Define the prompt for the AI
#     messages = [
#         {
#             "role": "system",
#             "content": (
#                 "You are a call center manager evaluating the performance of your agents. "
#                 "Your goal is to ensure agents express understanding, concern, and support toward customers. "
#                 "You score agents on empathy using a scale from 1 to 5, where 1 is lacking empathy and 5 is highly empathetic. "
#                 "Express score out of 5. "
#                 "Analyze the following call transcript and provide an empathy score with a brief reasoning."
#                 "You will also provide a brief summary of the call. "
#                 "The summary should include the main points of the conversation and any issues that were raised. "
#                 "The summary should be concise and to the point. "
#                 "{\"empathy_score\": <score>, \"summary\": <summary>, \"reasoning\": <reasoning>} "
#                 "Use double quotes for all keys and string values. Do NOT include any explanations, markdown, or extra text."
#                 "Respond ONLY with a valid JSON object. Use double quotes on all keys and values. Do NOT include any explanations or markdown formatting like ```json."            ),
#         },
#         {
#             "role": "user",
#             "content": f"Transcript:\n{call_transcript}\n\nEmpathy score and reasoning:",
#         },
#     ]

#     # Call the Azure OpenAI API
#     response = client.chat.completions.create(
#         messages=messages,
#         max_tokens=500,
#         temperature=0.7,
#         top_p=1.0,
#         model=os.getenv("AZURE_DEPLOYMENT_NAME"),
#     )

#     assessment = response.choices[0].message.content
#     logging.info(f"Assessment: {assessment}")

#     try:
#         logging.info(f"Assessment: {assessment}")
#         assessment_data = json.loads(assessment)
#     except json.JSONDecodeError:
#         return func.HttpResponse("AI response was not valid JSON.", status_code=500)

#     # Prepare Cosmos DB item
#     item = {
#         "id": str(uuid.uuid4()),
#         "transcript": call_transcript,
#         "assessment": assessment
#     }
    
#     # Add the answer from chat into your Cosmos DB
#     cosmos_client = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY"))
#     database = cosmos_client.create_database_if_not_exists(id=os.getenv("COSMOS_DATABASE"))
#     container = database.create_container_if_not_exists(
#         id=os.getenv("COSMOS_CONTAINER2"),
#         partition_key=PartitionKey(path="/id"),
#         offer_throughput=400
#     )

#     container.create_item(item)

#     if assessment:
#         return func.HttpResponse(json.dumps(item), status_code=200, mimetype="application/json") # Return 200 when things went good
#     else:
#         return func.HttpResponse("This HTTP triggered function executed but did not return a response.", status_code=500)  # Return 500 when there is a problem on the server

# @app.function_name(name="professionalism_from_queue")
# @app.service_bus_queue_trigger(arg_name="msg", queue_name="test1", connection="ServiceBusConnection")
# def professionalism_from_queue(msg: func.ServiceBusMessage) -> None:
#     logging.info('Python HTTP trigger function processed a request.')

#     # Initialize the Azure OpenAI client
#     client = AzureOpenAI(
#         api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
#         azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
#         api_key=os.getenv("AZURE_OPENAI_KEY"),
#     )

#     try:
#         data = req.get_json()
#         call_transcript = data.get("transcript")
#         if not call_transcript:
#             return func.HttpResponse(
#                 "Missing 'transcript' in request body.",
#                 status_code=400
#             )
#     except ValueError:
#         return func.HttpResponse(
#             "Invalid JSON in request body.",
#             status_code=400
#         )

#     # Define the prompt for the AI
#     messages = [
#         {
#             "role": "system",
#             "content": (
#                 "You are a call center manager evaluating the performance of your agents."
#                 "Your goal is to assess the AI agent’s level of professionalism."
#                 "Focus on whether the agent maintains a respectful and courteous tone, avoids inappropriate or dismissive language, and communicates in a clear and service-oriented manner."
#                 "Pay close attention to the agent’s word choice, tone consistency, and ability to remain composed and professional throughout the interaction."
#                 "You score agents on professionalism using a scale from 1 to 5, where 1 is unprofessional and 5 is highly professional."
#                 "Express score out of 5."
#                 "You will also provide a brief summary of the call. "
#                 "The summary should include the main points of the conversation and any issues that were raised. "
#                 "The summary should be concise and to the point. "
#                 "{\"professionalism_score\": <score>, \"summary\": <summary>, \"reasoning\": <reasoning>} "
#                 "Use double quotes for all keys and string values. Do NOT include any explanations, markdown, or extra text."
#                 "Respond ONLY with a valid JSON object. Use double quotes on all keys and values. Do NOT include any explanations or markdown formatting like ```json."            ),
#         },
#         {
#             "role": "user",
#             "content": f"Transcript:\n{call_transcript}\n\nProfessionalism score and reasoning:",
#         },
#     ]

#     # Call the Azure OpenAI API
#     response = client.chat.completions.create(
#         messages=messages,
#         max_tokens=500,
#         temperature=0.7,
#         top_p=1.0,
#         model=os.getenv("AZURE_DEPLOYMENT_NAME"),
#     )

#     assessment = response.choices[0].message.content
#     logging.info(f"Assessment: {assessment}")

#     try:
#         logging.info(f"Assessment: {assessment}")
#         assessment_data = json.loads(assessment)
#     except json.JSONDecodeError:
#         return func.HttpResponse("AI response was not valid JSON.", status_code=500)

#     # Prepare Cosmos DB item
#     item = {
#         "id": str(uuid.uuid4()),
#         "transcript": call_transcript,
#         "assessment": assessment
#     }

#     # Add the answer from chat into your Cosmos DB
#     cosmos_client = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY"))
#     database = cosmos_client.create_database_if_not_exists(id=os.getenv("COSMOS_DATABASE"))
#     container = database.create_container_if_not_exists(
#         id=os.getenv("COSMOS_CONTAINER2"),
#         partition_key=PartitionKey(path="/id"),
#         offer_throughput=400
#     )

#     container.create_item(item)

#     if assessment:
#         return func.HttpResponse(json.dumps(item), status_code=200, mimetype="application/json") # Return 200 when things went good
#     else:
#         return func.HttpResponse("This HTTP triggered function executed but did not return a response.", status_code=500)  # Return 500 when there is a problem on the server

@app.function_name(name="topic_to_cosmos")
@app.service_bus_topic_trigger(
    arg_name="msg",
    topic_name="transcriptstopic",
    subscription_name="topic_to_cosmos",
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