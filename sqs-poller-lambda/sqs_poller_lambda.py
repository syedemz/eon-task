import json
from urllib.parse import unquote
import os
import boto3
from botocore.exceptions import ClientError
from utils import handle_aws_exceptions, handle_key_json_exceptions


# Initialize the SQS client
sqs = boto3.client('sqs')
stepfunctions_client = boto3.client('stepfunctions')


sqs_queue_arn = os.environ['SQSQueueArn']
step_function_arn = os.environ['SQSStepFunctionArn']

# Get the queue URL using the ARN
response = sqs.get_queue_url(QueueName=sqs_queue_arn.split(':')[-1])
queue_url = response['QueueUrl']


# SQS queue URL - replace with your actual queue URL
#QUEUE_URL = 'https://sqs.eu-central-1.amazonaws.com/929860961607/eon-sqs-queue'
QUEUE_URL = queue_url

# Replace with your actual ARN
#step_function_arn = "arn:aws:states:eu-central-1:929860961607:stateMachine:EonStateMachine-T8VZzJBZXawJ"  

@handle_aws_exceptions
def lambda_handler(event, context):
    """_summary_

    Args:
        event (_type_): _description_
        context (_type_): _description_

    Returns:
        _type_: _description_
    """
    
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        AttributeNames=['All'],
        MaxNumberOfMessages=10,  # Adjust as needed
        MessageAttributeNames=['All'],
        VisibilityTimeout=60,    # 30 seconds
        WaitTimeSeconds=20       # Long polling
    )
    # Check if any messages were received
    if 'Messages' in response and len(response.get('Messages')) > 0:
        print(f"THE NUMBER OF MESSAGES FOUND ARE {len(response.get('Messages'))}")
        for message in response['Messages']:
            # Process the message
            process_message(message)
            # Delete the message from the queue
            delete_message(message)

    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed {len(response.get('Messages', []))} messages")
    }

@handle_key_json_exceptions
def process_message(message):
    """_summary_

    Args:
        message (_type_): _description_
    """  
    # Extract message body
    message_body = json.loads(message['Body'])
    # Extract the S3 bucket name and object key
    bucket = message_body['Records'][0]['s3']['bucket']['name']
    object_key = message_body['Records'][0]['s3']['object']['key']
    print(f"Processed S3 event - Bucket: {bucket}, Object Key: {unquote(object_key)}")
    invoke_step_function(object_key)

def invoke_step_function(objectkey):
    """_summary_

    Args:
        objectkey (_type_): _description_
    """
    input_data = {"key": objectkey}
    # Convert dictionary to JSON string
    payload = json.dumps(input_data)
    # Invoke the Step Function
    response = stepfunctions_client.start_execution(stateMachineArn=step_function_arn, input=payload)
    # Print the execution ARN for debugging purposes (optional)
    print(f"Step Function execution ARN: {response['executionArn']}")

@handle_aws_exceptions
def delete_message(message):
    """_summary_

    Args:
        message (_type_): _description_
    """
    # Delete the message from the queue
    sqs.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=message['ReceiptHandle']
    )
    print(f"Deleted message: {message['MessageId']}")