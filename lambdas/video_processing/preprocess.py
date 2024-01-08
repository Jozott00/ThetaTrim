import json
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_deliminator = "%3A"

def handler(event, context):
  """
  Handles the preprocessing of videos.

  Performs trimming and chunk splitting of the video.
  TODO: extend docs
  """

  logger.info(f"Invoked with event: {event}")

  db_client = boto3.client('dynamodb')
  job_id, s3_key = extract_data(event, context)

  logger.info(f"Job ID: {job_id}")

  item = db_client.get_item(
    TableName='jobs',
    Key={'id': {'S': job_id}}
  )
  
  logger.info(f"Success")


  # TODO: implement

  return {
    'statusCode': 200,
    'body': '{}'
  }

def extract_data(event, context):
  """
  Extracts relevant data from the event and context.
  """

  body = json.loads(event["Records"][0]["body"])
  s3_key = body["Records"][0]["s3"]["object"]["key"]
  job_id = s3_key.split(s3_deliminator)[0]
  return job_id, s3_key