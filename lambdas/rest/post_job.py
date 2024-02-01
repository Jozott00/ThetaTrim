import json
import logging
import os
import uuid
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from typing import Dict, Any
from utils.job_status import JobStatus
from utils import config_utils
from utils import utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
Handles post job REST API calls.
- Creates a job entry in the DynamoDB table.
- Generates a presigned URL for the job.
"""
  logger.info(f"Invoked with event: {event}")

  s3_client = boto3.client('s3')
  db_client = boto3.client('dynamodb')
  job_id = str(uuid.uuid1())

  presigned_url = generate_presigned_url(s3_client, job_id)

  try:
    body = json.loads(event.get("body", "{}"))
    operations = body.get("operations", [])
  except json.JSONDecodeError as e:
    logger.error(f"Error parsing request body: {e}")
    return {
      'statusCode': 400,
      'body': json.dumps({'error': 'Invalid request body'})
    }

  try:
    config_utils.Config(operations)
  except utils.ConfigError as e:
    return {
      'statusCode': 400,
      'body': json.dumps({'error': f'{e}'})
    }

  store_job_info(db_client, job_id, operations)

  logger.info(f"Successfully created job {job_id}")

  return {
    'statusCode': 200,
    'body': json.dumps({'url': presigned_url, 'jobId': job_id})
  }


def generate_presigned_url(s3_client, job_id: str) -> str:
  """Generates a presigned URL for S3 object upload."""
  try:
    return s3_client.generate_presigned_url(
      ClientMethod='put_object',
      Params={
        'Bucket': os.environ["OBJECT_BUCKET_NAME"],
        'Key': f"{job_id}/original.mp4",
      }
    )
  except ClientError as e:
    logger.exception(f"Error generating presigned URL: {e}")
    raise


def store_job_info(db_client, job_id: str, operations: list) -> None:
  """Stores job information in DynamoDB."""
  logger.info(f"Store new job {job_id} in table {JOB_TABLE_NAME}")
  utc_time_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

  # Convert operations to DynamoDB format
  dynamo_operations = [{'M': {key: {'S': str(value)} for key, value in operation.items()}} for operation in
                       operations]

  try:
    db_client.put_item(
      TableName=JOB_TABLE_NAME,
      Item={
        'PK': {'S': f"JOB#{job_id}"},
        'SK': {'S': "DATA"},
        'status': {'S': JobStatus.CREATED.value},
        'transformations': {'L': dynamo_operations},
        'ws_connections': {'L': []},
        'labels': {'S': ''},
        'created_at': {'S': utc_time_str},
      }
    )
  except ClientError as e:
    logger.exception(f"Error storing item in DynamoDB: {e}")
    # TODO: add s3 cleanup
    raise
