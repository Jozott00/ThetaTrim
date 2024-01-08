import json
import logging
import os
import uuid
import boto3
from enum import Enum
from botocore.exceptions import ClientError
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobStatus(Enum):
  CREATED = "CREATED"
  RUNNING = "RUNNING"
  COMPLETED = "COMPLETED"
  FAILED = "FAILED"


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

  store_job_info(db_client, job_id)

  logger.info(f"Successfully created job {job_id}")

  return {
    'statusCode': 200,
    'body': json.dumps({'url': presigned_url})
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


def store_job_info(db_client, job_id: str) -> None:
  """Stores job information in DynamoDB."""
  utc_time_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
  try:
    db_client.put_item(
      TableName="jobs",
      Item={
        'id': {'S': job_id},
        'status': {'S': JobStatus.CREATED.value},
        'transformations': {'S': ''},
        'labels': {'S': ''},
        'created_at': {'S': utc_time_str}
      }
    )
  except ClientError as e:
    logger.exception(f"Error storing item in DynamoDB: {e}")
    # TODO: add s3 cleanup
    raise