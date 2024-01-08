import json
import logging
import os
import uuid
import boto3
from enum import Enum
from botocore.exceptions import ClientError
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobStatus(Enum):
  CREATED = "CREATED"
  RUNNING = "RUNNING"
  COMPLETED = "COMPLETED"
  FAILED = "FAILED"


def handler(event, context):
  """
  Handles post job rest api calls.

  Creates a job and a presigned url for it.
  Also validates the job request.
  """

  fn_name = context.function_name

  logger.info(f"{fn_name}: Event: {event}")
  logger.info(f"{fn_name}: Context: {context}")
  logger.info(f"{fn_name}: Start creating job")

  s3_client = boto3.client('s3')
  job_id = str(uuid.uuid1())

  try:
    presigned_url = s3_client.generate_presigned_url(
      ClientMethod='put_object',
      Params={
        'Bucket': os.environ["OBJECT_BUCKET_NAME"],
        'Key': f"{job_id}/original.mp4",
      }
    )
  except ClientError as e:
    logger.exception(f"{fn_name}: Couldn't get a presigned URL for client: %s", e)
    raise

  utc_time_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

  try:
    db_client = boto3.client('dynamodb')
    db_client.put_item(
      TableName="jobs",
      Item={
        'id': {'S': str(job_id)},
        'status': {'S': str(JobStatus.CREATED.value)},
        'created_at': {'S': str(utc_time_str)}
      }
    )
  except ClientError as e:
    logger.exception(f"{fn_name}: Couldn't store item in DynamoDB: %s", e)
    # TODO: cleanup S3
    raise

  logger.info(f"{fn_name}: Finished creating job {job_id}")

  return {
    'statusCode': 200,
    'body': json.dumps({'url': presigned_url})
  }
