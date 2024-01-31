import logging
from typing import Dict, Any
import os
from utils.job_status import JobStatus
import boto3

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

job_table = boto3.resource('dynamodb').Table(JOB_TABLE_NAME)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  Handles an error in the flow, by updating the database entry.
  """

  logger.info(f"Invoked with event: {event}")
  job_id = extract_data(event, context)

  update_status(job_id)

  logger.info(f"Success")

  return {}


def update_status(job_id):
  job_table.update_item(
    Key={
      'PK': f"JOB#{job_id}",
      'SK': "DATA"
    },
    UpdateExpression='SET #status = :val',
    ExpressionAttributeValues={
      ':val': JobStatus.FAILED.value
    },
    ExpressionAttributeNames={
      '#status': 'status'
    },
    ReturnValues="UPDATED_NEW"
  )


def extract_data(event, context):
  return event['jobId']
