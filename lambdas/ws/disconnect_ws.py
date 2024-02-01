import logging
from typing import Dict, Any
import os
import boto3

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

job_table = boto3.resource('dynamodb').Table(JOB_TABLE_NAME)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  TODO: docs
  """

  connection_id, job_id = extract_data(event, context)
  logger.info(f"event: {event}")
  logger.info(f"connection id: {connection_id}")
  logger.info(f"job id: {job_id}")

  # TODO: implement remove connection
  # remove_connection(connection_id, job_id)

  logger.info(f"Success")

  return {}


def remove_connection(connection_id, job_id):
  job_table.update_item(
    Key={
      'PK': f"JOB#{job_id}",
      'SK': "DATA"
    },
    UpdateExpression="DELETE ws_connections :i",
    ExpressionAttributeValues={
      ':i': connection_id,
    },
    ReturnValues="UPDATED_NEW"
  )


def extract_data(event, context):
  connection_id = event["requestContext"]["connectionId"]
  job_id = event["headers"]["jobId"]
  return connection_id, job_id
