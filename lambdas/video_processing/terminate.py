import logging
from typing import Dict, Any
import os
from utils.job_status import JobStatus
import boto3
import json
from utils import utils

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]
WS_URL = os.environ["WS_URL"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

job_table = boto3.resource('dynamodb').Table(JOB_TABLE_NAME)
websocket_client = boto3.client('apigatewaymanagementapi', endpoint_url=WS_URL)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  Terminates in the workflow.
  Updates the status of the database entry and notifies websocket clients of the status.
  """

  try:
    error, job_id = extract_data(event, context)

    update_status(job_id, error)
    notify_clients(job_id, error)
    # TODO: if success, create presigned url of done video
  except Exception as e:
    raise utils.InternalError("Failed to terminate job", e)

  return {
    "jobId": job_id,
    "error": error
  }


def update_status(job_id, error):
  new_status = JobStatus.COMPLETED.value if error is None else JobStatus.FAILED.value
  job_table.update_item(
    Key={
      'PK': f"JOB#{job_id}",
      'SK': "DATA"
    },
    UpdateExpression='SET #status = :val',
    ExpressionAttributeValues={
      ':val': new_status
    },
    ExpressionAttributeNames={
      '#status': 'status'
    },
    ReturnValues="UPDATED_NEW"
  )


def notify_clients(job_id, error):
  item_res = job_table.get_item(
    Key={
      'PK': f"JOB#{job_id}",
      'SK': "DATA"
    }
  )
  item = item_res.get('Item', None)
  connections = item['ws_connections']
  logger.info("get msg")
  msg = "Job succeeded." if error is None else get_error_msg(error)
  logger.info(f"msg: {msg}")
  for connection_id in connections:
    try:
      websocket_client.post_to_connection(
        Data=json.dumps({'msg': msg}),
        ConnectionId=connection_id
      )
    except Exception:
      pass


def get_error_msg(error):
  msg = "An internal error has occurred."
  error_type = error["Error"]
  if error_type == utils.FFmpegError.__name__:
    msg = "Video processing failed due to an error. Please review your configuration settings."
  elif error_type == utils.ConfigError.__name__:
    err_msg_json = error["Cause"]
    msg = json.loads(err_msg_json)["errorMessage"]
  return msg


def extract_data(event, context):
  return event.get('error'), event['jobId']
