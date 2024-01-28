import json
import boto3
import os
import logging

from utils import utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
  """
  Triggers the video-processing state-machine.
  """

  logger.info(f"Invoked with event: {event}")

  sfn_client = boto3.client('stepfunctions')
  state_machine_arn = os.environ["STATE_MACHINE_ARN"]

  s3_event = event["Records"][0]
  object = s3_event["s3"]["object"]

  jobid = utils.get_jobid_from_key(object["key"])
  ext = utils.get_extension_from_key(object["key"])

  # example input:
  # {
  #   "key": "test/original.mp4",
  #   "size": 673896,
  #   "jobId": "test",
  #   "extension": "mp4"
  # }
  exec_input = {
    "jobId": jobid,
    "key": object['key'],
    "extension": ext,
    "size": object["size"]
  }

  logger.info(f"Invoke video processing with input: \n{exec_input}")

  # Start execution of the state machine
  response = sfn_client.start_execution(
    stateMachineArn=state_machine_arn,
    input=json.dumps(exec_input)
  )

  logger.info(f"Success")
