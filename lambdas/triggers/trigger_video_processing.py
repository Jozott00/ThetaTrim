import json
import boto3
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
  """
  Triggers the video-processing state-machine.
  """

  logger.info(f"Invoked with event: {event}")

  sfn_client = boto3.client('stepfunctions')
  state_machine_arn = os.environ["STATE_MACHINE_ARN"]

  try:
    # Start execution of the state machine
    response = sfn_client.start_execution(
      stateMachineArn=state_machine_arn,
      input=json.dumps(event)
    )

    logger.info(f"Success")

    # Return the response
    return {
      'statusCode': 200,
      'body': json.dumps('State machine execution started successfully'),
      'response': response
    }
  except Exception as e:
    logger.info(f"Error")
    return {
      'statusCode': 500,
      'body': json.dumps('Error starting state machine execution')
    }
