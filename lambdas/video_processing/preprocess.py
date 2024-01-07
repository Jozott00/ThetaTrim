import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
  """
  Handles the preprocessing of videos.

  Performs trimming and chunk splitting of the video.
  TODO: extend docs
  """

  logger.info("Received new event: " + str(event))
  logger.info(f"event: {event}\ncontext: {context}")

  # TODO: implement

  return {
    'statusCode': 200,
    'body': json.dumps({'url': presigned_url})
  }
