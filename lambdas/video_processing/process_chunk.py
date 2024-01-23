import json
import logging
import boto3
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
  """
  Processes a video chunk.
  """

  logger.info(f"Invoked with event: {event}")

  return {}
