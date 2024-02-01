import logging
from typing import Dict, Any
import os

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  Extracts the content-labels of a video chunk using image recognition.
  One image (the keyframe) of the chunk is being evaluated using amazon recognition.
  The labels are then being stored in dynamo-db.
  """

  logger.info(f"Invoked with event: {event}")

  # TODO: implement job

  logger.info(f"Success")

  # TODO: output should be input, being the url to the s3 resource
  return event
