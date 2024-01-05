import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def handler(event, context):
  """
  Handles post job rest api calls

  Creates a job and a presigned url for it.
  Also validates the job request.
  """

  s3_client = boto3.client('s3')

  try:
    presigned_url = s3_client.generate_presigned_url(
      ClientMethod='put_object',
      Params={
        'Bucket': os.environ["OBJECT_BUCKET_NAME"],
        'Key': "some-job-id/original.mp4",
      }
    )
  except ClientError:
    logger.exception("Couldn't get a presigned URL for client")
    raise

  return presigned_url
