import logging
from typing import Dict, Any
import os
import boto3

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]
OBJ_BUCKET_NAME = os.environ["OBJECT_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
s3_bucket = boto3.resource('s3').Bucket(OBJ_BUCKET_NAME)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  Generates a thumbnail.
  """

  logger.info(f"Invoked with event: {event}")

  label_results = event['labeledChunks']
  processed_chunks = event['processedChunks']
  job_id = event['jobId']

  max_len = float('-inf')
  max_ind = 0

  logger.info("Search for best thumbnail ...")
  # Iterate through the data
  for ind, item in enumerate(label_results):
    # Check if the length of labels array is greater than max_len
    if len(item['Labels']) > max_len:
      max_len = len(item['Labels'])
      max_ind = ind

  logger.info(f"Found chunk {max_ind} with {max_len} related labels!")

  refimg = processed_chunks[max_ind]['refimg_key']
  img_extension = os.path.basename(refimg).rsplit(".")[1]
  thumbnail = f"{job_id}/THUMBNAIL.{img_extension}"

  logger.info(f"Copy {refimg} to {thumbnail}...")
  move_s3_objects(refimg, thumbnail)

  logger.info(f"Success")

  return {}


def move_s3_objects(old_key, new_key):
  # Copy the object
  s3_client.copy_object(Bucket=OBJ_BUCKET_NAME,
                        CopySource={'Bucket': OBJ_BUCKET_NAME, 'Key': old_key},
                        Key=new_key)
