import logging
from typing import Dict, Any
import os

import boto3
import ffmpeg

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]
OBJ_BUCKET_NAME = os.environ["OBJECT_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_deliminator = "%3A"
s3_client = boto3.client('s3')


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  Reduces all processed chunks to a single video.
  """

  logger.info(f"Invoked with event: {event}")

  # todo: make it format agnostic
  out_path = "/tmp/OUTPUT.mp4"
  obj_key = "/test/RESULT.mp4"

  keys = extract_data(event, context)
  url_array = generate_presigned_urls(keys)

  logger.info(f"Concat videos: {url_array}")
  concat_videos(presigned_urls=url_array, output_path=out_path)

  logger.info(f"Upload {out_path} to {obj_key}")
  s3_client.upload_file(out_path, OBJ_BUCKET_NAME, obj_key)

  logger.info(f"Success")

  return {}


def concat_videos(presigned_urls: list[str], output_path: str):
  inputs = [ffmpeg.input(u) for u in presigned_urls]
  (
    ffmpeg.concat(*inputs)
    .output(output_path)
    # .overwrite_ouput()
    .run()
  )


def generate_presigned_urls(keys: list[str], expiration=3600) -> list[str]:
  return [s3_client.generate_presigned_url('get_object',
                                           Params={
                                             'Bucket': OBJ_BUCKET_NAME,
                                             'Key': key
                                           },
                                           ExpiresIn=expiration)
          for key in keys
          ]


def extract_data(event, context) -> list[str]:
  return [e['objectUrl'] for e in event['chunks']]
