import logging
import subprocess
from typing import Dict, Any
import os
import boto3

from utils import s3_utils

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]
OBJ_BUCKET_NAME = os.environ["OBJECT_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  Extracts the audio from the original video.
  """

  os.system("rm /tmp/*")

  logger.info(f"Invoked with event: {event}")

  object_key = event['key']
  job_id = event['jobId']
  acodec = event['acodec']

  chunk_url = s3_client.generate_presigned_url('get_object',
                                               Params={
                                                 'Bucket': OBJ_BUCKET_NAME,
                                                 'Key': object_key,
                                               },
                                               ExpiresIn=3600)
  result_key = f"{job_id}/AUDIO.{acodec}"
  local_path = f"/tmp/AUDIO.{acodec}"

  command = ['ffmpeg',
             '-v', 'error',
             '-i', chunk_url,
             '-vn',
             '-c:a', 'copy',
             local_path
             ]

  logger.info(f"Start audio extraction with command: \n{command}")
  try:
    process = subprocess.run(command, capture_output=True, text=True, check=True)
  except subprocess.CalledProcessError as e:
    logger.error(e.output)
    raise utils.FFmpegError("Failed to extract audio", e)

  logger.info(f"Upload {local_path} to {OBJ_BUCKET_NAME}/{result_key} ...")
  s3_client.upload_file(local_path, OBJ_BUCKET_NAME, result_key)

  logger.info(f"Success")

  os.system("rm /tmp/*")

  return {
    'key': result_key,
    'jobId': job_id,
    'acodec': acodec
  }
