import logging
from typing import Dict, Any
import os
import boto3
import subprocess
import ffmpeg

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]
OBJ_BUCKET_NAME = os.environ["OBJECT_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')


def print_directory(dir):
  contents = os.listdir(dir)
  print(f"### show {dir} directory")
  for item in contents:
    print(item)


def generate_result(job_id, thumbnail_path):
  return {
    "jobId": job_id,
    "thumbnail": thumbnail_path
  }


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  Generates a thumbnail.
  """
  job_id, chunks = event["jobId"], event["chunks"]
  key, extension, size = chunks[0]["key"], chunks[0]["extension"], chunks[0]["size"]

  bucket_name = OBJ_BUCKET_NAME
  video_path = '/tmp/' + key
  thumbnail_name = 'thumbnail.jpg'
  thumbnail_path = f'/tmp/{thumbnail_name}'

  print_directory('/tmp/')

  s3_client.download_file(bucket_name, key, video_path)

  command = ['ffmpeg', '-i', video_path, '-ss', '00:00:10', '-frames:v', '1', thumbnail_path]

  try:
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    print(result.stdout)
  except subprocess.CalledProcessError as e:
    print("Error executing FFmpeg: ", e.output)

  print_directory("/tmp")

  s3_client.upload_file(thumbnail_path, bucket_name, thumbnail_name)

  logger.info(f"Invoked with event: {event}")
  logger.info(f"Success")

  return generate_result(job_id, thumbnail_path)
