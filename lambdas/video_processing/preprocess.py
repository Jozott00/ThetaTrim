import json
import logging
import boto3
import os
import subprocess
import shlex
import glob
import ffmpeg
from utils import constants

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]
OBJ_BUCKET_NAME = os.environ["OBJECT_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_deliminator = "%3A"
s3_client = boto3.client('s3')


def handler(event, context):
  """
  Handles the preprocessing of videos.

  Performs trimming and chunk splitting of the video.
  """
  logger.info(f"Invoked with event: {event}")

  job_id, orig_video_key, extension = extract_data(event, context)

  # delete local storage
  os.system("rm /tmp/*")

  video_url = s3_client.generate_presigned_url('get_object',
                                               Params={
                                                 'Bucket': OBJ_BUCKET_NAME,
                                                 'Key': orig_video_key,
                                               },
                                               ExpiresIn=3600)

  chunk_output_format = f"/tmp/CHUNK-%d.{extension}"
  logger.info(
    f"Start splitting video to {chunk_output_format} with chunk size of ~{constants.TARGET_CHUNK_SECS} seconds")
  # (
  #   ffmpeg
  #   .input(video_url)
  #   .output(chunk_output_format,
  #           constants.TARGET_CHUNK_SECS,
  #           c='copy',
  #           f='segment',
  #           reset_timestamps=1
  #           )
  #   .run()
  # )
  command = [
    'ffmpeg',
    '-i', video_url,
    '-c', 'copy',
    '-f', 'segment',
    '-segment_time', str(constants.TARGET_CHUNK_SECS),
    '-reset_timestamps', '1',
    chunk_output_format
  ]
  subprocess.run(command, check=True)

  # upload chunks to s3
  logger.info("Uploading chunks to S3")
  chunks = []

  for filepath in glob.glob(f'/tmp/CHUNK-*.{extension}'):
    filename = os.path.basename(filepath)
    size = os.path.getsize(filepath)
    obj_key = f"{job_id}/{filename}"
    s3_client.upload_file(filepath, OBJ_BUCKET_NAME, obj_key)
    chunks.append({"key": obj_key, "jobId": job_id, "extension": extension, "size": size})

  logger.info("Chunks uploaded to S3")

  # delete local storage
  os.system("rm /tmp/*")

  return {
    'jobId': job_id,
    'chunks': chunks
  }


def extract_data(event, context):
  """
  Extracts relevant data from the event and context.
  """
  return event["jobId"], event["key"], event["extension"]
