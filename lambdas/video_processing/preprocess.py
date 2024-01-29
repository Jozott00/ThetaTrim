import json
import logging
import time

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
  os.system("rm -rf /tmp/*")
  os.system("mkdir /tmp/chunks")

  video_url = s3_client.generate_presigned_url('get_object',
                                               Params={
                                                 'Bucket': OBJ_BUCKET_NAME,
                                                 'Key': orig_video_key,
                                               },
                                               ExpiresIn=3600)

  chunk_file_format = f"CHUNK-%d.{extension}"
  chunk_output_format = f"/tmp/chunks/{chunk_file_format}"
  logger.info(
    f"Start splitting video to {chunk_output_format} with chunk size of ~{constants.TARGET_CHUNK_SECS} seconds")
  command = [
    'ffmpeg',
    '-i', video_url,
    '-c', 'copy',
    '-f', 'segment',
    '-segment_time', str(constants.TARGET_CHUNK_SECS),
    '-reset_timestamps', '1',
    chunk_output_format
  ]
  ffmpeg_process = subprocess.Popen(command)

  logger.info(f"Start watch and upload...")

  chunks = watch_and_upload("/tmp/chunks", ffmpeg_process, chunk_file_format)

  chunks = [{"key": obj_key, "jobId": job_id, "extension": extension, "size": size} for obj_key, size in chunks]

  # delete local storage
  os.system("rm /tmp/*")

  return {
    'jobId': job_id,
    'chunks': chunks
  }


def upload_to_s3(file_path, object_name):
  try:
    logger.info(f"Start upload {file_path}...")
    s3_client.upload_file(file_path, OBJ_BUCKET_NAME, object_name)
    file_size = os.path.getsize(file_path)
    os.system(f"rm {file_path}")
    logger.info(f"Uploaded {file_path} to {OBJ_BUCKET_NAME}/{object_name} ({file_size / 1024 / 1024})")
    return object_name, file_size
  except Exception as e:
    logger.info(f"Error uploading {file_path}: {e}")
    raise e


def watch_and_upload(directory, ffmpeg_process, file_pattern):
  """
  Watches for new chunks in directory and uploads them as soon as possible

  :param directory: chunks output directory
  :param ffmpeg_process: async process of ffmpeg
  :param file_pattern: chunk file pattern
  :return:
  """
  i = 0
  chunks = []

  while True:
    process_done = ffmpeg_process.poll() is not None
    current_file = os.path.join(directory, file_pattern % i)
    next_file = os.path.join(directory, file_pattern % (i + 1))

    if os.path.exists(current_file):
      if os.path.exists(next_file) or process_done:
        chunk_info = upload_to_s3(current_file, os.path.basename(current_file))
        chunks.append(chunk_info)
        i += 1
      else:
        logger.info(f"Sleep inner because of {current_file}")
        time.sleep(0.25)
    elif process_done:
      break
    else:
      # short timeout
      logger.info(f"Sleep outer because of {current_file}")
      time.sleep(0.25)

  return chunks


def extract_data(event, context):
  """
  Extracts relevant data from the event and context.
  """
  return event["jobId"], event["key"], event["extension"]
