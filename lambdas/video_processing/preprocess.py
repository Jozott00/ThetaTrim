import json
import logging
import boto3
import os
import subprocess
import shlex
import glob
import ffmpeg

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
  # TODO: Replace by id from event
  job_id = "test"
  orig_video_key = "test_small.mp4"
  # TODO: Replace upper by this
  # job_id, orig_video_key = extract_data(event, context)
  video_url = "/tmp/original.mp4"

  logger.info(f"Download video {orig_video_key} to {video_url}")
  s3_client.download_file(OBJ_BUCKET_NAME, orig_video_key, video_url)

  logger.info(f"Start splitting video")
  (
    ffmpeg
    .input(video_url)
    .output("/tmp/CHUNK-%d.mp4", segment_time=5, acodec='copy', f='segment', vcodec='copy',
            reset_timestamps=1, map=0)
    .run(pipe_stdout=True, pipe_stderr=True)
  )

  # upload chunks to s3
  logger.info("Uploading chunks to S3")
  chunks = []

  for filepath in glob.glob('/tmp/CHUNK-*.mp4'):
    filename = os.path.basename(filepath)
    obj_key = f"{job_id}/{filename}"
    s3_client.upload_file(filepath, OBJ_BUCKET_NAME, obj_key)
    chunks.append({"objectUrl": obj_key})

  logger.info("Chunks uploaded to S3")

  return {
    'chunks': chunks
  }


def extract_data(event, context):
  """
  Extracts relevant data from the event and context.
  """

  body = json.loads(event["Records"][0]["body"])
  s3_key = body["Records"][0]["s3"]["object"]["key"]
  job_id = s3_key.split(s3_deliminator)[0]
  return job_id, s3_key
