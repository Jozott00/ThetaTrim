import json
import logging
import boto3
import os
import subprocess
import shlex
import glob

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

  logger.info(f"Invoked with event: {event}")

  tmp_files = glob.glob("/tmp/CHUNK-*")
  logger.info(f"Current content of /tmp: {tmp_files}")
  # video_url = get_source_video_url("test_small.mp4")
  video_url = "/tmp/original.mp4"

  logger.info(f"Download video {orig_video_key} to {video_url}")
  s3_client.download_file(OBJ_BUCKET_NAME, orig_video_key, video_url)

  ƒfmpeg_cmd = f"/opt/bin/ffmpeg -i {video_url} -segment_time 10 -acodec copy -f segment -vcodec copy -reset_timestamps 1 -map 0 /tmp/CHUNK-%d.mp4"
  command1 = shlex.split(ƒfmpeg_cmd)
  logger.info("Start video splitting")
  p = subprocess.run(command1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  logger.info("process stopped")

  tmp_files = glob.glob("/tmp/CHUNK-*")
  logger.info(f"After split in /tmp: {tmp_files}")

  stdout = p.stdout.decode('utf-8')
  stderr = p.stderr.decode('utf-8')

  for line in stdout.split('\n'):
    logger.info(line.strip())

  for line in stderr.split('\n'):
    logger.info(line.strip())

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


def get_source_video_url(obj_key: str) -> str:
  return s3_client.generate_presigned_url(
    ClientMethod='get_object',
    Params={
      'Bucket': OBJ_BUCKET_NAME,
      'Key': f"{obj_key}",
    }
  )


def extract_data(event, context):
  """
  Extracts relevant data from the event and context.
  """

  body = json.loads(event["Records"][0]["body"])
  s3_key = body["Records"][0]["s3"]["object"]["key"]
  job_id = s3_key.split(s3_deliminator)[0]
  return job_id, s3_key
