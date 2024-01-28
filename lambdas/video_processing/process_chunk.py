import glob
import json
import logging
import boto3
import os
import ffmpeg

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]
OBJ_BUCKET_NAME = os.environ["OBJECT_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')


def handler(event, context):
  """
  Processes a video chunk.
  """

  job_id, object_key, size, extension = extract_data(event, context)

  basename = os.path.basename(object_key)
  local_in_path = f"/tmp/{basename}"
  local_out_path = f"/tmp/out-{basename}"
  logger.info(f"Processing {object_key}")

  mp4_files = glob.glob("/tmp/*.mp4")
  logger.info(f"MP4 files in tmp before execution {mp4_files}")

  logger.info(f"Download video {object_key} to {local_in_path}")
  chunk_url = s3_client.generate_presigned_url('get_object',
                                               Params={
                                                 'Bucket': OBJ_BUCKET_NAME,
                                                 'Key': object_key,
                                               },
                                               ExpiresIn=3600)

  logger.info(f"Start chunk processing...")
  process_chunk(chunk_url, local_out_path)

  logger.info(f"\nReplace {OBJ_BUCKET_NAME}/{object_key} by result...")
  s3_client.upload_file(local_out_path, OBJ_BUCKET_NAME, object_key)
  logger.info("Done.")

  return event


def process_chunk(input_path, output_path):
  (
    ffmpeg
    .input(input_path)
    .crop(x='in_w/2-160', y='in_h/2-90', width=320, height=180)
    .output(output_path)
    .overwrite_output()
    .run()
  )


def extract_data(event, context):
  return event['jobId'], event['key'], event['size'], event['extension']
