import logging
import os
import subprocess

import boto3
from utils import utils
from utils import config_utils

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]
OBJ_BUCKET_NAME = os.environ["OBJECT_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
job_table = dynamodb.Table(JOB_TABLE_NAME)


def handler(event, context):
  """
  Processes a video chunk.
  """

  os.system("rm /tmp/*")

  video_url = s3_client.generate_presigned_url('get_object',
                                               Params={
                                                 'Bucket': OBJ_BUCKET_NAME,
                                                 'Key': event["test_key"],
                                               },
                                               ExpiresIn=3600)

  command = ["ffmpeg",
             "-y",
             "-i", video_url,
             "-vf", "scale=640:480",
             "/tmp/outtest.mp4"
             ]

  subprocess.run(command, check=True)

  output_key = event["test_key"].replace(".mp4", "_cropped.mp4")
  s3_client.upload_file("/tmp/outtest.mp4", OBJ_BUCKET_NAME, output_key)

  os.system("rm /tmp/*")
