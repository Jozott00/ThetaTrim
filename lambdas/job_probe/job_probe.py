import logging
import subprocess
from typing import Dict, Any
import os
import boto3

from utils import utils
from utils import config_utils

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]
OBJ_BUCKET_NAME = os.environ["OBJECT_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
s3_bucket = boto3.resource('s3').Bucket(OBJ_BUCKET_NAME)
job_table = boto3.resource('dynamodb').Table(JOB_TABLE_NAME)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  Validates input data and obtains video information using ffprobe
  """

  job_id, video_key, extension = extract_event_data(event)
  video_url = s3_client.generate_presigned_url('get_object',
                                               Params={
                                                 'Bucket': OBJ_BUCKET_NAME,
                                                 'Key': video_key,
                                               },
                                               ExpiresIn=3600)

  config = config_utils.get_job_config(job_table, job_id)

  event["extractAudio"] = False
  if config.extract_audio:
    acodec = get_audio_codec(video_url)
    event["acodec"] = acodec
    event["extractAudio"] = True

  video_info = get_video_details(video_url)
  event.update(video_info)

  check_crop_dimensions(video_info, config)

  return event


def check_crop_dimensions(video_info: dict[str: Any], config: config_utils.Config):
  crop_filter = config.filters.get('crop', None)

  if not crop_filter:
    return

  video_width = video_info.get('width')
  video_height = video_info.get('height')

  crop_width = crop_filter.get('width')
  crop_height = crop_filter.get('height')

  offset_x = crop_filter.get('x', (video_width - crop_width) // 2)
  offset_y = crop_filter.get('y', (video_height - crop_height) // 2)

  if (crop_width + offset_x) > video_width or (crop_height + offset_y) > video_height:
    raise utils.InputSourceError('Crop dimensions and offsets are larger than video dimensions')


def get_audio_codec(video_url: str) -> str:
  command = [
    'ffprobe', '-v', 'error', '-select_streams', 'a:0',
    '-show_entries', 'stream=codec_name', '-of', 'default=noprint_wrappers=1:nokey=1', video_url
  ]

  try:
    completed_process = subprocess.run(command, capture_output=True, text=True, check=True)
  except subprocess.CalledProcessError as e:
    logger.error(e.output)
    raise utils.FFmpegError("Failed to extract audio codec", e)

  codec = completed_process.stdout.strip()
  return codec


def get_video_details(video_url: str) -> dict:
  command = [
    'ffprobe', '-v', 'error', '-select_streams', 'v:0',
    '-show_entries', 'stream=width,height,codec_name', '-of', 'default=noprint_wrappers=1', video_url
  ]

  try:
    output = subprocess.run(command, capture_output=True, text=True, check=True).stdout.strip().split('\n')

    details = {}

    for line in output:
      key, value = line.split('=')
      if key in ('width', 'height'):
        details[key] = int(value)
      elif key == 'codec_name':
        details['vcodec'] = value
      else:
        details[key] = value

    return details
  except subprocess.CalledProcessError as e:
    logger.error(e.output)
    raise utils.FFmpegError("Failed to extract video information", e)



def extract_event_data(event: Dict[str, Any]) -> tuple[str, str, str]:
  return event['jobId'], event['key'], event['extension']
