import logging
import subprocess
import threading
from typing import Any

import boto3
import os
import ffmpeg

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

  job_id, object_key, size, extension = extract_data(event, context)

  basename = os.path.basename(object_key)
  local_out_path = f"/tmp/out-{basename}"
  logger.info(f"Processing {object_key}")

  chunk_url = s3_client.generate_presigned_url('get_object',
                                               Params={
                                                 'Bucket': OBJ_BUCKET_NAME,
                                                 'Key': object_key,
                                               },
                                               ExpiresIn=3600)

  config = config_utils.get_job_config(job_table, job_id)
  logger.info(f"Loading config {config}")
  ffmpeg_command, outpath, format = build_command(chunk_url, local_out_path, config)
  logger.info(f"Executing command: \n{ffmpeg_command}")

  logger.info(f"Start chunk processing...")
  process_chunk(ffmpeg_command)

  key_base_name = os.path.basename(object_key)
  key_base_name_no_format = key_base_name.rsplit('.')[0]
  result_key = f"{job_id}/PROCESSED/{key_base_name_no_format}.{format}"

  # create reference image for chunk
  refimg_key = f"{job_id}/REFIMGS/{key_base_name_no_format}.jpg"
  refimg_thread = threading.Thread(target=process_ref_image, args=(outpath, refimg_key))
  refimg_thread.start()

  logger.info(f"\nReplace {OBJ_BUCKET_NAME}/{object_key} by result...")
  s3_client.upload_file(outpath, OBJ_BUCKET_NAME, result_key)
  logger.info("Done.")

  refimg_thread.join()
  logger.info("RefImage terminated.")

  os.system("rm /tmp/*")

  event['key'] = result_key
  event['refimg_key'] = refimg_key
  return event


def process_ref_image(local_video_path, result_key):
  logger.info("Start ref image generation...")
  ref_image_outpath = '/tmp/refimg.jpg'
  command = ['ffmpeg', '-i', local_video_path, '-vframes', '1', ref_image_outpath]

  try:
    subprocess.run(command, check=True)
  except subprocess.CalledProcessError as e:
    raise utils.FFmpegError("Failed to create reference image", e)

  logger.info(f"Upload refimage to {result_key}...")
  s3_client.upload_file(ref_image_outpath, OBJ_BUCKET_NAME, result_key)
  logger.info(f"Regimage uploaded.")


def process_chunk(ffmpeg_command):
  try:
    subprocess.run(ffmpeg_command, check=True)
  except subprocess.CalledProcessError as e:
    raise utils.FFmpegError("Failed to run ffmpeg process", e)


def build_command(chunk_url: str, outpath: str, config: config_utils.Config) -> tuple[list[str], str, str]:
  # TODO: filters must be within a single -vf flag!
  cmd = ["ffmpeg", "-i", chunk_url]
  out_no_format, format_ = outpath.rsplit(".", 1)

  vf_args = create_vf_args(config)
  cmd.append("-vf")
  cmd.append(",".join(vf_args))

  if config.format:
    format_ = config.format
  outpath = f"{out_no_format}.{format_}"
  cmd.append(outpath)

  return cmd, outpath, format_


def create_vf_args(config: config_utils.Config) -> list[str]:
  """
  Creates a list of all arguments that are part of the -vf option.
  It searches for filter operations in the provided config

  :param config: User defined configs
  :return: all configs that were not used (as they are no filters) and a list of arguments to the -vf option
  """

  filter_args = []
  for filter, opts in config.filters.items():
    if filter == 'crop':
      filter_args.append(create_crop_arg(opts))
    elif filter == 'resize':
      filter_args.append(create_resize_arg(opts))
    elif filter == 'brightness':
      filter_args.append(f"eq=brightness={opts['value']}")
    elif filter == 'grayscale':
      filter_args.append(f"hue=s=0")
    elif filter == 'sepia':
      filter_args.append(f"colorchannelmixer=.393:.769:.189:0:.349:.686:.168:0:.272:.534:.131")

  return filter_args


def create_filter_arg(opts):
  if opts == 'grayscale':
    return 'hue=s=0'
  elif opts == 'sepia':
    return 'colorchannelmixer=.393:.769:.189:0:.349:.686:.168:0:.272:.534:.131'
  elif 'brightness' in opts:
    brightness_value = float(opts.split('=')[1])
    return f'eq=brightness={brightness_value}'
  else:
    raise utils.ConfigError(f"Invalid filter {opts}")


def create_crop_arg(opts):
  width = opts['width']
  height = opts['height']
  x = opts.get('x', '(in_w-out_w)/2')
  y = opts.get('x', '(in_w-out_w)/2')
  return f'crop={width}:{height}:{x}:{y}'


def create_resize_arg(opts):
  width = opts['width']
  height = opts['height']
  return f'scale={width}:{height}'


def get_job_config(job_id: str) -> dict[Any: Any]:
  response = job_table.get_item(
    Key={
      'PK': f"JOB#{job_id}",
      'SK': "DATA"
    }
  )

  item = response.get('Item', None)

  if item is None:
    raise ValueError(f"Job information of {job_id} not found!")

  return item['transformations']


def extract_data(event, context):
  return event['jobId'], event['key'], event['size'], event['extension']
