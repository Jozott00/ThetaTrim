import glob
import json
import logging
import subprocess
from typing import Any

import boto3
import os
import ffmpeg

from utils import utils

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

  configs = get_job_config(job_id)
  logger.info(f"Loading config {configs}")
  ffmpeg_command, outpath, format = build_command(chunk_url, local_out_path, configs)
  logger.info(f"Executing command: \n{ffmpeg_command}")

  logger.info(f"Start chunk processing...")
  process_chunk(ffmpeg_command)

  key_base_name = os.path.basename(object_key)
  result_key = f"{job_id}/PROCESSED/{key_base_name.rsplit('.')[0]}.{format}"

  logger.info(f"\nReplace {OBJ_BUCKET_NAME}/{object_key} by result...")
  s3_client.upload_file(outpath, OBJ_BUCKET_NAME, result_key)
  logger.info("Done.")

  os.system("rm /tmp/*")

  event['key'] = result_key
  return event


def process_chunk(ffmpeg_command):
  process = subprocess.run(ffmpeg_command)
  if process.returncode != 0:
    raise utils.FFmpegError("Failed to process chunk")


def build_command(chunk_url: str, outpath: str, config: list[dict[str, any]]) -> tuple[list[str], str, str]:
  # TODO: filters must be within a single -vf flag!
  cmd = ["ffmpeg", "-i", chunk_url]
  out_no_format, format_ = outpath.rsplit(".", 1)

  config, vf_args = create_vf_args(config)
  cmd.append("-vf")
  cmd.extend(vf_args)

  config, format_ = create_format_arg(format_, config)
  outpath = f"{out_no_format}.{format_}"
  cmd.append(outpath)

  return cmd, outpath, format_


def create_vf_args(config: list[dict[str, any]]) -> tuple[list[dict[str, any]], list[str]]:
  """
  Creates a list of all arguments that are part of the -vf option.
  It searches for filter operations in the provided config

  :param config: User defined configs
  :return: all configs that were not used (as they are no filters) and a list of arguments to the -vf option
  """
  filter_operations = ["crop", "resize", "filter"]
  used_filters = set()
  unused_configs = []
  filter_args = []

  for operation in config:
    op_type = operation.get('operation')
    op_opts = operation.get('opts')

    if op_type in filter_operations:
      if op_type in used_filters:
        raise utils.ConfigError(
          f"Duplicate filter operation: '{op_type}'. Each filter operation can only be used once.")
      used_filters.add(op_type)
      try:
        if op_type == 'crop':
          filter_args.append(create_crop_arg(op_opts))
        elif op_type == 'resize':
          filter_args.append(create_resize_arg(op_opts))
        elif op_type == 'filter':
          filter_args.append(create_filter_arg(op_opts))
      except ValueError as e:
        raise utils.ConfigError(f"Invalid {op_type} options: {op_opts}", e)
    else:
      unused_configs.append(operation)

  return unused_configs, filter_args


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


def create_format_arg(default_format: str, config: list[dict[str, any]]) -> tuple[list[dict[str, any]], str]:
  formats = []
  remaining_configs = []

  for item in config:
    if item.get('operation') == 'format':
      formats.append(item)
    else:
      remaining_configs.append(item)

  if len(formats) > 1:
    raise utils.ConfigError(f"Only one output format can be set. You submitted: {formats}")

  if len(formats) == 0:
    return remaining_configs, default_format

  format = formats.get('opts').strip()
  valid_formats = ['mp4', 'm4p', 'm4v', 'mov', 'avi']
  if format in valid_formats:
    return remaining_configs, format
  else:
    raise utils.ConfigError(f"Configured format {format} is none of the valid formats: {valid_formats}")


def create_crop_arg(opts):
  op_opts = opts.split(' ')
  if len(op_opts) == 2:
    width, height = map(int, op_opts)
    x = y = 0
  elif len(op_opts) == 4:
    width, height, x, y = map(int, op_opts)
  else:
    raise utils.ConfigError(f"Invalid crop options: {opts}")
  return f'crop={width}:{height}:{x}:{y}'


def create_resize_arg(opts):
  op_opts = opts.split(' ')
  if len(op_opts) != 2:
    raise utils.ConfigError(f"Invalid crop options: {opts}")
  width, height = map(int, op_opts)
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
