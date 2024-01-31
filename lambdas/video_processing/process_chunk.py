import logging
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
  logger.info(f"Executing command: \n{ffmpeg_command.compile()}")

  logger.info(f"Start chunk processing...")
  process_chunk(ffmpeg_command)

  result_key = f"{object_key.rsplit('.')[0]}.{format}"

  logger.info(f"\nReplace {OBJ_BUCKET_NAME}/{object_key} by result...")
  s3_client.upload_file(outpath, OBJ_BUCKET_NAME, f"{object_key.rsplit('.')[0]}.{format}")
  logger.info("Done.")

  os.system("rm /tmp/*")

  event['key'] = result_key
  return event


def process_chunk(ffmpeg_command):
  try:
    (
      ffmpeg_command
      .run()
    )
  except Exception as e:
    raise utils.FFmpegError("Failed to process chunk", e)


def build_command(chunk_url: str, outpath: str, config: list[dict[str, any]]) -> Any:
  stream = ffmpeg.input(chunk_url)

  out_no_format, format = outpath.rsplit(".")

  for operation in config:
    op_type = operation.get('operation')
    op_opts = operation.get('opts')

    try:
      if op_type == 'crop':
        stream = append_crop(stream, op_opts)
      elif op_type == 'resize':
        stream = append_resize(stream, op_opts)
      elif op_type == 'format':
        format = validate_format(op_opts)
      elif op_type == 'filter':
        stream = append_filter(stream, op_opts)
      else:
        raise utils.ConfigError(f"No operation type '{op_type}' supported")

    except ValueError as e:
      raise utils.ConfigError(f"Invalid {op_type} options: {op_opts}", e)

  outpath = f"{out_no_format}.{format}"
  stream = stream.output(outpath)
  return stream, outpath, format


def append_filter(stream, opts) -> Any:
  if opts == 'grayscale':
    return ffmpeg.filter(stream, 'hue', 's=0')
  elif opts == 'sepia':
    return ffmpeg.filter(stream, 'colorchannelmixer', '.393:.769:.189:0:.349:.686:.168:0:.272:.534:.131')
  elif 'brightness' in opts:
    brightness_value = float(opts.split('=')[1])
    return ffmpeg.filter(stream, 'eq', brightness=brightness_value)
  else:
    raise utils.ConfigError(f"Invalid filter {opts}")


def validate_format(opts) -> Any:
  opt = opts.strip()

  valid_formats = ['mp4', 'm4p', 'm4v', 'mov', 'avi']

  if opt in valid_formats:
    return opt
  else:
    raise utils.ConfigError(f"Configured format {opts} is none of the valid formats: {valid_formats}")


def append_crop(stream, opts) -> Any:
  op_opts = opts.split(' ')
  if len(op_opts) == 2:
    # If user provides only width and height, assume x and y are both 0
    width, height = map(int, op_opts)
    x = y = 0
  elif len(op_opts) == 4:
    width, height, x, y = map(int, op_opts)
  else:
    raise utils.ConfigError(f"Invalid crop options: {opts}")

  return ffmpeg.filter(stream, 'crop', width, height, x, y)


def append_resize(stream, opts) -> Any:
  op_opts = opts.split(' ')
  if len(op_opts) != 2:
    raise utils.ConfigError(f"Invalid crop options: {opts}")
    # If user provides only width and height, assume x and y are both 0
  width, height = map(int, op_opts)
  return ffmpeg.filter(stream, 'scale', width, height)


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
