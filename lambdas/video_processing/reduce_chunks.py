import glob
import logging
import subprocess
import tempfile
from typing import Dict, Any
import os
from utils.job_status import JobStatus

import boto3
from utils import s3_utils
from utils import utils

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]
OBJ_BUCKET_NAME = os.environ["OBJECT_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
job_table = boto3.resource('dynamodb').Table(JOB_TABLE_NAME)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  Reduces all processed chunks to a single video.
  """

  os.system('rm /tmp/*')

  logger.info(f"Invoked with event: {event}")

  keys = extract_data(event, context)
  jobid = utils.get_jobid_from_key(keys[0])
  ext = utils.get_extension_from_key(keys[0])
  result_file = f"{jobid}/RESULT.{ext}"
  chunk_files = download_chunks(keys)

  tmpfiles = glob.glob("/tmp/*")
  logger.info(f"Found tmp file: {tmpfiles}")

  logger.info(f"Concat videos: {chunk_files}")

  with tempfile.NamedTemporaryFile('w') as f:
    create_seglist_in(chunk_files, file=f)
    logger.info(f"Seglist file written in {f.name}.")

    with open(f.name, 'r') as tmpf:
      l = tmpf.read()
      logger.info(f"Stored seglist is: {l}")

    process = exec_command(f.name, v="info")
    s3_utils.multipart_upload(process.stdout, OBJ_BUCKET_NAME, result_file)

    return_code = process.wait()
    logger.info(f"FFMPEG returned with code {return_code}")
    if return_code != 0:
      raise "FFMPEG exited not successful!"

  update_status(jobid)

  logger.info(f"Success")

  os.system('rm /tmp/*')

  return {
    "objectUrl": result_file,
    "jobid": jobid,
    "ext": ext,
  }


def create_seglist_in(urls: list[str], file):
  seglist = "ffconcat version 1.0\n"
  seglist += "\n".join([f"file {u}" for u in urls])
  file.write(seglist)
  file.flush()


def download_chunks(keys: list[str]) -> list[str]:
  logger.info("Download chunks...")
  dests = list(
    map(lambda i: "/tmp/CHUNK-{0:04}.{1}".format(i[0], os.path.splitext(i[1])[1].lstrip('.')), enumerate(keys)))
  s3_utils.download_all(OBJ_BUCKET_NAME, keys, dests)
  logger.info("Chunks downloaded.")
  return dests


def exec_command(seglist_file: str, v="info") -> subprocess.Popen:
  command = ["ffmpeg"]
  command.append("-y")
  command.append(f"-v {v}")
  command.append("-protocol_whitelist concat,file,http,https,tcp,tls,crypto")  # allows http sources
  command.append("-f concat")
  command.append("-safe 0")  # required for http sources
  command.append(f'-i {seglist_file}')  # concat file list
  command.append("-c copy")
  command.append("-f mp4")  # todo: make format agnostic
  command.append("-movflags frag_keyframe+empty_moov")  # todo: only required for mov like containers
  command.append("pipe:1")

  command = " ".join(command)

  return subprocess.Popen(f"{command}", stdout=subprocess.PIPE, shell=True)


def generate_presigned_urls(keys: list[str], expiration=3600) -> list[str]:
  return [s3_client.generate_presigned_url('get_object',
                                           Params={
                                             'Bucket': OBJ_BUCKET_NAME,
                                             'Key': key
                                           },
                                           ExpiresIn=expiration)
          for key in keys
          ]


def update_status(job_id):
  job_table.update_item(
    Key={
      'PK': f"JOB#{job_id}",
      'SK': "DATA"
    },
    UpdateExpression='SET #status = :val',
    ExpressionAttributeValues={
      ':val': JobStatus.COMPLETED.value
    },
    ExpressionAttributeNames={
      '#status': 'status'
    },
    ReturnValues="UPDATED_NEW"
  )


def extract_data(event, context) -> list[str]:
  return [e['key'] for e in event['chunks']]
