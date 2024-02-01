import boto3
import logging
import os
from typing import Dict, Any

JOB_TABLE_NAME = os.environ["JOB_TABLE_NAME"]
OBJ_BUCKET_NAME = os.environ["OBJECT_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
s3_bucket = boto3.resource('s3').Bucket(OBJ_BUCKET_NAME)
job_table = boto3.resource('dynamodb').Table(JOB_TABLE_NAME)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """
  Cleans up the resources that were used by the job (such as chunk videos, database entries, etc.).
  """

  error, job_id = extract_data(event, context)

  delete_chunks(job_id)

  return {
    'jobId': job_id,
    'success': error is None
  }


def delete_chunks(job_id):
  objects = s3_bucket.objects.filter(Prefix=f"{job_id}/")

  objects_to_delete = [{'Key': o.key} for o in objects if
                       o.key.lower().startswith(f"{job_id}/chunks".lower())
                       or o.key.lower().startswith(f"{job_id}/processed".lower())
                       or o.key.lower().startswith(f"{job_id}/refimgs".lower())
                       ]

  if len(objects_to_delete):
    s3_client.delete_objects(Bucket=OBJ_BUCKET_NAME, Delete={'Objects': objects_to_delete})


def extract_data(event, context):
  try:
    job_id = event[0]['jobId']
    error = event[0].get('error')
  except Exception:
    job_id = event['jobId']
    error = event.get('error')
  return error, job_id
