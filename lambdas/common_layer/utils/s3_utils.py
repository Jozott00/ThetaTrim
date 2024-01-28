import logging
import boto3
from concurrent.futures import ThreadPoolExecutor
import os
import concurrent.futures

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_PART_SIZE = 64 * 1024 * 1024  # 64 MB

s3_client = boto3.client('s3')


def _upload_part(s3_client, bucket_name, objectkey, part_number, upload_id, part_data):
  logger.info(
    f"Part {part_number} started upload of {len(part_data) / 1024 / 1024:.2f} MB to {bucket_name}/{objectkey} ...")
  part = s3_client.upload_part(
    Body=part_data,
    Bucket=bucket_name,
    Key=objectkey,
    UploadId=upload_id,
    PartNumber=part_number
  )
  logger.info(
    f"Part {part_number} finished upload.")
  return {'PartNumber': part_number, 'ETag': part['ETag']}


def multipart_upload(input_stream, bucket_name, objectkey, part_size=DEFAULT_PART_SIZE):
  s3_url = f"s3://{bucket_name}/{objectkey}"
  logger.info(f"Start multipart uploading to {s3_url}")

  # Initiate the multipart upload
  mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=objectkey)

  # Upload parts in parallel
  parts = []
  part_number = 1

  futures = []
  datasize = 0

  with ThreadPoolExecutor(os.cpu_count() * 5) as executor:
    while True:
      # Read part_size MB from the input stream
      data = input_stream.read(part_size)

      if not data:
        break

      datasize += len(data)
      logger.info(f"Upload {len(data) / 1024 / 1024:.2f} MB to {s3_url} ... ({datasize / 1024 / 1024:.2f} MB)")

      # Upload a part
      future = executor.submit(_upload_part, s3_client, bucket_name, objectkey, part_number, mpu['UploadId'], data)
      futures.append(future)

      part_number += 1

    # Ensure all uploads are complete
    for future in futures:
      parts.append(future.result())

    logger.info(f"All data uploaded to {s3_url}!")

  # Complete multipart upload
  s3_client.complete_multipart_upload(
    Bucket=bucket_name,
    Key=objectkey,
    UploadId=mpu['UploadId'],
    MultipartUpload={'Parts': parts}
  )


def download_file(bucket_name, key, destination):
  logger.info(f"Download {bucket_name}/{key} to {destination}...")
  s3_client.download_file(bucket_name, key, destination)
  logger.info(f"Done with {bucket_name}/{key}.")


# Download all s3 object into destinations
def download_all(bucket_name: str, keys: list[str], destinations: list[str]):
  # Validate that lists of keys and destinations have same length
  if len(keys) != len(destinations):
    raise ValueError("keys and destinations lists must have the same length")

  with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(download_file, [bucket_name] * len(keys), keys, destinations)
