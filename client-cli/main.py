import argparse
import requests
import yaml
from dotenv import load_dotenv
import os
from websockets.sync.client import connect
import json

load_dotenv()
rest_endpoint = os.getenv("REST_ENDPOINT")
ws_endpoint = os.getenv("WS_ENDPOINT")


def parse_arguments():
  if rest_endpoint is None:
    raise Exception("Please define a rest-api endpoint REST_ENDPOINT in your .env file.")
  if ws_endpoint is None:
    raise Exception("Please define a websockets-api endpoint WS_ENDPOINT in your .env file.")
  parser = argparse.ArgumentParser(description="Upload video and perform operations on it.")
  parser.add_argument("config", help="Path to the config.yaml file")
  parser.add_argument("video", help="Path to the video file")
  return parser.parse_args()


def read_config(config_path):
  with open(config_path, 'r') as file:
    return yaml.safe_load(file)


def post_job(config):
  print("Creating job…")
  try:
    response = requests.post(f"{rest_endpoint}/jobs", json=config)
    response.raise_for_status()
  except:
    raise Exception("Creating job failed.")
  return response.json()["jobId"], response.json()["url"]


def upload_to_s3(upload_url, video_path):
  print("Uploading video…")
  try:
    with open(video_path, 'rb') as file:
      response = requests.put(upload_url, data=file)
      response.raise_for_status()
      if response.status_code != 200:
        print(f"Failed to upload file: {video_path}")
  except:
    raise Exception(f"Failed to upload file: {video_path}")


def listen_for_result(job_id):
  print("Processing video…")
  try:
    headers = [("jobId", job_id)]
    with connect(ws_endpoint, additional_headers=headers) as websocket:
      data = json.loads(websocket.recv())
      print(data['msg'])
  except:
    print("Failed to listen for process updates. Please try again later.")


def main():
  try:
    args = parse_arguments()
    config = read_config(args.config)
    job_id, upload_url = post_job(config)
    upload_to_s3(upload_url, args.video)
    listen_for_result(job_id)
  except Exception as e:
    print(e)


if __name__ == "__main__":
  main()
