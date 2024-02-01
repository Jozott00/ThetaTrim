import argparse
import requests
import yaml
from dotenv import load_dotenv
import os
from websockets.sync.client import connect
import json

load_dotenv()


def parse_arguments():
  parser = argparse.ArgumentParser(description="Upload video and perform operations on it.")
  parser.add_argument("config", help="Path to the config.yaml file")
  parser.add_argument("video", help="Path to the video file")
  return parser.parse_args()


def read_config(config_path):
  with open(config_path, 'r') as file:
    return yaml.safe_load(file)


def post_job(config):
  endpoint_url = os.getenv("REST_ENDPOINT")
  print("Creating job…")
  response = requests.post(f"{endpoint_url}/jobs", json=config)
  response.raise_for_status()
  return response.json()["jobId"], response.json()["url"]


def upload_to_s3(upload_url, video_path):
  print("Uploading video…")
  with open(video_path, 'rb') as file:
    files = {'file': (video_path, file)}
    response = requests.put(upload_url, files=files)
    response.raise_for_status()
    if response.status_code == 200:
      print("Upload successful.")
    else:
      print(f"Failed to upload. Status code: {response.status_code}")


def listen_for_result(job_id):
  print("Processing video…")
  headers = [("jobId", job_id)]
  try:
    with connect(os.getenv("WS_ENDPOINT"), additional_headers=headers) as websocket:
      data = json.loads(websocket.recv())
      print(data['msg'])
  except:
    print("Failed to listen for process updates. Please try again later.")


def main():
  args = parse_arguments()
  config = read_config(args.config)
  job_id, upload_url = post_job(config)
  upload_to_s3(upload_url, args.video)
  listen_for_result(job_id)


if __name__ == "__main__":
  main()
