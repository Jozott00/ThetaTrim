import argparse
import requests
import yaml
import boto3
from botocore.exceptions import NoCredentialsError


def parse_arguments():
    parser = argparse.ArgumentParser(description="Upload video and perform operations on it.")
    parser.add_argument("config", help="Path to the config.yaml file")
    parser.add_argument("video", help="Path to the video file")
    return parser.parse_args()


def read_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def send_rest_call(config):
    # Replace 'endpoint_url' with your actual endpoint
    response = requests.post("endpoint_url", json=config)
    response.raise_for_status()
    return response.json()['upload_url']


def upload_to_s3(upload_url, video_path):
    with open(video_path, 'rb') as file:
        files = {'file': (video_path, file)}
        response = requests.put(upload_url, files=files)
        response.raise_for_status()
        if response.status_code == 200:
            print("Upload successful.")
        else:
            print(f"Failed to upload. Status code: {response.status_code}")


def main():
    args = parse_arguments()
    config = read_config(args.config)
    upload_url = send_rest_call(config)
    upload_to_s3(upload_url, args.video)


if __name__ == "__main__":
    main()
