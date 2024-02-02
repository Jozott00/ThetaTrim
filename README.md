# ThetaTrim


A highly parallized serverless video processing platform. It utilizes FFMPEG for video processing. Resizing a 4K h264 video with a length of 44 minutes and a size of 1.9 GB tooks around **49** seconds.

The infrastructure design utilizes video splitting at keyframes, which makes the chunk creation very efficient. Each chunk is processed in parallel. After chunk processing is done, all chunks are reduced to a single video. 

Additionally label detection using AWS Rekognition allows to find a suitable thumbnail for the video.

![Main Stepfunction](https://github.com/Jozott00/ThetaTrim/assets/12057307/50e5d54d-5ea0-41ab-ae91-ed51373982e6)

The whole processed is invoked by putting an object into the s3 bucket after creating a job using the REST API.

![Overall Architecture](https://github.com/Jozott00/ThetaTrim/assets/12057307/e999e0ac-2105-46e5-9fe3-ba1e02f1238c)

## General Information

This project utilizes the [Java AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/work-with-cdk-java.html), a framework
for defining cloud infrastructure in Java code and provisioning it
through [AWS CloudFormation](https://aws.amazon.com/cloudformation).
The `cdk.json` file tells the CDK Toolkit how to execute the application.
