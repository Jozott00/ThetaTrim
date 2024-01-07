# General Information

This project utilizes the [Java AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/work-with-cdk-java.html), a framework for defining cloud infrastructure in Java code and provisioning it through [AWS CloudFormation](https://aws.amazon.com/cloudformation). 
The `cdk.json` file tells the CDK Toolkit how to execute the application.

## Useful commands

* `mvn package`     compile and run tests
* `cdk ls`          list all stacks in the app
* `cdk synth`       emits the synthesized CloudFormation template
* `cdk deploy`      deploy this stack to your default AWS account/region
* `cdk diff`        compare deployed stack with current state
* `cdk docs`        open CDK documentation

# Cloud Development

To deploy the application on AWS, begin by installing the AWS Cloud Development Kit (CDK).
You can find the [aws-cdk package here](https://www.npmjs.com/package/aws-cdk).
Once installed, use the command `aws configure` to set up the AWS account you intend to deploy to.
You might want to create a new [IAM user](https://aws.amazon.com/iam/) for this purpose.
Note that the stack will then be deployed to the specified environment, such as `eu-west-3`, `us-east-1`, etc.

Once you've configured your user, you're ready to deploy the stack to your AWS environment.
Start by bootstrapping the stack using the command `cdk bootstrap`.
This step prepares your AWS environment for the deployment.
Following the bootstrap process, deploy your stack by executing the command `cdk deploy`.
This will initiate the deployment of your application on AWS.

# Local Development

TODO: Update according to Java. 

To develop the AWS application locally follow these steps:

- Download and install [LocalStack](https://www.localstack.cloud/)
- Install [cdklocal](https://github.com/localstack/aws-cdk-local) (a localstack cdk cli wrapper)
- Install [awslocal](https://github.com/localstack/awscli-local) (a localstack aws cli wrapper)

Then you are good to go.

## Demo

Run in one shell:

```bash
localstack start
```

Run in other shell:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

This will setup the cdk environment.

Now we deploy the cdk stack locally.

```bash
cdklocal bootstrap
cdklocal deploy
```

Lets check if everything is running. We want to call the post_job lambda.
But first we have to find its name.

```bash
awslocal lambda list-functions  
```

Copy the lambda function name and past it here instead of `<lambda-name>`

```bash
awslocal lambda invoke --function-name <lambda-name> /dev/stdout
```

This will output something like

```
"http://172.17.0.2:4566/job-object-storage/some-job-id/original.mp4?AWSAccessKeyId=LSIAQAAAAAAAO4SAB2V2&Signature=dZvuK0bBJuIZRWRwnjHl7ktDzmw%3D&x-amz-security-token=FQoGZXIvYXdzEBYaDIM8rzjREg9jw8L7ofrM88oGRiRB88ELEzKDrYFgXjwgtDnq6V3AZjPlki%2F9L%2FIQA6FRdgoYrwVHBG%2FJOCIZnbQvPWMq5bA7Udf%2BJinKnbuocR2r%2Bi%2B%2FqvHGf21l%2BkcCVZygtykT2BQRv0D0wpCLU2FjbUKG%2F1H2UBiRcT3WPDSWN%2FvVaOJguCyJVhlcxQA1kLEDv6PSIqo48X4o37%2B1rdzLjXXSSo7vnduRb6QZwoCx381EExMX1mYgdQkNkc6MqegYFeZ%2BoX2Pohy%2FUyJl7W5rfVlJZ4ygzuEqE2DBbpIpaKaRFJyToJla0%2FlDllYkYUe6SZPUFiNvpQiiZUQ%3D&Expires=1704476659"
{
    "StatusCode": 200,
    "ExecutedVersion": "$LATEST"
}
```

which is a presigned url the lambda generated for us to access `/some-job-id/original.mp4` in the `job-object-storage`
bucket.
