# Local Development Intro

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

# Original README:

This is a blank project for CDK development with Python.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project. The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory. To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

* `cdk ls`          list all stacks in the app
* `cdk synth`       emits the synthesized CloudFormation template
* `cdk deploy`      deploy this stack to your default AWS account/region
* `cdk diff`        compare deployed stack with current state
* `cdk docs`        open CDK documentation

Enjoy!