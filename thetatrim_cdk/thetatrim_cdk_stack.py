import aws_cdk
from aws_cdk import (
  Stack,
  aws_apigateway as _apigateway,
  aws_s3 as _s3,
  aws_lambda as _lambda,
)
from constructs import Construct


class ThetatrimCdkStack(Stack):

  def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
    super().__init__(scope, construct_id, **kwargs)

    ###
    # S3 Object Bucket Config
    ###

    job_bucket = _s3.Bucket(self, "JobObjectStorage",
                            bucket_name="job-object-storage",
                            )

    ###
    # API Service Configs
    ###

    post_handler = _lambda.Function(self, 'PostJobHandler',
                                    runtime=_lambda.Runtime.PYTHON_3_12,
                                    handler="post_job.handler",
                                    code=_lambda.Code.from_asset("lambdas/rest"),
                                    environment={
                                      "OBJECT_BUCKET_NAME": job_bucket.bucket_name
                                    }
                                    )

    job_bucket.grant_read_write(post_handler)

    ###
    # API Gateway Config
    ###

    api = _apigateway.RestApi(self, "ThetaTrimRestAPI",
                              rest_api_name="tehtatrim",
                              )

    jobs_resource = api.root.add_resource("jobs")
    jobs_resource.add_method("POST",
                             integration=_apigateway.LambdaIntegration(post_handler)
                             )
