package com.thetatrim;

import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.apigateway.LambdaIntegration;
import software.amazon.awscdk.services.apigateway.Resource;
import software.amazon.awscdk.services.apigateway.RestApi;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.Bucket;
import software.constructs.Construct;

import java.util.Map;

public class ThetaTrimStack extends Stack {
    public ThetaTrimStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public ThetaTrimStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);
        
        Bucket jobObjectBucket = Bucket.Builder.create(this, "JobObjectBucket")
            .bucketName("job-object-bucket")
            .versioned(true)
            .build();

        Function postJobHandler = Function.Builder.create(this, "PostJobHandler")
            .runtime(Runtime.PYTHON_3_12)
            .handler("post_job.handler")
            .code(Code.fromAsset("lambdas/rest"))
            .environment(Map.of(
                "OBJECT_BUCKET_NAME", jobObjectBucket.getBucketName()
            ))
            .build();
        
        jobObjectBucket.grantReadWrite(postJobHandler);

        RestApi restApi = RestApi.Builder.create(this, "RestAPI")
            .restApiName("thetatrim")
            .build();
        
        Resource jobsResource = restApi.getRoot().addResource("jobs");

        LambdaIntegration postJobIntegration = LambdaIntegration.Builder.create(postJobHandler).build();
        
        jobsResource.addMethod("POST", postJobIntegration);
    }
}
