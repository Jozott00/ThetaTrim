package com.thetatrim;

import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
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
    }
}
