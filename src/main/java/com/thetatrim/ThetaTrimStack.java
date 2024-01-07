package com.thetatrim;

import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.apigateway.LambdaIntegration;
import software.amazon.awscdk.services.apigateway.Resource;
import software.amazon.awscdk.services.apigateway.RestApi;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.EventType;
import software.amazon.awscdk.services.s3.NotificationKeyFilter;
import software.amazon.awscdk.services.s3.notifications.SqsDestination;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import java.util.Map;

public class ThetaTrimStack extends Stack {
    /**
     * Prefix of all resource names.
     */
    private static final String prefix = "thetatrim-";
    /**
     * S3 Bucket for storing job objects such as videos and frames.
     */
    private Bucket jobBucket;
    /**
     * Lambda function for creating jobs and presigned urls.
     */
    private Function postJobLambda;
    /**
     * Lambda function for preprocessing uploaded videos.
     */
    private Function preprocessLambda;
    /**
     * Rest API for handling different endpoints.
     */
    private RestApi restApi;
    /**
     * Queue for newly created videos to be further processed.
     */
    private Queue preprocessingQueue;

    public ThetaTrimStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public ThetaTrimStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);
        setupResources();
        grantPermissions();
        configureTriggers();
        configureEndpoints();
    }

    /**
     * Initializes all resources of the stack.
     */
    private void setupResources() {
        jobBucket = Bucket.Builder.create(this, "JobObjectBucket")
            .bucketName(prefix + "job-object-bucket")
            .versioned(true)
            .build();
        postJobLambda = Function.Builder.create(this, "PostJobHandler")
            .functionName(prefix + "post-job-handler")
            .runtime(Runtime.PYTHON_3_12)
            .handler("post_job.handler")
            .code(Code.fromAsset("lambdas/rest"))
            .environment(Map.of(
                "OBJECT_BUCKET_NAME", jobBucket.getBucketName()
            ))
            .build();
        preprocessLambda = Function.Builder.create(this, "PreprocessHandler")
            .functionName(prefix + "preprocess-handler")
            .runtime(Runtime.PYTHON_3_12)
            .handler("preprocess.handler")
            .code(Code.fromAsset("lambdas/video_processing"))
            .environment(Map.of(
                "OBJECT_BUCKET_NAME", jobBucket.getBucketName()
            ))
            .build();
        restApi = RestApi.Builder.create(this, "RestAPI")
            .restApiName(prefix + "rest-api")
            .build();
        preprocessingQueue = Queue.Builder.create(this, "PreprocessingQueue")
            .queueName(prefix + "preprocessing-queue")
            .build();
    }

    /**
     * Configures and adds all endpoints to the Rest API.
     */
    private void configureEndpoints() {
        Resource jobsResource = restApi.getRoot().addResource("jobs");
        LambdaIntegration postJobIntegration = LambdaIntegration.Builder.create(postJobLambda).build();
        jobsResource.addMethod("POST", postJobIntegration);
    }

    /**
     * Configures all triggers between services.
     */
    private void configureTriggers() {
        jobBucket.addEventNotification(
            EventType.OBJECT_CREATED,
            new SqsDestination(preprocessingQueue),
            NotificationKeyFilter.builder()
                .suffix("original.mp4")
                .build());
        preprocessLambda.addEventSource(new SqsEventSource(preprocessingQueue));
    }

    /**
     * Grants all necessary for interaction between services.
     */
    private void grantPermissions() {
        jobBucket.grantReadWrite(postJobLambda);
    }
}
