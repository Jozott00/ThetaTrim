package com.thetatrim

import software.amazon.awscdk.Stack
import software.amazon.awscdk.StackProps
import software.amazon.awscdk.services.apigateway.LambdaIntegration
import software.amazon.awscdk.services.apigateway.RestApi
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.Table
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource
import software.amazon.awscdk.services.s3.Bucket
import software.amazon.awscdk.services.s3.EventType
import software.amazon.awscdk.services.s3.NotificationKeyFilter
import software.amazon.awscdk.services.s3.notifications.SqsDestination
import software.amazon.awscdk.services.sqs.Queue
import software.amazon.awscdk.services.stepfunctions.Choice
import software.amazon.awscdk.services.stepfunctions.DefinitionBody
import software.amazon.awscdk.services.stepfunctions.StateMachine
import software.amazon.awscdk.services.stepfunctions.Condition
import software.amazon.awscdk.services.stepfunctions.Fail
import software.amazon.awscdk.services.stepfunctions.Parallel
import software.amazon.awscdk.services.stepfunctions.Pass
import software.amazon.awscdk.services.stepfunctions.tasks.LambdaInvoke
import software.constructs.Construct

class ThetaTrimStack @JvmOverloads constructor(scope: Construct?, id: String?, props: StackProps? = null) :
    Stack(scope, id, props) {
    /**
     * S3 Bucket for storing job objects such as videos and frames.
     */
    private lateinit var jobsBucket: Bucket

    /**
     * Lambda function for creating jobs and presigned urls.
     */
    private lateinit var postJobLambda: Function

    /**
     * Lambda function for preprocessing uploaded videos.
     */
    private lateinit var preprocessLambda: Function

    /**
     * Lambda function to check the jobs status and whether all chunks are processed.
     */
    private lateinit var checkJobStatusLambda: Function

    /**
     * Sate-machine for the reducer workflow.
     */
    private lateinit var reducerStateMachine: StateMachine

    /**
     * Rest API for handling different endpoints.
     */
    private lateinit var restApi: RestApi

    /**
     * Queue for newly created videos to be further processed.
     */
    private lateinit var preprocessingQueue: Queue
    private lateinit var jobsTable: Table

    init {
        setupResources()
        grantPermissions()
        configureTriggers()
        configureEndpoints()
    }

    /**
     * Initializes all resources of the stack.
     */
    private fun setupResources() {
        jobsBucket = Bucket.Builder.create(this, "JobObjectBucket")
            .bucketName("${PREFIX}job-object-bucket")
            .versioned(true)
            .build()
        postJobLambda = Function.Builder.create(this, "PostJobHandler")
            .functionName("${PREFIX}post-job-handler")
            .runtime(Runtime.PYTHON_3_12)
            .handler("post_job.handler")
            .code(Code.fromAsset("lambdas/rest"))
            .environment(
                mapOf(
                    "OBJECT_BUCKET_NAME" to jobsBucket.bucketName
                )
            )
            .build()
        preprocessLambda = Function.Builder.create(this, "PreprocessHandler")
            .functionName("${PREFIX}preprocess-handler")
            .runtime(Runtime.PYTHON_3_12)
            .handler("preprocess.handler")
            .code(Code.fromAsset("lambdas/video_processing"))
            .environment(
                mapOf(
                    "OBJECT_BUCKET_NAME" to jobsBucket.bucketName
                )
            )
            .build()
        checkJobStatusLambda = Function.Builder.create(this, "CheckJobStatusHandler")
            .functionName("${PREFIX}check-job-status-handler")
            .runtime(Runtime.PYTHON_3_12)
            .handler("check_job_status.handler")
            .code(Code.fromAsset("lambdas/reducer"))
            .build()
        reducerStateMachine = generateReducerSateMachine()
        restApi = RestApi.Builder.create(this, "RestAPI")
            .restApiName("${PREFIX}rest-api")
            .build()
        preprocessingQueue = Queue.Builder.create(this, "PreprocessingQueue")
            .queueName("${PREFIX}preprocessing-queue")
            .build()
        jobsTable = Table.Builder.create(this, "JobsTable")
            .tableName("jobs")
            .partitionKey(
                Attribute.builder().name("id").type(AttributeType.STRING).build()
            )
            .build()
    }

    /**
     * WIP: Generates the reducer step-functions state machine.
     */
    private fun generateReducerSateMachine(): StateMachine {
        val fail = Fail.Builder.create(this, "Fail").build()
        val reduceTask = Pass.Builder.create(this, "ReduceTask").build()
        val thumbnailGenerationTask = Pass.Builder.create(this, "ThumbnailGenerationTask").build()
        val jobStatusChoice = Choice.Builder.create(this, "JobDoneChoice").build()
            .`when`(
                Condition.booleanEquals("$.hasFailed", true),
                fail
            )
            .`when`(
                Condition.booleanEquals("$.isProcessed", true),
                Parallel.Builder.create(this, "Parallel")
                    .build()
                    .branch(reduceTask)
                    .branch(thumbnailGenerationTask)
            )
        val checkJobStatusTask = LambdaInvoke.Builder.create(this, "CheckJobStatusTask")
            .lambdaFunction(checkJobStatusLambda)
            .outputPath("$.Payload")
            .build()
            .next(jobStatusChoice)
        return StateMachine.Builder.create(this, "ReducerStateMachine")
            .definitionBody(DefinitionBody.fromChainable(checkJobStatusTask))
            .build()
    }

    /**
     * Configures and adds all endpoints to the Rest API.
     */
    private fun configureEndpoints() {
        val jobsResource = restApi.root.addResource("jobs")
        val postJobIntegration = LambdaIntegration.Builder.create(postJobLambda).build()
        jobsResource.addMethod("POST", postJobIntegration)
    }

    /**
     * Configures all triggers between services.
     */
    private fun configureTriggers() {
        jobsBucket.addEventNotification(
            EventType.OBJECT_CREATED,
            SqsDestination(preprocessingQueue),
            NotificationKeyFilter.builder()
                .suffix("original.mp4")
                .build()
        )
        preprocessLambda.addEventSource(SqsEventSource(preprocessingQueue))
    }

    /**
     * Grants all necessary permissions for interaction between services.
     */
    private fun grantPermissions() {
        jobsBucket.grantWrite(postJobLambda)
        jobsTable.grantWriteData(postJobLambda)
        jobsTable.grantReadWriteData(preprocessLambda)
    }

    companion object {
        /**
         * Prefix of all resource names.
         */
        private const val PREFIX = "thetatrim-"
    }
}