package com.thetatrim

import software.amazon.awscdk.Duration
import software.amazon.awscdk.Stack
import software.amazon.awscdk.StackProps
import software.amazon.awscdk.services.apigateway.LambdaIntegration
import software.amazon.awscdk.services.apigateway.RestApi
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.Table
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.EventSourceMapping
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.LayerVersion
import software.amazon.awscdk.services.lambda.python.alpha.PythonLayerVersion
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource
import software.amazon.awscdk.services.s3.Bucket
import software.amazon.awscdk.services.s3.EventType
import software.amazon.awscdk.services.s3.NotificationKeyFilter
import software.amazon.awscdk.services.s3.notifications.SqsDestination
import software.amazon.awscdk.services.sqs.Queue
import software.amazon.awscdk.services.stepfunctions.DefinitionBody
import software.amazon.awscdk.services.stepfunctions.StateMachine
import software.amazon.awscdk.services.stepfunctions.Map
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
     * Lambda function for processing a video chunk.
     */
    private lateinit var processChunkLambda: Function

    /**
     * Lambda function for extracting data from the whole original video.
     */
    private lateinit var extractDataLambda: Function

    /**
     * Lambda function to check the jobs status and whether all chunks are processed.
     */
    private lateinit var reduceChunksLambda: Function

    /**
     * Lambda function to generate a thumbnail.
     */
    private lateinit var generateThumbnailLambda: Function

    /**
     * Lambda function to cleanup all resources after the job is done.
     */
    private lateinit var cleanupLambda: Function

    /**
     * Sate-machine for the reducer workflow.
     */
    private lateinit var videoProcessingStateMachine: StateMachine

    /**
     * Rest API for handling different endpoints.
     */
    private lateinit var restApi: RestApi

    /**
     * Queue for newly created videos to be further processed.
     */
    private lateinit var preprocessingQueue: Queue
    private lateinit var jobsTable: Table

    /**
     * Lambda layers.
     */
    private lateinit var utilsLambdaLayer: LayerVersion
    private lateinit var ffmpegLambdaLayer: LayerVersion


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

        jobsBucket = Bucket.Builder.create(this, "JobObjectBucket1")
            .bucketName("${PREFIX}job-object-bucket-1")
            .versioned(true)
            .build()

        jobsTable = Table.Builder.create(this, "JobsTable")
            .partitionKey(
                Attribute.builder().name("PK").type(AttributeType.STRING).build()
            )
            .sortKey(
                Attribute.builder().name("SK").type(AttributeType.STRING).build()
            )
            .build()

        utilsLambdaLayer = PythonLayerVersion.Builder
            .create(this, "UtilsLayer")
            .entry("lambdas/common_layer")
            .compatibleRuntimes(mutableListOf(Runtime.PYTHON_3_11))
            .description("A layer that contains the utils module. Can be used by oder lambdas.")
            .build()

        ffmpegLambdaLayer = LayerVersion.Builder
            .create(this, "FFMpegLayer")
            .layerVersionName("ffmpeg")
            .code(Code.fromAsset("lambdas/ffmpeg_layer"))
            .compatibleRuntimes(mutableListOf(Runtime.PYTHON_3_11))
            .license("http://www.ffmpeg.org/legal.html")
            .build()

        postJobLambda = Function.Builder.create(this, "PostJobHandler")
            .functionName("${PREFIX}post-job-handler")
            .runtime(Runtime.PYTHON_3_11)
            .handler("post_job.handler")
            .code(Code.fromAsset("lambdas/rest"))
            .environment(
                mapOf(
                    "OBJECT_BUCKET_NAME" to jobsBucket.bucketName,
                    "JOB_TABLE_NAME" to jobsTable.tableName
                )
            )
            .layers(mutableListOf(utilsLambdaLayer))
            .build()
        preprocessLambda = Function.Builder.create(this, "PreprocessHandler")
            .functionName("${PREFIX}preprocess-handler")
            .timeout(Duration.seconds(10)) // TODO: Check how much we need
            .runtime(Runtime.PYTHON_3_11)
            .handler("preprocess.handler")
            .code(Code.fromAsset("lambdas/video_processing"))
            .environment(
                mapOf(
                    "OBJECT_BUCKET_NAME" to jobsBucket.bucketName,
                    "JOB_TABLE_NAME" to jobsTable.tableName
                )
            )
            .layers(mutableListOf(utilsLambdaLayer, ffmpegLambdaLayer))
            .build()
        processChunkLambda = Function.Builder.create(this, "ProcessChunkHandler")
            .functionName("${PREFIX}process-chunk-handler")
            .timeout(Duration.seconds(60)) // TODO: Check how much we need
            .runtime(Runtime.PYTHON_3_11)
            .handler("process_chunk.handler")
            .code(Code.fromAsset("lambdas/video_processing"))
            .environment(
                mapOf(
                    "OBJECT_BUCKET_NAME" to jobsBucket.bucketName,
                    "JOB_TABLE_NAME" to jobsTable.tableName
                )
            )
            .layers(mutableListOf(utilsLambdaLayer, ffmpegLambdaLayer))
            .build()
        extractDataLambda = Function.Builder.create(this, "ExtractDataHandler")
            .functionName("${PREFIX}extract-data-handler")
            .runtime(Runtime.PYTHON_3_11)
            .handler("extract_data.handler")
            .code(Code.fromAsset("lambdas/video_processing"))
            .environment(
                mapOf(
                    "OBJECT_BUCKET_NAME" to jobsBucket.bucketName,
                    "JOB_TABLE_NAME" to jobsTable.tableName
                )
            )
            .layers(mutableListOf(utilsLambdaLayer))
            .build()
        reduceChunksLambda = Function.Builder.create(this, "ReduceChunksHandler")
            .functionName("${PREFIX}reduce-chunks-handler")
            .runtime(Runtime.PYTHON_3_11)
            .handler("reduce_chunks.handler")
            .code(Code.fromAsset("lambdas/video_processing"))
            .environment(
                mapOf(
                    "JOB_TABLE_NAME" to jobsTable.tableName
                )
            )
            .layers(mutableListOf(utilsLambdaLayer))
            .build()
        generateThumbnailLambda = Function.Builder.create(this, "GenerateThumbnailHandler")
            .functionName("${PREFIX}generate-thumbnail-handler")
            .runtime(Runtime.PYTHON_3_11)
            .handler("generate_thumbnail.handler")
            .code(Code.fromAsset("lambdas/video_processing"))
            .environment(
                mapOf(
                    "JOB_TABLE_NAME" to jobsTable.tableName
                )
            )
            .layers(mutableListOf(utilsLambdaLayer))
            .build()
        cleanupLambda = Function.Builder.create(this, "CleanupHandler")
            .functionName("${PREFIX}cleanup-handler")
            .runtime(Runtime.PYTHON_3_11)
            .handler("cleanup.handler")
            .code(Code.fromAsset("lambdas/video_processing"))
            .environment(
                mapOf(
                    "JOB_TABLE_NAME" to jobsTable.tableName
                )
            )
            .layers(mutableListOf(utilsLambdaLayer))
            .build()
        videoProcessingStateMachine = generateVideoProcessingSateMachine()
        restApi = RestApi.Builder.create(this, "RestAPI")
            .restApiName("${PREFIX}rest-api")
            .build()


        preprocessingQueue = Queue.Builder.create(this, "PreprocessingQueue")
            .queueName("${PREFIX}preprocessing-queue")
            .build()
    }

    /**
     * WIP: Generates the reducer step-functions state machine.
     */
    private fun generateVideoProcessingSateMachine(): StateMachine {
        val handleError = Pass.Builder.create(this, "HandleError").build()
        val reduceChunksTask = LambdaInvoke.Builder.create(this, "ReduceTask")
            .lambdaFunction(reduceChunksLambda)
            .outputPath("$.Payload")
            .build()
        val thumbnailGenerationTask = LambdaInvoke.Builder.create(this, "ThumbnailGenerationTask")
            .lambdaFunction(generateThumbnailLambda)
            .outputPath("$.Payload")
            .build()
        val postProcessingParallel = Parallel.Builder.create(this, "PostProcessingParallel")
            .build()
            .branch(reduceChunksTask)
            .branch(thumbnailGenerationTask)
        val processChunkTask = LambdaInvoke.Builder.create(this, "ProcessChunkTask")
            .lambdaFunction(processChunkLambda)
            .outputPath("$.Payload")
            .build()
        val chunkMap = Map.Builder.create(this, "ChunkMap")
            .itemsPath("$.chunks")
            .resultPath("$.mapOutput")
            .build()
            .iterator(processChunkTask)
            .addCatch(handleError)
            .next(postProcessingParallel)
        val preprocessingTask = LambdaInvoke.Builder.create(this, "PreprocessingTask")
            .lambdaFunction(preprocessLambda)
            .outputPath("$.Payload")
            .build()
            .addCatch(handleError)
            .next(chunkMap)
        val extractDataTask = LambdaInvoke.Builder.create(this, "ExtractDataTask")
            .lambdaFunction(extractDataLambda)
            .outputPath("$.Payload")
            .build()
        val cleanupTask = LambdaInvoke.Builder.create(this, "CleanupTask")
            .lambdaFunction(cleanupLambda)
            .outputPath("$.Payload")
            .build()
        val notifyClient = Pass.Builder.create(this, "NotifyClient").build().next(cleanupTask)
        val processingParallel = Parallel.Builder.create(this, "ProcessingParallel")
            .build()
            .branch(extractDataTask)
            .branch(preprocessingTask)
            .next(notifyClient)
        return StateMachine.Builder.create(this, "VideoProcessingStateMachine")
            .definitionBody(DefinitionBody.fromChainable(processingParallel))
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
        jobsBucket.grantReadWrite(preprocessLambda)
        jobsBucket.grantReadWrite(processChunkLambda)
        jobsTable.grantWriteData(postJobLambda)
        jobsTable.grantReadWriteData(preprocessLambda)
        preprocessingQueue.grantConsumeMessages(preprocessLambda)
    }


    companion object {
        /**
         * Prefix of all resource names.
         */
        private const val PREFIX = "thetatrim-"
    }
}