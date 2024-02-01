package com.thetatrim

import com.amazonaws.services.servicequotas.AWSServiceQuotasClient
import com.amazonaws.services.servicequotas.model.GetServiceQuotaRequest
import software.amazon.awscdk.AssetOptions
import software.amazon.awscdk.Duration
import software.amazon.awscdk.Stack
import software.amazon.awscdk.StackProps
import software.amazon.awscdk.aws_apigatewayv2_integrations.WebSocketLambdaIntegration
import software.amazon.awscdk.services.apigateway.LambdaIntegration
import software.amazon.awscdk.services.apigateway.RestApi
import software.amazon.awscdk.services.apigatewayv2.WebSocketApi
import software.amazon.awscdk.services.apigatewayv2.WebSocketRouteOptions
import software.amazon.awscdk.services.apigatewayv2.WebSocketStage
import software.amazon.awscdk.services.dynamodb.Attribute
import software.amazon.awscdk.services.dynamodb.AttributeType
import software.amazon.awscdk.services.dynamodb.Table
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.LayerVersion
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.lambda.python.alpha.PythonLayerVersion
import software.amazon.awscdk.services.logs.LogGroup
import software.amazon.awscdk.services.s3.Bucket
import software.amazon.awscdk.services.s3.EventType
import software.amazon.awscdk.services.s3.NotificationKeyFilter
import software.amazon.awscdk.services.s3.notifications.LambdaDestination
import software.amazon.awscdk.services.stepfunctions.*
import software.amazon.awscdk.services.stepfunctions.Map
import software.amazon.awscdk.services.stepfunctions.tasks.LambdaInvoke
import software.constructs.Construct
import java.util.*
import kotlin.collections.hashMapOf
import kotlin.collections.joinToString
import kotlin.collections.mutableListOf

/** Converts a String from snake_case to PascalCase. */
fun String.snakeToPascalCase(): String = split('_').joinToString("") {
    it.replaceFirstChar {
        if (it.isLowerCase()) it.titlecase() else it.toString()
    }
}

/** Converts a String from snake_case to kebab-case. */
fun String.snakeToKebabCase(): String = replace('_', '-').lowercase()

class ThetaTrimStack @JvmOverloads constructor(val scope: Construct?, id: String?, props: StackProps? = null) :
    Stack(scope, id, props) {
    private var environmentMap = hashMapOf<String, String>()

    /**
     * S3 Bucket for storing job objects such as videos and frames.
     */
    private lateinit var jobsBucket: Bucket

    /**
     * Lambda function for triggering the video processing state-machine.
     */
    private lateinit var triggerVideoProcessingLambda: Function

    /**
     * Lambda function for creating jobs and presigned urls.
     */
    private lateinit var postJobLambda: Function

    /**
     * Lambda function to obtain initial video information
     */
    private lateinit var jobProbeLambda: Function

    /**
     * Lambda function for preprocessing uploaded videos.
     */
    private lateinit var preprocessLambda: Function

    /**
     * Lambda function for processing a video chunk.
     */
    private lateinit var processChunkLambda: Function

    /**
     * Lambda function for extracting meta-data from the original video.
     */
    private lateinit var extractMetadataLambda: Function

    /**
     * Lambda function for extracting the audio from the original video.
     */
    private lateinit var extractAudioLambda: Function

    /**
     * Lambda function to check the jobs status and whether all chunks are processed.
     */
    private lateinit var reduceChunksLambda: Function

    /**
     * Lambda function for extracting the content-labels of a video chunk.
     */
    private lateinit var extractLabelsLambda: Function

    /**
     * Lambda function to generate a thumbnail.
     */
    private lateinit var generateThumbnailLambda: Function

    /**
     * Lambda function for terminating the processing flow.
     */
    private lateinit var terminateLambda: Function

    /**
     * Lambda function to cleanup all resources after the job is done.
     */
    private lateinit var cleanupLambda: Function

    /** Lambda function to handle a websocket connection. */
    private lateinit var connectWsLambda: Function

    /** Lambda function to handle a websocket disconnect. */
    private lateinit var disconnectWsLambda: Function

    /**
     * Sate-machine for the reducer workflow.
     */
    private lateinit var videoProcessingStateMachine: StateMachine

    /**
     * Rest API for handling different endpoints.
     */
    private lateinit var restApi: RestApi

    /** Websocket API for pushed-based notifications. */
    private lateinit var websocketApi: WebSocketApi
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

        jobsTable = Table.Builder.create(this, "JobsTable")
            .partitionKey(
                Attribute.builder().name("PK").type(AttributeType.STRING).build()
            )
            .sortKey(
                Attribute.builder().name("SK").type(AttributeType.STRING).build()
            )
            .build()

        jobsBucket = Bucket.Builder.create(this, "JobObjectBucket")
            .bucketName("${PREFIX}job-object-bucket-${this.account}") // account suffix to avoid name conflicts
            .versioned(true)
            .build()

        environmentMap.put("OBJECT_BUCKET_NAME", jobsBucket.bucketName)

        environmentMap.put("JOB_TABLE_NAME", jobsTable.tableName)

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

        connectWsLambda = lambdaBuilderFactory("lambdas/ws/connect_ws")
            .timeout(Duration.seconds(10))
            .build()

        disconnectWsLambda = lambdaBuilderFactory("lambdas/ws/disconnect_ws")
            .timeout(Duration.seconds(10))
            .build()

        websocketApi = WebSocketApi.Builder.create(this, "WebSocketApi")
            .apiName("${PREFIX}websocket-api")
            .connectRouteOptions(
                WebSocketRouteOptions.Builder().integration(
                    WebSocketLambdaIntegration("ConnectIntegration", connectWsLambda)
                ).build()
            )
            .disconnectRouteOptions(
                WebSocketRouteOptions.Builder().integration(
                    WebSocketLambdaIntegration("DisconnectIntegration", disconnectWsLambda)
                ).build()
            )
            .build()

        val webSocketStage = WebSocketStage.Builder.create(this, "WebSocketApiDevStage")
            .webSocketApi(websocketApi)
            .stageName("prod")
            .autoDeploy(true)
            .build()

        environmentMap.put("WS_URL", webSocketStage.callbackUrl)

        postJobLambda = lambdaBuilderFactory("lambdas/rest/post_job")
            .timeout(Duration.seconds(60))
            .build()

        jobProbeLambda = lambdaBuilderFactory("lambdas/job_probe/job_probe")
            .timeout(Duration.seconds(10))
            .memorySize(2048)
            .build()

        preprocessLambda = lambdaBuilderFactory("lambdas/video_processing/preprocess")
            .timeout(Duration.minutes(2))
            .memorySize(2048)
//            .ephemeralStorageSize(Size.gibibytes(1))
            .build()

        processChunkLambda = lambdaBuilderFactory("lambdas/video_processing/process_chunk")
            .timeout(Duration.seconds(120))
            .memorySize(2048)
//            .ephemeralStorageSize(Size.gibibytes(1))
            .build()

        extractMetadataLambda = lambdaBuilderFactory("lambdas/video_processing/extract_metadata")
            .timeout(Duration.seconds(60))
            .build()

        extractAudioLambda = lambdaBuilderFactory("lambdas/video_processing/extract_audio")
            .timeout(Duration.seconds(60))
            .build()

        reduceChunksLambda = lambdaBuilderFactory("lambdas/video_processing/reduce_chunks")
            .timeout(Duration.seconds(60))
            .memorySize(2048)
//            .memorySize(1024)
//            .ephemeralStorageSize(Size.gibibytes(1))
            .build()

        extractLabelsLambda = lambdaBuilderFactory("lambdas/video_processing/extract_labels")
            .timeout(Duration.seconds(60))
            .build()

        generateThumbnailLambda = lambdaBuilderFactory("lambdas/video_processing/generate_thumbnail")
                .functionName("${PREFIX}generate-thumbnail-handler")
                .timeout(Duration.seconds(60))
                .runtime(Runtime.PYTHON_3_11)
                .handler("generate_thumbnail.handler")
                .code(Code.fromAsset("lambdas/video_processing"))
                .environment(
                        mapOf(
                                "OBJECT_BUCKET_NAME" to jobsBucket.bucketName,
                                "JOB_TABLE_NAME" to jobsTable.tableName
                        )
                )
                .layers(mutableListOf(utilsLambdaLayer, ffmpegLambdaLayer))
                .build()

        terminateLambda = lambdaBuilderFactory("lambdas/video_processing/terminate")
            .timeout(Duration.seconds(60))
            .build()

        cleanupLambda = lambdaBuilderFactory("lambdas/video_processing/cleanup")
            .timeout(Duration.seconds(60))
            .build()

        videoProcessingStateMachine = generateVideoProcessingSateMachine()

        environmentMap.put("STATE_MACHINE_ARN", videoProcessingStateMachine.stateMachineArn)

        triggerVideoProcessingLambda = lambdaBuilderFactory("lambdas/triggers/trigger_video_processing")
            .timeout(Duration.seconds(60))
            .build()


        restApi = RestApi.Builder.create(this, "RestAPI")
            .restApiName("${PREFIX}rest-api")
            .build()
    }

    /**
     * WIP: Generates the reducer step-functions state machine.
     */
    private fun generateVideoProcessingSateMachine(): StateMachine {
        val pass = Pass.Builder.create(this, "Pass").build()
        val cleanupTask = LambdaInvoke.Builder.create(this, "CleanupTask")
            .lambdaFunction(cleanupLambda)
            .outputPath("$.Payload")
            .build()
        val notifySuccess = LambdaInvoke.Builder.create(this, "HandleSuccessTask")
            .lambdaFunction(terminateLambda)
            .outputPath("$.Payload")
            .build()
            .addCatch(pass, CatchProps.builder().resultPath("$.error").build())
        val handleProcessingErrorTask = LambdaInvoke.Builder.create(this, "HandleProcessingErrorTask")
            .lambdaFunction(terminateLambda)
            .outputPath("$.Payload")
            .build()
            .addCatch(cleanupTask, CatchProps.builder().resultPath("$.error").build())
            .next(cleanupTask)
        val handleReduceErrorTask = LambdaInvoke.Builder.create(this, "HandleReduceErrorTask")
            .lambdaFunction(terminateLambda)
            .outputPath("$.Payload")
            .build()
            .addCatch(pass, CatchProps.builder().resultPath("$.error").build())
        val thumbnailGenerationTask = LambdaInvoke.Builder.create(this, "ThumbnailGenerationTask")
            .lambdaFunction(generateThumbnailLambda)
            .outputPath("$.Payload")
            .build()
        val extractLabelsTask = LambdaInvoke.Builder.create(this, "ExtractLabelsTask")
            .lambdaFunction(extractLabelsLambda)
            .outputPath("$.Payload")
            .build()
            .next(thumbnailGenerationTask)
        val reduceChunksTask = LambdaInvoke.Builder.create(this, "ReduceChunksTask")
            .lambdaFunction(reduceChunksLambda)
            .outputPath("$.Payload")
            .build()
            .addCatch(handleReduceErrorTask, CatchProps.builder().resultPath("$.error").build())
            .next(notifySuccess)
        val postProcessingParallel = Parallel.Builder.create(this, "PostProcessingParallel")
            .build()
            .branch(reduceChunksTask)
            .branch(extractLabelsTask)
            .addCatch(cleanupTask, CatchProps.builder().resultPath("$.error").build())
            .next(cleanupTask)
        val processChunkTask = LambdaInvoke.Builder.create(this, "ProcessChunkTask")
            .lambdaFunction(processChunkLambda)
            .outputPath("$.Payload")
            .build()

        // depending on the account we may use a different maximal concurrency for the map
        val lambdaConcurrencyQuota = findAccountLambdaConcurrencyQuota()
        println("Using map max concurrency of $lambdaConcurrencyQuota")

        val chunkMap = Map.Builder.create(this, "ChunkMap")
            .itemsPath("$.chunks")
            .resultPath("$.processedChunks")
            .maxConcurrency(lambdaConcurrencyQuota)
            .build()
            .iterator(processChunkTask)
            .addCatch(handleProcessingErrorTask, CatchProps.builder().resultPath("$.error").build())
            .next(postProcessingParallel)
        val preprocessingTask = LambdaInvoke.Builder.create(this, "PreprocessingTask")
            .lambdaFunction(preprocessLambda)
            .outputPath("$.Payload")
            .build()
            .addCatch(handleProcessingErrorTask, CatchProps.builder().resultPath("$.error").build())
            .next(chunkMap)
        val extractMetadataTask = LambdaInvoke.Builder.create(this, "ExtractMetadataTask")
            .lambdaFunction(extractMetadataLambda)
            .outputPath("$.Payload")
            .build()

        val extractAudioTask = LambdaInvoke.Builder.create(this, "ExtractAudioTask")
            .lambdaFunction(extractAudioLambda)
            .outputPath("$.Payload")
            .build()
            
        val extractAudioChoice = Choice.Builder.create(this, "ExtractAudioChoice")
            .build()
            .`when`(Condition.booleanEquals("$.extractAudio", true), extractAudioTask)
            .afterwards(AfterwardsOptions.builder().includeOtherwise(true).build())
            .next(Pass.Builder.create(this, "OtherwiseNothing").build())
        val processingParallel = Parallel.Builder.create(this, "ProcessingParallel")
            .build()
            .branch(extractMetadataTask)
            .branch(extractAudioChoice)
            .branch(preprocessingTask)
        val jobProbeTask = LambdaInvoke.Builder.create(this, "JobProbeTask")
            .lambdaFunction(jobProbeLambda)
            .outputPath("$.Payload")
            .build()
            .next(processingParallel)
        val logGroup = LogGroup.Builder.create(this, "VideoProcessingLogGroup")
            .build()
        return StateMachine.Builder.create(this, "VideoProcessingStateMachine")
            .definitionBody(DefinitionBody.fromChainable(jobProbeTask))
            .logs(
                LogOptions.builder().destination(logGroup).level(LogLevel.ALL).build()
            )
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
        videoProcessingStateMachine.grantStartExecution(triggerVideoProcessingLambda)
        jobsBucket.addEventNotification(
            EventType.OBJECT_CREATED,
            LambdaDestination(triggerVideoProcessingLambda),
            NotificationKeyFilter.builder()
                .suffix("original.mp4")
                .build()
        )
    }

    /**
     * Grants all necessary permissions for interaction between services.
     */
    private fun grantPermissions() {
        jobsBucket.grantWrite(postJobLambda)
        jobsBucket.grantReadWrite(jobProbeLambda)
        jobsBucket.grantReadWrite(extractAudioLambda)
        jobsBucket.grantReadWrite(preprocessLambda)
        jobsBucket.grantReadWrite(processChunkLambda)
        jobsBucket.grantReadWrite(reduceChunksLambda)
        jobsBucket.grantReadWrite(cleanupLambda)
        jobsBucket.grantReadWrite(generateThumbnailLambda)
        jobsTable.grantWriteData(postJobLambda)
        jobsTable.grantReadWriteData(jobProbeLambda)
        jobsTable.grantReadWriteData(preprocessLambda)
        jobsTable.grantReadWriteData(processChunkLambda)
        jobsTable.grantReadWriteData(reduceChunksLambda)
        jobsTable.grantReadWriteData(cleanupLambda)
        jobsTable.grantReadWriteData(terminateLambda)
        jobsTable.grantReadWriteData(connectWsLambda)
        jobsTable.grantReadWriteData(disconnectWsLambda)
        websocketApi.grantManageConnections(terminateLambda)
    }

    /**
     * Factory to create a lambda builder with basic configuration.
     * @param fileName The name of the lambda file (in snake case).
     */
    private fun lambdaBuilderFactory(filePath: String): Function.Builder {
        val directoryPath = filePath.substringBeforeLast("/")
        val fileName = filePath.substringAfterLast("/")
        return Function.Builder.create(this, fileName.snakeToPascalCase())
            .functionName("${PREFIX}${fileName.snakeToKebabCase()}")
            .runtime(Runtime.PYTHON_3_11)
            .handler("${fileName}.handler")
            .code(Code.fromAsset(directoryPath))
            .environment(environmentMap)
            .layers(mutableListOf(utilsLambdaLayer, ffmpegLambdaLayer))
    }

    private fun findAccountLambdaConcurrencyQuota(): Double {
        val client = AWSServiceQuotasClient.builder()
            .build()

        val result = client.getServiceQuota(
            GetServiceQuotaRequest()
                .withQuotaCode("L-B99A9384")
                .withServiceCode("lambda")
        )
        return result.quota.value
    }

    companion object {
        /**
         * Prefix of all resource names.
         */
        private const val PREFIX = "thetatrim-"
    }
}