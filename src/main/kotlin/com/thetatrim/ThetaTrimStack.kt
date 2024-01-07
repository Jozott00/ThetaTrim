import software.amazon.awscdk.Stack
import software.amazon.awscdk.StackProps
import software.amazon.awscdk.services.apigateway.LambdaIntegration
import software.amazon.awscdk.services.apigateway.RestApi
import software.amazon.awscdk.services.lambda.Code
import software.amazon.awscdk.services.lambda.Function
import software.amazon.awscdk.services.lambda.Runtime
import software.amazon.awscdk.services.s3.Bucket
import software.constructs.Construct
import java.util.Map

class ThetaTrimStack(scope: Construct?, id: String?, props: StackProps?) :
    Stack(scope, id, props) {
    /**
     * S3 Bucket for storing job objects such as videos and frames.
     */
    private lateinit var jobObjectBucket: Bucket

    /**
     * Lambda function for creating jobs and presigned urls.
     */
    private lateinit var postJobHandler: Function

    /**
     * Rest API for handling different endpoints.
     */
    private lateinit var restApi: RestApi


    init {
        setupResources()
        grantPermissions()
        configureEndpoints()
    }

    /**
     * Initializes all resources of the stack.
     */
    private fun setupResources() {
        jobObjectBucket = Bucket.Builder.create(this, "JobObjectBucket")
            .bucketName("job-object-bucket")
            .versioned(true)
            .build()
        postJobHandler = Function.Builder.create(this, "PostJobHandler")
            .runtime(Runtime.PYTHON_3_12)
            .handler("post_job.handler")
            .code(Code.fromAsset("lambdas/rest"))
            .environment(
                Map.of(
                    "OBJECT_BUCKET_NAME", jobObjectBucket.bucketName
                )
            )
            .build()
        restApi = RestApi.Builder.create(this, "RestAPI")
            .restApiName("com/thetatrim")
            .build()
    }

    /**
     * Configures and adds all endpoints to the Rest API.
     */
    private fun configureEndpoints() {
        val jobsResource = restApi.root.addResource("jobs")
        val postJobIntegration = LambdaIntegration.Builder.create(postJobHandler).build()
        jobsResource.addMethod("POST", postJobIntegration)
    }

    /**
     * Grants all necessary for interaction between services.
     */
    private fun grantPermissions() {
        jobObjectBucket.grantReadWrite(postJobHandler)
    }
}