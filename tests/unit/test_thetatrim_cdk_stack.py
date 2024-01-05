import aws_cdk as core
import aws_cdk.assertions as assertions

from thetatrim_cdk.thetatrim_cdk_stack import ThetatrimCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in thetatrim_cdk/thetatrim_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ThetatrimCdkStack(app, "thetatrim-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
