import aws_cdk as core
import aws_cdk.assertions as assertions

from aws_spotify.aws_spotify_stack import AwsSpotifyStack

# example tests. To run these tests, uncomment this file along with the example
# resource in aws_spotify/aws_spotify_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AwsSpotifyStack(app, "aws-spotify")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
