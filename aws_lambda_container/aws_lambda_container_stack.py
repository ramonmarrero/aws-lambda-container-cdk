from aws_cdk import (
    aws_lambda as _lambda,
    core
)
# For consistency with other languages, `cdk` is the preferred import name for
# the CDK's core module.  The following line also imports it as `core` for use
# with examples from the CDK Developer's Guide, which are in the process of
# being updated to use `cdk`.  You may delete this import if you don't need it.

class AwsLambdaContainerStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here
        function = _lambda.DockerImageFunction(self, "lambda_function",
                                    code=_lambda.DockerImageCode.from_image_asset("./assets"))