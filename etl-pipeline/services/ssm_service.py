import os
import boto3

from logger import create_log

logger = create_log(__name__)


class SSMService:
    def __init__(self, environment: str = None):
        self.ssm_client = boto3.client(
            "ssm",
            region_name=os.environ.get("AWS_REGION", None),
        )

        self.environment = environment or os.environ.get("ENVIRONMENT")
        if not self.environment:
            raise ValueError(
                "Environment must be provided either as a parameter or via ENVIRONMENT environment variable"
            )

    def get_parameter(
        self, parameter_name: str, raise_on_error: bool = False
    ) -> dict | None:
        full_parameter_name = f"drb/{self.environment}/{parameter_name}"
        try:
            response = self.ssm_client.get_parameter(
                Name=f"arn:aws:ssm:us-east-1:946183545209:parameter/{full_parameter_name}",
                WithDecryption=True,
            )
            return response["Parameter"]["Value"]

        except Exception as err:
            logger.exception(
                f"Parameter store retrieval for '{full_parameter_name}' failed"
            )
            if raise_on_error:
                raise
            return None
