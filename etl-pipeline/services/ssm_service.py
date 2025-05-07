import os
import boto3
from typing import Optional

from logger import create_log

logger = create_log(__name__)


class SSMService:
    def __init__(self):
        self.ssm_client = boto3.client(
            "ssm",
            aws_access_key_id=os.environ.get("AWS_ACCESS", None),
            aws_secret_access_key=os.environ.get("AWS_SECRET", None),
            region_name=os.environ.get("AWS_REGION", None),
        )

        self.environment = (
            "production"
            if os.environ.get("ENVIRONMENT", "qa") == "production"
            else "qa"
        )

    def get_parameter(self, parameter_name: str) -> Optional[dict]:
        try:
            response = self.ssm_client.get_parameter(
                Name=f"arn:aws:ssm:us-east-1:946183545209:parameter/drb/{self.environment}/{parameter_name}",
                WithDecryption=True,
            )
            return response["Parameter"]["Value"]

        except Exception as err:
            logger.exception(f"Parameter store retrieval for {parameter_name} failed")
            return None
