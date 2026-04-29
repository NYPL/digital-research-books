import time
from typing import Optional

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session


def get_boto3_session_with_assumed_role(
    role_arn: str,
    session_name: Optional[str] = None,
    region_name: Optional[str] = None,
    duration_seconds: Optional[int] = None,
    boto_session: Optional[boto3.Session] = None,
) -> boto3.Session:
    """Return a boto3 Session configured with auto-refreshing credentials from
    an assumed-role. The default boto3 session needs to have permissions to
    assume the `role_arn`.

    Args:
        role_arn: ARN of the IAM role to assume.
        session_name: Identifier attached to the STS session ([\w+=,.@-], 2-64 chars).
                      Defaults to "AssumedRoleSession-<uuid4_hex>".
        region_name: AWS region for the STS call and the returned session.
                     Falls back to the environment / config default when None.
        duration_seconds: Lifetime of each set of temporary credentials (900-43200).
                          Omit to use the STS default (3600 s).
        boto_session: boto3 Session to use for the STS AssumeRole call. Defaults
                      to the global default session when None.
    """

    _session_name = session_name or f"AssumedRoleSession-{int(time.time())}"

    _boto_session = boto_session or boto3.Session()

    def _fetch_credentials() -> dict:
        sts_client = _boto_session.client("sts", region_name=region_name)
        assume_role_kwargs = {
            "RoleArn": role_arn,
            "RoleSessionName": _session_name,
        }
        if duration_seconds is not None:
            assume_role_kwargs["DurationSeconds"] = duration_seconds
        response = sts_client.assume_role(**assume_role_kwargs)
        creds = response["Credentials"]
        return {
            "access_key": creds["AccessKeyId"],
            "secret_key": creds["SecretAccessKey"],
            "token": creds["SessionToken"],
            # isoformat() produces an ISO-8601 string botocore can parse
            "expiry_time": creds["Expiration"].isoformat(),
        }

    refreshable_credentials = RefreshableCredentials.create_from_metadata(
        metadata=_fetch_credentials(),
        refresh_using=_fetch_credentials,
        method="sts-assume-role",
    )

    botocore_session = get_session()
    botocore_session._credentials = refreshable_credentials
    if region_name:
        botocore_session.set_config_variable("region", region_name)

    return boto3.Session(botocore_session=botocore_session)
