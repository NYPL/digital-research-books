import os
from pathlib import Path

import boto3
from dotenv import dotenv_values, load_dotenv

from .common import batched

# NOTE: print() is used instead of a logger bc the environment is expected to \
# be loaded before loggers are configured in an application.

# NOTE: aws region must be explicitly set when authorizing from the deployed \
# ECS environment's credentials.
AWS_REGION = "us-east-1"


def load_secrets(path, raise_if_no_file=False, override=False, region=AWS_REGION):
    """
    Load secrets from AWS Parameter Store based on a .env file containing
    AWS Parameter Store parameters.

    .env File:
    - Env var values should be either full parameter ARNs or simply parameter
      names (starting with `/`). If parameter names only are used AWS account and region are
      inferred from the default config of the AWS client. If full ARN is used,
      AWS region relies on the default configuration of the AWS client.
    - Duplicate env var names are overwritten by the later name in the env file.

    Args:
        path (str): Path to the .env file containing ARNs.
        raise_if_no_file (bool): If True, raises FileNotFoundError when the file
            does not exist. Defaults to False.
        override (bool): Whether to override existing environment variables.
            Defaults to False.
        region (str): AWS region for the SSM client. Defaults to AWS_REGION.

    Raises:
        FileNotFoundError: If raise_if_no_file is True and the file does not exist.
        ValueError: If any parameters cannot be fetched.
    """

    if not Path(path).exists():
        msg = f"Secrets file '{path}' does not exist"
        if raise_if_no_file:
            raise FileNotFoundError(msg)
        else:
            print(msg)

    # Load the .env file
    env_vars = dotenv_values(path)
    # Map ARN to environment variable name (for map retrieved values)
    # NOTE: list of tup instead of dict allows the same ARN for multiple env vars
    # ignore variables already in env if override=False
    # ignore variables that have an empty value
    arn_to_key_pairs = [
        (v, k) for k, v in env_vars.items() if (override or (k not in os.environ)) and v
    ]
    # early return if nothing to fetch
    if not arn_to_key_pairs:
        return

    ssm = boto3.client("ssm", region_name=region)
    arn_to_value = {}

    unique_arns = set(arn for arn, _ in arn_to_key_pairs)
    # get-parameters API method only handles 10 params at a time (and does not support pagination)
    for batch in batched(unique_arns, 10):
        # Retrieve param values
        response = ssm.get_parameters(Names=batch, WithDecryption=True)

        # Error for unfetched params
        invalid_params = response.get("InvalidParameters", [])
        if invalid_params:
            raise ValueError(
                f"The following names could not be retrieved from SSM Parameter Store: {invalid_params}"
            )

        # Map param values back to environment variable names (via ARN)
        arn_to_value.update(
            {param["ARN"]: param["Value"] for param in response.get("Parameters", [])}
        )

    secrets = {name: arn_to_value[arn] for arn, name in arn_to_key_pairs}
    # ALT: if we switch to not erroring for invalid params in future, check if ARN is in `arn_to_value`

    # Load fetched secrets into env (overrides existing env vars)
    os.environ.update(secrets)


def load_env(
    env_path: str | os.PathLike,
    secrets_path: str | os.PathLike | None = None,
    override: bool = False,
    raise_if_no_file=False,
    region=AWS_REGION,
):
    """
    Load environment variables and secrets values in from .env file(s).

    Secrets are loaded from AWS Parameter Store using ARNs configured as the values in secrets_path.

    - Duplicate env var names in each .env file are overwritten by the later name.
    - For secrets, AWS region relies on the default configuration of the AWS client.

    Args:
        env_path (str): Path to the standard .env file.
        secrets_path (str, optional): Path to the .env file containing ARNs for secrets.
                                      Defaults to `env_path` with a '.secrets' suffix.
        override (bool): Whether to override existing environment variables. Defaults to False.
                         This applies to both standard environment variables and secrets.
        raise_if_no_file (bool): If True, raises FileNotFoundError when either file
            does not exist. Defaults to False.
        region (str): AWS region for the SSM client. Defaults to AWS_REGION.
    Raises:
        FileNotFoundError: If raise_if_no_file is True and either file does not exist.
    """

    if secrets_path is None:
        secrets_path = str(env_path) + ".secrets"

    print(f"Reading env at '{env_path}'")
    if not Path(env_path).exists():
        msg = f"Env file '{env_path}' does not exist"
        if raise_if_no_file:
            raise FileNotFoundError(msg)
        else:
            print(msg)
    load_dotenv(env_path, override=override)

    print(f"Reading env at '{secrets_path}'")
    load_secrets(
        secrets_path,
        override=override,
        raise_if_no_file=raise_if_no_file,
        region=region,
    )
