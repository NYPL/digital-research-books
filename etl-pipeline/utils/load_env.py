import os
from collections.abc import Iterable
from pathlib import Path

import boto3
from dotenv import dotenv_values, load_dotenv

from .common import batched

# NOTE: print() is used instead of a logger bc the environment is expected to \
# be loaded before loggers are configured in an application.

# NOTE: aws region must be explicitly set when authorizing from the deployed \
# ECS environment's credentials. Region is set in ENV files but this code is
# executed before env files are loaded.
AWS_REGION = "us-east-1"


def parameter_values(
    arns: Iterable[str],
    region: str = AWS_REGION,
) -> dict[str, str]:
    """
    Fetch decrypted values from SSM Parameter Store.

    Args:
        arns: Parameter ARNs or names (starting with '/') to fetch.
        region: AWS region for the SSM client. Defaults to AWS_REGION.

    Returns:
        A dict mapping each parameter's canonical ARN to its decrypted value.

    Raises:
        ValueError: If any parameters cannot be fetched.
    """
    # NOTE: does not deduplicate if the same param is specified by both arn and parameter name
    unique = set(arns)
    if not unique:
        return {}

    ssm = boto3.client("ssm", region_name=region)
    result: dict[str, str] = {}

    # get-parameters API method only handles 10 params at a time (and does not support pagination)
    for batch in batched(unique, 10):
        response = ssm.get_parameters(Names=list(batch), WithDecryption=True)
        invalid = response.get("InvalidParameters", [])
        if invalid:
            raise ValueError(
                f"The following names could not be retrieved from SSM Parameter Store: {invalid}"
            )
        result.update({p["ARN"]: p["Value"] for p in response["Parameters"]})

    return result


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

    unique_arns = {arn for arn, _ in arn_to_key_pairs}
    arn_to_value = parameter_values(unique_arns, region=region)

    # Map param values back to environment variable names (via ARN)
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


def find_duplicate_parameter_values(
    prefix: str,
    region: str = AWS_REGION,
) -> list[list[str]]:
    """
    Identify SSM Parameter Store parameters that share the same value under a path prefix.
    Useful for cleaning up parameters.

    Fetches all parameters recursively under `prefix` from Parameter Store and groups
    them by value.

    Args:
        prefix: A path hierarchy prefix (e.g. '/myapp/prod/'). Must be a valid SSM
                parameter path hierarchy — it must start with '/' and use '/' as a
                delimiter between levels. Arbitrary string prefixes (e.g. '/myapp/prod-')
                are NOT supported; only path-boundary prefixes work with this API.
        region: AWS region for the SSM client. Defaults to AWS_REGION.

    Returns:
        A list of ARN groups, where each group is a list of parameter ARNs that share
        the same value. Only groups with 2 or more ARNs are included.
    """
    ssm = boto3.client("ssm", region_name=region)
    paginator = ssm.get_paginator("get_parameters_by_path")

    arn_to_value: dict[str, str] = {}
    for page in paginator.paginate(Path=prefix, Recursive=True, WithDecryption=True):
        for param in page["Parameters"]:
            arn_to_value[param["ARN"]] = param["Value"]

    value_to_arns: dict[str, list[str]] = {}
    for arn, value in arn_to_value.items():
        value_to_arns.setdefault(value, []).append(arn)

    return [arns for arns in value_to_arns.values() if len(arns) > 1]
