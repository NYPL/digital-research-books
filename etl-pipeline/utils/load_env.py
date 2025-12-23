import boto3
import os
from dotenv import dotenv_values, load_dotenv
from .utils import batched


def load_secrets(path, override=False):
    """
    Load secrets from AWS Parameter Store based on a .env file containing
    AWS Parameter Store parameters.

    .env File:
    - Env var values should be either parameter full ARNs or simply parameter
      names. If parameter names only are used AWS account and region are
      inferred from the default config of the AWS client. If full ARN is used,
      AWS region relies on the default configuration of the AWS client.
    - Duplicate env var names are overwritten by the later name in the env file.

    No error is raised if <path> does not exist.

    Args:
        path (str): Path to the .env file containing ARNs.
        override (bool): Whether to override existing environment variables.
            Defaults to False.

    Raises:
        ValueError: If any parameters are invalid.
    """
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

    ssm = boto3.client("ssm")
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


def load_env(env_path, secrets_path=None, override=False):
    """
    Load environment variables and secrets values in from .env file(s).

    Secrets are loaded from AWS Parameter Store using ARNs configured as the values in secrets_path.

    - Duplicate env var names in each .env file are overwritten by the later name.
    - No error is raised if the .env files do not exist.
    - For secrets, AWS region relies on the default configuration of the AWS client.

    Args:
        env_path (str): Path to the standard .env file.
        secrets_path (str, optional): Path to the .env file containing ARNs for secrets.
                                      Defaults to `env_path` with a '.secrets' suffix.
        override (bool): Whether to override existing environment variables. Defaults to False.
                         This applies to both standard environment variables and secrets.
    """
    # MAYBE: raise error if paths do not exist

    if secrets_path is None:
        secrets_path = env_path + ".secrets"

    load_dotenv(env_path, override=override)

    load_secrets(secrets_path, override=override)
