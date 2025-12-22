import boto3
import os
from dotenv import dotenv_values, load_dotenv
from .utils import batched


def load_secrets(path, override=False):
    """
    Load secrets from AWS Parameter Store based on a .env file containing ARNs.

    Duplicate env var names are overwritten by the later name in the env file.

    aws region relies on the default configuration of the AWS client.

    No error is raised if <path> does not exist.

    Args:
        path (str): Path to the .env file containing ARNs.
        override (bool): Whether to override existing environment variables. Defaults to False.

    Returns:
        dict: A dictionary of secrets where keys are the environment variable names
              and values are the retrieved secrets.

    Raises:
        ValueError: If any parameters are invalid.
    """
    # Load the .env file
    env_vars = dotenv_values(path)
    # Create a mapping of ARN to environment variable name (for selecting return value)
    # ignore variables that have an empty value
    arn_to_envvar = {v: k for k, v in env_vars.items() if v}
    # early return if .env is empty
    if not arn_to_envvar:
        return {}

    ssm = boto3.client("ssm")
    secrets = {}

    # get-parameters API method can only handle 10 params at a time
    for batch in batched(arn_to_envvar.items(), 10):
        batch = dict(batch)
        response = ssm.get_parameters(Names=list(batch.keys()), WithDecryption=True)

        # Error for unfetched params
        invalid_params = response.get("InvalidParameters", [])
        if invalid_params:
            raise ValueError(
                f"The following names could not be retrieved from SSM Parameter Store: {invalid_params}"
            )

        # Map retrieved values back to environment variable names
        for param in response.get("Parameters", []):
            arn = param["ARN"]
            env_name = arn_to_envvar[arn]
            secrets[env_name] = param["Value"]

    # Load fetched secrets into env
    for key, value in secrets.items():
        if override or (key not in os.environ):
            os.environ[key] = value

    return secrets


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
