#!/usr/bin/env python3
"""
Reusable module for copying SSM parameters between environments.
"""

import boto3
from typing import Dict, Optional
from logger import create_log

logger = create_log(__name__)


def copy_ssm_parameters_between_envs(
    env_var_to_ssm_name: Dict[str, str],
    source_env: str,
    target_env: str,
    path_prefix: str = "/drb",
    aws_region: str = "us-east-1",
    aws_account_id: Optional[str] = None,
    overwrite: bool = True,
) -> Dict[str, any]:
    """
    Copy SSM parameters from one environment to another.

    Args:
        env_var_to_ssm_name: Dictionary mapping environment variable names to SSM parameter names
        source_env: Source environment name (e.g., 'production', 'qa')
        target_env: Target environment name (e.g., 'vra', 'staging')
        path_prefix: SSM parameter path prefix (default: '/drb')
        aws_region: AWS region (default: 'us-east-1')
        aws_account_id: AWS account ID (optional, only needed if using ARN format)
        overwrite: Whether to overwrite existing parameters in target environment

    Returns:
        Dictionary containing:
            - 'success_count': Number of successfully copied parameters
            - 'failure_count': Number of failed copy operations
            - 'total': Total number of parameters attempted
            - 'failed_params': List of parameter names that failed to copy
    """
    ssm_client = boto3.client("ssm", region_name=aws_region)

    success_count = 0
    failure_count = 0
    failed_params = []
    successful_targets = []

    for env_var, param_name in env_var_to_ssm_name.items():
        source_param_path = f"{path_prefix}/{source_env}/{param_name}"
        target_param_path = f"{path_prefix}/{target_env}/{param_name}"

        # If account ID is provided, use ARN format, otherwise use path format
        if aws_account_id:
            source_name = f"arn:aws:ssm:{aws_region}:{aws_account_id}:parameter{source_param_path}"
        else:
            source_name = source_param_path

        try:
            # Read from source environment
            logger.info(f"Reading parameter: {source_param_path}")
            response = ssm_client.get_parameter(Name=source_name, WithDecryption=True)

            param_value = response["Parameter"]["Value"]
            param_type = response["Parameter"]["Type"]

            # Write to target environment
            logger.info(f"Writing parameter: {target_param_path}")
            ssm_client.put_parameter(
                Name=target_param_path,
                Value=param_value,
                Type=param_type,
                Overwrite=overwrite,
                Description=f"Copied from {source_param_path}",
            )

            logger.info(
                f"✓ Successfully copied {env_var}: {source_param_path} → {target_param_path}"
            )
            success_count += 1
            successful_targets.append(target_param_path)

        except ssm_client.exceptions.ParameterNotFound:
            logger.warning(f"✗ Parameter not found in source: {source_param_path}")
            failure_count += 1
            failed_params.append(env_var)

        except ssm_client.exceptions.ParameterAlreadyExists:
            logger.error(
                f"✗ Parameter already exists in target and overwrite=False: {target_param_path}"
            )
            failure_count += 1
            failed_params.append(env_var)

        except Exception as err:
            logger.error(f"✗ Failed to copy {env_var} ({source_param_path}): {err}")
            failure_count += 1
            failed_params.append(env_var)

    # Return summary
    result = {
        "success_count": success_count,
        "failure_count": failure_count,
        "total": len(env_var_to_ssm_name),
        "failed_params": failed_params,
        "successful_targets": successful_targets,
    }

    return result


def print_copy_summary(result: Dict[str, any]):
    """
    Print a formatted summary of the copy operation.

    Args:
        result: Dictionary returned from copy_ssm_parameters_between_envs
    """
    print("\n" + "=" * 60)
    print(f"Copy Summary:")
    print(f"  Total parameters: {result['total']}")
    print(f"  Successfully copied: {result['success_count']}")
    print(f"  Failed: {result['failure_count']}")

    if result.get("successful_targets"):
        print(f"\n  Successfully written target parameters:")
        for target in result["successful_targets"]:
            print(f"    - {target}")

    if result["failed_params"]:
        print(f"\n  Failed parameters:")
        for param in result["failed_params"]:
            print(f"    - {param}")

    print("=" * 60)


if __name__ == "__main__":
    # Example usage
    from load_env import ENV_VAR_TO_SSM_NAME

    AWS_REGION = "us-east-1"
    AWS_ACCOUNT_ID = "946183545209"

    # complete copy prod to vra
    # SOURCE_ENV = "production"
    # TARGET_ENV = "vra"
    # env_var_ssm_mapper = ENV_VAR_TO_SSM_NAME

    # load local with select QA # COMPLETE!
    SOURCE_ENV = "qa"
    TARGET_ENV = "local"
    env_var_ssm_mapper = {
        "NYPL_API_CLIENT_ID": "nypl-api/client-id",
        "NYPL_API_CLIENT_PUBLIC_KEY": "nypl-api/public-key",
        "NYPL_API_CLIENT_SECRET": "nypl-api/client-secret",
        "NYPL_BIB_PSWD": "postgres/nypl-pswd",
        "NYPL_BIB_USER": "postgres/nypl-user",
        "OCLC_METADATA_ID": "oclc-metadata-clientid",
        "OCLC_METADATA_SECRET": "oclc-metadata-secret",
        "OCLC_CLIENT_ID": "oclc-search-clientid",
        "OCLC_CLIENT_SECRET": "oclc-search-secret",
    }

    print(
        f"Copying SSM parameters from '{SOURCE_ENV}' to '{TARGET_ENV}' environment..."
    )
    print(f"Total parameters to copy: {len(env_var_ssm_mapper)}\n")

    result = copy_ssm_parameters_between_envs(
        env_var_to_ssm_name=env_var_ssm_mapper,
        source_env=SOURCE_ENV,
        target_env=TARGET_ENV,
        aws_region=AWS_REGION,
        aws_account_id=AWS_ACCOUNT_ID,
        overwrite=True,
    )

    print_copy_summary(result)
