#!/usr/bin/env python3
"""
Reusable module for copying SSM parameters between environments.
"""

import boto3
from typing import Dict, Optional
from logger import create_log

logger = create_log(__name__)


def copy_parameter(
    ssm_client,
    source_path: str,
    target_path: str,
    overwrite: bool = True,
    aws_region: Optional[str] = None,
    aws_account_id: Optional[str] = None,
) -> Dict[str, any]:
    """
    Copy a single SSM parameter from source path to target path.

    Args:
        ssm_client: Boto3 SSM client instance
        source_path: Full source parameter path (e.g., '/drb/prod/db-host')
        target_path: Full target parameter path (e.g., '/drb/vra/db-host')
        overwrite: Whether to overwrite existing target parameter
        aws_region: AWS region (optional, for ARN format)
        aws_account_id: AWS account ID (optional, for ARN format)

    Returns:
        Dictionary containing:
            - 'success': Boolean indicating if copy succeeded
            - 'source': Source path
            - 'target': Target path
            - 'error': Error message if failed (optional)
    """
    # Build source identifier (ARN or path)
    if aws_account_id and aws_region:
        source_name = (
            f"arn:aws:ssm:{aws_region}:{aws_account_id}:parameter{source_path}"
        )
    else:
        source_name = source_path

    try:
        # Read from source
        logger.info(f"Reading parameter: {source_path}")
        response = ssm_client.get_parameter(Name=source_name, WithDecryption=True)

        param_value = response["Parameter"]["Value"]
        param_type = response["Parameter"]["Type"]

        # Write to target
        logger.info(f"Writing parameter: {target_path}")
        ssm_client.put_parameter(
            Name=target_path,
            Value=param_value,
            Type=param_type,
            Overwrite=overwrite,
            Description=f"Copied from {source_path}",
        )

        logger.info(f"✓ Successfully copied: {source_path} → {target_path}")
        return {"success": True, "source": source_path, "target": target_path}

    except ssm_client.exceptions.ParameterNotFound:
        logger.warning(f"✗ Parameter not found in source: {source_path}")
        return {
            "success": False,
            "source": source_path,
            "target": target_path,
            "error": "ParameterNotFound",
        }

    except ssm_client.exceptions.ParameterAlreadyExists:
        logger.error(
            f"✗ Parameter already exists in target and overwrite=False: {target_path}"
        )
        return {
            "success": False,
            "source": source_path,
            "target": target_path,
            "error": "ParameterAlreadyExists",
        }

    except Exception as err:
        logger.error(f"✗ Failed to copy ({source_path}): {err}")
        return {
            "success": False,
            "source": source_path,
            "target": target_path,
            "error": str(err),
        }


def copy_ssm_parameters_between_envs(
    param_names: list[str],
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
        param_names: List of SSM parameter names to copy (e.g., ['db-host', 'api-key'])
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

    print(
        f"Copying SSM parameters from '{source_env}' to '{target_env}' environment..."
    )
    print(f"Total parameters to copy: {len(param_names)}\n")

    success_count = 0
    failure_count = 0
    failed_params = []
    successful_targets = []

    for param_name in param_names:
        source_param_path = f"{path_prefix}/{source_env}/{param_name}"
        target_param_path = f"{path_prefix}/{target_env}/{param_name}"

        result = copy_parameter(
            ssm_client=ssm_client,
            source_path=source_param_path,
            target_path=target_param_path,
            overwrite=overwrite,
            aws_region=aws_region,
            aws_account_id=aws_account_id,
        )

        if result["success"]:
            success_count += 1
            successful_targets.append(target_param_path)
        else:
            failure_count += 1
            failed_params.append(param_name)

    # Return summary
    result = {
        "success_count": success_count,
        "failure_count": failure_count,
        "total": len(param_names),
        "failed_params": failed_params,
        "successful_targets": successful_targets,
    }

    return result


def copy_parameters_with_path_mapping(
    path_mapping: Dict[str, str],
    aws_region: str = "us-east-1",
    aws_account_id: Optional[str] = None,
    overwrite: bool = True,
    delete_source: bool = False,
) -> Dict[str, any]:
    """
    Copy SSM parameters using a mapping of old path to new path.

    Args:
        path_mapping: Dictionary mapping source paths to target paths {'/old/path': '/new/path'}
        aws_region: AWS region (default: 'us-east-1')
        aws_account_id: AWS account ID (optional, only needed if using ARN format)
        overwrite: Whether to overwrite existing parameters in target
        delete_source: Whether to delete source parameter after successful copy

    Returns:
        Dictionary containing:
            - 'success_count': Number of successfully copied parameters
            - 'failure_count': Number of failed copy operations
            - 'total': Total number of parameters attempted
            - 'failed_params': List of source paths that failed to copy
            - 'successful_targets': List of successfully created target paths
    """
    ssm_client = boto3.client("ssm", region_name=aws_region)

    success_count = 0
    failure_count = 0
    failed_params = []
    successful_targets = []

    for source_path, target_path in path_mapping.items():
        result = copy_parameter(
            ssm_client=ssm_client,
            source_path=source_path,
            target_path=target_path,
            overwrite=overwrite,
            aws_region=aws_region,
            aws_account_id=aws_account_id,
        )

        if result["success"]:
            success_count += 1
            successful_targets.append(target_path)

            # Delete source if requested
            if delete_source:
                try:
                    logger.info(f"Deleting source parameter: {source_path}")
                    ssm_client.delete_parameter(Name=source_path)
                    logger.info(f"✓ Deleted source parameter: {source_path}")
                except Exception as err:
                    logger.error(
                        f"✗ Failed to delete source parameter {source_path}: {err}"
                    )
        else:
            failure_count += 1
            failed_params.append(source_path)

    return {
        "success_count": success_count,
        "failure_count": failure_count,
        "total": len(path_mapping),
        "failed_params": failed_params,
        "successful_targets": successful_targets,
    }


def print_copy_summary(result: Dict[str, any]):
    """
    Print a formatted summary of the copy operation.

    Args:
        result: Dictionary returned from copy_ssm_parameters_between_envs
    """
    print("\n" + "=" * 60)
    print("Copy Summary:")
    print(f"  Total parameters: {result['total']}")
    print(f"  Successfully copied: {result['success_count']}")
    print(f"  Failed: {result['failure_count']}")

    if result.get("successful_targets"):
        print("\n  Successfully written target parameters:")
        for target in result["successful_targets"]:
            print(f"    - {target}")

    if result["failed_params"]:
        print("\n  Failed parameters:")
        for param in result["failed_params"]:
            print(f"    - {param}")

    print("=" * 60)


if __name__ == "__main__":
    AWS_REGION = "us-east-1"
    AWS_ACCOUNT_ID = "946183545209"

    ## complete copy prod to vra
    # SOURCE_ENV = "production"
    # TARGET_ENV = "vra"
    # from load_env import ENV_VAR_TO_SSM_NAME
    # param_names = list(ENV_VAR_TO_SSM_NAME.values())

    ## select qa to local
    # SOURCE_ENV = "qa"
    # TARGET_ENV = "local"
    # param_names = [
    #     "nypl-api/client-id",
    #     "nypl-api/public-key",
    #     "nypl-api/client-secret",
    #     "postgres/nypl-pswd",
    #     "postgres/nypl-user",
    #     "oclc-metadata-clientid",
    #     "oclc-metadata-secret",
    #     "oclc-search-clientid",
    #     "oclc-search-secret",
    # ]

    # vra ES standardize naming convention
    path_mapping = {
        "/drb/vra-elasticsearch/host": "/drb/production/vra-elasticsearch/host",
        "/drb/vra-elasticsearch/port": "/drb/production/vra-elasticsearch/port",
        "/drb/vra/elasticsearch/user": "/drb/production/vra-elasticsearch/user",
        "/drb/vra/elasticsearch/pswd": "/drb/production/vra-elasticsearch/pswd",
    }

    # result = copy_ssm_parameters_between_envs(
    #     param_names=param_names,
    #     source_env=SOURCE_ENV,
    #     target_env=TARGET_ENV,
    #     aws_region=AWS_REGION,
    #     aws_account_id=AWS_ACCOUNT_ID,
    #     overwrite=True,
    # )

    result = copy_parameters_with_path_mapping(
        path_mapping=path_mapping,
        aws_region=AWS_REGION,
        aws_account_id=AWS_ACCOUNT_ID,
        overwrite=True,
        delete_source=False,
    )

    print_copy_summary(result)
