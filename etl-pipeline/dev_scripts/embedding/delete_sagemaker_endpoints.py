"""
Clean up SageMaker resources created by deploy_hf_sagemaker.py.

Usage:
    # Delete a specific endpoint (+ its endpoint config + model):
    uv run scratch/embedding/delete_sagemaker_endpoints.py --endpoint-name <name>

    # Delete ALL endpoints (+ their endpoint configs + models):
    uv run scratch/embedding/delete_sagemaker_endpoints.py --all

    # Dry-run (print what would be deleted without deleting):
    uv run scratch/embedding/delete_sagemaker_endpoints.py --all --dry-run

    # Use a non-default AWS profile:
    uv run scratch/embedding/delete_sagemaker_endpoints.py --all --profile sandbox
"""

import argparse
import sys

import boto3
from botocore.exceptions import ClientError


# TODO: handle case where endpoint is still "creating" (10's of minutes), poll or abort the creation
def delete_endpoint_resources(sm_client, endpoint_name: str, dry_run: bool) -> None:
    """Delete endpoint + endpoint config + model(s) using boto3 directly."""
    # Resolve endpoint config name from the endpoint description
    try:
        endpoint_desc = sm_client.describe_endpoint(EndpointName=endpoint_name)
        endpoint_config_name = endpoint_desc["EndpointConfigName"]
    except ClientError as e:
        print(f"  [warning] could not describe endpoint: {e}")
        endpoint_config_name = None

    # Resolve model names from the endpoint config
    model_names = []
    if endpoint_config_name:
        try:
            config_desc = sm_client.describe_endpoint_config(
                EndpointConfigName=endpoint_config_name
            )
            model_names = [v["ModelName"] for v in config_desc["ProductionVariants"]]
        except ClientError as e:
            print(f"  [warning] could not describe endpoint config: {e}")

    print(f"  endpoint        : {endpoint_name}")
    print(f"  endpoint config : {endpoint_config_name or '[not found]'}")
    print(f"  model(s)        : {model_names or '[not found]'}")

    if dry_run:
        print("  [dry-run] skipping deletion\n")
        return

    # Delete models first
    for model_name in model_names:
        try:
            sm_client.delete_model(ModelName=model_name)
            print(f"  deleted model: {model_name}")
        except ClientError as e:
            print(f"  [warning] could not delete model {model_name}: {e}")

    # Delete endpoint config
    if endpoint_config_name:
        try:
            sm_client.delete_endpoint_config(EndpointConfigName=endpoint_config_name)
            print(f"  deleted endpoint config: {endpoint_config_name}")
        except ClientError as e:
            print(f"  [warning] could not delete endpoint config: {e}")

    # Delete endpoint
    try:
        sm_client.delete_endpoint(EndpointName=endpoint_name)
        print(f"  deleted endpoint: {endpoint_name}")
    except ClientError as e:
        print(f"  [warning] could not delete endpoint: {e}")

    print()


def list_all_endpoint_names(sm_client) -> list[str]:
    """Return all endpoint names."""
    names = []
    paginator = sm_client.get_paginator("list_endpoints")
    for page in paginator.paginate():
        names.extend(ep["EndpointName"] for ep in page["Endpoints"])
    return names


def main() -> None:
    """Parse CLI args and delete the specified endpoint(s)."""
    parser = argparse.ArgumentParser(
        description="Delete SageMaker endpoint + endpoint config + model(s)"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--endpoint-name", help="Name of a specific endpoint to delete")
    group.add_argument(
        "--all", action="store_true", help="Delete ALL endpoints in the account/region"
    )
    parser.add_argument("--profile", default="sandbox", help="AWS profile name")
    parser.add_argument(
        "--region", default=None, help="AWS region (default: profile default)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be deleted without actually deleting",
    )
    args = parser.parse_args()

    boto3.setup_default_session(profile_name=args.profile, region_name=args.region)
    sm_client = boto3.client("sagemaker")

    if args.dry_run:
        print("[dry-run mode — nothing will be deleted]\n")

    if args.endpoint_name:
        endpoint_names = [args.endpoint_name]
    else:
        endpoint_names = list_all_endpoint_names(sm_client)
        if not endpoint_names:
            print("No endpoints found.")
            sys.exit(0)
        print(f"Found {len(endpoint_names)} endpoint(s):\n")

    for name in endpoint_names:
        print(f"Cleaning up endpoint: {name}")
        delete_endpoint_resources(sm_client, name, args.dry_run)

    print("Done.")


if __name__ == "__main__":
    main()
