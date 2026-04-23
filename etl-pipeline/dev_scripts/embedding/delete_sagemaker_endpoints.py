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
import sagemaker
from sagemaker.predictor import Predictor


def delete_endpoint_resources(
    sagemaker_session: sagemaker.Session, endpoint_name: str, dry_run: bool
) -> None:
    """Delete endpoint + endpoint config + model(s) via the HuggingFacePredictor SDK."""
    predictor = Predictor(
        endpoint_name=endpoint_name,
        sagemaker_session=sagemaker_session,
    )

    print(f"  endpoint : {endpoint_name}")
    print(f"  model(s) : {predictor._get_model_names()}")

    if dry_run:
        print("  [dry-run] skipping deletion\n")
        return

    predictor.delete_model()
    predictor.delete_endpoint(delete_endpoint_config=True)
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
    sm_session = sagemaker.Session()

    if args.dry_run:
        print("[dry-run mode — nothing will be deleted]\n")

    if args.endpoint_name:
        endpoint_names = [args.endpoint_name]
    else:
        endpoint_names = list_all_endpoint_names(sm_session.sagemaker_client)
        if not endpoint_names:
            print("No endpoints found.")
            sys.exit(0)
        print(f"Found {len(endpoint_names)} endpoint(s):\n")

    for name in endpoint_names:
        print(f"Cleaning up endpoint: {name}")
        delete_endpoint_resources(sm_session, name, args.dry_run)

    print("Done.")


if __name__ == "__main__":
    main()
