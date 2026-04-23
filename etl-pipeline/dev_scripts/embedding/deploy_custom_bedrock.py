"""
Deploy a HuggingFace model to Amazon Bedrock via Custom Model Import.

Pipeline:
  1. Download model from HuggingFace to a local cache dir
  2. Upload model files to a user-specified S3 prefix
  3. Submit a Bedrock CreateModelImportJob
  4. Poll until the job reaches Completed or Failed

Usage:
    uv run scratch/embedding/deploy_custom_bedrock.py \
        --role-arn arn:aws:iam::123456789012:role/BedrockModelImportRole \
        [--bucket my-bedrock-models] \
        [--hf-model-id Qwen/Qwen3-Embedding-8B] \
        [--region us-east-1] \
        [--force-upload] \
        [--no-poll]

Derived from --hf-model-id automatically:
  s3-prefix, bedrock model name  ->  lowercase tail of the model ID
  local cache dir                ->  /tmp/<short-model-name>
  job name                       ->  <short-model-name>-import-<unix-timestamp>

IAM requirements
----------------
Caller identity (running this script):
  - bedrock:CreateModelImportJob
  - bedrock:GetModelImportJob
  - iam:PassRole  (to hand the service role to Bedrock)

Bedrock service role (--role-arn):
  - s3:GetObject   on the model bucket/prefix
  - s3:ListBucket  on the model bucket
  Trust policy principal: bedrock.amazonaws.com
  see: https://docs.aws.amazon.com/bedrock/latest/userguide/model-import-iam-role.html

Other:
  - AWS SSO session active (aws sso login)
  - uv pip install huggingface_hub boto3
"""

import argparse
import os
import sys
import time
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from huggingface_hub import snapshot_download


POLL_INTERVAL_SECONDS = 60
MAX_POLL_ATTEMPTS = 180  # 3 hours max

BUCKET = "vra-bedrock-models-test"

# Multipart threshold: 1 GB (safetensor shards are typically 4–5 GB each)
_GB = 1024**3
_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=1 * _GB,
    multipart_chunksize=256 * 1024 * 1024,  # 256 MB chunks
    max_concurrency=4,
)


def _short_name(hf_model_id: str) -> str:
    """'Qwen/Qwen3-Embedding-8B' -> 'qwen3-embedding-8b'"""
    return hf_model_id.split("/")[-1].lower()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Deploy a HuggingFace model to Amazon Bedrock via Custom Model Import"
    )
    p.add_argument("--bucket", default=BUCKET, help="S3 bucket name")
    p.add_argument(
        "--hf-model-id",
        default="Qwen/Qwen3-Embedding-8B",
        help="HuggingFace model ID (default: Qwen/Qwen3-Embedding-8B)",
    )
    p.add_argument(
        "--role-arn",
        required=True,
        help="ARN of the IAM role Bedrock will assume to read from S3",
    )
    p.add_argument(
        "--region", default="us-east-1", help="AWS region (default: us-east-1)"
    )
    p.add_argument(
        "--force-upload",
        action="store_true",
        help="Upload all files even if they already exist in S3 (skips head_object check)",
    )
    p.add_argument(
        "--no-poll",
        action="store_true",
        help="Submit the import job and exit without polling for completion",
    )
    return p.parse_args()


def download_model(hf_model_id: str, local_cache: str) -> Path:
    cache_path = Path(local_cache)
    print(f"Downloading {hf_model_id} to {cache_path} ...")
    snapshot_download(
        repo_id=hf_model_id,
        local_dir=str(cache_path),
        ignore_patterns=[
            "*.msgpack",
            "*.h5",
            "flax_model*",
            "tf_model*",
            "rust_model*",
        ],
    )
    print(f"Download complete: {cache_path}")
    return cache_path


def _s3_content_length(s3_client, bucket: str, key: str) -> int | None:
    """Return the ContentLength of an S3 object, or None if it does not exist."""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response["ContentLength"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "404":
            return None
        raise


def upload_to_s3(
    local_path: Path, bucket: str, prefix: str, region: str, force: bool = False
) -> str:
    s3 = boto3.client("s3", region_name=region)
    files = sorted(f for f in local_path.rglob("*") if f.is_file())
    if not files:
        raise RuntimeError(f"No files found in {local_path}")

    print(f"Uploading {len(files)} files to s3://{bucket}/{prefix}/ ...")
    skipped = 0
    for i, file_path in enumerate(files, 1):
        relative = file_path.relative_to(local_path)
        s3_key = f"{prefix}/{relative}"
        if not force:
            local_size = file_path.stat().st_size
            s3_size = _s3_content_length(s3, bucket, s3_key)
            if s3_size is not None and s3_size == local_size:
                print(
                    f"  [{i}/{len(files)}] skipping (size match {local_size} bytes): {s3_key}"
                )
                skipped += 1
                continue
            if s3_size is not None and s3_size != local_size:
                print(
                    f"  [{i}/{len(files)}] size mismatch (local={local_size} s3={s3_size}), re-uploading: {s3_key}"
                )
        print(f"  [{i}/{len(files)}] {s3_key}")
        s3.upload_file(
            Filename=str(file_path),
            Bucket=bucket,
            Key=s3_key,
            # TransferConfig ensures files above 1 GB use multipart upload automatically.
            Config=_TRANSFER_CONFIG,
        )

    print(
        f"Upload complete: {bucket}/{prefix}/ ({skipped} skipped, {len(files) - skipped} uploaded)"
    )
    s3_uri = f"s3://{bucket}/{prefix}/"
    return s3_uri


def submit_import_job(
    s3_uri: str,
    role_arn: str,
    model_name: str,
    job_name: str,
    region: str,
) -> str:
    bedrock = boto3.client("bedrock", region_name=region)
    print(f"Submitting Bedrock import job '{job_name}' ...")
    response = bedrock.create_model_import_job(
        jobName=job_name,
        importedModelName=model_name,
        roleArn=role_arn,
        modelDataSource={
            "s3DataSource": {
                "s3Uri": s3_uri,
            }
        },
    )
    job_arn = response["jobArn"]
    print(f"Import job submitted: {job_arn}")
    return job_arn


def poll_import_job(job_arn: str, region: str) -> None:
    bedrock = boto3.client("bedrock", region_name=region)
    print(
        f"Polling import job every {POLL_INTERVAL_SECONDS}s (max {MAX_POLL_ATTEMPTS} attempts) ..."
    )

    for attempt in range(1, MAX_POLL_ATTEMPTS + 1):
        try:
            response = bedrock.get_model_import_job(jobIdentifier=job_arn)
        except ClientError as exc:
            print(f"Failed to poll job: {exc}")
            raise

        status = response["status"]
        print(f"  [attempt {attempt}] status: {status}")

        if status == "Completed":
            model_arn = response.get("importedModelArn", "(arn not yet available)")
            print(f"Import job COMPLETED. Model ARN: {model_arn}")
            return
        if status == "Failed":
            msg = response.get("failureMessage", "no failure message returned")
            print(f"Import job FAILED: {msg}")
            sys.exit(1)

        time.sleep(POLL_INTERVAL_SECONDS)

    print(
        f"Timed out waiting for import job to complete after {MAX_POLL_ATTEMPTS} attempts."
    )
    sys.exit(1)


def main() -> None:
    args = parse_args()

    short = _short_name(args.hf_model_id)
    model_name = short
    s3_prefix = short
    local_cache = f"/tmp/{short}"
    job_name = f"{short}-import-{int(time.time())}"

    # Step 1: download
    local_path = download_model(args.hf_model_id, local_cache)

    # Step 2: upload
    s3_uri = upload_to_s3(
        local_path, args.bucket, s3_prefix, args.region, force=args.force_upload
    )

    # Step 3: submit import job
    job_arn = submit_import_job(
        s3_uri=s3_uri,
        role_arn=args.role_arn,
        model_name=model_name,
        job_name=job_name,
        region=args.region,
    )

    # Step 4: poll
    if args.no_poll:
        print(f"--no-poll set. Job submitted, exiting. Job ARN: {job_arn}")
        return

    poll_import_job(job_arn, args.region)


if __name__ == "__main__":
    main()
