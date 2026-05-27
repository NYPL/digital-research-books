#!/usr/bin/env bash
set -euo pipefail

# MUST be called with CWD containing Dockerfile

# Optional env vars
: "${AWS_PROFILE:=}"       # AWS CLI profile — honored natively by the CLI
: "${ECR_REPOSITORY:=custom-sagemaker-tei}"
: "${IMAGE_TAG:=latest}"
: "${TEI_IMAGE:=ghcr.io/huggingface/text-embeddings-inference:89-1.9}"

# EC2 g6e = NVIDIA L40S Tensor Core GPU = Ada/Lovelace architecture -> tag 89-1.9 (also 89-latest)

# Resolve AWS_REGION from env or CLI config
AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region 2>/dev/null || true)}}"
if [[ -z "$AWS_REGION" ]]; then
  echo "Unable to determine AWS region. Set AWS_REGION/AWS_DEFAULT_REGION or configure a default region in AWS CLI." >&2
  exit 1
fi

# Resolve AWS_ACCOUNT_ID from CLI credentials
AWS_ACCOUNT_ID="$(aws --region "$AWS_REGION" sts get-caller-identity --query Account --output text 2>/dev/null || true)"
if [[ -z "$AWS_ACCOUNT_ID" || "$AWS_ACCOUNT_ID" == "None" ]]; then
  echo "Unable to determine AWS account ID via STS. Verify your AWS credentials/session (and run aws sso login if needed)." >&2
  exit 1
fi

LOCAL_IMAGE="${ECR_REPOSITORY}:${IMAGE_TAG}"
REMOTE_IMAGE="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${IMAGE_TAG}"

# Create ECR repository if not exists
aws --region "$AWS_REGION" ecr describe-repositories \
  --repository-names "$ECR_REPOSITORY" >/dev/null 2>&1 \
  || aws --region "$AWS_REGION" ecr create-repository \
      --repository-name "$ECR_REPOSITORY" >/dev/null

# Give docker ECR login creds
aws --region "$AWS_REGION" ecr get-login-password \
  | docker login \
      --username AWS \
      --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Build image and push directly to ECR
# Enforce a concrete image manifest rather than an mutable OCI image index, as required by SageMaker CreateModel.
# Disable provenance/SBOM to prevent pushing attestations (which might create OCI image index).
docker buildx build \
  --platform=linux/amd64 \
  --provenance=false \
  --sbom=false \
  --build-arg TEI_IMAGE="$TEI_IMAGE" \
  --output "type=image,name=$REMOTE_IMAGE,push=true,oci-mediatypes=false" \
  .

# # Pull to local and tag for convenience.
# docker pull "$REMOTE_IMAGE" >/dev/null
# docker tag "$REMOTE_IMAGE" "$LOCAL_IMAGE"

echo "ECR REPOSITORY: $REMOTE_IMAGE"