#!/usr/bin/env bash
set -euo pipefail

# MUST be called from CWD containing Dockerfile

# Required env var
: "${INSTANCE_TYPE:=}"      # Required. SageMaker instance type, e.g. ml.g6e.xlarge.

# Optional env vars
: "${AWS_PROFILE:=}"       # AWS CLI profile — honored natively by the CLI
: "${ECR_REPOSITORY:=custom-sagemaker-tei}"
: "${TEI_VERSION:=latest}"  # Tag version suffix, based on the TEI image tagging convention. 
                            # "latest" (default) or a specific version like "1.9.3".
                            # Check repo for exact available tags, they change regularly. 
                            # See : https://github.com/huggingface/text-embeddings-inference/pkgs/container/text-embeddings-inference

# Derive TEI_IMAGE from INSTANCE_TYPE.
#
# SageMaker instance type → TEI image tag
# Sources:
#   Docs:     https://github.com/huggingface/text-embeddings-inference/#docker-images
#   Registry: https://github.com/huggingface/text-embeddings-inference/pkgs/container/text-embeddings-inference
#
# Instance family → GPU           → Architecture         → Full tag (TEI_VERSION=latest)
#   g4dn          → T4            → Turing (sm_75)       → turing-latest  (experimental; Flash Attention off by default)
#   g5            → A10G          → Ampere 8.6 (sm_86)   → 86-latest
#   g6            → L4            → Ada Lovelace (sm_89) → 89-latest
#   g6e           → L40S          → Ada Lovelace (sm_89) → 89-latest
#   p3            → V100          → Volta (sm_70)         → NOT SUPPORTED
#   p4d           → A100 (40GB)  → Ampere 8.0 (sm_80)   → latest  (no prefix)
#   p4de          → A100 (80GB)  → Ampere 8.0 (sm_80)   → latest  (no prefix)
#   p5            → H100          → Hopper (sm_90)        → hopper-latest
#   p5e           → H200          → Hopper (sm_90)        → hopper-latest

TEI_BASE_REPO="ghcr.io/huggingface/text-embeddings-inference"

if [[ -z "$INSTANCE_TYPE" ]]; then
  echo "INSTANCE_TYPE is required (e.g. ml.g6e.xlarge)." >&2
  exit 1
fi

# Extract instance family (e.g. ml.g6e.xlarge → g6e)
INSTANCE_FAMILY="${INSTANCE_TYPE#ml.}"
INSTANCE_FAMILY="${INSTANCE_FAMILY%%.*}"

case "$INSTANCE_FAMILY" in
  g4dn)
    # T4 — Turing (sm_75); Flash Attention disabled by default due to precision issues
    TEI_IMAGE="${TEI_BASE_REPO}:turing-${TEI_VERSION}"
    ;;
  g5)
    # A10G — Ampere 8.6 (sm_86)
    TEI_IMAGE="${TEI_BASE_REPO}:86-${TEI_VERSION}"
    ;;
  g6)
    # L4 — Ada Lovelace (sm_89)
    TEI_IMAGE="${TEI_BASE_REPO}:89-${TEI_VERSION}"
    ;;
  g6e)
    # L40S — Ada Lovelace (sm_89)
    TEI_IMAGE="${TEI_BASE_REPO}:89-${TEI_VERSION}"
    ;;
  p4d)
    # A100 40GB — Ampere 8.0 (sm_80); base tag has no architecture prefix
    TEI_IMAGE="${TEI_BASE_REPO}:${TEI_VERSION}"
    ;;
  p4de)
    # A100 80GB — Ampere 8.0 (sm_80); base tag has no architecture prefix
    TEI_IMAGE="${TEI_BASE_REPO}:${TEI_VERSION}"
    ;;
  p5)
    # H100 — Hopper (sm_90)
    TEI_IMAGE="${TEI_BASE_REPO}:hopper-${TEI_VERSION}"
    ;;
  p5e)
    # H200 — Hopper (sm_90)
    TEI_IMAGE="${TEI_BASE_REPO}:hopper-${TEI_VERSION}"
    ;;
  p3)
    echo "SageMaker instance type '$INSTANCE_TYPE' (V100/Volta sm_70) is not supported by TEI." >&2
    exit 1
    ;;
  *)
    echo "Unknown SageMaker instance type '$INSTANCE_TYPE'." >&2
    echo "Add a case for this instance type, or check that INSTANCE_TYPE is a valid SageMaker GPU instance." >&2
    exit 1
    ;;
esac

echo "Derived TEI_IMAGE='$TEI_IMAGE' for INSTANCE_TYPE='$INSTANCE_TYPE'"

# Tag the ECR image by instance family (e.g. ml.g6e.xlarge → g6e-latest)
IMAGE_TAG="${INSTANCE_FAMILY}-latest"

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