#!/usr/bin/env bash
set -euo pipefail

# ── Required env vars ────────────────────────────────────────────────────────
# HF_MODEL_ID   – HuggingFace model ID (e.g. Qwen/Qwen3-Embedding-0.6B)
# INSTANCE_TYPE – SageMaker instance type (e.g. ml.g6e.xlarge)
#
# Optional env vars (defaults below)
# AWS_PROFILE      – AWS CLI profile (no default)
# AWS_REGION       – defaults to `aws configure get region`
# ROLE_ARN         – SageMaker execution role ARN (defaults to arn:aws:iam::260496020663:role/SageMakerExecutionRole)
# ECR_REPOSITORY   – ECR repository name (default: custom-sagemaker-tei)
# TEI_EXTRA_ARGS   – extra args appended to text-embeddings-router (ex: "--dtype float32 --max-batch-tokens 32768")

AWS_REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || true)}" # MAYBE: handle fetch error more explicitly like AWS_ACCOUNT_ID
ROLE_ARN="${ROLE_ARN:-arn:aws:iam::260496020663:role/SageMakerExecutionRole}"
ECR_REPOSITORY="${ECR_REPOSITORY:-custom-sagemaker-tei}"
TEI_EXTRA_ARGS="${TEI_EXTRA_ARGS:-}"

missing=()
[[ -z "${HF_MODEL_ID:-}"    ]] && missing+=("HF_MODEL_ID")
[[ -z "${INSTANCE_TYPE:-}"  ]] && missing+=("INSTANCE_TYPE")
[[ -z "${AWS_REGION:-}"     ]] && missing+=("AWS_REGION")

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "Error: missing required environment variable(s): ${missing[*]}" >&2
  exit 1
fi

# Resolve AWS_ACCOUNT_ID
AWS_ACCOUNT_ID="$(aws --region "$AWS_REGION" sts get-caller-identity --query Account --output text 2>/dev/null || true)"
if [[ -z "$AWS_ACCOUNT_ID" || "$AWS_ACCOUNT_ID" == "None" ]]; then
  echo "Unable to determine AWS account ID from credentials via STS. Verify your AWS credentials/session." >&2
  exit 1
fi

# Derive IMAGE_URI from INSTANCE_TYPE (e.g. ml.g6e.xlarge → g6e-latest)
INSTANCE_FAMILY="${INSTANCE_TYPE#ml.}"
INSTANCE_FAMILY="${INSTANCE_FAMILY%%.*}"
IMAGE_TAG="${INSTANCE_FAMILY}-latest"
IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${IMAGE_TAG}"

# Verify the image exists in ECR before attempting deployment
aws --region "$AWS_REGION" ecr describe-images \
  --repository-name "$ECR_REPOSITORY" \
  --image-ids "imageTag=${IMAGE_TAG}" \
  >/dev/null 2>&1 \
  || { echo "Image '${IMAGE_URI}' not found in ECR. Run build_and_push.sh with INSTANCE_TYPE='${INSTANCE_TYPE}' first." >&2; exit 1; }

echo "Using IMAGE_URI='$IMAGE_URI'"

# Normalize HF_MODEL_ID and INSTANCE_TYPE and construct valid AWS endpoint name
sanitize_name() {
  # Keep only [A-Za-z0-9-], collapse repeated '-', and trim edge '-'.
  local s
  s="$(printf '%s' "$1" | tr -c '[:alnum:]-' '-' | sed -E 's/-+/-/g; s/^-+//; s/-+$//')"
  [[ -z "$s" ]] && s="x"
  printf '%s' "$s"
}

TIMESTAMP="$(date +%Y%m%d%H%M%S)"
INSTANCE_SLUG="$(sanitize_name "$INSTANCE_TYPE")"
HF_MODEL_TAIL="${HF_MODEL_ID#*/}"
HF_MODEL_SLUG="$(sanitize_name "$HF_MODEL_TAIL")"

# Hard-coded length budgets to keep BASE_NAME <= 63:
# 4("tei-") + 27(model) + 1 + 16(instance) + 1 + 14(timestamp) = 63
MAX_MODEL_SLUG_LEN=27
INSTANCE_SLUG="${INSTANCE_SLUG%-}"
INSTANCE_SLUG="${INSTANCE_SLUG#-}"
MODEL_SLUG="${HF_MODEL_SLUG:0:MAX_MODEL_SLUG_LEN}"
MODEL_SLUG="${MODEL_SLUG%-}"
MODEL_SLUG="${MODEL_SLUG#-}"
BASE_NAME="tei-${MODEL_SLUG}-${INSTANCE_SLUG}-${TIMESTAMP}"

MODEL_NAME="$BASE_NAME"
ENDPOINT_CONFIG_NAME="$BASE_NAME"
ENDPOINT_NAME="$BASE_NAME"


# Create model
aws sagemaker create-model \
  --region "$AWS_REGION" \
  --model-name "$MODEL_NAME" \
  --execution-role-arn "$ROLE_ARN" \
  --primary-container "{
    \"Image\": \"${IMAGE_URI}\",
    \"Environment\": {
      \"HF_MODEL_ID\": \"${HF_MODEL_ID}\",
      \"TEI_EXTRA_ARGS\": \"${TEI_EXTRA_ARGS}\"
    }
  }"
# NOTE: optional env vars to set:
# - from Dockerfile -> HUGGINGFACE_HUB_CACHE, HF_HOME 
# - from serve.py -> STARTUP_TIMEOUT_SECONDS, REQUEST_TIMEOUT_SECONDS

# Create endpoint config
aws sagemaker create-endpoint-config \
  --region "$AWS_REGION" \
  --endpoint-config-name "$ENDPOINT_CONFIG_NAME" \
  --production-variants "[
    {
      \"VariantName\": \"AllTraffic\",
      \"ModelName\": \"${MODEL_NAME}\",
      \"InitialInstanceCount\": 1,
      \"InstanceType\": \"${INSTANCE_TYPE}\",
      \"InitialVariantWeight\": 1.0
    }
  ]"

# Create endpoint
aws sagemaker create-endpoint \
  --region "$AWS_REGION" \
  --endpoint-name "$ENDPOINT_NAME" \
  --endpoint-config-name "$ENDPOINT_CONFIG_NAME"

echo "Endpoint name: $ENDPOINT_NAME"

# # Invoke Endpoint
# aws sagemaker-runtime invoke-endpoint \
#   --region "$AWS_REGION" \
#   --endpoint-name "$ENDPOINT_NAME" \
#   --content-type application/json \
#   --body '{"inputs":["hello world","test embedding"]}' \
#   /dev/stdout