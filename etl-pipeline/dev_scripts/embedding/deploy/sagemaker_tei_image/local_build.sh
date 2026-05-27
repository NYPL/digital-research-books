#!/usr/bin/env bash

# Start Server
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-sagemaker-tei-wrapper:local}"
HF_MODEL_ID="${HF_MODEL_ID:-Qwen/Qwen3-Embedding-0.6B}"

docker build -t "$IMAGE_NAME" . #TODO: needs platform specified

docker run --rm \
  --gpus all \
  -p 8080:8080 \
  -e HF_MODEL_ID="$HF_MODEL_ID" \
  -v "$PWD/data:/data" \
  "$IMAGE_NAME"



# Run request in another terminal

# curl -s http://localhost:8080/ping | jq

# curl -s \
#   -X POST http://localhost:8080/invocations \
#   -H 'content-type: application/json' \
#   --data-binary @sample_payload.json \
#   | jq