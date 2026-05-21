ARG TEI_IMAGE=ghcr.io/huggingface/text-embeddings-inference:cuda-latest
FROM ${TEI_IMAGE}

# The official TEI CUDA image is Ubuntu/NVIDIA CUDA based.
# Add only the small Python HTTP wrapper dependencies.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      python3-venv \
      ca-certificates \
      curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/program

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY serve.py .

# SageMaker real-time inference expects the container web server on 8080.
EXPOSE 8080

# TEI model cache location.
ENV HUGGINGFACE_HUB_CACHE=/data
ENV HF_HOME=/data

# Wrapper listens here.
ENV SAGEMAKER_BIND_HOST=0.0.0.0
ENV SAGEMAKER_BIND_PORT=8080

# Internal TEI server listens here.
ENV TEI_HOST=127.0.0.1
ENV TEI_PORT=8081

# Override at deploy time.
ENV HF_MODEL_ID=Qwen/Qwen3-Embedding-0.6B

# Optional extra args appended to text-embeddings-router.
# Example:
# ENV TEI_EXTRA_ARGS="--max-client-batch-size 32"
ENV TEI_EXTRA_ARGS=""

# SageMaker may append "serve"; this entrypoint ignores extra args.
ENTRYPOINT ["python3", "/opt/program/serve.py"]