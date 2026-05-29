# Sagemaker Inference wrapper image for HF TEI

## Overview
Build and push a custom sagemaker inference endpoint docker image based on HF TEI image. 
The target instance architecture is configured via appropriate choice of TEI_IMAGE.
Target HF_MODEL_ID is specified when deploying to sagemaker, and the model weights are downloaded when the server starts up.

Useful when the HF provided AWS images (https://huggingface.co/docs/sagemaker/main/en/dlcs/available) do not support the most recent TEI versions.

Specifically, as of May 2026, PPLX models require TEI >=1.9.2 but the available official sagemaker inference TEI images only support <=1.8.2. see: https://huggingface.co/perplexity-ai/pplx-embed-v1-4b

## Nvidia Driver Compatibility
As of May 2026, TEI >=1.9 is compatible with nvidia driver versions ≥535.x and CUDA >=12.2, as specified in NVIDIA_REQUIRE_CUDA env var in the TEI image config blob and https://github.com/huggingface/text-embeddings-inference/.

As of May 2026, the default CUDA on the following instance types do not meet these requirements: ml.p2.*, ml.p3.*, ml.p4d.*, ml.p4de.*, ml.g4dn.*, ml.g5.*
See Sagemaker Instance nvidia driver versions: https://docs.aws.amazon.com/sagemaker/latest/dg/inference-gpu-drivers.html

Explicitly setting the instance AMI (and thus CUDA version) can be done with the the INFERENCE_AMI_VERSION var in deploy.sh

## Usage 

### Examples
Push Custom TEI wrapper image:
```
AWS_PROFILE=vra-sandbox INSTANCE_TYPE=ml.g6e.xlarge ./build_and_push.sh 
```
Deploy image with Model:
```
AWS_PROFILE=vra-sandbox HF_MODEL_ID=perplexity-ai/pplx-embed-v1-4b INSTANCE_TYPE=ml.g6e.xlarge ./deploy.sh
```

NOTE: by default, any input longer than --max-batch-tokens [default: 16384] is silently truncated. See --max-batch-tokens and --auto-truncate.

### Model Specific Configurations
```
AWS_PROFILE=vra-sandbox HF_MODEL_ID=perplexity-ai/pplx-embed-v1-4b INSTANCE_TYPE=ml.g6e.xlarge TEI_EXTRA_ARGS="--dtype float32 --max-batch-tokens 3000" ./deploy.sh
```
- PPLX g6e
- --max-batch-tokens 3000" -> our avg chunk is 730 tokens so 3000 should be plenty + save memory. Q: Is this an issue with multiple docs per request?

```
AWS_PROFILE=vra-sandbox HF_MODEL_ID=perplexity-ai/pplx-embed-v1-4b INSTANCE_TYPE=ml.g5.2xlarge INFERENCE_AMI_VERSION=al2-ami-sagemaker-inference-gpu-2 TEI_EXTRA_ARGS="--dtype float32 --max-batch-tokens 3000" ./deploy.sh
```
- PPLX g5
- ml.g5 series have cuda 11.4 by default, TEI requires >=12.2


References:
- https://github.com/huggingface/text-embeddings-inference/
- https://docs.aws.amazon.com/sagemaker/latest/dg/adapt-inference-container.html



<!-- 

"""
modules.json parse warning for st_quantize.FlexibleQuantizer:
TEI cannot parse that custom sentence-transformers module type, so it skips dense module loading.
"""
This is from the logs. Seems ok but verifying against sentence transformers local build could be a nice check

-->
