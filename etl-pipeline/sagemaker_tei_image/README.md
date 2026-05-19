# Sagemaker Inference wrapper image for HF TEI

Build and push a custom sagemaker inference endpoint docker image based on HF TEI image. 
The target instance architecture is configured via appropriate choice of TEI_IMAGE.
Target HF_MODEL_ID is specified when deploying to sagemaker, and the model weights are downloaded when the server starts up.

Useful when the HF provided AWS images (https://huggingface.co/docs/sagemaker/main/en/dlcs/available) do not support the most recent TEI versions.

Examples:
Push image:
```
AWS_PROFILE=vra-sandbox TEI_IMAGE=ghcr.io/huggingface/text-embeddings-inference:89-1.9 ./build_and_push.sh 
```
Deploy image and Model:
```
AWS_PROFILE=vra-sandbox HF_MODEL_ID=perplexity-ai/pplx-embed-v1-4b INSTANCE_TYPE=ml.g6e.xlarge ./deploy.sh
```

NOTE: by default, any input longer than --max-batch-tokens [default: 16384] is silently truncated. See --max-batch-tokens and --auto-truncate.


Resources:
- https://github.com/huggingface/text-embeddings-inference/
- https://docs.aws.amazon.com/sagemaker/latest/dg/adapt-inference-container.html



<!-- 
# our avg chunk is 730 tokens so 3000 should be plenty + save memory
AWS_PROFILE=vra-sandbox HF_MODEL_ID=perplexity-ai/pplx-embed-v1-4b INSTANCE_TYPE=ml.g6e.xlarge TEI_EXTRA_ARGS="--dtype float32 --max-batch-tokens 3000" ./deploy.sh

This is in the logs. Seems ok but verifying against sentence transformers local build could be a nice check
"modules.json parse warning for st_quantize.FlexibleQuantizer:
TEI cannot parse that custom sentence-transformers module type, so it skips dense module loading."
-->
