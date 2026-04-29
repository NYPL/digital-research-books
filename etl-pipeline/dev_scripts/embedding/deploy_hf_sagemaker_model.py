# WARNING: This snippet is not yet compatible with SageMaker version >= 3.0.0.
# To use this snippet, install a compatible version:
# pip install 'sagemaker<3.0.0'
import datetime as dt
import os
from pathlib import Path
import re


import sagemaker
import boto3
from sagemaker.huggingface import HuggingFaceModel, get_huggingface_llm_image_uri
from dotenv import load_dotenv


# --- CLI args ---
import argparse

parser = argparse.ArgumentParser(
    description="Deploy Harrier embedding model to SageMaker"
)
parser.add_argument("--hf-model-id", required=True)
parser.add_argument("--instance-type", required=True)
parser.add_argument("--profile", default="vra-sandbox")
parser.add_argument(
    "--no-cleanup",
    action="store_true",
    default=False,
    help="Skip deleting SageMaker resources (model, endpoint config, endpoint) on deployment failure.",
)
args = parser.parse_args()
# --------------


# --- Config ---
HF_MODEL_ID = args.hf_model_id
INSTANCE_TYPE = args.instance_type
AWS_PROFILE = args.profile
print(f"[config] model={HF_MODEL_ID}  instance={INSTANCE_TYPE}  profile={AWS_PROFILE}")
# --------------


# --- Functions ---


def _sanitize_name(name: str) -> str:
    """Replace any non word or hyphe char"""
    return re.sub(r"[^a-zA-Z0-9-]", r"-", name)


# TODO: duplicated in bedrock deploy script, create shared utils
def _short_name(hf_model_id: str) -> str:
    """'Qwen/Qwen3-Embedding-8B' -> 'qwen3-embedding-8b'"""
    return hf_model_id.split("/")[-1].lower()


def _make_endpoint_name(hf_model_id: str, instance_type: str) -> str:
    """Build a SageMaker endpoint name
    Max len 62 chars
    See: https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateEndpoint.html#API_CreateEndpoint_RequestSyntax
    """
    datetime_str = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    # fixed overhead: "hf-tei-" + "-" + instance_type + "-" + datetime_str
    overhead = len("hf-tei-") + 1 + len(instance_type) + 1 + len(datetime_str)
    max_endpoint_name_len = 63
    max_model_len = max_endpoint_name_len - overhead
    short_model = _short_name(hf_model_id)[:max_model_len]
    return _sanitize_name(f"hf-tei-{short_model}-{instance_type}-{datetime_str}")


def run_instance_recommendation_job(
    model: HuggingFaceModel,
    payload_key: str = "harrier/payload.tar.gz",
) -> HuggingFaceModel:
    """Run a SageMaker Inference Recommender job and return the model with recommendations.

    Uploads a sample payload, triggers a Default recommendation job (~15-45 min, real cost),
    then deletes the payload. See:
    https://docs.aws.amazon.com/sagemaker/latest/dg/instance-recommendation-create.html
    """
    bucket = model.sagemaker_session.default_bucket()
    payload_s3_url = f"s3://{bucket}/{payload_key}"
    print(f"[auto] sample payload URL: {payload_s3_url}")

    model_with_rec = model.right_size(
        sample_payload_url=payload_s3_url,
        supported_content_types=["application/json"],
        # supported_instance_types=["ml.g4dn.xlarge", "ml.g5.xlarge", "ml.g6.2xlarge"],
    )

    model.sagemaker_session.boto_session.client("s3").delete_object(
        Bucket=bucket, Key=payload_key
    )
    print(f"[cleanup] deleted {payload_s3_url}")
    return model_with_rec


# TODO: handle case where endpoint fails to be deleted while it is "creating" \
# which can take 10s of minutes
def _cleanup_resources(
    sm_client, model_name: str | None, endpoint_config_name: str, endpoint_name: str
) -> None:
    """Delete the SageMaker endpoint, endpoint config, and model created before a failed deploy."""
    try:
        sm_client.delete_endpoint(EndpointName=endpoint_name)
        print(f"[cleanup] deleted endpoint: {endpoint_name}")
    except Exception as e:
        print(f"[cleanup] could not delete endpoint {endpoint_name}: {e}")
    try:
        sm_client.delete_endpoint_config(EndpointConfigName=endpoint_config_name)
        print(f"[cleanup] deleted endpoint config: {endpoint_config_name}")
    except Exception as e:
        print(f"[cleanup] could not delete endpoint config {endpoint_config_name}: {e}")
    if model_name:
        try:
            sm_client.delete_model(ModelName=model_name)
            print(f"[cleanup] deleted model: {model_name}")
        except Exception as e:
            print(f"[cleanup] could not delete model {model_name}: {e}")


# ----------------


# --- Script ---

# set default sso auth profile (used by sagemaker.Session())
boto3.setup_default_session(profile_name=AWS_PROFILE)  # ALT: set AWS_PROFILE env var

# NOTE: The sagemaker service needs a IAM role (with necessary permissions) it
# can assume while running the inference container.

# # Set inference IAM role ARN - Fetches role derived from caller identity available to default boto3 session
# role = sagemaker.get_execution_role()

# Set inference IAM role ARN - use pre-created sagemaker execution role
# https://docs.aws.amazon.com/sagemaker/latest/dg/sagemaker-roles.html
iam_client = boto3.client("iam")
roles = {
    "vra-sandbox": "SageMakerExecutionRole",  # nypl-vra-sandbox account
    "sandbox": "AmazonSageMaker-ExecutionRole-20180212T130350",  # nypl-sandbox account # pragma: allowlist secret
}
role_name = roles[args.profile]
role_arn = iam_client.get_role(RoleName=role_name)["Role"]["Arn"]
print(f"[iam] role ARN: {role_arn}")


# create Sagemaker `Model` class
print("[model] fetching HuggingFace TEI image URI...")
image_uri = get_huggingface_llm_image_uri(
    "huggingface-tei",
    version="1.8.2",
)
print(f"[model] image_uri={image_uri}")
huggingface_model = HuggingFaceModel(
    image_uri=image_uri,
    env={"HF_MODEL_ID": HF_MODEL_ID},
    role=role_arn,
)
print("[model] HuggingFaceModel created")

# Define deployment configuration
endpoint_name = _make_endpoint_name(HF_MODEL_ID, INSTANCE_TYPE)
if INSTANCE_TYPE == "auto":
    # TODO: add model cleanup if error in instance recommender job
    model_with_rec = run_instance_recommendation_job(huggingface_model)
    best = model_with_rec.inference_recommendations[0]
    recommended_instance = best["EndpointConfiguration"]["InstanceType"]
    recommendation_id = best["RecommendationId"]
    print(f"Recommended instance: {recommended_instance}  (id={recommendation_id})")
    deploy_model = model_with_rec
    deploy_args = {
        "inference_recommendation_id": recommendation_id,
        "endpoint_name": endpoint_name,
    }
else:
    deploy_model = huggingface_model
    deploy_args = {
        "initial_instance_count": 1,
        "instance_type": INSTANCE_TYPE,
        "endpoint_name": endpoint_name,
    }


# Create SageMaker `Endpoint`
print(f"[deploy] deploying endpoint (instance_type={INSTANCE_TYPE})...")
t0 = dt.datetime.now()
try:
    predictor = deploy_model.deploy(**deploy_args)
except Exception as exc:
    print(f"[deploy] failed: {exc}")
    if args.no_cleanup:
        print("[cleanup] --no-cleanup set; skipping resource deletion")
    else:
        # NOTE: sagemaker uses the same name for endpoint and endpoint_config
        _cleanup_resources(
            huggingface_model.sagemaker_session.sagemaker_client,
            deploy_model.name,
            endpoint_name,
            endpoint_name,
        )
    raise
print(f"[deploy] elapsed took: {dt.datetime.now() - t0}")
print()
print(f"[deploy] endpoint ready: {predictor.endpoint_name}")
# NOTE: seems to return AsyncPredictor... why?


# # send request
# predictor.predict({
# 	"inputs": "My name is Clara and I am",
# })
# internally calls `predictor.sagemaker_session.sagemaker_runtime_client.invoke_endpoint_async()`
# https://docs.aws.amazon.com/boto3/latest/reference/services/sagemaker-runtime/client/invoke_endpoint_async.html
# Why? isn't a sync call slightly cheaper?


# --------------
