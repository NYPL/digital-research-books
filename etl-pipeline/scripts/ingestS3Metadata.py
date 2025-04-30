import boto3
import json

s3_client = boto3.client("s3")

bucketName = "pdf-pipeline-store-production"


def main():
    """Loading and opening the two JSON objects from the University of SC in the bucket"""

    metaDataList = []
    batches = load_batch()
    for batch in batches:
        # Iterating through all the Content objects to access the JSON muse objects
        for c in batch["Contents"]:
            print(c)
            currKey = str(c["Key"])
            # Skipping empty folder in first batch
            if currKey == "metadata_files/2023-06-02/":
                continue
            print(currKey)
            metaDataObject = s3_client.get_object(Bucket=bucketName, Key=f"{currKey}")
            metadataJSON = json.loads(metaDataObject["Body"].read().decode("utf-8"))
            metaDataList.append(metadataJSON)

    with open("UofSC_metadata.json", "w", encoding="utf-8") as write_file:
        json.dump(metaDataList, write_file, ensure_ascii=False, indent=6)


def load_batch():
    """# Loading batches of JSON records using a paginator until there are no more batches"""

    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucketName, Prefix="metadata_files/2023-06-02/"
    )
    return page_iterator
