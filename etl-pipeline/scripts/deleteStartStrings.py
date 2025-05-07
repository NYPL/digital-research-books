import boto3
import json

s3_client = boto3.client("s3")

bucketName = "drb-files-qa"


def main():
    """
    Loading and updating batches of ProjectMuse JSON records with a query parameter to skip interstitial pages
    """

    batches = load_batch()
    for batch in batches:
        # Iterating through all the Content objects to access the JSON muse objects
        for c in batch["Contents"]:
            currKey = c["Key"]
            museObject = s3_client.get_object(Bucket=bucketName, Key=f"{currKey}")
            update_batch(museObject, bucketName, currKey)


def load_batch():
    """# Loading batches of 1000 ProjectMuse JSON records using a paginator until there are no more batches"""
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucketName, Prefix="manifests/muse/")
    return page_iterator


def update_batch(museObject, bucketName, currKey):
    """Updating and returning one ProjectMuse JSON file record"""

    # Decode UTF-8 bytes to Unicode, and convert single quotes
    # to double quotes to make it valid JSON
    for i in museObject["Body"].iter_lines():
        museStrObject = i.decode("utf8")
        # Load the JSON to a Python list and access readingOrder link
        museDictObject = json.loads(museStrObject)
        museDictObject["toc"] = deleteURL(museDictObject["toc"])

    return s3_client.put_object(
        Bucket=bucketName,
        Key=f"{currKey}",
        Body=json.dumps(museDictObject),
        ACL="public-read",
        ContentType="application/json",
    )


def deleteURL(museDict):
    """Deleting extra urls in toc objects that were originally empty"""

    r = 1

    while r < len(museDict):
        if museDict[r]["href"] == "?start=2":
            museDict[r]["href"] = ""

        r += 1

    return museDict


if __name__ == "__main__":
    main()
