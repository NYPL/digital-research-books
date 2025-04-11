import argparse
import io
import traceback

import boto3
import PIL

from digital_assets import cover_images


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="")
    args = parser.parse_args()
    session = boto3.Session(profile_name=args.profile)
    s3 = session.client("s3")
    for cover in list_covers(s3):
        key = cover["Key"]
        print(f"Processing {key}")
        try:
            resize_cover(s3, cover["Key"])
        except Exception as e:
            print(f"Resize failed: {str(e)}")
            traceback.print_exc()
        else:
            print("Resize succeeded")


def list_covers(s3):
    paginator = s3.get_paginator("list_objects_v2")
    responses = paginator.paginate(
        Bucket="drb-files-qa",
        Prefix="covers/publisher_backlist",
    )
    for response in responses:
        for cover_object in response["Contents"]:
            yield cover_object


def resize_cover(s3, key: str) -> None:
    image_stream = io.BytesIO()
    print("Downloading cover")
    s3.download_fileobj(Bucket="drb-files-qa", Key=key, Fileobj=image_stream)
    image = PIL.Image.open(image_stream)
    cover = cover_images.resize_image_for_cover(image)
    cover_stream = io.BytesIO()
    cover.save(cover_stream, format=image.format)
    cover_stream.seek(0)
    print("Uploading resized cover")
    s3.upload_fileobj(
        Bucket="drb-files-qa", Key=key, Fileobj=cover_stream,
        ExtraArgs={"ACL": "public-read"},
    )
    # While we're here, let's throw these in prod too
    s3.copy(
        CopySource={"Bucket": "drb-files-qa", "Key": key},
        Bucket="drb-files-production",
        Key=key,
        ExtraArgs={"ACL": "public-read"},
    )


if __name__ == "__main__":
    main()
