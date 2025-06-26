import pathlib
import io
import os
import typing
import urllib.parse

import boto3

s3_key: typing.TypeAlias = str | pathlib.Path


class Bucket:
    def __init__(self, bucket: str):
        self.s3_endpoint = os.environ.get("S3_ENDPOINT_URL")
        self._client = None
        self.bucket = bucket

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("s3", endpoint_url=self.s3_endpoint)

        return self._client

    def get_public_url(self, key: s3_key) -> str:
        return (
            f"https://{self.bucket}.s3.{self.client.meta.region_name}."
            f"amazonaws.com/{key}"
        )

    def generate_presigned_upload_url(self, key: s3_key, expires_in: int = 3600) -> str:
        url = self.client.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": self.bucket, "Key": str(key)},
            ExpiresIn=expires_in,
        )
        parsed_url = urllib.parse.urlparse(url)
        # Hack for localdev
        if parsed_url.hostname == "host.docker.internal":
            url = urllib.parse.urlunparse(
                parsed_url._replace(netloc=f"localhost:{parsed_url.port}"),
            )
        return url

    def get_head(self, key: s3_key):
        return self.client.head_object(Bucket=self.bucket, Key=str(key))

    def get(self, key: s3_key):
        return self.client.get_object(Bucket=self.bucket, Key=str(key))

    def get_prefixed_keys(self, prefix="") -> list[str]:
        keys = []

        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
        for page in page_iterator:
            if int(page["KeyCount"]) == 0:
                break
            for object in page["Contents"]:
                keys.append(object["Key"])
        return keys

    def download_file(self, key: s3_key, out_location: str | pathlib.Path):
        self.client.download_file(self.bucket, str(key), str(out_location))

    def upload_file(
        self,
        file: io.BytesIO,
        key: s3_key,
        metadata: dict[str, str] | None = None,
    ):
        extra_args = {}
        if metadata:
            extra_args["Metadata"] = metadata

        return self.client.upload_fileobj(
            file,
            Bucket=self.bucket,
            Key=str(key),
            ExtraArgs=extra_args,
        )
