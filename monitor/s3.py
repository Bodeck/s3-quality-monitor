import boto3
from typing import Generator


def list_s3_files(
    bucket: str,
    prefix: str,
    extensions: tuple[str, ...] = (".csv", ".parquet"),
) -> Generator[str, None, None]:
    s3 = boto3.client("s3")
    kwargs = {"Bucket": bucket, "Prefix": prefix}

    while True:
        response = s3.list_objects_v2(**kwargs)

        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith(extensions):
                yield key

        next_token = response.get("NextContinuationToken", None)
        if next_token is None:
            return
        kwargs["ContinuationToken"] = next_token


def list_s3_files_with_paginator(
    bucket: str, prefix: str, extentions: tuple[str, ...] = (".csv", ".parquet")
) -> Generator[str, None, None]:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.ends_with(extentions):
                yield key
