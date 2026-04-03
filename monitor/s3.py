import boto3
from typing import Generator

def list_s3_files(
    bucket: str,
    prefix: str,
    extensions: tuple[str, ...] = (".csv", ".parquet"),
    s3_client=None,
) -> Generator[str, None, None]:
    client = s3_client or boto3.client("s3")
    kwargs = {"Bucket": bucket, "Prefix": prefix}

    while True:
        response = client.list_objects_v2(**kwargs)

        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith(extensions):
                yield key

        next_token = response.get("NextContinuationToken")
        if next_token is None:
            return
        kwargs["ContinuationToken"] = next_token


def list_s3_files_with_paginator(
    bucket: str,
    prefix: str,
    extensions: tuple[str, ...] = (".csv", ".parquet"),
    s3_client=None,
) -> Generator[str, None, None]:
    client = s3_client or boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(extensions):
                yield key


def upload_report(
        bucket: str,
        key: str,
        content: str,
        s3_client = None
) -> None:
    client = s3_client or boto3.client("s3")
    
    client.put_object(
        Bucket = bucket,
        Key = key,
        Body = content.encode(),
        ContentType = "application/json"
    )