import pytest
from moto import mock_aws
import boto3
from monitor.s3 import list_s3_files_with_paginator, upload_report
from monitor.report import SummaryReport


@pytest.fixture()
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        yield client


def load_sample_files(s3_client, keys: list[str]):
    for key in keys:
        s3_client.put_object(
            Bucket="test-bucket",
            Key=f"{key}",
            ContentType="text/csv",
            Body="name,surname,id,is_valid\nlucy,smith,1,Y\n".encode(),
        )


def test_list_s3_returns_valid_keys(s3_client):
    load_sample_files(s3_client=s3_client, keys=["inbound/new_users.csv"])
    files = list(
        list_s3_files_with_paginator(
            bucket="test-bucket", prefix="inbound/", s3_client=s3_client
        )
    )
    assert "inbound/new_users.csv" in files


def test_list_s3_returns_filtered_extensions(s3_client):
    load_sample_files(
        s3_client=s3_client, keys=["inbound/sample1.csv", "inbound/sample.txt", "inbound/sample.parquet"]
    )

    files = list(
        list_s3_files_with_paginator(
            bucket="test-bucket", prefix="inbound/", s3_client=s3_client
        )
    )

    assert "inbound/sample.txt" not in files
    assert "inbound/sample1.csv" in files
    assert "inbound/sample.parquet" in files
    assert len(files) == 2


def test_upload_report(s3_client):
    report_content = SummaryReport([]).to_json()
    bucket = "test-bucket"
    key = "report/sample_report.json"
    upload_report(
        bucket="test-bucket", key=key, content=report_content, s3_client=s3_client
    )

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    obj_body = obj["Body"]
    raw_content = obj_body.read()
    content = raw_content.decode()

    assert obj["ContentType"] == "application/json"
    assert report_content == content
