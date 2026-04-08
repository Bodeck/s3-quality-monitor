
import pytest
import boto3

from moto import mock_aws
from monitor.report import FileReport
from monitor.validators import NullRatioCheck, SchemaCheck, RowCountCheck
from monitor.runner import run_checks


@pytest.fixture()
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket = "test-bucket")
        client.put_object(
            Bucket="test-bucket",
            Key="inbound/correct_file.csv",
            ContentType="text/csv",
            Body="name,surname,id,is_valid\nlucy,smith,1,Y\n".encode(),
        )

        client.put_object(
            Bucket="test-bucket",
            Key="inbound/file_with_nulls.csv",
            ContentType="text/csv",
            Body="name,surname,id,is_valid\nlucy,,1,Y\n".encode(),
        )

        yield client

def test_run_checks_integration(s3_client):
    validators = [
        NullRatioCheck(0.1),
        SchemaCheck(["name", "surname","id","is_valid"]),
        RowCountCheck(1)
    ]
    file_reports = run_checks(
        bucket="test-bucket",
        prefix="inbound/",
        validators=validators,
        s3_client=s3_client
    )

    assert len(file_reports) == 2
    assert all(type(el) is FileReport for el in file_reports)
    assert all(len(r.results) == 3 for r in file_reports)

    # find the report for the clean file
    correct = next(r for r in file_reports if "correct_file" in r.key)
    assert all(result.passed for result in correct.results)

    # find the report for the file with nulls
    nulls = next(r for r in file_reports if "nulls" in r.key)
    null_check = next(r for r in nulls.results if "NullRatio" in r.check_name)
    assert not null_check.passed