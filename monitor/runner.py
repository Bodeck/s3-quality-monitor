from monitor.report import FileReport
from monitor.validators import Validator, ValidationResults
from monitor.s3 import list_s3_files_with_paginator
import io
import boto3
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


def run_checks(
    bucket: str,
    prefix: str,
    validators: list[Validator],
    max_workers: int = 10,
    s3_client = None
) -> list[FileReport]:
    
    client = s3_client or boto3.client("s3")
    s3_file_list = list_s3_files_with_paginator(
        bucket=bucket, prefix=prefix, s3_client=s3_client
    )
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_process_file, key, bucket, validators, client) for key in s3_file_list
        ]
            
        reports = []
        for f in as_completed(futures):
            try:
                reports.append(f.result())
            except Exception as err:
                reports.append(FileReport(key="uknown", results=[], error=str(err)))
                
    return reports


def _process_file(
    key: str, bucket: str, validators: list[Validator], s3_client=None
) -> FileReport:
    client = s3_client or boto3.client("s3")

    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        obj_body = obj["Body"]
        raw_bytes = obj_body.read()
        buffer = io.BytesIO(raw_bytes)

        if key.endswith(".csv"):
            df = pd.read_csv(buffer)
        else:
            df = pd.read_parquet(buffer)

        validation_results: list[ValidationResults] = []

        for validator in validators:
            validation_data = validator.validate(df=df)
            validation_results.append(validation_data)

        return FileReport(key=key, results=validation_results)

    except Exception as err:
        return FileReport(key=key, results=[], error=str(err))
