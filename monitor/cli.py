from typing import Annotated
from monitor.runner import run_checks
from monitor.validators import RowCountCheck, NullRatioCheck, SchemaCheck
from monitor.report import SummaryReport
from monitor.s3 import upload_report

import boto3
import typer

app = typer.Typer()

@app.command()
def main(
    bucket: Annotated[str, typer.Option(help="AWS S3 bucket name")],
    prefix: str = typer.Option(help="S3 object name prefix"),
    output_prefix: str = typer.Option(),
    fail_on_error: bool = typer.Option(False)
) -> None:
    client = boto3.client("s3")

    validators = [RowCountCheck(10), NullRatioCheck(threshold=0.1), SchemaCheck(["date_effective", "country", "surcharge_1", "surcharge_2", "surcharge_3"])]
    
    try: 
        check_results = run_checks(
            bucket=bucket,
            prefix=prefix,
            validators=validators,
            s3_client=client
        )

        summary_report = SummaryReport(files=check_results)

        key = f"{output_prefix}/report_{summary_report.timestamp.isoformat()}.json"
        upload_report(
            bucket=bucket,
            key=key,
            content=summary_report.to_json(),
            s3_client=client
        )

        typer.secho(summary_report.to_json(), fg=typer.colors.GREEN)
    
    except Exception as err:
        typer.secho(f"Error: {err}", fg=typer.colors.RED)
        if fail_on_error:
            raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
