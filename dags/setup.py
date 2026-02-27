from airflow.sdk import  dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError
import duckdb as duck

BUCKET_NAMES = ["crypto-bronze", "crypto-silver"]


# ================================================================
# DAG Setup to check that the buckets are created and DuckDB is ready
# Normally would be IaaC (Terraform, Helm), but in this instance, 
# given the scopt of the project, a setup DAG will suffice.
# ================================================================


@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    tags=["setup"],
    doc_md=__doc__,
)
def crypto_setup():
    @task
    def ensure_localstack_bucket():
        hook = S3Hook(aws_conn_id="aws_localstack")
        client = hook.get_conn()

        for bucket_name in BUCKET_NAMES:
            try:
                client.head_bucket(Bucket=bucket_name)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in {"404", "NoSuchBucket", "NotFound"}:
                    client.create_bucket(Bucket=bucket_name)
                else:
                    raise e

    @task
    def ensure_duckdb_ready(db_path: str = "/usr/local/airflow/warehouse_ddb/crypto.duckdb"):
        conn = duck.connect(db_path)   # creates file if missing
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.close()

    ensure_localstack_bucket() >> ensure_duckdb_ready()

crypto_setup()