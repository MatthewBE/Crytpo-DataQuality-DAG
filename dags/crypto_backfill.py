"""
## Backfilling the previous thirty days of crypto data from the Coingecko API into the Bronze/Silver Moto S3 mock
## When this is done, the daily task in crytpodaily will run as normal 
## Need to make sure that the task runs only once at start up of the Docker container and not on every DAG run
"""


from airflow.sdk import Asset, dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError
import include.crypto_helpers as ch

BUCKET_NAMES = ["crypto-bronze", "crypto-silver"]

 
# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2026, 2, 26),
    schedule="@once",
    doc_md=__doc__,
    default_args={"owner": "Matthew", "retries": 3},
    tags=["crypto_backfill"],
    max_active_runs=1,
    catchup=False,
)
def crypto_backfill():
    @task
    def ensure_moto_bucket():
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
    # Define tasks
    @task( outlets=[Asset("crypto_backfill_complete")] )
    def backfill_crypto_to_bronze():
        ds = get_current_context()["ds"]
        ch.backfill_crypto_data_to_bronze(coin_id, ds)

    @task()
    def validate_bronze_backfill():
        pass

    @task()
    def backfill_crypto_to_silver():
        ds = get_current_context()["ds"]
        ch.backfill_crypto_data_to_silver(coin_id, ds)

    @task()
    def validate_silver_backfill():
        pass

    