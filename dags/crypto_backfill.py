"""
## Backfilling the previous thirty days of crypto data from the Coingecko API into the Bronze/Silver Moto S3 mock
## When this is done, the daily task in crytpodaily will run as normal 
## Need to make sure that the task runs only once at start up of the Docker container and not on every DAG run
"""


from airflow.sdk import Asset, dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError
import requests

def _get_previous_thirty_days_of_dates():
    """
    This function returns a list of the previous thirty days of dates 
    based on the {ds} in airflow to not mismatch the times when the DAG runs
    """
   

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2026, 2, 26),
    schedule="@once",
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["crypto_backfill"],
)
def crypto_backfill():
    @task
    def ensure_moto_bucket(bucket_name: str = "crypto-raw"):
        hook = S3Hook(aws_conn_id="aws_moto")
        client = hook.get_conn()
        try:
            client.head_bucket(Bucket=bucket_name)
        except ClientError:
            client.create_bucket(Bucket=bucket_name)


    # Define tasks
    @task( outlets=[Asset("crypto_backfill_complete")] )
    def backfill_crypto_to_bronze():
