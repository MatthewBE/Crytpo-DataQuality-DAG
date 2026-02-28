"""
## Backfilling the previous thirty days of crypto data from the Coingecko API into the Bronze/Silver Moto S3 mock
## When this is done, the daily task in crytpodaily will run as normal 
## Need to make sure that the task runs only once at start up of the Docker container and not on every DAG run
"""

from airflow.sdk import Asset, dag, task, get_current_context
from airflow.models import Variable
from pendulum import datetime
import include.crypto_helpers as ch

@dag(
    start_date=datetime(2026, 2, 26),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Matthew", "retries": 3},
    tags=["crypto_backfill"],
    doc_md=__doc__,
)
def crypto_backfill():
    coin_id = Variable.get("crypto_default_coin_id", default_var="ethereum")

    @task(outlets=[Asset("crypto_backfill_complete")])
    def backfill_crypto_to_bronze():
        ds = get_current_context()["ds"]
        return ch.backfill_crypto_data_to_bronze(coin_id=coin_id, ds=ds)

    @task
    def validate_bronze_backfill():
        pass

    @task
    def backfill_crypto_to_silver():
        return ch.backfill_crypto_data_to_silver(coin_id=coin_id)

    @task
    def validate_silver_backfill():
        pass

    bronze = backfill_crypto_to_bronze()
    bronze_ok = validate_bronze_backfill()
    silver = backfill_crypto_to_silver()
    silver_ok = validate_silver_backfill()

    bronze >> bronze_ok >> silver >> silver_ok

crypto_backfill()