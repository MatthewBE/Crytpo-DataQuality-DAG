import pendulum
import include.crypto_helpers as ch
import subprocess
import duckdb
from airflow.exceptions import AirflowFailException
from airflow.sdk import Asset, dag, task, get_current_context
from airflow.models import Variable
from pendulum import datetime

BATCH_SIZE = 20
# This is an event identifier, publisehed to the asset catalog by the Gold DAG.
GOLD_READY_ASSET = Asset("crypto/gold/daily_ready")

@dag(
    start_date=datetime(2026, 2, 26),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Matthew", "retries": 3},
    tags=["crypto_daily"],
    doc_md=__doc__,
)
def get_crypto_daily_data():
    @task
    def build_dates() -> list[str]:
        ctx = get_current_context()
        ds = ctx["ds"]  # YYYY-MM-DD logical date
        dag_run = ctx.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}

        # Optional hard guard to prevent repeated historical loads
        backfill_complete = Variable.get("crypto_backfill_complete", default_var="false").lower() == "true"
        run_backfill = bool(conf.get("run_backfill", not backfill_complete))
        backfill_days = int(conf.get("backfill_days", 30))
        if run_backfill and backfill_complete:
            # choose one behavior: skip to daily, or raise
            return [ds]  # safe fallback

        if not run_backfill:
            return [ds]  # daily mode only

        anchor = pendulum.parse(ds).date()
        yesterday = pendulum.now("UTC").subtract(days=1).date()
        if anchor > yesterday:
            anchor = yesterday

        # most recent first, excluding anchor day itself
        return [
            anchor.subtract(days=i).format("YYYY-MM-DD")
            for i in range(1, backfill_days + 1)
        ]

    @task
    def chunk_dates(dates: list[str], batch_size: int = BATCH_SIZE) -> list[list[str]]:
        return [dates[i:i + batch_size] for i in range(0, len(dates), batch_size)]

    @task
    def ingest_batch(batch: list[str], coin_id: str = "ethereum") -> int:
        count = 0
        for d in batch:
            ch.get_crypto_daily_bronze_data(coin_id=coin_id, ds=d)  # one-date helper
            count += 1
        return count
       
    @task
    def validate_bronze_data():
        # calling the sqtg_coingecko_bronze.sql DBT model
        cmd = [
            "dbt",
            "build",
            "--project-dir", "/usr/local/airflow",
            "--profiles-dir", "/usr/local/airflow",
            "--select", "stg_coingecko_bronze",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise AirflowFailException(
                "Bronze dbt validation failed.\n"
                f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
            )

    @task
    def create_silver_data(dates: list[str], coin_id: str = "ethereum") -> int:
        return ch.create_crypto_daily_silver_data(coin_id=coin_id, dates=dates)

    @task
    def validate_silver_data():
        cmd = [
            "dbt",
            "build",
            "--project-dir", "/usr/local/airflow",
            "--profiles-dir", "/usr/local/airflow",
            "--select", "stg_coingecko_silver",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise AirflowFailException(
                "Silver dbt validation failed.\n"
                f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
            )

    @task
    def create_gold_data():
        cmd = [
            "dbt",
            "build",
            "--project-dir", "/usr/local/airflow",
            "--profiles-dir", "/usr/local/airflow",
            "--select", "fct_coin_daily_metrics",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise AirflowFailException(
                "Gold dbt validation failed.\n"
                f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
            )
    @task
    def check_gold_freshness(expected_latest_date: str):

        conn = duckdb.connect("/usr/local/airflow/warehouse_ddb/crypto.duckdb")
        max_date = conn.execute(
            "select cast(max(as_of_date) as varchar) from main_analytics.fct_coin_daily_metrics"
        ).fetchone()[0]
        conn.close()

        if max_date is None or max_date < expected_latest_date:
            raise AirflowFailException(
                f"Gold freshness check failed: max_date={max_date}, expected>={expected_latest_date}"
            )
    @task(outlets=[GOLD_READY_ASSET])
    # Creating the Data Asset for use in the Reporting DAG
    def monitor_run_completeness(
        dates: list[str],
        bronze_counts: list[int],
        silver_count: int,
    ) -> None:
        expected = len(dates)
        bronze_total = sum(bronze_counts or [])
        silver_total = silver_count or 0

        if bronze_total != expected:
            raise AirflowFailException(
                f"Bronze completeness failed: expected={expected}, processed={bronze_total}"
            )

        if silver_total != expected:
            raise AirflowFailException(
                f"Silver completeness failed: expected={expected}, processed={silver_total}"
            )

        print(
            {
                "event": "run_completeness_ok",
                "expected_dates": expected,
                "bronze_processed": bronze_total,
                "silver_processed": silver_total,
                "min_date": min(dates) if dates else None,
                "max_date": max(dates) if dates else None,
            }
        )
        # Mark first successful historical run so future scheduled runs stay daily-only.
        if expected > 1:
            Variable.set("crypto_backfill_complete", "true")

    dates = build_dates()
    batches = chunk_dates(dates)
    bronze = ingest_batch.expand(batch=batches)

    bronze_ok = validate_bronze_data()
    silver = create_silver_data(dates)
    silver_ok = validate_silver_data()
    gold = create_gold_data()
    monitor = monitor_run_completeness(dates, bronze, silver)
 
    bronze >> bronze_ok >> silver >> silver_ok >> gold >> monitor 

get_crypto_daily_data()