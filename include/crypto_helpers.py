import json
import pendulum
import requests
import duckdb
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook



BUCKET_NAMES = ["crypto-bronze", "crypto-silver"]


#========================================================
# HELPER functions for the DAG
#========================================================


def _get_previous_thirty_days_of_dates(ds: str) -> list[str]:
    """
    This function returns a list of the previous thirty days of dates 
    based on the {ds} in airflow to not mismatch the times when the DAG runs
    """
    # Parsing the date in Airflow style to crate a list of string with dates
    start_date = pendulum.parse(ds)
    return [
        start_date.subtract(days=i).format("YYYY-MM-DD")
        for i in range(1, 31)
    ]

def _build_history_requests(
    coin_id: str,
    dates: list[str],
    conn_id: str = "coingecko_api",
) -> list[dict]:
    conn = BaseHook.get_connection(conn_id)
    base_url = conn.host.rstrip("/")

    return [
        {
            "url": f"{base_url}/api/v3/coins/{coin_id}/history",
            "params": {"date": d},
        }
        for d in dates
    ]
  

def get_duckdb_conn(db_path: str = "warehouse_ddb/crypto.duckdb"):
    conn = duckdb.connect(db_path)
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    # LocalStack S3 settings
    conn.execute("SET s3_region='us-east-1';")
    conn.execute("SET s3_access_key_id='test';")
    conn.execute("SET s3_secret_access_key='test';")
    conn.execute("SET s3_endpoint='localstack:4566';")
    conn.execute("SET s3_url_style='path';")
    conn.execute("SET s3_use_ssl='false';")

    return conn

#========================================================
# LOADER functions for Bronze and Silver buckets
#========================================================


def backfill_crypto_data_to_bronze(
    coin_id: str,
    ds: str,
    bucket_name: str = "crypto-bronze",
    aws_conn_id: str = "aws_localstack",
    conn_id: str = "coingecko_api",
) -> list[str]:
    """
    Fetch CoinGecko history payloads for the previous 30 days and write
    one JSON object per day into the bronze S3 bucket.
    """
    # Build request list from Airflow ds + HTTP connection host.
    dates = _get_previous_thirty_days_of_dates(ds)
    requests_to_make = _build_history_requests(coin_id=coin_id, dates=dates, conn_id=conn_id)

    # Pull API key from Airflow HTTP connection.
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}
    api_key = extra.get("x_cg_pro_api_key")
    headers = {"x-cg-pro-api-key": api_key} if api_key else {}

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    written_keys: list[str] = []

    for req, date in zip(requests_to_make, dates):
        response = requests.get(
            req["url"],
            params=req["params"],
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        key = f"bronze/coingecko/coin={coin_id}/date={date}/raw.json"
        s3_hook.load_string(
            string_data=json.dumps(data),
            bucket_name=bucket_name,
            key=key,
            replace=True,  # idempotent reruns
        )
        written_keys.append(key)

    return written_keys

def backfill_crypto_data_to_silver(
    coin_id: str,
    bucket_name: str = "crypto-silver",
    db_path: str = "warehouse_ddb/crypto.duckdb",
) -> None:
    """
    This function saves the crypto data to the Silver bucket in S3
    """
    with get_duckdb_conn(db_path) as conn:
        conn.execute(f"""
        COPY (
        SELECT *
        FROM read_json_auto('s3://crypto-bronze/bronze/coingecko/coin={coin_id}/date=*/raw.json')
        )
        TO 's3://{bucket_name}/silver/coingecko_history/coin={coin_id}/'
        (FORMAT PARQUET, PARTITION_BY (as_of_date));
        """)
    

