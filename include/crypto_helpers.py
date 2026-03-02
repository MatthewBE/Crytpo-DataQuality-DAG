import json
import pendulum
import requests
import duckdb
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.bases.hook import BaseHook

BUCKET_NAMES = ["crypto-bronze", "crypto-silver"]

#========================================================
# HELPER functions for the DAG
#========================================================


def format_api_request(
    coin_id: str,
    date: str,
    conn_id: str = "coingecko_api",
) -> dict:
    conn = BaseHook.get_connection(conn_id)
    base_url = conn.host.rstrip("/")
    return {
        "url": f"{base_url}/api/v3/coins/{coin_id}/history",
        "params": {"date": date},
    }


def get_duckdb_conn(db_path: str = "warehouse_ddb/crypto.duckdb"):
    conn = duckdb.connect(db_path)
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    aws_conn = BaseHook.get_connection("aws_localstack")
    aws_extra = aws_conn.extra_dejson or {}
    endpoint_url = aws_extra.get("endpoint_url", "http://172.17.0.1:4566")
    endpoint = endpoint_url.replace("http://", "").replace("https://", "")
    use_ssl = endpoint_url.startswith("https://")

    # LocalStack S3 settings
    conn.execute("SET s3_region='us-east-1';")
    conn.execute("SET s3_access_key_id='test';")
    conn.execute("SET s3_secret_access_key='test';")
    conn.execute(f"SET s3_endpoint='{endpoint}';")
    conn.execute("SET s3_url_style='path';")
    conn.execute(f"SET s3_use_ssl='{'true' if use_ssl else 'false'}';")

    return conn

#========================================================
# LOADER functions for Bronze and Silver buckets
#========================================================


def get_crypto_daily_bronze_data(
    coin_id: str,
    ds: str,
    bucket_name: str = "crypto-bronze",
    aws_conn_id: str = "aws_localstack",
    conn_id: str = "coingecko_api",
) -> str:
    """
    Fetch one CoinGecko history payload for `ds` and write one JSON object
    into the bronze S3 bucket.
    """
    request_payload = format_api_request(coin_id=coin_id, date=ds, conn_id=conn_id)

    # Pull API key from Airflow HTTP connection.
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}

    api_key = (
        extra.get("x_cg_demo_api_key")
        or extra.get("api_key")
        or conn.password
    )

    headers = {"x-cg-demo-api-key": api_key} if api_key else {}

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    response = requests.get(
            request_payload["url"],
            params=request_payload["params"],
            headers=headers,
            timeout=30,
        )
    response.raise_for_status()
    data = response.json()

    partition_date = pendulum.from_format(ds, "YYYY-MM-DD").format("YYYY-MM-DD")
    key = f"bronze/coingecko/coin={coin_id}/date={partition_date}/raw.json"
    s3_hook.load_string(
        string_data=json.dumps(data),
        bucket_name=bucket_name,
        key=key,
        replace=True,  # idempotent reruns
    )
    return key


def create_crypto_daily_silver_data(
    coin_id: str,
    dates: list[str],
    bucket_name: str = "crypto-silver",
    bronze_bucket: str = "crypto-bronze",
    aws_conn_id: str = "aws_localstack",
    db_path: str = "warehouse_ddb/crypto.duckdb",
) -> int:
    """
    Idempotent Silver loader.
    Rewrites only the requested date partitions in Silver.
    Returns number of date partitions processed. 
    We do not want to reprocess the histroical data , only the new daily data.
    """
    if not dates:
        return 0

    s3 = S3Hook(aws_conn_id=aws_conn_id)

    with get_duckdb_conn(db_path) as conn:
        for ds in dates:
            # 1) Delete existing target partition so reruns are idempotent
            partition_prefix = f"silver/coingecko_history/coin={coin_id}/as_of_date={ds}/"
            existing = s3.list_keys(bucket_name=bucket_name, prefix=partition_prefix) or []
            if existing:
                s3.delete_objects(bucket=bucket_name, keys=existing)

            # 2) Load only this date from Bronze and write parquet partition
            conn.execute(f"""
                COPY (
                    WITH raw AS (
                        SELECT *
                        FROM read_json_auto(
                            's3://{bronze_bucket}/bronze/coingecko/coin={coin_id}/date={ds}/raw.json',
                            filename = true
                        )
                    )
                    SELECT
                        regexp_extract(filename, 'coin=([^/]+)', 1) AS coin_id,
                        CAST(regexp_extract(filename, 'date=([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})', 1) AS DATE) AS as_of_date,
                        current_timestamp AS ingested_at,
                        CAST(raw.market_data.current_price.usd AS DOUBLE) AS price_usd,
                        CAST(raw.market_data.market_cap.usd AS DOUBLE) AS market_cap_usd,
                        CAST(raw.market_data.total_volume.usd AS DOUBLE) AS volume_usd,
                        to_json(raw.market_data) AS payload
                    FROM raw
                )
                TO 's3://{bucket_name}/silver/coingecko_history/coin={coin_id}/'
                (FORMAT PARQUET, PARTITION_BY (as_of_date));
            """)

    return len(dates)