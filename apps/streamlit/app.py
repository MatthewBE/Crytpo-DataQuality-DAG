from pathlib import Path

import duckdb
import streamlit as st

DB_PATH = "/usr/local/airflow/warehouse_ddb/crypto.duckdb"
GOLD_TABLE = "main_analytics.fct_coin_daily_metrics"


@st.cache_data(ttl=300)
def load_gold_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        return conn.execute(
            f"""
            SELECT
                coin_id,
                as_of_date,
                price_usd
            FROM {GOLD_TABLE}
            ORDER BY as_of_date
            """
        ).fetch_df()
    finally:
        conn.close()


st.set_page_config(page_title="Crypto Gold Report", layout="wide")
st.title("Crypto Daily Price Report")

db_file = Path(DB_PATH)
if not db_file.exists():
    st.error("DuckDB file does not exist yet.")
    st.info(
        "Run `crypto_setup` first, then run `get_crypto_daily_data` to populate Gold data."
    )
    st.stop()

try:
    df = load_gold_data()
except Exception as exc:
    st.error(f"Failed to load Gold data from DuckDB: {exc}")
    st.stop()

if df.empty:
    st.warning(f"No rows found in {GOLD_TABLE}.")
    st.stop()

coin_name = df["coin_id"].iloc[0]
st.caption(f"Coin: {coin_name}")
st.caption(f"Data from {df['as_of_date'].min()} to {df['as_of_date'].max()}")
st.line_chart(data=df, x="as_of_date", y="price_usd", x_label="Day Date", y_label="Price (USD)")
