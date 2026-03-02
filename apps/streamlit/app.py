import duckdb
import streamlit as st

DB_PATH = "/usr/local/airflow/warehouse_ddb/crypto.duckdb"


@st.cache_data(ttl=300)
def load_gold_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        return conn.execute(
            """
            SELECT
                coin_id,
                as_of_date,
                price_usd
            FROM analytics.fct_coin_daily_metrics
            ORDER BY as_of_date
            """
        ).fetch_df()
    finally:
        conn.close()


st.set_page_config(page_title="Crypto Gold Report", layout="wide")
st.title("Crypto Daily Price Report")

try:
    df = load_gold_data()
except Exception as exc:
    st.error(f"Failed to load Gold data from DuckDB: {exc}")
    st.stop()

if df.empty:
    st.warning("No rows found in analytics.fct_coin_daily_metrics.")
    st.stop()

coin_name = df["coin_id"].iloc[0]
st.caption(f"Coin: {coin_name}")
st.caption(f"Data from {df['as_of_date'].min()} to {df['as_of_date'].max()}")
st.line_chart(data=df, x="as_of_date", y="price_usd")
