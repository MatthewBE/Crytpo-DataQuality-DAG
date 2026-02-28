#!/usr/bin/env bash
set -euo pipefail

echo "Local install bootstrap starting..."

if [[ -z "${AIRFLOW_CONN_COINGECKO_API:-}" ]]; then
  echo "ERROR: AIRFLOW_CONN_COINGECKO_API is not set in the environment."
  echo "Copy .env.example to .env and set your demo API key."
  exit 1
fi

if [[ "${AIRFLOW_CONN_COINGECKO_API}" == *"<YOUR_DEMO_API_KEY>"* ]]; then
  echo "ERROR: .env still contains the placeholder API key."
  echo "Replace <YOUR_DEMO_API_KEY> with your real CoinGecko demo key."
  exit 1
fi

if ! command -v astro >/dev/null 2>&1; then
  echo "ERROR: Astro CLI is not installed or not on PATH."
  exit 1
fi

echo "Starting Astro (or restarting if already running)..."
if ! astro dev start; then
  echo "astro dev start failed, retrying with astro dev restart..."
  astro dev restart
fi

echo "Triggering one-time infrastructure DAG: crypto_setup"
astro dev run dags trigger crypto_setup

if [[ "${1:-}" == "--with-backfill" ]]; then
  echo "Triggering backfill DAG: crypto_backfill"
  astro dev run dags trigger crypto_backfill
fi

echo "Done."
echo "Airflow UI: http://localhost:8080"
echo "Run with '--with-backfill' to trigger backfill automatically."