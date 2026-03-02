# Crytpo-DataQuality-DAG
Downloading from the Coingecko API, into Bronze /Silver Moto S3 mock and loading into DuckDB. All while using the updated Task Flow and Astronomer Docker image.

## First-time local setup

Run these commands from the repository root:

1. Create local env file from template:
   - `cp .env.example .env`
2. Set your CoinGecko demo API key in `.env` by editing:
   - `AIRFLOW_CONN_COINGECKO_API=http://:@api.coingecko.com?extra__http__x_cg_demo_api_key=<YOUR_DEMO_API_KEY>`
3. Run the clean install/bootstrap entrypoint:
   - `bash scripts/bootstrap_connections.sh`
4. Optional: trigger setup + backfill in one command:
   - `bash scripts/bootstrap_connections.sh --with-backfill`

After this, Airflow reads `AIRFLOW_CONN_COINGECKO_API` automatically from `.env`.
No manual UI connection edits are required.

## Infrastructure baseline

- Orchestration/runtime: Astro Runtime (`Dockerfile` base image)
- Object storage (Bronze/Silver): LocalStack S3 (`docker-compose.override.yml`)
- Warehouse (Gold): DuckDB
- Transformation/testing/lineage: dbt (`dbt_project.yml` and `models/`)
- dbt profile resolution: container-native via `DBT_PROFILES_DIR=/usr/local/airflow`
- Reporting UI: Streamlit app at `apps/streamlit/app.py` (served on `http://localhost:8501`)

## Local Airflow connection bootstrap

Connections and variables for local development are defined in `airflow_settings.yaml`:

- `aws_localstack` (AWS) -> LocalStack endpoint for S3
- `crypto_default_coin_id` variable -> default `ethereum`

`coingecko_api` is sourced from `AIRFLOW_CONN_COINGECKO_API` in `.env` (env-only strategy).
The bootstrap script starts Astro, then triggers `crypto_setup` so required infra is created consistently.
With `--with-backfill`, the script triggers `get_crypto_daily_data` in backfill mode via DAG conf:
`{"run_backfill": true, "backfill_days": 30}`.

## dbt in container

- `profiles.yml` is stored at the repo root and mounted to `/usr/local/airflow/profiles.yml` in Astro containers.
- `DBT_PROFILES_DIR` is set in `Dockerfile` so dbt resolves profiles inside the container (no host `~/.dbt` required).

## Streamlit service

- Streamlit runs as a separate Docker service defined in `docker-compose.override.yml`.
- App entrypoint: `apps/streamlit/app.py`
- URL: `http://localhost:8501`
