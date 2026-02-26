# Crytpo-DataQuality-DAG
Downloading from the Coingecko API, into Bronze /Silver Moto S3 mock and loading into DuckDB. All while using the updated Task Flow and Astronomer Docker image.

## Infrastructure baseline

- Orchestration/runtime: Astro Runtime (`Dockerfile` base image)
- Object storage (Bronze/Silver): LocalStack S3 (`docker-compose.override.yml`)
- Warehouse (Gold): DuckDB
- Transformation/testing/lineage: dbt (`dbt_project.yml` and `models/`)

## Local Airflow connection bootstrap

Connections and variables for local development are defined in `airflow_settings.yaml`:

- `aws_localstack` (AWS) -> LocalStack endpoint for S3
- `coingecko_api` (HTTP) -> CoinGecko base host
- `crypto_default_coin_id` variable -> default `ethereum`

Add your CoinGecko API key in Airflow UI (`Admin` -> `Connections` -> `coingecko_api` -> `Password`) before running ingestion.
