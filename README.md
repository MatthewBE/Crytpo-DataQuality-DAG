# Crytpo-DataQuality-DAG
Downloading from the Coingecko API, into Bronze /Silver Moto S3 mock and loading into DuckDB. All while using the updated Task Flow and Astronomer Docker image.

## First-time local setup

Run these commands from the repository root:

1. Add your API key to `.env`:
   - `COINGECKO_API_KEY=your_key_here`
2. Start the local Astro environment:
   - `astro dev start`
3. Bootstrap the CoinGecko Airflow connection from your env var:
   - `chmod +x scripts/bootstrap_connections.sh`
   - `source .env`
   - `./scripts/bootstrap_connections.sh`

After this, open Airflow UI at `http://localhost:8080` and verify `coingecko_api` under `Admin` -> `Connections`.

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

`coingecko_api` credentials can be loaded via `scripts/bootstrap_connections.sh` using `COINGECKO_API_KEY` from `.env`.
