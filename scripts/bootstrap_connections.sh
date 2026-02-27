#!/usr/bin/env bash
set -euo pipefail

: "${COINGECKO_API_KEY:?Set COINGECKO_API_KEY first}"

astro dev bash -c "airflow connections delete coingecko_api >/dev/null 2>&1 || true"
astro dev bash -c "airflow connections add coingecko_api \
  --conn-type http \
  --conn-host https://pro-api.coingecko.com \
  --conn-extra '{\"x_cg_pro_api_key\":\"${COINGECKO_API_KEY}\"}'"