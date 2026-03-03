{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'as_of_date'],
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

select
  coin_id,
  as_of_date,
  price_usd,
  market_cap_usd,
  volume_usd
from {{ ref('stg_coingecko_silver') }}

{% if is_incremental() %}
-- Incremental table load: process new dates plus a small rolling window
-- to pick up late-arriving corrections without reloading full history.
where as_of_date >= (
  select coalesce(max(as_of_date), date '1900-01-01') - interval 35 day
  from {{ this }}
)
{% endif %}