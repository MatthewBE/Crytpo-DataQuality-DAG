{{ config(
    materialized='incremental',
    unique_key=['coin_id', 'as_of_date'],
    incremental_strategy='delete+insert'
) }}

select
  coin_id,
  as_of_date,
  price_usd,
  market_cap_usd,
  volume_usd
from {{ ref('stg_coingecko_silver') }}

-- The belowmeans “look at the max date already loaded in the current Gold table.”
--That’s how incremental logic knows to only load newer rows on subsequent runs.

{% if is_incremental() %}
where as_of_date > (
  select coalesce(max(as_of_date), date '1900-01-01')
  from {{ this }}
)
{% endif %}