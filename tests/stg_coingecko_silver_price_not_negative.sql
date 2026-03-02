select *
from {{ ref('stg_coingecko_silver') }}
where price_usd < 0 