select
    placeholder_id
from {{ ref('stg_coingecko_history') }}
