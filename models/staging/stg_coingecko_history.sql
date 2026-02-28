with raw as (
    select *
    from read_json_auto(
        's3://crypto-bronze/bronze/coingecko/coin=*/date=*/raw.json',
        filename = true
    )
)
select
    regexp_extract(filename, 'coin=([^/]+)', 1) as coin_id,
    cast(regexp_extract(filename, 'date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1) as date) as as_of_date,
    current_timestamp as ingested_at,

    cast(json_extract(raw.json, '$.market_data.current_price.usd') as double) as price_usd,
    cast(json_extract(raw.json, '$.market_data.market_cap.usd') as double) as market_cap_usd,
    cast(json_extract(raw.json, '$.market_data.total_volume.usd') as double) as volume_usd,

    raw.json as payload
from raw