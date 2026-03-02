-- Silver contract model: normalized parquet-backed dataset consumed by Gold marts.
with silver as (
    select *
    from read_parquet(
        -- Silver parquet is written to partitioned S3 paths:
        -- coin=<id>/as_of_date=<YYYY-MM-DD>/...
        's3://crypto-silver/silver/coingecko_history/coin=*/as_of_date=*/*.parquet',
        -- Keep filename available for fallback partition parsing.
        filename = true,
        -- Parse hive-style partitions into columns when available.
        hive_partitioning = true
    )
)

select
    -- Prefer partition columns from hive parsing, with filename fallback.
    -- This allows incrementa loading to the gold layer as we are reading the parquest files 
    -- directly from the silver layer in the mocked S3 bucket.
    coalesce(coin_id, regexp_extract(filename, 'coin=([^/]+)', 1)) as coin_id,
    cast(
        coalesce(
            cast(as_of_date as varchar),
            regexp_extract(filename, 'as_of_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1)
        ) as date
    ) as as_of_date,
    cast(price_usd as double) as price_usd,
    cast(market_cap_usd as double) as market_cap_usd,
    cast(volume_usd as double) as volume_usd
from silver