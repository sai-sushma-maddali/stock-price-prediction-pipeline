{{ config(materialized='view', schema='ANALYTICS') }}

with base as (
  select
    upper(symbol) as symbol,
    cast(date as date) as trade_date,
    cast(open as number(18,6)) as open_price,
    cast(max as number(18,6)) as high_price,
    cast(min as number(18,6)) as low_price,
    cast(close as number(18,6)) as close_price,
    cast(volume as number(38,0)) as volume
  from {{ source('RAW', 'yfinance_stock') }}
),

dedup as (
  select
    *,
    row_number() over (partition by symbol, trade_date order by trade_date desc) as rn
  from base
)

select
  symbol,
  trade_date,
  open_price,
  high_price,
  low_price,
  close_price,
  volume
from dedup
where rn = 1
order by symbol, trade_date