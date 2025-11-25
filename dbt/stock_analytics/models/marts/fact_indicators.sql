{{ config(materialized='table', schema='ANALYTICS') }}

with base as (
    select
        symbol,
        trade_date,
        close_price,
        lag(close_price) over (partition by symbol order by trade_date) as prev_close_price
    from {{ ref('stg_prices') }}
),

returns as (
    select
        symbol,
        trade_date,
        close_price,
        prev_close_price,
        case 
            when prev_close_price is null then null
            else (close_price - prev_close_price) / prev_close_price
        end as daily_return,
        case 
            when prev_close_price is null then null
            when close_price > prev_close_price then close_price - prev_close_price
            else 0 
        end as gain,
        case 
            when prev_close_price is null then null
            when close_price < prev_close_price then prev_close_price - close_price
            else 0 
        end as loss
    from base
),

metrics as (
    select
        symbol,
        trade_date,
        close_price,
        prev_close_price,
        daily_return,
        avg(close_price) over (
            partition by symbol order by trade_date 
            rows between 6 preceding and current row
        ) as ma_7,
        avg(close_price) over (
            partition by symbol order by trade_date 
            rows between 29 preceding and current row
        ) as ma_30,
        stddev_samp(close_price) over (
            partition by symbol order by trade_date 
            rows between 29 preceding and current row
        ) as vol_30,
        gain,
        loss
    from returns
),

rsi_calc as (
    select
        symbol,
        trade_date,
        close_price,
        prev_close_price,
        daily_return,
        ma_7,
        ma_30,
        vol_30,
        avg(gain) over (
            partition by symbol order by trade_date 
            rows between 13 preceding and current row
        ) as avg_gain_14,
        avg(loss) over (
            partition by symbol order by trade_date 
            rows between 13 preceding and current row
        ) as avg_loss_14
    from metrics
)

select
    symbol,
    trade_date,
    close_price,
    prev_close_price,
    daily_return,
    ma_7,
    ma_30,
    vol_30,
    case
        when avg_loss_14 is null or avg_loss_14 = 0 then null
        else 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
    end as rsi_14
from rsi_calc
order by symbol, trade_date