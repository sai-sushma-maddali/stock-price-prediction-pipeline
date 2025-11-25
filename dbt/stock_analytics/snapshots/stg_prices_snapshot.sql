{% snapshot stg_prices_test_snapshot %}
{{
    config(
        target_schema='ANALYTICS',
        unique_key='symbol || trade_date',
        strategy='check',
        check_cols=['close_price'],
        materialized='snapshot'
    )
}}

select * from ANALYTICS.stg_prices_test

{% endsnapshot %}