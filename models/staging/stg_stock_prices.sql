-- models/staging/stg_stock_prices.sql
{{ config(
    materialized='table'
) }}
-- models/staging/stg_stock_prices.sql

with source_data as (

    select
        row_id,
        synced_timestamp,
        high_iot,
        volume_shak,
        adj_close_tsn,
        low_wrb,
        open_nvda,
        low_nvda,
        close_wrb,
        open_shak,
        close_tsn,
        high_nvda,
        volume_nvda,
        low_shak,
        low_tsn,
        high_wrb,
        high_shak,
        volume_iot,
        high_tsn,
        open_wrb,
        open_tsn,
        adj_close_nvda,
        low_iot,
        close_iot,
        adj_close_wrb,
        close_nvda,
        adj_close_shak,
        close_shak,
        open_iot,
        volume_tsn,
        volume_wrb
    from {{ source('stocklist', 'raw_data') }}

),
cleaned_data as (

    select
        *,
        row_number() over (partition by row_id order by synced_timestamp desc) as row_num
    from source_data
    where 
        -- Remove rows with any null values
        high_iot is not null
        and volume_shak is not null
        and adj_close_tsn is not null
        and low_wrb is not null
        and open_nvda is not null
        and low_nvda is not null
        and close_wrb is not null
        and open_shak is not null
        and close_tsn is not null
        and high_nvda is not null
        and volume_nvda is not null
        and low_shak is not null
        and low_tsn is not null
        and high_wrb is not null
        and high_shak is not null
        and volume_iot is not null
        and high_tsn is not null
        and open_wrb is not null
        and open_tsn is not null
        and adj_close_nvda is not null
        and low_iot is not null
        and close_iot is not null
        and adj_close_wrb is not null
        and close_nvda is not null
        and adj_close_shak is not null
        and close_shak is not null
        and open_iot is not null
        and volume_tsn is not null
        and volume_wrb is not null

        -- Optional: Filter out rows with extreme outliers or invalid data
        and high_iot > 0
        and volume_shak > 0
        and adj_close_tsn > 0
        and low_wrb > 0
        -- You can continue adding checks for other columns if needed

)

select
    *
from cleaned_data
qualify row_num = 1
