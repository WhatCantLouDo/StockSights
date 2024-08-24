{{ config(
    materialized='table'
) }}

with base_data as (
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
        volume_wrb,
        row_number() over (order by synced_timestamp desc) as row_num -- Ensure rows are in descending order
    from {{ ref('stg_stock_prices') }} -- Reference the staging model
),

-- Create a sequence of dates starting from August 7, 2024
date_sequence as (
    select
        row_num,
        dateadd(day, -(row_num - 1), '2024-08-07') as stock_date -- Generate dates in descending order
    from base_data
)

-- Join the dates back to the original data
select
    base_data.*,
    date_sequence.stock_date as date -- Add the date column to each row
from base_data
join date_sequence
on base_data.row_num = date_sequence.row_num
order by date desc -- Order by the generated date in descending order
