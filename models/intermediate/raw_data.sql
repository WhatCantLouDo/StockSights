{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        _ROW AS row_id,
        _FIVETRAN_SYNCED AS synced_timestamp,
        _HIGH_IOT_ AS high_iot,
        _VOLUME_SHAK_ AS volume_shak,
        _ADJ_CLOSE_TSN_ AS adj_close_tsn,
        _LOW_WRB_ AS low_wrb,
        _OPEN_NVDA_ AS open_nvda,
        _LOW_NVDA_ AS low_nvda,
        _CLOSE_WRB_ AS close_wrb,
        _OPEN_SHAK_ AS open_shak,
        _CLOSE_TSN_ AS close_tsn,
        _HIGH_NVDA_ AS high_nvda,
        _VOLUME_NVDA_ AS volume_nvda,
        _LOW_SHAK_ AS low_shak,
        _LOW_TSN_ AS low_tsn,
        _HIGH_WRB_ AS high_wrb,
        _HIGH_SHAK_ AS high_shak,
        _VOLUME_IOT_ AS volume_iot,
        _HIGH_TSN_ AS high_tsn,
        _OPEN_WRB_ AS open_wrb,
        _OPEN_TSN_ AS open_tsn,
        _ADJ_CLOSE_NVDA_ AS adj_close_nvda,
        _LOW_IOT_ AS low_iot,
        _CLOSE_IOT_ AS close_iot,
        _ADJ_CLOSE_WRB_ AS adj_close_wrb,
        _CLOSE_NVDA_ AS close_nvda,
        _ADJ_CLOSE_SHAK_ AS adj_close_shak,
        _CLOSE_SHAK_ AS close_shak,
        _OPEN_IOT_ AS open_iot,
        _VOLUME_TSN_ AS volume_tsn,
        _VOLUME_WRB_ AS volume_wrb
    FROM {{ source('raw', 'stock_prices') }}
)

SELECT *
FROM source_data
