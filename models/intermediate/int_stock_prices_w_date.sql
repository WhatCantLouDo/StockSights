WITH base_data AS (
    SELECT
        ROW_ID,
        DATE,
        'IOT' AS symbol,
        HIGH_IOT AS high_price,
        VOLUME_IOT AS volume,
        ADJ_CLOSE_TSN AS adj_close,  -- Use the appropriate adjusted close column for IOT
        LOW_IOT AS low_price,
        CLOSE_IOT AS close_price,
        OPEN_IOT AS open_price
    FROM {{ ref('stg_stock_prices') }}

    UNION ALL

    SELECT
        ROW_ID,
        DATE,
        'SHAK' AS symbol,
        HIGH_SHAK AS high_price,
        VOLUME_SHAK AS volume,
        ADJ_CLOSE_SHAK AS adj_close,
        LOW_SHAK AS low_price,
        CLOSE_SHAK AS close_price,
        OPEN_SHAK AS open_price
    FROM {{ ref('stg_stock_prices') }}

    UNION ALL

    SELECT
        ROW_ID,
        DATE,
        'TSN' AS symbol,
        HIGH_TSN AS high_price,
        VOLUME_TSN AS volume,
        ADJ_CLOSE_TSN AS adj_close,
        LOW_TSN AS low_price,
        CLOSE_TSN AS close_price,
        OPEN_TSN AS open_price
    FROM {{ ref('stg_stock_prices') }}

    UNION ALL

    SELECT
        ROW_ID,
        DATE,
        'WRB' AS symbol,
        HIGH_WRB AS high_price,
        VOLUME_WRB AS volume,
        ADJ_CLOSE_WRB AS adj_close,
        LOW_WRB AS low_price,
        CLOSE_WRB AS close_price,
        OPEN_WRB AS open_price
    FROM {{ ref('stg_stock_prices') }}

    UNION ALL

    SELECT
        ROW_ID,
        DATE,
        'NVDA' AS symbol,
        HIGH_NVDA AS high_price,
        VOLUME_NVDA AS volume,
        ADJ_CLOSE_NVDA AS adj_close,
        LOW_NVDA AS low_price,
        CLOSE_NVDA AS close_price,
        OPEN_NVDA AS open_price
    FROM {{ ref('stg_stock_prices') }}
)

SELECT
    r.symbol_id,
    b.date,
    b.high_price,
    b.volume,
    b.adj_close,
    b.low_price,
    b.close_price,
    b.open_price
FROM base_data b
JOIN {{ ref('ref_symbols') }} r ON b.symbol = r.symbol
ORDER BY r.symbol_id, b.date
