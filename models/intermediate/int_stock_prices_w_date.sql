WITH base_data AS (
    SELECT
        'IOT' AS symbol,
        SYNCED_TIMESTAMP AS date,
        HIGH_IOT AS high_price,
        LOW_IOT AS low_price,
        CLOSE_IOT AS close_price,
        VOLUME_IOT AS volume
    FROM {{ ref('stg_stock_prices') }}
    
    UNION ALL
    
    SELECT
        'SHAK' AS symbol,
        SYNCED_TIMESTAMP AS date,
        HIGH_SHAK AS high_price,
        LOW_SHAK AS low_price,
        CLOSE_SHAK AS close_price,
        VOLUME_SHAK AS volume
    FROM {{ ref('stg_stock_prices') }}
    
    UNION ALL
    
    SELECT
        'TSN' AS symbol,
        SYNCED_TIMESTAMP AS date,
        HIGH_TSN AS high_price,
        LOW_TSN AS low_price,
        CLOSE_TSN AS close_price,
        VOLUME_TSN AS volume
    FROM {{ ref('stg_stock_prices') }}
    
    UNION ALL
    
    SELECT
        'WRB' AS symbol,
        SYNCED_TIMESTAMP AS date,
        HIGH_WRB AS high_price,
        LOW_WRB AS low_price,
        CLOSE_WRB AS close_price,
        VOLUME_WRB AS volume
    FROM {{ ref('stg_stock_prices') }}
    
    UNION ALL
    
    SELECT
        'NVDA' AS symbol,
        SYNCED_TIMESTAMP AS date,
        HIGH_NVDA AS high_price,
        LOW_NVDA AS low_price,
        CLOSE_NVDA AS close_price,
        VOLUME_NVDA AS volume
    FROM {{ ref('stg_stock_prices') }}
)

SELECT
    symbol,
    date,
    high_price,
    low_price,
    close_price,
    volume
FROM base_data
ORDER BY symbol, date
