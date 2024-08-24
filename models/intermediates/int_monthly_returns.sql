{{ config(
    materialized='table'
) }}

WITH monthly_data AS (
    SELECT
        date_trunc('month', DATE) AS month_start,
        'IOT' AS symbol,
        FIRST_VALUE(CLOSE_IOT) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE) AS start_price,
        LAST_VALUE(CLOSE_IOT) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE) AS end_price
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT
        date_trunc('month', DATE) AS month_start,
        'SHAK' AS symbol,
        FIRST_VALUE(ADJ_CLOSE_SHAK) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE) AS start_price,
        LAST_VALUE(ADJ_CLOSE_SHAK) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE) AS end_price
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT
        date_trunc('month', DATE) AS month_start,
        'TSN' AS symbol,
        FIRST_VALUE(ADJ_CLOSE_TSN) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE) AS start_price,
        LAST_VALUE(ADJ_CLOSE_TSN) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE) AS end_price
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT
        date_trunc('month', DATE) AS month_start,
        'WRB' AS symbol,
        FIRST_VALUE(ADJ_CLOSE_WRB) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE) AS start_price,
        LAST_VALUE(ADJ_CLOSE_WRB) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE) AS end_price
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT
        date_trunc('month', DATE) AS month_start,
        'NVDA' AS symbol,
        FIRST_VALUE(ADJ_CLOSE_NVDA) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE) AS start_price,
        LAST_VALUE(ADJ_CLOSE_NVDA) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE) AS end_price
    FROM {{ ref('int_stock_prices_w_date') }}
),

monthly_returns_calculations AS (
    SELECT
        symbol,
        month_start,
        start_price,
        end_price,
        (end_price - start_price) / start_price * 100 AS monthly_return
    FROM monthly_data
)

SELECT
    symbol,
    month_start,
    monthly_return
FROM monthly_returns_calculations
ORDER BY symbol, month_start
