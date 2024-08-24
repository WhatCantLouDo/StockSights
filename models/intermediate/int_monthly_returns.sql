

-- Ensure the data contains entries for each symbol
WITH symbol_check AS (
    SELECT DISTINCT
        'IOT' AS symbol
    FROM {{ ref('int_stock_prices_w_date') }}
    
    UNION ALL

    SELECT DISTINCT
        'SHAK' AS symbol
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT DISTINCT
        'TSN' AS symbol
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT DISTINCT
        'WRB' AS symbol
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT DISTINCT
        'NVDA' AS symbol
    FROM {{ ref('int_stock_prices_w_date') }}
),

monthly_data AS (
    SELECT
        date_trunc('month', DATE) AS month_start,
        'IOT' AS symbol,
        FIRST_VALUE(CLOSE_IOT) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS start_price,
        LAST_VALUE(CLOSE_IOT) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT
        date_trunc('month', DATE) AS month_start,
        'SHAK' AS symbol,
        FIRST_VALUE(ADJ_CLOSE_SHAK) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS start_price,
        LAST_VALUE(ADJ_CLOSE_SHAK) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT
        date_trunc('month', DATE) AS month_start,
        'TSN' AS symbol,
        FIRST_VALUE(ADJ_CLOSE_TSN) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS start_price,
        LAST_VALUE(ADJ_CLOSE_TSN) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT
        date_trunc('month', DATE) AS month_start,
        'WRB' AS symbol,
        FIRST_VALUE(ADJ_CLOSE_WRB) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS start_price,
        LAST_VALUE(ADJ_CLOSE_WRB) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT
        date_trunc('month', DATE) AS month_start,
        'NVDA' AS symbol,
        FIRST_VALUE(ADJ_CLOSE_NVDA) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS start_price,
        LAST_VALUE(ADJ_CLOSE_NVDA) OVER (PARTITION BY date_trunc('month', DATE) ORDER BY DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price
    FROM {{ ref('int_stock_prices_w_date') }}
),

monthly_returns_calculations AS (
    SELECT DISTINCT
        symbol,
        month_start,
        start_price,
        end_price,
        (end_price - start_price) / start_price * 100 AS monthly_return
    FROM monthly_data
)

SELECT
    m.symbol,
    m.month_start,
    m.monthly_return
FROM monthly_returns_calculations AS m
JOIN symbol_check AS s
    ON m.symbol = s.symbol
ORDER BY m.symbol, m.month_start
