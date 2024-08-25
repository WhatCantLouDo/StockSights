WITH volatility_data AS (
    SELECT
        symbol_id,
        DATE_TRUNC('month', date) AS month_start,
        MAX(high_price) - MIN(low_price) AS price_volatility
    FROM {{ ref('int_stock_prices_w_date') }}
    GROUP BY symbol_id, DATE_TRUNC('month', date)
)

SELECT
    symbol_id,
    month_start,
    price_volatility
FROM volatility_data
ORDER BY symbol_id, month_start
