WITH monthly_data AS (
    SELECT
        symbol_id,
        DATE_TRUNC('month', date) AS month_start,
        FIRST_VALUE(close_price) OVER (PARTITION BY symbol_id, DATE_TRUNC('month', date) ORDER BY date) AS start_price,
        LAST_VALUE(close_price) OVER (PARTITION BY symbol_id, DATE_TRUNC('month', date) ORDER BY date) AS end_price,
        SUM(volume) OVER (PARTITION BY symbol_id, DATE_TRUNC('month', date)) AS total_volume
    FROM {{ ref('int_stock_prices_w_date') }}
)

SELECT
    symbol_id,
    month_start,
    ((end_price - start_price) / start_price) * 100 AS monthly_return,
    total_volume
FROM monthly_data
ORDER BY symbol_id, month_start
