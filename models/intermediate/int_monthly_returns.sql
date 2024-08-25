WITH monthly_data AS (
    SELECT
        symbol,
        DATE_TRUNC('month', date) AS month_start,
        FIRST_VALUE(close_price) OVER (PARTITION BY symbol, DATE_TRUNC('month', date) ORDER BY date ASC) AS start_price,
        LAST_VALUE(close_price) OVER (PARTITION BY symbol, DATE_TRUNC('month', date) ORDER BY date ASC) AS end_price,
        SUM(volume) OVER (PARTITION BY symbol, DATE_TRUNC('month', date)) AS total_volume
    FROM {{ ref('int_stock_prices_w_date') }}
)

SELECT
    symbol,
    month_start,
    (end_price - start_price) / start_price * 100 AS monthly_return,
    total_volume
FROM monthly_data
ORDER BY symbol, month_start
