WITH daily_returns_data AS (
    SELECT
        symbol_id,
        date,
        close_price,
        LAG(close_price) OVER (PARTITION BY symbol_id ORDER BY date) AS prev_close_price,
        ((close_price - LAG(close_price) OVER (PARTITION BY symbol_id ORDER BY date)) / LAG(close_price) OVER (PARTITION BY symbol_id ORDER BY date)) * 100 AS daily_return
    FROM {{ ref('int_stock_prices_w_date') }}
)

SELECT
    symbol_id,
    date,
    daily_return
FROM daily_returns_data
WHERE prev_close_price IS NOT NULL
ORDER BY symbol_id, date
