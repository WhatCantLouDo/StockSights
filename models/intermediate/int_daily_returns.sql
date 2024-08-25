WITH base_data AS (
    SELECT
        symbol,
        date,
        close_price,
        LAG(close_price) OVER (PARTITION BY symbol ORDER BY date) AS prev_close_price
    FROM {{ ref('int_stock_prices_w_date') }}
)

SELECT
    symbol,
    date AS day_start,
    close_price,
    prev_close_price,
    CASE
        WHEN prev_close_price IS NOT NULL THEN (close_price - prev_close_price) / prev_close_price * 100
        ELSE NULL
    END AS daily_return
FROM base_data
WHERE prev_close_price IS NOT NULL
ORDER BY symbol, day_start
