WITH six_months_data AS (
    SELECT
        symbol,
        DATE_TRUNC('month', date) AS month_start,
        MAX(high_price) AS max_price,
        MIN(low_price) AS min_price
    FROM {{ ref('int_stock_prices_w_date') }}
    WHERE date >= DATEADD(month, -6, CURRENT_DATE)
    GROUP BY symbol, DATE_TRUNC('month', date)
)

SELECT
    symbol,
    month_start,
    (max_price - min_price) AS price_volatility
FROM six_months_data
ORDER BY symbol, month_start
