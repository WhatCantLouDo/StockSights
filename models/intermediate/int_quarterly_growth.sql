WITH quarterly_data AS (
    SELECT
        symbol,
        DATE_TRUNC('quarter', date) AS quarter_start,
        AVG(close_price) AS avg_adj_close
    FROM {{ ref('int_stock_prices_w_date') }}
    GROUP BY symbol, DATE_TRUNC('quarter', date)
)

-- Now apply the LAG function in the main SELECT
SELECT
    symbol,
    quarter_start,
    avg_adj_close,
    LAG(avg_adj_close) OVER (PARTITION BY symbol ORDER BY quarter_start) AS prev_avg_adj_close,
    CASE
        WHEN LAG(avg_adj_close) OVER (PARTITION BY symbol ORDER BY quarter_start) IS NOT NULL THEN
            (avg_adj_close - LAG(avg_adj_close) OVER (PARTITION BY symbol ORDER BY quarter_start)) / LAG(avg_adj_close) OVER (PARTITION BY symbol ORDER BY quarter_start) * 100
        ELSE NULL
    END AS growth_rate
FROM quarterly_data
ORDER BY symbol, quarter_start
