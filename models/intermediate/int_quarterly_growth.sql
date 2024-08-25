WITH quarterly_data AS (
    SELECT
        r.symbol_id,
        DATE_TRUNC('quarter', b.date) AS quarter_start,
        AVG(b.adj_close) AS avg_adj_close
    FROM {{ ref('int_stock_prices_w_date') }} b
    JOIN {{ ref('ref_symbols') }} r ON b.symbol_id = r.symbol_id
    GROUP BY r.symbol_id, DATE_TRUNC('quarter', b.date)
),

quarterly_growth_calculations AS (
    SELECT
        symbol_id,
        quarter_start,
        avg_adj_close,
        LAG(avg_adj_close) OVER (PARTITION BY symbol_id ORDER BY quarter_start) AS prev_avg_adj_close,
        -- Calculate growth rate percentage
        CASE
            WHEN LAG(avg_adj_close) OVER (PARTITION BY symbol_id ORDER BY quarter_start) IS NULL THEN NULL
            ELSE (avg_adj_close - LAG(avg_adj_close) OVER (PARTITION BY symbol_id ORDER BY quarter_start)) / LAG(avg_adj_close) OVER (PARTITION BY symbol_id ORDER BY quarter_start) * 100
        END AS growth_rate
    FROM quarterly_data
)

SELECT
    symbol_id,
    quarter_start,
    avg_adj_close,
    prev_avg_adj_close,
    growth_rate
FROM quarterly_growth_calculations
WHERE prev_avg_adj_close IS NOT NULL
ORDER BY symbol_id, quarter_start
