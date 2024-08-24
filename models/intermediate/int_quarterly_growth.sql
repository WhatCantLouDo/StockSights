

WITH pivoted_data AS (
    SELECT
        date_trunc('quarter', date) AS quarter_start,
        'IOT' AS symbol,
        AVG(HIGH_IOT) AS avg_adj_close
    FROM {{ ref('int_stock_prices_w_date') }}
    GROUP BY quarter_start

    UNION ALL

    SELECT
        date_trunc('quarter', date) AS quarter_start,
        'SHAK' AS symbol,
        AVG(ADJ_CLOSE_SHAK) AS avg_adj_close
    FROM {{ ref('int_stock_prices_w_date') }}
    GROUP BY quarter_start

    UNION ALL

    SELECT
        date_trunc('quarter', date) AS quarter_start,
        'TSN' AS symbol,
        AVG(ADJ_CLOSE_TSN) AS avg_adj_close
    FROM {{ ref('int_stock_prices_w_date') }}
    GROUP BY quarter_start

    UNION ALL

    SELECT
        date_trunc('quarter', date) AS quarter_start,
        'WRB' AS symbol,
        AVG(ADJ_CLOSE_WRB) AS avg_adj_close
    FROM {{ ref('int_stock_prices_w_date') }}
    GROUP BY quarter_start

    UNION ALL

    SELECT
        date_trunc('quarter', date) AS quarter_start,
        'NVDA' AS symbol,
        AVG(ADJ_CLOSE_NVDA) AS avg_adj_close
    FROM {{ ref('int_stock_prices_w_date') }}
    GROUP BY quarter_start
),

quarterly_growth_calculations AS (
    SELECT
        symbol,
        quarter_start,
        avg_adj_close,
        LAG(avg_adj_close) OVER (PARTITION BY symbol ORDER BY quarter_start) AS prev_avg_adj_close,
        -- Calculate growth rate percentage
        CASE
            WHEN LAG(avg_adj_close) OVER (PARTITION BY symbol ORDER BY quarter_start) IS NULL THEN NULL
            ELSE (avg_adj_close - LAG(avg_adj_close) OVER (PARTITION BY symbol ORDER BY quarter_start)) / LAG(avg_adj_close) OVER (PARTITION BY symbol ORDER BY quarter_start) * 100
        END AS growth_rate
    FROM pivoted_data
)

SELECT
    symbol,
    quarter_start,
    avg_adj_close,
    prev_avg_adj_close,
    growth_rate
FROM quarterly_growth_calculations
WHERE prev_avg_adj_close IS NOT NULL
ORDER BY symbol, quarter_start
