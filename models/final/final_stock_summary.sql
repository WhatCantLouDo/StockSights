WITH monthly_returns AS (
    SELECT
        symbol,
        month_start,
        coalesce(monthly_return, 0) AS monthly_return,
        coalesce(total_volume, 0) AS total_volume  -- Ensure total_volume is correctly passed
    FROM {{ ref('int_monthly_returns') }}
),

quarterly_growth AS (
    SELECT
        symbol,
        quarter_start,
        coalesce(growth_rate, 0) AS growth_rate
    FROM {{ ref('int_quarterly_growth') }}
),

price_volatility AS (
    SELECT
        symbol,
        month_start,
        coalesce(price_volatility, 0) AS price_volatility
    FROM {{ ref('int_price_volatility') }}
)

-- Combine the data into a final summary table
SELECT
    m.symbol,
    m.month_start AS period,
    m.monthly_return,
    coalesce(q.growth_rate, 0) AS growth_rate,
    coalesce(p.price_volatility, 0) AS price_volatility,
    m.total_volume  -- Ensure this is available from monthly_returns
FROM monthly_returns m
LEFT JOIN quarterly_growth q
    ON m.symbol = q.symbol
    AND DATE_TRUNC('quarter', m.month_start) = q.quarter_start
LEFT JOIN price_volatility p
    ON m.symbol = p.symbol
    AND m.month_start = p.month_start
ORDER BY symbol, period
