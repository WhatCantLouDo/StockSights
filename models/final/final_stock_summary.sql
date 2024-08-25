WITH monthly_returns AS (
    SELECT
        symbol_id,
        month_start,
        COALESCE(monthly_return, 0) AS monthly_return,
        COALESCE(total_volume, 0) AS total_volume
    FROM {{ ref('int_monthly_returns') }}
),

quarterly_growth AS (
    SELECT
        symbol_id,
        quarter_start,
        COALESCE(growth_rate, 0) AS growth_rate
    FROM {{ ref('int_quarterly_growth') }}
),

price_volatility AS (
    SELECT
        symbol_id,
        month_start,
        COALESCE(price_volatility, 0) AS price_volatility
    FROM {{ ref('int_price_volatility') }}
),

daily_returns AS (
    SELECT
        symbol_id,
        date,
        COALESCE(daily_return, 0) AS daily_return
    FROM {{ ref('int_daily_returns') }}
)

SELECT
    m.symbol_id,
    m.month_start AS period,
    m.monthly_return,
    q.growth_rate,
    p.price_volatility,
    d.daily_return,
    m.total_volume
FROM monthly_returns m
LEFT JOIN quarterly_growth q
    ON m.symbol_id = q.symbol_id
    AND DATE_TRUNC('quarter', m.month_start) = q.quarter_start
LEFT JOIN price_volatility p
    ON m.symbol_id = p.symbol_id
    AND m.month_start = p.month_start
LEFT JOIN daily_returns d
    ON m.symbol_id = d.symbol_id
    AND m.month_start = DATE_TRUNC('month', d.date)
ORDER BY symbol_id, period
