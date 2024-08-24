-- models/price_volatility.sql

WITH six_months_data AS (
    SELECT
        date_trunc('month', date) AS month_start,
        'IOT' AS symbol,
        MAX(HIGH_IOT) AS max_price,
        MIN(LOW_IOT) AS min_price
    FROM {{ ref('int_stock_prices_w_date') }}
    WHERE date >= dateadd(month, -6, current_date)
    GROUP BY month_start

    UNION ALL

    SELECT
        date_trunc('month', date) AS month_start,
        'SHAK' AS symbol,
        MAX(HIGH_SHAK) AS max_price,
        MIN(LOW_SHAK) AS min_price
    FROM {{ ref('int_stock_prices_w_date') }}
    WHERE date >= dateadd(month, -6, current_date)
    GROUP BY month_start

    UNION ALL

    SELECT
        date_trunc('month', date) AS month_start,
        'TSN' AS symbol,
        MAX(HIGH_TSN) AS max_price,
        MIN(LOW_TSN) AS min_price
    FROM {{ ref('int_stock_prices_w_date') }}
    WHERE date >= dateadd(month, -6, current_date)
    GROUP BY month_start

    UNION ALL

    SELECT
        date_trunc('month', date) AS month_start,
        'WRB' AS symbol,
        MAX(HIGH_WRB) AS max_price,
        MIN(LOW_WRB) AS min_price
    FROM {{ ref('int_stock_prices_w_date') }}
    WHERE date >= dateadd(month, -6, current_date)
    GROUP BY month_start

    UNION ALL

    SELECT
        date_trunc('month', date) AS month_start,
        'NVDA' AS symbol,
        MAX(HIGH_NVDA) AS max_price,
        MIN(LOW_NVDA) AS min_price
    FROM {{ ref('int_stock_prices_w_date') }}
    WHERE date >= dateadd(month, -6, current_date)
    GROUP BY month_start
),

price_volatility_calculations AS (
    SELECT
        symbol,
        month_start,
        max_price,
        min_price,
        (max_price - min_price) AS price_volatility
    FROM six_months_data
)

SELECT
    symbol,
    month_start,
    price_volatility
FROM price_volatility_calculations
ORDER BY symbol, month_start
