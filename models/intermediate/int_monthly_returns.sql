WITH symbol_check AS (
    SELECT DISTINCT
        'IOT' AS symbol
    FROM {{ ref('int_stock_prices_w_date') }}
    
    UNION ALL

    SELECT DISTINCT
        'SHAK' AS symbol
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT DISTINCT
        'TSN' AS symbol
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT DISTINCT
        'WRB' AS symbol
    FROM {{ ref('int_stock_prices_w_date') }}

    UNION ALL

    SELECT DISTINCT
        'NVDA' AS symbol
    FROM {{ ref('int_stock_prices_w_date') }}
),

monthly_data AS (
    SELECT
        date_trunc('month', date) AS month_start,
        'IOT' AS symbol,
        FIRST_VALUE(close_iot) OVER (PARTITION BY date_trunc('month', date) ORDER BY date ASC) AS start_price,
        LAST_VALUE(close_iot) OVER (PARTITION BY date_trunc('month', date) ORDER BY date ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price,
        SUM(volume_iot) OVER (PARTITION BY date_trunc('month', date)) AS total_volume
    FROM {{ ref('int_stock_prices_w_date') }}
    WHERE symbol = 'IOT'

    UNION ALL

    SELECT
        date_trunc('month', date) AS month_start,
        'SHAK' AS symbol,
        FIRST_VALUE(adj_close_shak) OVER (PARTITION BY date_trunc('month', date) ORDER BY date ASC) AS start_price,
        LAST_VALUE(adj_close_shak) OVER (PARTITION BY date_trunc('month', date) ORDER BY date ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price,
        SUM(volume_shak) OVER (PARTITION BY date_trunc('month', date)) AS total_volume
    FROM {{ ref('int_stock_prices_w_date') }}
    WHERE symbol = 'SHAK'

    UNION ALL

    SELECT
        date_trunc('month', date) AS month_start,
        'TSN' AS symbol,
        FIRST_VALUE(adj_close_tsn) OVER (PARTITION BY date_trunc('month', date) ORDER BY date ASC) AS start_price,
        LAST_VALUE(adj_close_tsn) OVER (PARTITION BY date_trunc('month', date) ORDER BY date ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price,
        SUM(volume_tsn) OVER (PARTITION BY date_trunc('month', date)) AS total_volume
    FROM {{ ref('int_stock_prices_w_date') }}
    WHERE symbol = 'TSN'

    UNION ALL

    SELECT
        date_trunc('month', date) AS month_start,
        'WRB' AS symbol,
        FIRST_VALUE(adj_close_wrb) OVER (PARTITION BY date_trunc('month', date) ORDER BY date ASC) AS start_price,
        LAST_VALUE(adj_close_wrb) OVER (PARTITION BY date_trunc('month', date) ORDER BY date ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price,
        SUM(volume_wrb) OVER (PARTITION BY date_trunc('month', date)) AS total_volume
    FROM {{ ref('int_stock_prices_w_date') }}
    WHERE symbol = 'WRB'

    UNION ALL

    SELECT
        date_trunc('month', date) AS month_start,
        'NVDA' AS symbol,
        FIRST_VALUE(adj_close_nvda) OVER (PARTITION BY date_trunc('month', date) ORDER BY date ASC) AS start_price,
        LAST_VALUE(adj_close_nvda) OVER (PARTITION BY date_trunc('month', date) ORDER BY date ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_price,
        SUM(volume_nvda) OVER (PARTITION BY date_trunc('month', date)) AS total_volume
    FROM {{ ref('int_stock_prices_w_date') }}
    WHERE symbol = 'NVDA'
),

monthly_returns_calculations AS (
    SELECT DISTINCT
        symbol,
        month_start,
        start_price,
        end_price,
        (end_price - start_price) / start_price * 100 AS monthly_return,
        total_volume
    FROM monthly_data
)

SELECT
    m.symbol,
    m.month_start,
    m.monthly_return,
    m.total_volume
FROM monthly_returns_calculations AS m
JOIN symbol_check AS s
    ON m.symbol = s.symbol
ORDER BY m.symbol, m.month_start
