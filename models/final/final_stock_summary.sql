WITH price_volatility AS (
    SELECT
        SYMBOL,
        MONTH_START,
        PRICE_VOLATILITY
    FROM {{ ref('int_price_volatility') }}
),
monthly_returns AS (
    SELECT
        SYMBOL,
        MONTH_START,
        MONTHLY_RETURN
    FROM {{ ref('int_monthly_returns') }}
),
quarterly_growth AS (
    SELECT
        SYMBOL,
        QUARTER_START,
        AVG_ADJ_CLOSE,
        PREV_AVG_ADJ_CLOSE,
        GROWTH_RATE
    FROM {{ ref('int_quarterly_growth') }}
),
combined_data AS (
    SELECT
        pv.SYMBOL,
        pv.MONTH_START,
        pv.PRICE_VOLATILITY,
        mr.MONTHLY_RETURN,
        qg.QUARTER_START,
        qg.AVG_ADJ_CLOSE,
        qg.PREV_AVG_ADJ_CLOSE,
        qg.GROWTH_RATE,
        ROW_NUMBER() OVER (
            PARTITION BY 
                pv.SYMBOL, 
                pv.MONTH_START, 
                qg.QUARTER_START
            ORDER BY 
                pv.SYMBOL, 
                pv.MONTH_START, 
                qg.QUARTER_START
        ) AS row_num
    FROM 
        price_volatility pv
    LEFT JOIN 
        monthly_returns mr
    ON 
        pv.SYMBOL = mr.SYMBOL AND pv.MONTH_START = mr.MONTH_START
    LEFT JOIN 
        quarterly_growth qg
    ON 
        pv.SYMBOL = qg.SYMBOL
)
SELECT
    SYMBOL,
    MONTH_START,
    PRICE_VOLATILITY,
    MONTHLY_RETURN,
    QUARTER_START,
    AVG_ADJ_CLOSE,
    PREV_AVG_ADJ_CLOSE,
    GROWTH_RATE
FROM 
    combined_data
WHERE 
    row_num = 1  -- Keeps only the first occurrence of each combination
ORDER BY 
    SYMBOL, 
    MONTH_START
