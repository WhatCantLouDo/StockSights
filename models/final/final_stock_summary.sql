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
corrected_quarterly_growth AS (
    -- Only select the closest QUARTER_START before or equal to MONTH_START
    SELECT
        qg.SYMBOL,
        qg.QUARTER_START,
        qg.AVG_ADJ_CLOSE,
        qg.PREV_AVG_ADJ_CLOSE,
        qg.GROWTH_RATE,
        ROW_NUMBER() OVER (
            PARTITION BY 
                qg.SYMBOL, 
                pv.MONTH_START 
            ORDER BY 
                qg.QUARTER_START DESC
        ) AS row_num
    FROM 
        quarterly_growth qg
    JOIN
        price_volatility pv
    ON 
        qg.SYMBOL = pv.SYMBOL 
        AND qg.QUARTER_START <= pv.MONTH_START
),
final_data AS (
    SELECT
        pv.SYMBOL,
        pv.MONTH_START,
        pv.PRICE_VOLATILITY,
        mr.MONTHLY_RETURN,
        cqg.QUARTER_START,
        cqg.AVG_ADJ_CLOSE,
        cqg.PREV_AVG_ADJ_CLOSE,
        cqg.GROWTH_RATE
    FROM 
        price_volatility pv
    LEFT JOIN 
        monthly_returns mr
    ON 
        pv.SYMBOL = mr.SYMBOL AND pv.MONTH_START = mr.MONTH_START
    LEFT JOIN 
        corrected_quarterly_growth cqg
    ON 
        pv.SYMBOL = cqg.SYMBOL AND cqg.row_num = 1
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
    final_data
ORDER BY 
    SYMBOL, 
    MONTH_START
