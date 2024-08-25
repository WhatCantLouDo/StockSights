-- models/final/stock_summary.sql
{{ config(
    materialized='table'
) }}

with monthly_returns as (
    -- Reference the intermediate model for monthly returns
    select
        symbol,
        month_start,
        monthly_return
    from {{ ref('int_monthly_returns') }}
),

quarterly_growth as (
    -- Reference the intermediate model for quarterly growth
    select
        symbol,
        quarter_start,
        growth_rate
    from {{ ref('int_quarterly_growth') }}
),

price_volatility as (
    -- Reference the intermediate model for price volatility
    select
        symbol,
        month_start,
        price_volatility
    from {{ ref('int_price_volatility') }}
),

-- Combine the data into a final summary table
final_summary as (
    select
        m.symbol,
        m.month_start as period,
        m.monthly_return,
        q.growth_rate,
        p.price_volatility
    from monthly_returns m
    left join quarterly_growth q
        on m.symbol = q.symbol
        and date_trunc('quarter', m.month_start) = q.quarter_start
    left join price_volatility p
        on m.symbol = p.symbol
        and m.month_start = p.month_start
)

select
    symbol,
    period,
    monthly_return,
    growth_rate,
    price_volatility
from final_summary
order by symbol, period
