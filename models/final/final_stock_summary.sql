with monthly_returns as (
    select
        symbol,
        month_start,
        coalesce(monthly_return, 0) as monthly_return,
        total_volume
    from {{ ref('int_monthly_returns') }}
),

quarterly_growth as (
    select
        symbol,
        quarter_start,
        coalesce(growth_rate, 0) as growth_rate
    from {{ ref('int_quarterly_growth') }}
),

price_volatility as (
    select
        symbol,
        month_start,
        coalesce(price_volatility, 0) as price_volatility
    from {{ ref('int_price_volatility') }}
),

final_summary as (
    select
        m.symbol,
        m.month_start as period,
        m.monthly_return,
        coalesce(q.growth_rate, 0) as growth_rate,
        coalesce(p.price_volatility, 0) as price_volatility,
        m.total_volume
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
    price_volatility,
    total_volume
from final_summary
order by symbol, period
