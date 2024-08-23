WITH price_changes AS (
  SELECT
    symbol,
    DATE_TRUNC('quarter', _FIVETRAN_SYNCED) AS quarter,
    (MAX(_ADJ_CLOSE_IOT_) - MIN(_ADJ_CLOSE_IOT_)) / MIN(_ADJ_CLOSE_IOT_) AS growth_rate
  FROM {{ source('raw', 'stock_prices') }}
  GROUP BY symbol, quarter
)

SELECT
  symbol,
  quarter,
  growth_rate
FROM price_changes
