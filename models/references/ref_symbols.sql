-- Assuming you are creating a reference table for stock symbols with assigned IDs

WITH symbol_data AS (
    SELECT 'IOT' AS symbol
    UNION ALL
    SELECT 'SHAK'
    UNION ALL
    SELECT 'TSN'
    UNION ALL
    SELECT 'WRB'
    UNION ALL
    SELECT 'NVDA'
)

SELECT
    symbol,
    ROW_NUMBER() OVER (ORDER BY symbol) AS symbol_id  -- Assign unique IDs to each symbol
FROM symbol_data
