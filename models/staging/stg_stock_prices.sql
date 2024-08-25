WITH base_data AS (
    SELECT
        ROW_ID,
        SYNCED_TIMESTAMP,
        HIGH_IOT,
        VOLUME_SHAK,
        ADJ_CLOSE_TSN,
        LOW_WRB,
        OPEN_NVDA,
        LOW_NVDA,
        CLOSE_WRB,
        OPEN_SHAK,
        CLOSE_TSN,
        HIGH_NVDA,
        VOLUME_NVDA,
        LOW_SHAK,
        LOW_TSN,
        HIGH_WRB,
        HIGH_SHAK,
        VOLUME_IOT,
        HIGH_TSN,
        OPEN_WRB,
        OPEN_TSN,
        ADJ_CLOSE_NVDA,
        LOW_IOT,
        CLOSE_IOT,
        ADJ_CLOSE_WRB,
        CLOSE_NVDA,
        ADJ_CLOSE_SHAK,
        CLOSE_SHAK,
        OPEN_IOT,
        VOLUME_TSN,
        VOLUME_WRB,
        ROW_NUMBER() OVER (ORDER BY SYNCED_TIMESTAMP DESC) AS row_num  -- Ensure rows are ordered correctly
    FROM {{ source('stocklist', 'raw_data') }}  -- Assuming 'raw_data' is your source
),

date_assigned_data AS (
    SELECT
        ROW_ID,
        HIGH_IOT,
        VOLUME_SHAK,
        ADJ_CLOSE_TSN,
        LOW_WRB,
        OPEN_NVDA,
        LOW_NVDA,
        CLOSE_WRB,
        OPEN_SHAK,
        CLOSE_TSN,
        HIGH_NVDA,
        VOLUME_NVDA,
        LOW_SHAK,
        LOW_TSN,
        HIGH_WRB,
        HIGH_SHAK,
        VOLUME_IOT,
        HIGH_TSN,
        OPEN_WRB,
        OPEN_TSN,
        ADJ_CLOSE_NVDA,
        LOW_IOT,
        CLOSE_IOT,
        ADJ_CLOSE_WRB,
        CLOSE_NVDA,
        ADJ_CLOSE_SHAK,
        CLOSE_SHAK,
        OPEN_IOT,
        VOLUME_TSN,
        VOLUME_WRB,
        -- Assign the correct date based on row_num
        DATEADD(day, -(row_num - 1), '2024-08-07') AS date
    FROM base_data
)

SELECT
    *
FROM date_assigned_data
ORDER BY date DESC
