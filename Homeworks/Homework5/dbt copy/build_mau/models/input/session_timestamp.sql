WITH session_data AS (
    SELECT
        sessionId,
        ts
    FROM {{ source('raw', 'session_timestamp') }}
)

SELECT
    *
FROM session_data
