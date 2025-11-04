WITH user_session_data AS (
    SELECT
        userId,
        sessionId,
        channel
    FROM {{ source('raw', 'user_session_channel') }}
    WHERE sessionId IS NOT NULL
)

SELECT
    *
FROM user_session_data
