-- Daily data optimized for visualizations
-- models/marts/weather_daily_viz.sql

SELECT
    FORECAST_DATE,
    
    -- Temperature
    MAX_TEMP,
    MIN_TEMP,
    AVG_TEMP,
    
    -- Precipitation
    PRECIPITATION,
    CASE WHEN PRECIPITATION > 0 THEN 'Rainy' ELSE 'Dry' END AS RAIN_STATUS,
    
    -- Wind
    WIND_SPEED,
    
    -- Date parts for filtering
    YEAR,
    MONTH,
    EXTRACT(DAY FROM FORECAST_DATE) AS DAY,
    TO_CHAR(FORECAST_DATE, 'Mon') AS MONTH_NAME,
    DAYNAME(FORECAST_DATE) AS DAY_OF_WEEK,
    
    -- Season
    CASE
        WHEN MONTH IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS SEASON,
    
    -- Categories for color coding
    CASE
        WHEN AVG_TEMP < 10 THEN 'Cold'
        WHEN AVG_TEMP < 20 THEN 'Mild'
        ELSE 'Warm'
    END AS TEMP_CATEGORY,
    
    -- 7-day moving average for smooth trend lines
    AVG(AVG_TEMP) OVER (
        ORDER BY FORECAST_DATE 
        ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
    ) AS TEMP_7DAY_MA

FROM USER_DB_PUMA.ANALYTICS.stg_weather
ORDER BY FORECAST_DATE