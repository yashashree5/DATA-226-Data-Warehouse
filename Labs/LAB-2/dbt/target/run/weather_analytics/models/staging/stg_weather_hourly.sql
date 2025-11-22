
  create or replace   view USER_DB_PUMA.ANALYTICS_staging.stg_weather_hourly
  
   as (
    -- Staging model to clean and prepare raw weather data


WITH source_data AS (
    SELECT
        FORECAST_DATE,
        MAX_TEMP,
        MIN_TEMP,
        PRECIPITATION,
        WIND_SPEED
    FROM USER_DB_PUMA.raw.weather_history
)

SELECT
    FORECAST_DATE,
    MAX_TEMP AS max_temperature_c,
    MIN_TEMP AS min_temperature_c,
    PRECIPITATION AS precipitation_mm,
    WIND_SPEED AS wind_speed_kmh,
    -- Derived fields
    (MAX_TEMP + MIN_TEMP) / 2 AS avg_temperature_c,
    MAX_TEMP - MIN_TEMP AS temperature_range_c,
    CASE 
        WHEN PRECIPITATION > 0 THEN TRUE 
        ELSE FALSE 
    END AS is_rainy_day,
    CASE
        WHEN MAX_TEMP < 0 THEN 'Freezing'
        WHEN MAX_TEMP BETWEEN 0 AND 10 THEN 'Cold'
        WHEN MAX_TEMP BETWEEN 10 AND 20 THEN 'Mild'
        WHEN MAX_TEMP BETWEEN 20 AND 30 THEN 'Warm'
        ELSE 'Hot'
    END AS temperature_category
FROM source_data
WHERE FORECAST_DATE IS NOT NULL
  );

