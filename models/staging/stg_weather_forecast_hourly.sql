-- Weather forecast data transformation with outlier handling
-- This model extracts and standardizes weather forecast parameters from raw JSON data

{{ config(
    materialized='incremental',
    unique_key=['wmo_station_id', 'timestamp_utc'],
    on_schema_change='append_new_columns'
) }}

WITH deduplicated_weather AS (
    -- Remove duplicates, keep latest record per station-timestamp
    SELECT DISTINCT ON (wmo_station_id, timestamp_utc) *
    FROM {{ source('raw', 'weather_hourly_forecast_raw') }}
    WHERE raw->>'temperature' IS NOT NULL 
    {% if is_incremental() %}
        AND load_dts > (SELECT MAX(load_dts) FROM {{ this }})
    {% endif %}
    ORDER BY wmo_station_id, timestamp_utc, load_dts DESC
),

weather_standardized AS (
    -- Extract all weather parameters from raw JSON and apply outlier handling
    SELECT 
        dw.wmo_station_id,
        dw.timestamp_utc,
        
        -- Core weather measurements with outlier handling
        CASE 
            WHEN (dw.raw->>'temperature')::DOUBLE PRECISION < -50 OR (dw.raw->>'temperature')::DOUBLE PRECISION > 50 THEN NULL
            ELSE (dw.raw->>'temperature')::DOUBLE PRECISION 
        END as temperature,
        
        CASE 
            WHEN (dw.raw->>'precipitation')::DOUBLE PRECISION < 0 OR (dw.raw->>'precipitation')::DOUBLE PRECISION > 200 THEN NULL
            ELSE (dw.raw->>'precipitation')::DOUBLE PRECISION 
        END as precipitation,
        
        CASE 
            WHEN (dw.raw->>'wind_speed')::DOUBLE PRECISION < 0 OR (dw.raw->>'wind_speed')::DOUBLE PRECISION > 100 THEN NULL
            ELSE (dw.raw->>'wind_speed')::DOUBLE PRECISION 
        END as wind_speed,
        
        CASE 
            WHEN (dw.raw->>'wind_direction')::DOUBLE PRECISION < 0 OR (dw.raw->>'wind_direction')::DOUBLE PRECISION > 360 THEN NULL
            ELSE (dw.raw->>'wind_direction')::DOUBLE PRECISION 
        END as wind_direction,
        
        -- Additional weather parameters with outlier handling
        CASE 
            WHEN (dw.raw->>'pressure_msl')::DOUBLE PRECISION < 800 OR (dw.raw->>'pressure_msl')::DOUBLE PRECISION > 1100 THEN NULL
            ELSE (dw.raw->>'pressure_msl')::DOUBLE PRECISION 
        END as pressure_msl,
        
        CASE 
            WHEN (dw.raw->>'relative_humidity')::DOUBLE PRECISION < 0 OR (dw.raw->>'relative_humidity')::DOUBLE PRECISION > 100 THEN NULL
            ELSE (dw.raw->>'relative_humidity')::DOUBLE PRECISION 
        END as relative_humidity,
        
        CASE 
            WHEN (dw.raw->>'cloud_cover')::DOUBLE PRECISION < 0 OR (dw.raw->>'cloud_cover')::DOUBLE PRECISION > 100 THEN NULL
            ELSE (dw.raw->>'cloud_cover')::DOUBLE PRECISION 
        END as cloud_cover,
        
        CASE 
            WHEN (dw.raw->>'visibility')::DOUBLE PRECISION < 0 OR (dw.raw->>'visibility')::DOUBLE PRECISION > 100000 THEN NULL
            ELSE (dw.raw->>'visibility')::DOUBLE PRECISION 
        END as visibility,
        
        CASE 
            WHEN (dw.raw->>'dew_point')::DOUBLE PRECISION < -50 OR (dw.raw->>'dew_point')::DOUBLE PRECISION > 50 THEN NULL
            ELSE (dw.raw->>'dew_point')::DOUBLE PRECISION 
        END as dew_point,
        
        CASE 
            WHEN (dw.raw->>'solar')::DOUBLE PRECISION < 0 OR (dw.raw->>'solar')::DOUBLE PRECISION > 1 THEN NULL
            ELSE (dw.raw->>'solar')::DOUBLE PRECISION 
        END as solar,
        
        CASE 
            WHEN (dw.raw->>'sunshine')::DOUBLE PRECISION < 0 OR (dw.raw->>'sunshine')::DOUBLE PRECISION > 60 THEN NULL
            ELSE (dw.raw->>'sunshine')::DOUBLE PRECISION 
        END as sunshine,
        
        CASE 
            WHEN (dw.raw->>'wind_gust_speed')::DOUBLE PRECISION < 0 OR (dw.raw->>'wind_gust_speed')::DOUBLE PRECISION > 150 THEN NULL
            ELSE (dw.raw->>'wind_gust_speed')::DOUBLE PRECISION 
        END as wind_gust_speed,
        
        CASE 
            WHEN (dw.raw->>'wind_gust_direction')::DOUBLE PRECISION < 0 OR (dw.raw->>'wind_gust_direction')::DOUBLE PRECISION > 360 THEN NULL
            ELSE (dw.raw->>'wind_gust_direction')::DOUBLE PRECISION 
        END as wind_gust_direction,
        
        CASE 
            WHEN (dw.raw->>'precipitation_probability')::DOUBLE PRECISION < 0 OR (dw.raw->>'precipitation_probability')::DOUBLE PRECISION > 100 THEN NULL
            ELSE (dw.raw->>'precipitation_probability')::DOUBLE PRECISION 
        END as precipitation_probability,
        
        CASE 
            WHEN (dw.raw->>'precipitation_probability_6h')::DOUBLE PRECISION < 0 OR (dw.raw->>'precipitation_probability_6h')::DOUBLE PRECISION > 100 THEN NULL
            ELSE (dw.raw->>'precipitation_probability_6h')::DOUBLE PRECISION 
        END as precipitation_probability_6h,
        
        -- Weather condition and metadata (no outlier handling needed)
        dw.raw->>'condition' as weather_condition,
        dw.raw->>'icon' as weather_icon,
        
        -- Source information
        dw.bright_sky_source_mapping,
        dw.record_source,
        dw.load_dts,
        
        -- Quality flags
        CASE 
            WHEN (dw.raw->>'temperature')::DOUBLE PRECISION < -50 OR (dw.raw->>'temperature')::DOUBLE PRECISION > 50 THEN 'extreme_temp'
            WHEN (dw.raw->>'precipitation')::DOUBLE PRECISION < 0 OR (dw.raw->>'precipitation')::DOUBLE PRECISION > 200 THEN 'extreme_precip'
            WHEN (dw.raw->>'wind_speed')::DOUBLE PRECISION < 0 OR (dw.raw->>'wind_speed')::DOUBLE PRECISION > 100 THEN 'extreme_wind'
            WHEN (dw.raw->>'wind_direction')::DOUBLE PRECISION < 0 OR (dw.raw->>'wind_direction')::DOUBLE PRECISION > 360 THEN 'extreme_wind_dir'
            WHEN (dw.raw->>'pressure_msl')::DOUBLE PRECISION < 800 OR (dw.raw->>'pressure_msl')::DOUBLE PRECISION > 1100 THEN 'extreme_pressure'
            WHEN (dw.raw->>'relative_humidity')::DOUBLE PRECISION < 0 OR (dw.raw->>'relative_humidity')::DOUBLE PRECISION > 100 THEN 'extreme_humidity'
            WHEN (dw.raw->>'cloud_cover')::DOUBLE PRECISION < 0 OR (dw.raw->>'cloud_cover')::DOUBLE PRECISION > 100 THEN 'extreme_cloud_cover'
            WHEN (dw.raw->>'visibility')::DOUBLE PRECISION < 0 OR (dw.raw->>'visibility')::DOUBLE PRECISION > 100000 THEN 'extreme_visibility'
            WHEN (dw.raw->>'dew_point')::DOUBLE PRECISION < -50 OR (dw.raw->>'dew_point')::DOUBLE PRECISION > 50 THEN 'extreme_dew_point'
            WHEN (dw.raw->>'solar')::DOUBLE PRECISION < 0 OR (dw.raw->>'solar')::DOUBLE PRECISION > 1 THEN 'extreme_solar'
            WHEN (dw.raw->>'sunshine')::DOUBLE PRECISION < 0 OR (dw.raw->>'sunshine')::DOUBLE PRECISION > 60 THEN 'extreme_sunshine'
            WHEN (dw.raw->>'wind_gust_speed')::DOUBLE PRECISION < 0 OR (dw.raw->>'wind_gust_speed')::DOUBLE PRECISION > 150 THEN 'extreme_wind_gust'
            WHEN (dw.raw->>'wind_gust_direction')::DOUBLE PRECISION < 0 OR (dw.raw->>'wind_gust_direction')::DOUBLE PRECISION > 360 THEN 'extreme_wind_gust_dir'
            WHEN (dw.raw->>'precipitation_probability')::DOUBLE PRECISION < 0 OR (dw.raw->>'precipitation_probability')::DOUBLE PRECISION > 100 THEN 'extreme_precip_prob'
            WHEN (dw.raw->>'precipitation_probability_6h')::DOUBLE PRECISION < 0 OR (dw.raw->>'precipitation_probability_6h')::DOUBLE PRECISION > 100 THEN 'extreme_precip_prob_6h'
            ELSE NULL
        END as outlier_flag,
        
        -- Confidence score based on outlier detection
        CASE 
            WHEN (dw.raw->>'temperature')::DOUBLE PRECISION < -50 OR (dw.raw->>'temperature')::DOUBLE PRECISION > 50 
                 OR (dw.raw->>'precipitation')::DOUBLE PRECISION < 0 OR (dw.raw->>'precipitation')::DOUBLE PRECISION > 200
                 OR (dw.raw->>'wind_speed')::DOUBLE PRECISION < 0 OR (dw.raw->>'wind_speed')::DOUBLE PRECISION > 100
            THEN 0.3
            WHEN (dw.raw->>'temperature')::DOUBLE PRECISION IS NULL 
                 OR (dw.raw->>'precipitation')::DOUBLE PRECISION IS NULL 
                 OR (dw.raw->>'wind_speed')::DOUBLE PRECISION IS NULL
            THEN 0.5
            ELSE 1.0
        END as confidence_score
        
    FROM deduplicated_weather dw
)

SELECT 
    wmo_station_id, timestamp_utc, temperature, precipitation, wind_speed, wind_direction,
    pressure_msl, relative_humidity, cloud_cover, visibility, dew_point, solar, sunshine,
    wind_gust_speed, wind_gust_direction, precipitation_probability, precipitation_probability_6h,
    weather_condition, weather_icon, bright_sky_source_mapping, record_source, load_dts,
    outlier_flag, confidence_score
FROM weather_standardized
