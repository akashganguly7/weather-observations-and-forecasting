-- Aggregated weather observed data by postal code
-- This model creates aggregated statistics for weather observed data grouped by postal code and timestamp

{{ config(materialized='materialized_view') }}

SELECT 
    plz,
    timestamp_utc,
    
    -- Temperature statistics
    AVG(temperature) as temperature_avg,
    MIN(temperature) as temperature_min,
    MAX(temperature) as temperature_max,
    STDDEV(temperature) as temperature_std,
    
    -- Precipitation statistics
    SUM(precipitation) as precipitation_sum,
    AVG(precipitation) as precipitation_avg,
    MAX(precipitation) as precipitation_max,
    STDDEV(precipitation) as precipitation_std,
    
    -- Wind statistics
    AVG(wind_speed) as wind_speed_avg,
    MAX(wind_speed) as wind_speed_max,
    STDDEV(wind_speed) as wind_speed_std,
    
    -- Wind direction (circular average)
    CASE 
        WHEN AVG(SIN(RADIANS(wind_direction))) >= 0 
        THEN DEGREES(ATAN2(
            AVG(SIN(RADIANS(wind_direction))),
            AVG(COS(RADIANS(wind_direction)))
        ))
        ELSE DEGREES(ATAN2(
            AVG(SIN(RADIANS(wind_direction))),
            AVG(COS(RADIANS(wind_direction)))
        )) + 360
    END as wind_direction_avg,
    STDDEV(wind_direction) as wind_direction_std,
    
    -- Pressure statistics
    AVG(pressure_msl) as pressure_msl_avg,
    MIN(pressure_msl) as pressure_msl_min,
    MAX(pressure_msl) as pressure_msl_max,
    STDDEV(pressure_msl) as pressure_msl_std,
    
    -- Humidity statistics
    AVG(relative_humidity) as relative_humidity_avg,
    MIN(relative_humidity) as relative_humidity_min,
    MAX(relative_humidity) as relative_humidity_max,
    STDDEV(relative_humidity) as relative_humidity_std,
    
    -- Cloud cover statistics
    AVG(cloud_cover) as cloud_cover_avg,
    STDDEV(cloud_cover) as cloud_cover_std,
    
    -- Visibility statistics
    AVG(visibility) as visibility_avg,
    MIN(visibility) as visibility_min,
    MAX(visibility) as visibility_max,
    STDDEV(visibility) as visibility_std,
    
    -- Dew point statistics
    AVG(dew_point) as dew_point_avg,
    STDDEV(dew_point) as dew_point_std,
    
    -- Solar statistics
    AVG(solar) as solar_avg,
    MAX(solar) as solar_max,
    STDDEV(solar) as solar_std,
    
    -- Sunshine statistics
    AVG(sunshine) as sunshine_avg,
    MAX(sunshine) as sunshine_max,
    STDDEV(sunshine) as sunshine_std,
    
    -- Wind gust statistics
    AVG(wind_gust_speed) as wind_gust_speed_avg,
    MAX(wind_gust_speed) as wind_gust_speed_max,
    STDDEV(wind_gust_speed) as wind_gust_speed_std,
    
    -- Wind gust direction (circular average)
    CASE 
        WHEN AVG(SIN(RADIANS(wind_gust_direction))) >= 0 
        THEN DEGREES(ATAN2(
            AVG(SIN(RADIANS(wind_gust_direction))),
            AVG(COS(RADIANS(wind_gust_direction)))
        ))
        ELSE DEGREES(ATAN2(
            AVG(SIN(RADIANS(wind_gust_direction))),
            AVG(COS(RADIANS(wind_gust_direction)))
        )) + 360
    END as wind_gust_direction_avg,
    STDDEV(wind_gust_direction) as wind_gust_direction_std,
    
    -- Precipitation probability statistics
    AVG(precipitation_probability) as precipitation_probability_avg,
    STDDEV(precipitation_probability) as precipitation_probability_std,
    
    AVG(precipitation_probability_6h) as precipitation_probability_6h_avg,
    STDDEV(precipitation_probability_6h) as precipitation_probability_6h_std,
    
    -- Data quality metrics
    COUNT(*) as station_count,
    COUNT(CASE WHEN temperature IS NOT NULL THEN 1 END)::DOUBLE PRECISION / COUNT(*) as data_completeness,
    COUNT(CASE WHEN outlier_flag = 'outlier' THEN 1 END) as outlier_count,
    AVG(confidence_score) as confidence_score_avg,
    
    -- Most common weather conditions
    MODE() WITHIN GROUP (ORDER BY weather_condition) as weather_condition_mode,
    MODE() WITHIN GROUP (ORDER BY weather_icon) as weather_icon_mode,
    
    -- Metadata
    'aggregated_matview' as record_source,
    MAX(load_dts) as load_dts
    
FROM {{ ref('fact_weather_observed_hourly') }}
WHERE temperature IS NOT NULL
GROUP BY plz, timestamp_utc
ORDER BY plz, timestamp_utc
