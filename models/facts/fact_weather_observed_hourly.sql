-- Fact table for weather observation data at postal code level
-- This model joins staging observation data with dimensions and spatial linking to get postal code level data

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['plz', 'timestamp_utc'], 'type': 'btree'},
        {'columns': ['wmo_station_id', 'timestamp_utc'], 'type': 'btree'},
        {'columns': ['timestamp_utc'], 'type': 'btree'}
    ]
) }}

SELECT 
    sf.wmo_station_id,
    ds.id as station_id,
    pa.plz,
    sf.timestamp_utc,
    sf.temperature,
    sf.precipitation,
    sf.wind_speed,
    sf.wind_direction,
    sf.pressure_msl,
    sf.relative_humidity,
    sf.cloud_cover,
    sf.visibility,
    sf.dew_point,
    sf.solar,
    sf.sunshine,
    sf.wind_gust_speed,
    sf.wind_gust_direction,
    sf.precipitation_probability,
    sf.precipitation_probability_6h,
    sf.weather_condition,
    sf.weather_icon,
    sf.bright_sky_source_mapping,
    sf.record_source,
    sf.load_dts,
    sf.outlier_flag,
    sf.confidence_score
FROM {{ ref('stg_weather_observed_hourly') }} sf
JOIN {{ ref('dim_station') }} ds ON ds.wmo_station_id = sf.wmo_station_id
JOIN {{ ref('fact_link_postcode_station') }} lps ON lps.station_id = ds.id
JOIN {{ ref('dim_postal_area') }} pa ON pa.id = lps.postal_area_id
