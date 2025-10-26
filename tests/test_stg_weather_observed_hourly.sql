-- Unit tests for stg_weather_observed_hourly model
-- This test combines multiple validation checks using UNION ALL

with observed_checks as (
  -- Test 1: Temperature values are within reasonable range (-50 to 50°C)
  select 'temperature_out_of_range' as check_name, *
  from {{ ref('stg_weather_observed_hourly') }}
  where temperature is not null and (temperature < -50 or temperature > 50)
  
  union all
  
  -- Test 2: Precipitation values are non-negative
  select 'negative_precipitation' as check_name, *
  from {{ ref('stg_weather_observed_hourly') }}
  where precipitation is not null and precipitation < 0
  
  union all
  
  -- Test 3: Wind speed values are non-negative
  select 'negative_wind_speed' as check_name, *
  from {{ ref('stg_weather_observed_hourly') }}
  where wind_speed is not null and wind_speed < 0
  
  union all
  
  -- Test 4: Wind direction values are between 0 and 360 degrees
  select 'wind_direction_out_of_range' as check_name, *
  from {{ ref('stg_weather_observed_hourly') }}
  where wind_direction is not null and (wind_direction < 0 or wind_direction > 360)
  
  union all
  
  -- Test 5: Humidity values are between 0 and 100 percent
  select 'humidity_out_of_range' as check_name, *
  from {{ ref('stg_weather_observed_hourly') }}
  where relative_humidity is not null and (relative_humidity < 0 or relative_humidity > 100)
  
  union all
  
  -- Test 6: Pressure values are within reasonable range (800 to 1100 hPa)
  select 'pressure_out_of_range' as check_name, *
  from {{ ref('stg_weather_observed_hourly') }}
  where pressure_msl is not null and (pressure_msl < 800 or pressure_msl > 1100)
  
  union all
  
  -- Test 7: Confidence scores are between 0 and 1
  select 'confidence_score_out_of_range' as check_name, *
  from {{ ref('stg_weather_observed_hourly') }}
  where confidence_score is not null and (confidence_score < 0 or confidence_score > 1)
)

select *
from observed_checks
