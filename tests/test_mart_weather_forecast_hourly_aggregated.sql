-- Unit tests for mart_weather_forecast_hourly_aggregated model
-- This test combines multiple validation checks using UNION ALL

with aggregated_checks as (
  -- Test 1: Station count is positive
  select 'non_positive_station_count' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where station_count <= 0
  
  union all
  
  -- Test 2: Temperature min is less than or equal to temperature max
  select 'temperature_min_greater_than_max' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where temperature_min is not null and temperature_max is not null and temperature_min > temperature_max
  
  union all
  
  -- Test 3: Precipitation sum is non-negative
  select 'negative_precipitation_sum' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where precipitation_sum is not null and precipitation_sum < 0
  
  union all
  
  -- Test 4: Wind speed max is non-negative
  select 'negative_wind_speed_max' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where wind_speed_max is not null and wind_speed_max < 0
  
  union all
  
  -- Test 5: Pressure min is less than or equal to pressure max
  select 'pressure_min_greater_than_max' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where pressure_msl_min is not null and pressure_msl_max is not null and pressure_msl_min > pressure_msl_max
  
  union all
  
  -- Test 6: Humidity min is less than or equal to humidity max
  select 'humidity_min_greater_than_max' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where relative_humidity_min is not null and relative_humidity_max is not null and relative_humidity_min > relative_humidity_max
  
  union all
  
  -- Test 7: Visibility min is less than or equal to visibility max
  select 'visibility_min_greater_than_max' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where visibility_min is not null and visibility_max is not null and visibility_min > visibility_max
  
  union all
  
  -- Test 8: Solar max is non-negative
  select 'negative_solar_max' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where solar_max is not null and solar_max < 0
  
  union all
  
  -- Test 9: Sunshine max is non-negative
  select 'negative_sunshine_max' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where sunshine_max is not null and sunshine_max < 0
  
  union all
  
  -- Test 10: Wind gust speed max is non-negative
  select 'negative_wind_gust_speed_max' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where wind_gust_speed_max is not null and wind_gust_speed_max < 0
  
  union all
  
  -- Test 11: Outlier count is non-negative
  select 'negative_outlier_count' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where outlier_count is not null and outlier_count < 0
  
  union all
  
  -- Test 12: All postal codes have valid format (assuming 5-digit German postal codes)
  select 'invalid_postal_code_format' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where plz is not null and (length(plz::text) != 5 or plz::text !~ '^[0-9]{5}$')
  
  union all
  
  -- Test 13: Average confidence score is between 0 and 1
  select 'confidence_score_avg_out_of_range' as check_name, *
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where confidence_score_avg is not null and (confidence_score_avg < 0 or confidence_score_avg > 1)
)

select *
from aggregated_checks
