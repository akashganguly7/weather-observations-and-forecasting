-- Integration test for data integrity across models
-- This test combines multiple data integrity checks using UNION ALL

with integrity_checks as (
  -- Test 1: All weather observed records have valid station IDs
  select 'weather_observed_invalid_station_ids' as check_name, wo.wmo_station_id, wo.timestamp_utc
  from {{ ref('stg_weather_observed_hourly') }} wo
  left join {{ ref('stg_dim_station') }} s on wo.wmo_station_id = s.wmo_station_id
  where s.wmo_station_id is null
  
  union all
  
  -- Test 2: All weather forecast records have valid station IDs
  select 'weather_forecast_invalid_station_ids' as check_name, wf.wmo_station_id, wf.timestamp_utc
  from {{ ref('stg_weather_forecast_hourly') }} wf
  left join {{ ref('stg_dim_station') }} s on wf.wmo_station_id = s.wmo_station_id
  where s.wmo_station_id is null
  
  union all
  
  -- Test 3: All aggregated records have valid postal codes
  select 'aggregated_invalid_postal_codes' as check_name, ma.plz, ma.timestamp_utc
  from {{ ref('mart_weather_forecast_hourly_aggregated') }} ma
  left join {{ ref('dim_postal_area') }} pa on ma.plz = pa.plz
  where pa.plz is null
  
  union all
  
  -- Test 4: All postal area IDs in link table exist in postal area dimension
  select 'link_table_invalid_postal_area_ids' as check_name, lps.postal_area_id, lps.station_id
  from {{ ref('facts_link_postcode_station') }} lps
  left join {{ ref('dim_postal_area') }} pa on lps.postal_area_id = pa.id
  where pa.id is null
  
  union all
  
  -- Test 5: All station IDs in link table exist in station dimension
  select 'link_table_invalid_station_ids' as check_name, lps.station_id, lps.postal_area_id
  from {{ ref('facts_link_postcode_station') }} lps
  left join {{ ref('dim_station') }} s on lps.station_id = s.id
  where s.id is null
  
  union all
  
  -- Test 6: Weather data timestamps are reasonable (not in the future beyond forecast horizon)
  select 'forecast_timestamps_too_far_future' as check_name, wmo_station_id, timestamp_utc
  from {{ ref('stg_weather_forecast_hourly') }}
  where timestamp_utc > current_timestamp + interval '7 days'
  
  union all
  
  -- Test 7: Weather observed data timestamps are not too far in the past (data freshness)
  select 'observed_timestamps_too_old' as check_name, wmo_station_id, timestamp_utc
  from {{ ref('stg_weather_observed_hourly') }}
  where timestamp_utc < current_timestamp - interval '30 days'
  
  union all
  
  -- Test 8: Aggregated data has reasonable station counts (not too many or too few)
  select 'aggregated_unreasonable_station_counts' as check_name, plz, timestamp_utc
  from {{ ref('mart_weather_forecast_hourly_aggregated') }}
  where station_count < 1 or station_count > 100
)

select *
from integrity_checks
