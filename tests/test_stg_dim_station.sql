-- Unit tests for stg_dim_station model
-- This test combines multiple validation checks using UNION ALL

with station_checks as (
  -- Test 1: All stations have valid geometry
  select 'null_geometry' as check_name, *
  from {{ ref('stg_dim_station') }}
  where geometry is null
  
  union all
  
  -- Test 2: All stations have WMO station ID
  select 'null_wmo_station_id' as check_name, *
  from {{ ref('stg_dim_station') }}
  where wmo_station_id is null
  
  union all
  
  -- Test 3: All stations have station ID
  select 'null_station_id' as check_name, *
  from {{ ref('stg_dim_station') }}
  where id is null
  
  union all
  
  -- Test 4: WMO station IDs are numeric (assuming they should be)
  select 'invalid_wmo_station_id_format' as check_name, *
  from {{ ref('stg_dim_station') }}
  where wmo_station_id is not null and wmo_station_id::text !~ '^[0-9]+$'
)

select *
from station_checks
