-- Unit tests for fact_link_postcode_station model
-- This test combines multiple validation checks using UNION ALL

with link_table_checks as (
  -- Test 1: All records have a valid station_id
  select 'null_station_ids' as check_name, *
  from {{ ref('fact_link_postcode_station') }}
  where station_id is null
  
  union all
  
  -- Test 2: All records have a valid postal_area_id
  select 'null_postal_area_ids' as check_name, *
  from {{ ref('fact_link_postcode_station') }}
  where postal_area_id is null
  
  union all
  
  -- Test 3: All station_ids exist in the station dimension table
  select 'invalid_station_ids' as check_name, lps.*
  from {{ ref('fact_link_postcode_station') }} lps
  left join {{ ref('dim_station') }} s on lps.station_id = s.id
  where s.id is null
  
  union all
  
  -- Test 4: All postal_area_ids exist in the postal area dimension table
  select 'invalid_postal_area_ids' as check_name, lps.*
  from {{ ref('fact_link_postcode_station') }} lps
  left join {{ ref('dim_postal_area') }} p on lps.postal_area_id = p.id
  where p.id is null
  
  union all
  
  -- Test 5: All records have a created_at timestamp
  select 'null_created_at' as check_name, *
  from {{ ref('fact_link_postcode_station') }}
  where created_at is null
)

select *
from link_table_checks
