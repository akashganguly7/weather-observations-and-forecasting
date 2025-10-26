-- Unit tests for int_link_postcode_station model
-- This test combines multiple validation checks using UNION ALL

with link_table_checks as (
  -- Test 1: All postal codes have a valid station_id
  select 'null_station_ids' as check_name, *
  from {{ ref('int_link_postcode_station') }}
  where station_id is null
  
  union all
  
  -- Test 2: All station_ids exist in the station dimension table
  select 'invalid_station_ids' as check_name, lps.*
  from {{ ref('int_link_postcode_station') }} lps
  left join {{ ref('stg_dim_station') }} s on lps.station_id = s.id
  where s.id is null
  
  union all
  
  -- Test 3: All postal codes exist in the postal area dimension table
  select 'invalid_postal_codes' as check_name, lps.*
  from {{ ref('int_link_postcode_station') }} lps
  left join {{ ref('stg_dim_postal_area') }} p on lps.plz = p.plz
  where p.plz is null
  
  union all
  
  -- Test 4: No null postal codes
  select 'null_postal_codes' as check_name, *
  from {{ ref('int_link_postcode_station') }}
  where plz is null
)

select *
from link_table_checks
