-- Unit tests for stg_dim_postal_area model
-- This test combines multiple validation checks using UNION ALL

with postal_area_checks as (
  -- Test 1: All postal areas have valid geometry
  select 'null_geometry' as check_name, *
  from {{ ref('stg_dim_postal_area') }}
  where geometry is null
  
  union all
  
  -- Test 2: All postal areas have postal code
  select 'null_postal_codes' as check_name, *
  from {{ ref('stg_dim_postal_area') }}
  where plz is null
  
  union all
  
  -- Test 3: Postal codes have valid format (assuming 5-digit German postal codes)
  select 'invalid_postal_code_format' as check_name, *
  from {{ ref('stg_dim_postal_area') }}
  where plz is not null 
     and (length(plz::text) != 5 or plz::text !~ '^[0-9]{5}$')
  
  union all
  
  -- Test 4: Postal codes are within valid German range (01000-99999)
  select 'postal_codes_out_of_range' as check_name, *
  from {{ ref('stg_dim_postal_area') }}
  where plz is not null and (plz::integer < 1000 or plz::integer > 99999)
)

select *
from postal_area_checks
