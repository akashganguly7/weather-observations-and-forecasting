-- Station dimension table
-- This model promotes staging station data to the dimensions schema

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['wmo_station_id'], 'unique': True},
        {'columns': ['geometry'], 'type': 'gist'}
    ]
) }}

SELECT 
    ROW_NUMBER() OVER (ORDER BY wmo_station_id) as id,
    wmo_station_id,
    station_name,
    lat,
    lon,
    geometry,
    properties,
    record_source,
    load_dts
FROM {{ ref('stg_dim_station') }}
