-- Postal area dimension table
-- This model promotes staging postal area data to the dimensions schema

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['plz'], 'unique': True},
        {'columns': ['geometry'], 'type': 'gist'}
    ]
) }}

SELECT 
    ROW_NUMBER() OVER (ORDER BY plz) as id,
    plz,
    wkt,
    geometry,
    record_source,
    load_dts
FROM {{ ref('stg_dim_postal_area') }}
