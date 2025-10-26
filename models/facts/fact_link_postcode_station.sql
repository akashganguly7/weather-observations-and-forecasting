-- Fact table for spatial linking between postal codes and weather stations
-- This model creates the spatial relationships using PostGIS distance calculations

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['postal_area_id'], 'unique': True},
        {'columns': ['station_id'], 'type': 'btree'},
        {'columns': ['created_at'], 'type': 'btree'}
    ]
) }}

SELECT 
    p.id AS postal_area_id,
    s.id AS station_id,
    now() AS created_at
FROM {{ ref('dim_postal_area') }} p
CROSS JOIN LATERAL (
    SELECT s.id
    FROM {{ ref('dim_station') }} s
    WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
    ORDER BY ST_Distance(p.geometry, ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326))
    LIMIT 1
) s
WHERE p.plz IS NOT NULL
