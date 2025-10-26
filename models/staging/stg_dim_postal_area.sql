-- Postal area dimension staging model
-- This model transforms raw postal area data into clean, standardized format with deduplication

{{ config(materialized='view') }}

WITH deduplicated_postal_areas AS (
    SELECT 
        plz,
        wkt,
        geometry,
        record_source,
        load_dts,
        ROW_NUMBER() OVER (
            PARTITION BY plz 
            ORDER BY load_dts DESC
        ) as rn
    FROM {{ source('raw', 'postal_area_raw') }}
    WHERE plz IS NOT NULL
      AND geometry IS NOT NULL
)

SELECT 
    plz,
    wkt,
    geometry,
    record_source,
    load_dts
FROM deduplicated_postal_areas
WHERE rn = 1
