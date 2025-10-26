-- Station dimension staging model
-- This model transforms raw station data into clean, standardized format with deduplication

{{ config(materialized='view') }}

WITH deduplicated_stations AS (
    SELECT 
        wmo_station_id,
        station_name,
        lat,
        lon,
        ST_SetSRID(ST_MakePoint(lon, lat), 4326) as geometry,
        properties,
        record_source,
        load_dts,
        ROW_NUMBER() OVER (
            PARTITION BY wmo_station_id 
            ORDER BY load_dts DESC
        ) as rn
    FROM {{ source('raw', 'station_raw') }}
    WHERE wmo_station_id IS NOT NULL
      AND lat IS NOT NULL 
      AND lon IS NOT NULL
      AND station_name IS NOT NULL
)

SELECT 
    wmo_station_id,
    station_name,
    lat,
    lon,
    geometry,
    properties,
    record_source,
    load_dts
FROM deduplicated_stations
WHERE rn = 1
