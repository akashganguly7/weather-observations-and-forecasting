import requests
from utils.db import get_psycopg_conn, get_sqlalchemy_engine, ensure_postgis_extension, ensure_station_schema
from utils.config import DIMENSIONS_SCHEMA
from utils.logger import logger
from sqlalchemy import text
import json
import io

from configparser import ConfigParser
from utils.config import BRIGHTSKY_BASE

def download_wmo_stations(url: str):
    """
    Download WMO station list from DWD and parse into station records.
    Returns list of station dictionaries.
    """
    logger.info("Downloading WMO station list from %s", url)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    
    stations = []
    lines = r.text.strip().split('\n')
    
    # Skip header line
    for line in lines[1:]:
        if not line.strip():
            continue
            
        # Parse semicolon-separated format: WMO-StationID;StationName;Latitude;Longitude;Height;Country
        parts = line.split(';')
        if len(parts) >= 6:
            try:
                wmo_id = parts[0].strip()
                station_name = parts[1].strip()
                lat = float(parts[2].strip()) if parts[2].strip() else None
                lon = float(parts[3].strip()) if parts[3].strip() else None
                height = float(parts[4].strip()) if parts[4].strip() else None
                country = parts[5].strip()
                
                # Skip if no coordinates
                if lat is None or lon is None:
                    continue
                    
                station = {
                    'station_id': wmo_id,
                    'station_name': station_name,
                    'wmo_station_id': wmo_id,
                    'lat': lat,
                    'lon': lon,
                    'height': height,
                    'country': country,
                    'record_source': 'wmo_dwd'
                }
                stations.append(station)
                
            except (ValueError, IndexError) as e:
                logger.warning("Failed to parse station line: %s - %s", line, e)
                continue
    
    logger.info("Parsed %d WMO stations", len(stations))
    return stations


def upsert_stations(sources, record_source="brightsky"):
    """
    upsert list of station dicts into dim_station using SCD Type 1 pattern
    expects fields: wmo_station_id, station_name, lat, lon
    """
    conn = get_psycopg_conn()
    cur = conn.cursor()
    sql = f"""
    INSERT INTO {DIMENSIONS_SCHEMA}.dim_station (station_name, wmo_station_id, lat, lon, geometry, properties, record_source)
    VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s),4326), %s::jsonb, %s)
    ON CONFLICT (wmo_station_id) DO UPDATE SET
      station_name = EXCLUDED.station_name,
      lat = EXCLUDED.lat,
      lon = EXCLUDED.lon,
      geometry = EXCLUDED.geometry,
      properties = EXCLUDED.properties,
      record_source = EXCLUDED.record_source,
      load_dts = now();
    """
    count = 0
    for s in sources:
        wmo = s.get('wmo_station_id')
        name = s.get('station_name') or s.get('name') or None
        lat = s.get('lat') or s.get('latitude') or None
        lon = s.get('lon') or s.get('longitude') or None
        props = s
        if lat is None or lon is None or wmo is None:
            # skip if no coordinates or WMO ID
            continue
        try:
            cur.execute(sql, (name, wmo, lat, lon, lon, lat, json.dumps(props), record_source))
            conn.commit()  # Commit after each successful row
            count += 1
        except Exception as e:
            logger.warning("Failed to upsert station %s: %s", wmo, e)
            conn.rollback()  # Rollback the failed transaction
    cur.close()
    conn.close()
    logger.info("Upserted %d stations", count)
    return count

def ingest_wmo_stations(url: str):
    """
    Download and ingest WMO station data from DWD.
    """
    logger.info("Ingesting WMO stations from %s", url)
    
    # Ensure required schemas exist
    ensure_postgis_extension()
    ensure_station_schema()
    
    stations = download_wmo_stations(url)
    return upsert_stations(stations, record_source="wmo_dwd")

