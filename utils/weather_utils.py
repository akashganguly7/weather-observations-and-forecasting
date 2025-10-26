"""
Common utilities for weather data ingestion.
Shared functions for both observations and forecasts.
"""
import json
from datetime import datetime

import backoff
import requests
from psycopg2.extras import execute_values
from sqlalchemy import text

from utils.config import BRIGHTSKY_BASE
from utils.db import get_psycopg_conn, get_sqlalchemy_engine
from utils.config import DIMENSIONS_SCHEMA, RAW_SCHEMA, FACT_SCHEMA
from utils.logger import logger


@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException,), max_tries=5)
def fetch_observations_for_station_timestamp(station_ids, timestamp_str_from, timestamp_str_to):
    """
    Fetch observations for a specific station and timestamp.
    timestamp_str should be in format like '2023-08-07T23:00+02:00'
    """
    try:
        # BrightSky weather endpoint uses station param in some versions; adjust if required.
        params = {"wmo_station_id": station_ids, "date": timestamp_str_from, "last_date": timestamp_str_to}
        url = f"{BRIGHTSKY_BASE}/weather"

        logger.debug("Fetching observations for station %s at %s from %s", station_ids, timestamp_str_from, url)

        r = requests.get(url, params=params, timeout=30)

        # Handle 404 specifically - no data available for this station/timestamp
        if r.status_code == 404:
            logger.warning("No data available for station %s at %s (404)", station_ids, timestamp_str_from)
            # Return special marker to indicate no data
            return {"weather": [], "sources": [], "_no_data": True, "_station_id": station_ids,
                    "_timestamp_str": timestamp_str_from, "_api_url": url, "_http_status": r.status_code,
                    "_response_message": r.text}

        r.raise_for_status()

        response_data = r.json()

        # Log response details
        weather_count = len(response_data.get('weather', []))
        sources_count = len(response_data.get('sources', []))
        logger.debug("Received %d weather observations and %d sources for station %s",
                     weather_count, sources_count, station_ids)

        return response_data

    except requests.exceptions.Timeout:
        logger.error("Timeout fetching observations for station %s at %s", station_ids, timestamp_str_from)
        raise
    except requests.exceptions.RequestException as e:
        logger.error("Request error fetching observations for station %s at %s: %s",
                     station_ids, timestamp_str_from, str(e))
        raise
    except json.JSONDecodeError as e:
        logger.error("JSON decode error for station %s at %s: %s", station_ids, timestamp_str_from, str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error fetching observations for station %s at %s: %s",
                     station_ids, timestamp_str_from, str(e))
        raise


def parse_and_prepare( raw_response, record_source="brightsky_weather"):
    """
    Parse BrightSky API response and prepare records for insertion.
    raw_response should be the full API response dict with 'weather' key.
    """
    recs = []

    # Check if this is a no-data response
    if raw_response.get('_no_data'):
        # Return empty records but keep the no-data info for batch logging
        return recs, raw_response

    # Extract weather observations from the response
    weather_observations = raw_response.get('weather', [])
    bright_sky_sources = raw_response.get('sources', [])
    source_dict = {}
    # Convert into dict in order to fetch wmo_station_id later by bright sky source id
    for source in bright_sky_sources:
        source_dict[source['id']] = source

    for observation in weather_observations:
        ts = observation.get('timestamp')
        # normalize to python datetime
        try:
            if ts.endswith("Z"):
                ts = ts.replace("Z", "+00:00")
            ts_dt = datetime.fromisoformat(ts)
        except Exception:
            continue
        # Use provided load_dts or current timestamp

        recs.append((
            source_dict[observation.get("source_id")]['wmo_station_id'],
            ts_dt,
            json.dumps(observation),
            json.dumps(source_dict[observation.get("source_id")]),
            record_source
        ))
    return recs, None

def load_station_id_mapping(wmo_station_ids):
    """
    Load WMO station ID to internal ID mapping into memory for specific stations.
    Returns a dictionary mapping wmo_station_id -> internal_id
    """
    try:
        engine = get_sqlalchemy_engine()
        with engine.begin() as conn:
            # Only load mappings for the stations we're processing
            query = text(f"SELECT wmo_station_id, id FROM {DIMENSIONS_SCHEMA}.dim_station WHERE wmo_station_id = ANY(:station_ids)")
            results = conn.execute(query, {"station_ids": wmo_station_ids}).fetchall()
            
            mapping = {row[0]: row[1] for row in results}
            logger.info("Loaded %d station ID mappings into memory for %d requested stations", len(mapping), len(wmo_station_ids))
            return mapping
    except Exception as e:
        logger.error("Error loading station ID mapping: %s", str(e))
        return {}

def log_no_data_stations_batch(no_data_stations):
    """
    Log multiple no-data stations in batch to the dq_no_data_stations table.
    """
    if not no_data_stations:
        return
        
    try:
        conn = get_psycopg_conn()
        cur = conn.cursor()
        
        # Prepare batch data
        batch_data = []
        for station_info in no_data_stations:
            batch_data.append((
                station_info.get('_station_id'),
                station_info.get('_timestamp_str'),
                station_info.get('_api_url'),
                station_info.get('_http_status'),
                station_info.get('_response_message')
            ))
        
        # Batch insert
        insert_sql = f"""
        INSERT INTO {RAW_SCHEMA}.dq_no_data_stations 
        (wmo_station_id, timestamp_str, api_url, http_status, response_message)
        VALUES %s
        """
        
        execute_values(cur, insert_sql, batch_data, page_size=100)
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info("Logged %d no-data stations to dq_no_data_stations table", len(no_data_stations))
        
    except Exception as e:
        logger.error("Failed to log no-data stations batch: %s", str(e))

def get_station_ids_for_scope(plz3_prefix=None, country=None):
    """
    Get WMO station IDs for the specified scope.
    Uses simple cross-schema queries within the same database.
    """
    if plz3_prefix:
        # Simple cross-schema join within the same database
        engine = get_sqlalchemy_engine()
        with engine.begin() as conn:
            q = text(f"""
                SELECT DISTINCT ds.wmo_station_id 
                FROM {FACT_SCHEMA}.link_postcode_station lps
                JOIN {DIMENSIONS_SCHEMA}.dim_station ds ON lps.station_id = ds.id
                WHERE lps.plz ILIKE :plz3_prefix
            """)
            rows = conn.execute(q, {"plz3_prefix": f"{plz3_prefix}%"}).fetchall()
            return [r[0] for r in rows]
    
    elif country:
        # For country filtering, query dimensions schema directly
        engine = get_sqlalchemy_engine()
        with engine.begin() as conn:
            q = text(f"SELECT wmo_station_id FROM {DIMENSIONS_SCHEMA}.dim_station WHERE (properties->>'country')::text ILIKE :c")
            rows = conn.execute(q, {"c": f"%{country}%"}).fetchall()
            return [r[0] for r in rows]
    else:
        # Get all stations from dimensions schema
        engine = get_sqlalchemy_engine()
        with engine.begin() as conn:
            rows = conn.execute(text(f"SELECT wmo_station_id FROM {DIMENSIONS_SCHEMA}.dim_station")).fetchall()
            return [r[0] for r in rows]
