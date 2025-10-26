"""
Weather observation ingestion module.
Handles fetching and storing historical weather observation data.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

from psycopg2.extras import execute_values

from utils.config import HTTP_CONCURRENCY
from utils.db import get_psycopg_conn, ensure_postgis_extension, ensure_weather_observed_schema
from utils.config import RAW_SCHEMA
from utils.logger import logger
from utils.weather_utils import fetch_observations_for_station_timestamp, parse_and_prepare


def upsert_observations_batch(values):
    if not values:
        return 0
    conn = get_psycopg_conn()
    cur = conn.cursor()
    insert_sql = f"""
    INSERT INTO {RAW_SCHEMA}.weather_hourly_observed_raw
      (wmo_station_id, timestamp_utc, raw, bright_sky_source_mapping, record_source)
    VALUES %s;
    """
    execute_values(cur, insert_sql, values, page_size=500)
    conn.commit()
    cur.close()
    conn.close()
    return len(values)

def ingest_observed_weather(wmo_station_ids):
    """
    Ingest observations for given stations and timestamp.
    """
    # Ensure required schemas exist
    ensure_postgis_extension()
    ensure_weather_observed_schema()
    
    # Calculate timestamp_str_from and timestamp_str_to, example, from '2023-08-07T23:00:00' to '2023-08-07T23:01:00'
    # This will fetch data for 2300 hour
    try:
        now = datetime.now(timezone.utc) # Current UTC time
        from_dt = now.replace(minute=0, second=0, microsecond=0)  # Round down to the last full hour
        from_dt = from_dt - timedelta(hours=1)
        to_dt = from_dt + timedelta(minutes=59)  # End of that hour (HH:59:00)
        # Format as strings
        timestamp_str_from = from_dt.strftime("%Y-%m-%dT%H:%M:%S")
        timestamp_str_to = to_dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        logger.error("Error calculating timestamp_str_to: %s", str(e))
        return 0
    
    logger.info("Ingesting observations for %s stations from %s to %s", len(wmo_station_ids), timestamp_str_from, timestamp_str_to)
    
    # Create batches for parallel processing
    batch_size = max(1, len(wmo_station_ids) // HTTP_CONCURRENCY)
    
    # Create batches
    batches = []
    for i in range(0, len(wmo_station_ids), batch_size):
        batch = wmo_station_ids[i:i + batch_size]
        batches.append(batch)
    
    logger.info("Created %d batches of max size %d", len(batches), batch_size)
    
    def worker_batch(station_batch):
        try:
            total_count = 0
            no_data_stations = []
            
            # Fetch data for the entire batch of stations
            try:
                raw = fetch_observations_for_station_timestamp(station_batch, timestamp_str_from, timestamp_str_to)
                
                if raw.get('_no_data'):
                    # Collect no-data station info for batch logging
                    no_data_stations.append(raw)
                else:
                    # Process each station in the batch
                    for wmo_station_id in station_batch:
                        recs, no_data_info = parse_and_prepare(raw)
                        
                        if no_data_info:
                            # Collect no-data station info for batch logging
                            no_data_stations.append(no_data_info)
                        else:
                            count = upsert_observations_batch(recs)
                            logger.info("Inserted %d observations for station %s", count, wmo_station_id)
                            total_count += count
                            
            except Exception as e:
                logger.exception("Error fetching observations for station batch %s: %s", station_batch, e)
            # Note: Data quality logging will be handled at mart level
            
            return total_count
        except Exception as e:
            logger.exception("Error processing batch: %s", e)
            return 0

    total = 0
    with ThreadPoolExecutor(max_workers=HTTP_CONCURRENCY) as ex:
        batch_totals = list(ex.map(worker_batch, batches))
        total = sum(batch_totals)
    
    logger.info("Inserted %d observation rows", total)
    return total

