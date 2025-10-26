"""
Main entrypoint for headless ingestion service.
This implementation runs as a simple scheduler loop.
In production prefer Kubernetes CronJob or Airflow to orchestrate.
"""

import time
from datetime import datetime, timezone

from src.ingest.postal_ingest import load_postal_topojson
from src.ingest.station_ingest import ingest_wmo_stations
from src.ingest.weather_forecast_ingest import ingest_forecast_weather
from src.ingest.weather_observation_ingest import ingest_observed_weather
from src.pipelines.spatial_linking import create_spatial_linking_table
from utils.config import DEFAULT_COUNTRY, POSTAL_TOPO_URL, WMO_STATIONS_URL, DEFAULT_PLZ3_PREFIX
from utils.db import ensure_schema
from utils.logger import logger
from utils.weather_utils import get_station_ids_for_scope


def run_one_time_setup():
    """Run one-time setup operations: schema creation, postal data loading, and spatial linking."""
    logger.info("Running one-time setup operations...")
    
    # 1) Ensure DB schema
    ensure_schema()
    
    # 2) Ingest postal shapes - run once when shapes change
    load_postal_topojson(POSTAL_TOPO_URL, plz3_prefix=DEFAULT_PLZ3_PREFIX)
    
    # 3) Create spatial linking table
    if not create_spatial_linking_table():
        logger.error("Failed to create spatial linking table")
        raise Exception("Spatial linking table creation failed")
    
    logger.info("One-time setup completed successfully")


def run_hourly_cycle():
    """Run hourly operations: station ingestion and weather data collection."""
    logger.info("Running hourly data ingestion cycle...")
    
    # 1) Ingest station metadata from WMO (can be run periodically to update station info)
    logger.info("Ingesting WMO stations")
    ingest_wmo_stations(WMO_STATIONS_URL)

    # 2) Scoping postal area
    station_ids = get_station_ids_for_scope(plz3_prefix=DEFAULT_PLZ3_PREFIX, country=DEFAULT_COUNTRY)
    if not station_ids:
        logger.warning("No stations found for scope; skipping observation ingestion.")
    else:
        # 3) Ingest Forecast data
        ingest_forecast_weather(station_ids)
        # 4) Ingest Observed data
        ingest_observed_weather(station_ids)
    
    logger.info("Hourly data ingestion cycle completed successfully")

if __name__ == "__main__":
    logger.info("Starting ingestion worker...")
    
    # Run one-time setup operations first
    try:
        run_one_time_setup()
    except Exception as e:
        logger.exception("One-time setup failed: %s", e)
        logger.error("Exiting due to setup failure")
        exit(1)
    
    logger.info("Starting hourly ingestion loop...")
    # Simple schedule: run once immediately, then sleep until 1st minute of next hour.
    while True:
        try:
            run_hourly_cycle()
        except Exception as e:
            logger.exception("Hourly cycle failed: %s", e)
        # sleep until 1st minute of next hour (e.g., 14:01:00, 15:01:00)
        now = datetime.now(timezone.utc)
        seconds = 3660 - (now.minute*60 + now.second)  # 3660 = 3600 + 60 (1 hour + 1 minute)
        logger.info("Sleeping %s seconds until next run (1st minute of next hour)", seconds)
        time.sleep(max(60, seconds))
