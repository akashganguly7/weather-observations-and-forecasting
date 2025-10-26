"""
Spatial linking functionality for mapping postal codes to weather stations.

This module provides functions to create spatial relationships between
postal areas and weather stations using PostGIS distance calculations.
"""

from utils.db import get_psycopg_conn, get_sqlalchemy_engine, ensure_postgis_extension, ensure_spatial_linking_schema
from utils.config import FACT_SCHEMA, DIMENSIONS_SCHEMA
from utils.logger import logger
from sqlalchemy import text


def create_spatial_linking_table():
    """
    Create the spatial linking table that maps postal codes to their nearest weather stations.
    
    This function creates a table called 'int_link_postcode_station' that contains
    the mapping between postal codes (plz) and their nearest weather station IDs.
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Creating spatial linking table...")
    
    # Ensure required schemas exist
    ensure_postgis_extension()
    ensure_spatial_linking_schema()
    
    try:
        # Simple cross-schema query within the same database
        engine = get_sqlalchemy_engine()
        
        with engine.begin() as conn:
            # Clear existing data
            conn.execute(text(f"TRUNCATE TABLE {FACT_SCHEMA}.link_postcode_station"))
            
            # Insert spatial links using cross-schema join
            spatial_query = f"""
            INSERT INTO {FACT_SCHEMA}.link_postcode_station (plz, station_id)
            SELECT 
                p.plz,
                s.id AS station_id
            FROM {DIMENSIONS_SCHEMA}.dim_postal_area p
            CROSS JOIN LATERAL (
                SELECT s.id
                FROM {DIMENSIONS_SCHEMA}.dim_station s
                ORDER BY ST_Distance(p.geometry, s.geometry)
                LIMIT 1
            ) s
            WHERE p.plz IS NOT NULL;
            """
            
            result = conn.execute(text(spatial_query))
            logger.info(f"Inserted spatial links using cross-schema join")
                
        logger.info("Successfully created spatial linking table")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create spatial linking table: {e}")
        return False

