from psycopg2 import connect
from sqlalchemy import create_engine, text

from .config import get_database_url, MAIN_DB, RAW_SCHEMA
from .logger import logger

# Engine cache
_engine = None

def get_sqlalchemy_engine():
    """Get SQLAlchemy engine for the main database."""
    global _engine
    if _engine is None:
        db_url = get_database_url()
        _engine = create_engine(db_url, pool_pre_ping=True)
    return _engine

def get_psycopg_conn():
    """Get psycopg2 connection for the main database."""
    db_url = get_database_url()
    conn = connect(db_url)
    return conn

def ensure_postgis_extension():
    """Create PostGIS extension if it doesn't exist."""
    sql = "CREATE EXTENSION IF NOT EXISTS postgis;"
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        logger.info(f"Ensuring PostGIS extension exists in {MAIN_DB} database...")
        conn.execute(text(sql))


def ensure_schemas():
    """Create raw schema if it doesn't exist."""
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))
        logger.info(f"Ensured raw schema exists: {RAW_SCHEMA}")


def ensure_weather_observed_schema():
    """Create weather observed raw table and indexes in raw schema."""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.weather_hourly_observed_raw (
        wmo_station_id VARCHAR,
        timestamp_utc TIMESTAMP,
        raw JSONB,
        bright_sky_source_mapping JSONB,
        record_source TEXT,
        load_dts TIMESTAMP DEFAULT now()
    );
    
    CREATE INDEX IF NOT EXISTS idx_weather_hourly_observed_raw_station_time 
    ON {RAW_SCHEMA}.weather_hourly_observed_raw (wmo_station_id, timestamp_utc);
    
    CREATE INDEX IF NOT EXISTS idx_weather_hourly_observed_raw_load_dts 
    ON {RAW_SCHEMA}.weather_hourly_observed_raw (load_dts);
    """
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        logger.info(f"Ensuring weather observed schema exists in {RAW_SCHEMA} schema...")
        conn.execute(text(sql))

def ensure_weather_forecast_schema():
    """Create weather forecast raw table and indexes in raw schema."""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.weather_hourly_forecast_raw (
        wmo_station_id VARCHAR,
        timestamp_utc TIMESTAMP,
        raw JSONB,
        bright_sky_source_mapping JSONB,
        record_source TEXT,
        load_dts TIMESTAMP DEFAULT now()
    );
    
    CREATE INDEX IF NOT EXISTS idx_weather_hourly_forecast_raw_station_time 
    ON {RAW_SCHEMA}.weather_hourly_forecast_raw (wmo_station_id, timestamp_utc);
    
    CREATE INDEX IF NOT EXISTS idx_weather_hourly_forecast_raw_load_dts 
    ON {RAW_SCHEMA}.weather_hourly_forecast_raw (load_dts);
    """
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        logger.info(f"Ensuring weather forecast schema exists in {RAW_SCHEMA} schema...")
        conn.execute(text(sql))

def ensure_station_raw_schema():
    """Create station raw table and indexes in raw schema."""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.station_raw (
        wmo_station_id VARCHAR,
        station_name TEXT,
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        properties JSONB,
        record_source TEXT,
        load_dts TIMESTAMP DEFAULT now()
    );
    
    CREATE INDEX IF NOT EXISTS idx_station_raw_wmo_station_id 
    ON {RAW_SCHEMA}.station_raw (wmo_station_id);
    
    CREATE INDEX IF NOT EXISTS idx_station_raw_load_dts 
    ON {RAW_SCHEMA}.station_raw (load_dts);
    """
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        logger.info(f"Ensuring station raw schema exists in {RAW_SCHEMA} schema...")
        conn.execute(text(sql))

def ensure_postal_raw_schema():
    """Create postal area raw table and indexes in raw schema."""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.postal_area_raw (
        plz VARCHAR,
        wkt TEXT,
        geometry GEOMETRY(MULTIPOLYGON,4326),
        record_source TEXT,
        load_dts TIMESTAMP DEFAULT now()
    );
    
    CREATE INDEX IF NOT EXISTS idx_postal_area_raw_plz 
    ON {RAW_SCHEMA}.postal_area_raw (plz);
    
    CREATE INDEX IF NOT EXISTS idx_postal_area_raw_geometry 
    ON {RAW_SCHEMA}.postal_area_raw USING GIST(geometry);
    
    CREATE INDEX IF NOT EXISTS idx_postal_area_raw_load_dts 
    ON {RAW_SCHEMA}.postal_area_raw (load_dts);
    """
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        logger.info(f"Ensuring postal area raw schema exists in {RAW_SCHEMA} schema...")
        conn.execute(text(sql))



def create_main_database():
    """Create the main database if it doesn't exist."""
    
    # Get the base database URL (without database name)
    from .config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
    base_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres"
    
    # Use autocommit mode for database creation
    engine = create_engine(base_url, isolation_level="AUTOCOMMIT")
    
    with engine.connect() as conn:
        try:
            # Check if database exists
            result = conn.execute(text(
                "SELECT 1 FROM pg_database WHERE datname = :db_name"
            ), {"db_name": MAIN_DB})
            
            if result.fetchone() is None:
                # Create database
                conn.execute(text(f'CREATE DATABASE "{MAIN_DB}"'))
                logger.info(f"Created database: {MAIN_DB}")
            else:
                logger.info(f"Database already exists: {MAIN_DB}")
                
        except Exception as e:
            logger.error(f"Error creating database {MAIN_DB}: {e}")
            raise

def setup_database_extensions():
    """Set up required extensions in the main database."""
    try:
        ensure_postgis_extension()
        logger.info(f"Extensions set up in {MAIN_DB} database")
            
    except Exception as e:
        logger.error(f"Error setting up extensions in {MAIN_DB}: {e}")
        raise


def setup_raw_schema_architecture():
    """Set up the raw schema architecture for data ingestion."""
    logger.info("Starting raw schema setup...")
    
    try:
        create_main_database()
        setup_database_extensions()
        ensure_schemas()
        logger.info("Raw schema setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Raw schema setup failed: {e}")
        raise

def ensure_schema():
    """
    Create all raw tables required by ingestion pipelines if they don't exist.
    This function only creates raw schema tables - all other tables are created by dbt models.
    """
    # First ensure the raw schema architecture is set up
    setup_raw_schema_architecture()
    
    # Create all raw tables (Bronze layer)
    ensure_station_raw_schema()
    ensure_postal_raw_schema()
    ensure_weather_observed_schema()
    ensure_weather_forecast_schema()
    
    logger.info("All raw schema tables ensured successfully")
