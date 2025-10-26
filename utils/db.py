from psycopg2 import connect
from sqlalchemy import create_engine, text

from .config import DATABASE_URL, get_database_url, MAIN_DB, RAW_SCHEMA, STAGING_SCHEMA, DIMENSIONS_SCHEMA, FACT_SCHEMA, BUSINESS_MART_SCHEMA
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
    """Create all required schemas if they don't exist."""
    schemas = [RAW_SCHEMA, STAGING_SCHEMA, DIMENSIONS_SCHEMA, FACT_SCHEMA, BUSINESS_MART_SCHEMA]
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        for schema in schemas:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        logger.info(f"Ensured all schemas exist: {schemas}")

def ensure_postal_area_schema():
    """Create postal area dimension table and indexes in dimensions schema."""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {DIMENSIONS_SCHEMA}.dim_postal_area (
        plz VARCHAR PRIMARY KEY,
        wkt TEXT,
        geometry GEOMETRY(MULTIPOLYGON,4326)
    );
    CREATE INDEX IF NOT EXISTS idx_dim_postal_geom ON {DIMENSIONS_SCHEMA}.dim_postal_area USING GIST(geometry);
    """
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        logger.info(f"Ensuring postal area schema exists in {DIMENSIONS_SCHEMA} schema...")
        conn.execute(text(sql))

def ensure_station_schema():
    """Create station dimension table and indexes in dimensions schema."""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {DIMENSIONS_SCHEMA}.dim_station (
        id SERIAL PRIMARY KEY,
        station_name TEXT,
        wmo_station_id VARCHAR UNIQUE,
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        geometry GEOMETRY(POINT,4326),
        properties JSONB,
        record_source TEXT,
        load_dts TIMESTAMP DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_dim_station_geometry ON {DIMENSIONS_SCHEMA}.dim_station USING GIST(geometry);
    """
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        logger.info(f"Ensuring station schema exists in {DIMENSIONS_SCHEMA} schema...")
        conn.execute(text(sql))

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

def ensure_dq_schema():
    """Create data quality tables and indexes in raw schema."""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.dq_no_data_stations (
        id SERIAL PRIMARY KEY,
        wmo_station_id VARCHAR,
        timestamp_str VARCHAR,
        api_url TEXT,
        http_status INTEGER,
        response_message TEXT,
        created_at TIMESTAMP DEFAULT now()
    );
    
    CREATE INDEX IF NOT EXISTS idx_dq_no_data_stations_station_id 
    ON {RAW_SCHEMA}.dq_no_data_stations (wmo_station_id);
    
    CREATE INDEX IF NOT EXISTS idx_dq_no_data_stations_created_at 
    ON {RAW_SCHEMA}.dq_no_data_stations (created_at);
    """
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        logger.info(f"Ensuring data quality schema exists in {RAW_SCHEMA} schema...")
        conn.execute(text(sql))

def ensure_spatial_linking_schema():
    """Create spatial linking table and indexes in fact schema."""
    sql = f"""
    CREATE TABLE IF NOT EXISTS {FACT_SCHEMA}.link_postcode_station (
        plz VARCHAR PRIMARY KEY,
        station_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT now()
    );
    
    CREATE INDEX IF NOT EXISTS idx_link_postcode_station_station_id 
    ON {FACT_SCHEMA}.link_postcode_station (station_id);
    
    CREATE INDEX IF NOT EXISTS idx_link_postcode_station_created_at 
    ON {FACT_SCHEMA}.link_postcode_station (created_at);
    """
    engine = get_sqlalchemy_engine()
    with engine.begin() as conn:
        logger.info(f"Ensuring spatial linking schema exists in {FACT_SCHEMA} schema...")
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


def setup_schema_architecture():
    """Set up the complete schema-based architecture."""
    logger.info("Starting schema-based setup...")
    
    try:
        create_main_database()
        setup_database_extensions()
        ensure_schemas()
        logger.info("Schema-based setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Schema setup failed: {e}")
        raise

def ensure_schema():
    """
    Create all tables required by pipelines if they don't exist.
    This is a convenience function that calls all individual schema functions.
    """
    # First ensure the schema-based architecture is set up
    setup_schema_architecture()
    
    # Then create all tables in their appropriate schemas
    ensure_postal_area_schema()
    ensure_station_schema()
    ensure_weather_observed_schema()
    ensure_weather_forecast_schema()
    ensure_dq_schema()
    ensure_spatial_linking_schema()
    logger.info("All schema tables ensured successfully")
