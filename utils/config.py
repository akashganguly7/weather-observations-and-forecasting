import os

# Base database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# Main database name
MAIN_DB = os.getenv("MAIN_DB", "weatherdb")

# Schema names for different layers
RAW_SCHEMA = os.getenv("RAW_SCHEMA", "raw")
STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", "staging")
DIMENSIONS_SCHEMA = os.getenv("DIMENSIONS_SCHEMA", "dimensions")
FACT_SCHEMA = os.getenv("FACT_SCHEMA", "fact")
BUSINESS_MART_SCHEMA = os.getenv("BUSINESS_MART_SCHEMA", "business_mart")

# Legacy support - default to main database
DATABASE_URL = os.getenv("DATABASE_URL", f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{MAIN_DB}")

def get_database_url():
    """Get database URL for the main database."""
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{MAIN_DB}"

# BrightSky base
BRIGHTSKY_BASE = os.getenv("BRIGHTSKY_BASE", "https://api.brightsky.dev")

# Ingestion scope defaults
DEFAULT_COUNTRY = os.getenv("DEFAULT_COUNTRY", "Germany")
DEFAULT_PLZ3_PREFIX = os.getenv("DEFAULT_PLZ3_PREFIX", '10') # For Berlin
DEFAULT_IS_SCOPE_PLZ3 = os.getenv("DEFAULT_IS_SCOPE_PLZ3", None)  # e.g., True

# Concurrency
HTTP_CONCURRENCY = int(os.getenv("HTTP_CONCURRENCY", "6"))

# Retry/backoff config
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Postal TopoJSON URL
POSTAL_TOPO_URL = os.getenv("POSTAL_TOPO_URL", "https://github.com/yetzt/postleitzahlen/releases/download/2024.12/postleitzahlen.topojson.br")

# WMO Station Data URL
WMO_STATIONS_URL = os.getenv("WMO_STATIONS_URL", "https://opendata.dwd.de/climate_environment/CDC/help/stations_list_CLIMAT_data.txt")

# Forecast days configuration
FORECAST_DAYS_BY = int(os.getenv("FORECAST_DAYS_BY", 1))