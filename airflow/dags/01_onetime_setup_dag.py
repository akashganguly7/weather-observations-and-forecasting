"""
One-time Setup DAG for Weather Observations and Forecasting System

This DAG handles the initial setup operations that need to be run once:
1. Database schema creation
2. Postal data ingestion
3. Spatial linking table creation

This DAG should be run manually after initial deployment.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add the project root to Python path
sys.path.append('/opt/airflow/project')

# Import project modules
from utils.config import DEFAULT_COUNTRY, POSTAL_TOPO_URL, DEFAULT_PLZ3_PREFIX
from utils.db import ensure_postgis_extension, ensure_postal_area_schema
from utils.logger import logger
from src.ingest.postal_ingest import load_postal_topojson

# Default arguments for the DAG
default_args = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'start_date': datetime.now().replace(minute=0, second=0, microsecond=0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Create the DAG
dag = DAG(
    '01-weather_onetime_setup',
    default_args=default_args,
    description='One-time setup operations for weather data pipeline',
    schedule_interval=None,  # Manual trigger only
    max_active_runs=1,
    tags=['weather', 'setup', 'onetime'],
)

def ensure_database_schema():
    """Ensure database schema exists with all required tables."""
    logger.info("Creating database schema...")
    try:
        ensure_postgis_extension()
        ensure_postal_area_schema()
        logger.info("Database schema created successfully")
        return "Schema creation completed"
    except Exception as e:
        logger.error(f"Failed to create database schema: {e}")
        raise

def ingest_postal_data():
    """Load postal area data from TopoJSON."""
    logger.info("Loading postal area data...")
    try:
        load_postal_topojson(POSTAL_TOPO_URL, plz3_prefix=DEFAULT_PLZ3_PREFIX)
        logger.info("Postal data ingestion completed successfully")
        return "Postal data ingestion completed"
    except Exception as e:
        logger.error(f"Failed to ingest postal data: {e}")
        raise


# Define tasks
task_ensure_schema = PythonOperator(
    task_id='ensure_database_schema',
    python_callable=ensure_database_schema,
    dag=dag,
)

task_ingest_postal = PythonOperator(
    task_id='ingest_postal_data',
    python_callable=ingest_postal_data,
    dag=dag,
)

# Define task dependencies
task_ensure_schema >> task_ingest_postal
