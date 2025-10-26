"""
Hourly Ingestion DAG for Weather Observations and Forecasting System

This DAG handles the regular hourly data ingestion operations:
1. WMO station metadata ingestion
2. Weather forecast data ingestion
3. Weather observation data ingestion
4. dbt model execution (downstream)

This DAG runs hourly at the 1st minute of each hour.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.time_sensor import TimeSensor
import sys
import os

# Add the project root to Python path
sys.path.append('/opt/airflow/project')

# Import project modules
from utils.config import DEFAULT_COUNTRY, WMO_STATIONS_URL, DEFAULT_PLZ3_PREFIX
from utils.logger import logger
from utils.weather_utils import get_station_ids_for_scope
from src.ingest.station_ingest import ingest_wmo_stations
from src.ingest.weather_forecast_ingest import ingest_forecast_weather
from src.ingest.weather_observation_ingest import ingest_observed_weather
from src.pipelines.spatial_linking import create_spatial_linking_table

# Default arguments for the DAG
default_args = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'start_date': datetime.now().replace(minute=0, second=0, microsecond=0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Create the DAG
dag = DAG(
    '02-weather_hourly_ingestion',
    default_args=default_args,
    description='Hourly weather data ingestion and dbt processing',
    schedule_interval='1 * * * *',  # Run at 1st minute of every hour
    max_active_runs=1,
    tags=['weather', 'ingestion', 'hourly', 'dbt'],
)

def ingest_station_metadata():
    """Ingest WMO station metadata from the API."""
    logger.info("Ingesting WMO station metadata...")
    try:
        ingest_wmo_stations(WMO_STATIONS_URL)
        logger.info("Station metadata ingestion completed successfully")
        return "Station metadata ingestion completed"
    except Exception as e:
        logger.error(f"Failed to ingest station metadata: {e}")
        raise

def get_station_scope():
    """Get station IDs for the configured scope (postal areas and country)."""
    logger.info("Getting station IDs for scope...")
    try:
        station_ids = get_station_ids_for_scope(
            plz3_prefix=DEFAULT_PLZ3_PREFIX, 
            country=DEFAULT_COUNTRY
        )
        if not station_ids:
            logger.warning("No stations found for scope")
            return []
        logger.info(f"Found {len(station_ids)} stations for scope")
        return station_ids
    except Exception as e:
        logger.error(f"Failed to get station scope: {e}")
        raise

def create_spatial_links():
    """Create spatial linking table between postal codes and weather stations."""
    logger.info("Creating spatial linking table...")
    try:
        if not create_spatial_linking_table():
            raise Exception("Spatial linking table creation failed")
        logger.info("Spatial linking table created successfully")
        return "Spatial linking completed"
    except Exception as e:
        logger.error(f"Failed to create spatial linking table: {e}")
        raise

def ingest_forecast_data(**context):
    """Ingest weather forecast data for the given station IDs."""
    station_ids = context['task_instance'].xcom_pull(task_ids='get_station_scope')
    if not station_ids:
        logger.warning("No station IDs found, skipping forecast ingestion")
        return "Forecast data ingestion skipped - no stations"
    
    logger.info(f"Ingesting forecast data for {len(station_ids)} stations...")
    try:
        ingest_forecast_weather(station_ids)
        logger.info("Forecast data ingestion completed successfully")
        return "Forecast data ingestion completed"
    except Exception as e:
        logger.error(f"Failed to ingest forecast data: {e}")
        raise

def ingest_observed_data(**context):
    """Ingest weather observation data for the given station IDs."""
    station_ids = context['task_instance'].xcom_pull(task_ids='get_station_scope')
    if not station_ids:
        logger.warning("No station IDs found, skipping observed data ingestion")
        return "Observed data ingestion skipped - no stations"
    
    logger.info(f"Ingesting observed data for {len(station_ids)} stations...")
    try:
        ingest_observed_weather(station_ids)
        logger.info("Observed data ingestion completed successfully")
        return "Observed data ingestion completed"
    except Exception as e:
        logger.error(f"Failed to ingest observed data: {e}")
        raise

def run_dbt_models():
    """Run dbt models to process the ingested data."""
    logger.info("Running dbt models...")
    try:
        # Change to project directory and run dbt
        os.chdir('/opt/airflow/project')
        
        # Run dbt deps and dbt run
        import subprocess
        result = subprocess.run(
            ['dbt', 'deps'], 
            capture_output=True, 
            text=True, 
            cwd='/opt/airflow/project'
        )
        if result.returncode != 0:
            raise Exception(f"dbt deps failed: {result.stderr}")
        
        result = subprocess.run(
            ['dbt', 'run'], 
            capture_output=True, 
            text=True, 
            cwd='/opt/airflow/project'
        )
        if result.returncode != 0:
            raise Exception(f"dbt run failed: {result.stderr}")
        
        logger.info("dbt models executed successfully")
        return "dbt models completed"
    except Exception as e:
        logger.error(f"Failed to run dbt models: {e}")
        raise

def run_dbt_tests():
    """Run dbt tests to validate data quality."""
    logger.info("Running dbt tests...")
    try:
        import subprocess
        result = subprocess.run(
            ['dbt', 'test'], 
            capture_output=True, 
            text=True, 
            cwd='/opt/airflow/project'
        )
        if result.returncode != 0:
            logger.warning(f"dbt tests failed: {result.stderr}")
            # Don't fail the DAG for test failures, just log them
        else:
            logger.info("dbt tests passed successfully")
        return "dbt tests completed"
    except Exception as e:
        logger.error(f"Failed to run dbt tests: {e}")
        # Don't fail the DAG for test failures
        return "dbt tests failed but continuing"

# Define tasks
task_ingest_stations = PythonOperator(
    task_id='ingest_station_metadata',
    python_callable=ingest_station_metadata,
    dag=dag,
)

task_get_scope = PythonOperator(
    task_id='get_station_scope',
    python_callable=get_station_scope,
    dag=dag,
)

task_create_spatial_links = PythonOperator(
    task_id='create_spatial_links',
    python_callable=create_spatial_links,
    dag=dag,
)

task_ingest_forecast = PythonOperator(
    task_id='ingest_forecast_data',
    python_callable=ingest_forecast_data,
    dag=dag,
)

task_ingest_observed = PythonOperator(
    task_id='ingest_observed_data',
    python_callable=ingest_observed_data,
    dag=dag,
)

task_run_dbt = PythonOperator(
    task_id='run_dbt_models',
    python_callable=run_dbt_models,
    dag=dag,
)

task_run_dbt_tests = PythonOperator(
    task_id='run_dbt_tests',
    python_callable=run_dbt_tests,
    dag=dag,
)

# Define task dependencies
task_ingest_stations >> task_create_spatial_links
task_create_spatial_links >> task_get_scope
task_get_scope >> [task_ingest_forecast, task_ingest_observed]
[task_ingest_forecast, task_ingest_observed] >> task_run_dbt
task_run_dbt >> task_run_dbt_tests
