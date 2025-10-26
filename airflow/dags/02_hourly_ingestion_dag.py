"""
Hourly Ingestion DAG for Weather Observations and Forecasting System

This DAG handles the regular hourly data ingestion operations:
1. Weather forecast data ingestion
2. Weather observation data ingestion
3. dbt model execution (downstream)

This DAG runs hourly at the 1st minute of each hour.
"""

import sys
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow.utils.task_group import TaskGroup

from airflow import DAG

# Add the project root to Python path
sys.path.append('/opt/airflow/project')

# Import project modules
from utils.config import DEFAULT_STATION
from utils.logger import logger
from utils.weather_utils import get_station_ids_for_scope
from src.ingest.weather_forecast_ingest import ingest_forecast_weather
from src.ingest.weather_observation_ingest import ingest_observed_weather


# Default arguments for the DAG
default_args = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'start_date': datetime.now().replace(minute=0, second=0, microsecond=0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': True,
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


def get_station_scope():
    """Get station IDs for the configured scope (postal areas and country)."""
    logger.info("Getting station IDs for scope...")
    try:
        station_ids = get_station_ids_for_scope(
            station_name=DEFAULT_STATION
        )
        if not station_ids:
            logger.warning("No stations found for scope")
            return []
        logger.info(f"Found {len(station_ids)} stations for scope")
        return station_ids
    except Exception as e:
        logger.error(f"Failed to get station scope: {e}")
        raise

def ingest_forecast_data_raw(**context):
    """Ingest weather forecast raw data for the given station IDs."""
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

def ingest_observed_data_raw(**context):
    """Ingest weather observation raw data for the given station IDs."""
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

def run_dbt_staging_models():
    """Run dbt staging models to process raw data."""
    logger.info("Running dbt staging models...")
    try:
        import subprocess
        
        result = subprocess.run(
            ['dbt', 'run', '--models', 'staging'],
            capture_output=True, 
            text=True, 
            cwd='/opt/airflow/project'
        )
        # Log the output for debugging
        if result.stdout:
            logger.info(f"dbt staging stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"dbt staging stderr: {result.stderr}")
            
        if result.returncode != 0:
            raise Exception(f"dbt staging models failed: {result.stderr}")
        
        logger.info("dbt staging models executed successfully")
        return "dbt staging models completed"
    except Exception as e:
        logger.error(f"Failed to run dbt staging models: {e}")
        raise

def run_dbt_dimension_models():
    """Run dbt dimension models."""
    logger.info("Running dbt dimension models...")
    try:
        import subprocess
        
        result = subprocess.run(
            ['dbt', 'run', '--models', 'dimensions'],
            capture_output=True, 
            text=True, 
            cwd='/opt/airflow/project'
        )
        # Log the output for debugging
        if result.stdout:
            logger.info(f"dbt dimensions stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"dbt dimensions stderr: {result.stderr}")
            
        if result.returncode != 0:
            raise Exception(f"dbt dimension models failed: {result.stderr}")
        
        logger.info("dbt dimension models executed successfully")
        return "dbt dimension models completed"
    except Exception as e:
        logger.error(f"Failed to run dbt dimension models: {e}")
        raise

def run_dbt_fact_models():
    """Run dbt fact models."""
    logger.info("Running dbt fact models...")
    try:
        import subprocess
        
        result = subprocess.run(
            ['dbt', 'run', '--models', 'facts'],
            capture_output=True, 
            text=True, 
            cwd='/opt/airflow/project'
        )
        # Log the output for debugging
        if result.stdout:
            logger.info(f"dbt facts stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"dbt facts stderr: {result.stderr}")
            
        if result.returncode != 0:
            raise Exception(f"dbt fact models failed: {result.stderr}")
        
        logger.info("dbt fact models executed successfully")
        return "dbt fact models completed"
    except Exception as e:
        logger.error(f"Failed to run dbt fact models: {e}")
        raise

def run_dbt_mart_models():
    """Run dbt mart models."""
    logger.info("Running dbt mart models...")
    try:
        import subprocess
        
        result = subprocess.run(
            ['dbt', 'run', '--models', 'marts'],
            capture_output=True, 
            text=True, 
            cwd='/opt/airflow/project'
        )
        # Log the output for debugging
        if result.stdout:
            logger.info(f"dbt marts stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"dbt marts stderr: {result.stderr}")
            
        if result.returncode != 0:
            raise Exception(f"dbt mart models failed: {result.stderr}")
        
        logger.info("dbt mart models executed successfully")
        return "dbt mart models completed"
    except Exception as e:
        logger.error(f"Failed to run dbt mart models: {e}")
        raise

def run_dbt_deps():
    """Install dbt dependencies."""
    logger.info("Installing dbt dependencies...")
    try:
        import subprocess
        
        result = subprocess.run(
            ['dbt', 'deps'],
            capture_output=True, 
            text=True, 
            cwd='/opt/airflow/project'
        )
        # Log the output for debugging
        if result.stdout:
            logger.info(f"dbt deps stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"dbt deps stderr: {result.stderr}")
            
        if result.returncode != 0:
            raise Exception(f"dbt deps failed: {result.stderr}")
        
        logger.info("dbt dependencies installed successfully")
        return "dbt deps completed"
    except Exception as e:
        logger.error(f"Failed to install dbt dependencies: {e}")
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
        # Log the output for debugging
        if result.stdout:
            logger.info(f"dbt tests stdout: {result.stdout}")
        if result.stderr:
            logger.warning(f"dbt tests stderr: {result.stderr}")
            
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
task_get_scope = PythonOperator(
    task_id='get_station_scope',
    python_callable=get_station_scope,
    dag=dag,
)

task_run_dbt_deps = PythonOperator(
    task_id='run_dbt_deps',
    python_callable=run_dbt_deps,
    dag=dag,
)

task_ingest_forecast_raw = PythonOperator(
    task_id='ingest_forecast_data_raw',
    python_callable=ingest_forecast_data_raw,
    dag=dag,
)

task_ingest_observed_raw = PythonOperator(
    task_id='ingest_observed_data_raw',
    python_callable=ingest_observed_data_raw,
    dag=dag,
)

# Create task group for staging layer
with TaskGroup("staging_layer", dag=dag) as staging_group:
    task_run_dbt_staging = PythonOperator(
        task_id='run_dbt_staging_models',
        python_callable=run_dbt_staging_models,
    )

# Create task group for dimension layer
with TaskGroup("dimension_layer", dag=dag) as dimension_group:
    task_run_dbt_dimensions = PythonOperator(
        task_id='run_dbt_dimension_models',
        python_callable=run_dbt_dimension_models,
    )

# Create task group for fact layer
with TaskGroup("fact_layer", dag=dag) as fact_group:
    task_run_dbt_facts = PythonOperator(
        task_id='run_dbt_fact_models',
        python_callable=run_dbt_fact_models,
    )

# Create task group for mart layer
with TaskGroup("mart_layer", dag=dag) as mart_group:
    task_run_dbt_marts = PythonOperator(
        task_id='run_dbt_mart_models',
        python_callable=run_dbt_mart_models,
    )

task_run_dbt_tests = PythonOperator(
    task_id='run_dbt_tests',
    python_callable=run_dbt_tests,
    dag=dag,
)

# Define task dependencies
task_get_scope >> [task_ingest_forecast_raw, task_ingest_observed_raw]
[task_ingest_forecast_raw, task_ingest_observed_raw] >> task_run_dbt_deps
task_run_dbt_deps >> staging_group
staging_group >> dimension_group
dimension_group >> fact_group
fact_group >> mart_group
mart_group >> task_run_dbt_tests
