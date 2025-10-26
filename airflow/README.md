# Airflow Orchestration Setup

This directory contains the Airflow orchestration setup for the Weather Observations and Forecasting System.

## Overview

The Airflow setup includes two DAGs:

1. **`weather_onetime_setup`** - One-time setup operations (manual trigger only)
   - Database schema creation
   - Postal data ingestion

2. **`weather_hourly_ingestion`** - Hourly data ingestion pipeline (scheduled)
   - Station metadata ingestion
   - Spatial linking table creation
   - Weather forecast data ingestion
   - Weather observation data ingestion
   - dbt model execution
   - dbt tests

## Prerequisites

- Docker and Docker Compose installed
- Main project dependencies available
- Environment variables configured (`.env` file)

## Quick Start

### 1. Start Airflow Services

```bash
# Navigate to the airflow directory
cd airflow

# Start all Airflow services
docker-compose -f docker-compose.airflow.yml up -d

# Check service status
docker-compose -f docker-compose.airflow.yml ps
```

### 2. Access Airflow Web UI

- **URL**: http://localhost:8080
- **Username**: admin
- **Password**: admin

### 3. Run One-time Setup

1. Go to the Airflow Web UI
2. Find the `weather_onetime_setup` DAG
3. Click the "Trigger DAG" button (play icon)
4. Monitor the execution in the DAG view

### 4. Monitor Hourly Ingestion

The `weather_hourly_ingestion` DAG will automatically start running hourly at the 1st minute of each hour (e.g., 14:01:00, 15:01:00).

## Service Details

### Services

- **postgres**: PostgreSQL with PostGIS (port 5433)
- **airflow-webserver**: Airflow web interface (port 8080)
- **airflow-scheduler**: DAG scheduler
- **airflow-worker**: Task execution worker
- **airflow-init**: One-time initialization

### Ports

- **8080**: Airflow Web UI
- **5433**: PostgreSQL (different from main project to avoid conflicts)

## DAG Details

### One-time Setup DAG

**Schedule**: Manual trigger only
**Tasks**:
1. `ensure_database_schema` - Create PostGIS extension and postal area schema
2. `ingest_postal_data` - Load postal area data from TopoJSON

### Hourly Ingestion DAG

**Schedule**: `1 * * * *` (1st minute of every hour)
**Tasks**:
1. `ingest_station_metadata` - Ingest WMO station data
2. `create_spatial_links` - Create postal code to station mappings
3. `get_station_scope` - Get station IDs for configured scope
4. `ingest_forecast_data` - Ingest weather forecast data
5. `ingest_observed_data` - Ingest weather observation data
6. `run_dbt_models` - Execute dbt models
7. `run_dbt_tests` - Run dbt data quality tests

## Management Commands

### Start Services
```bash
docker-compose -f docker-compose.airflow.yml up -d
```

### Stop Services
```bash
docker-compose -f docker-compose.airflow.yml down
```

### View Logs
```bash
# All services
docker-compose -f docker-compose.airflow.yml logs

# Specific service
docker-compose -f docker-compose.airflow.yml logs airflow-scheduler
```

### Restart Services
```bash
docker-compose -f docker-compose.airflow.yml restart
```

### Rebuild Services
```bash
docker-compose -f docker-compose.airflow.yml down
docker-compose -f docker-compose.airflow.yml build --no-cache
docker-compose -f docker-compose.airflow.yml up -d
```

## Troubleshooting

### Common Issues

1. **Port Conflicts**: If port 8080 is in use, modify the port mapping in `docker-compose.airflow.yml`
2. **Database Connection**: Ensure the main project's PostgreSQL is running on port 5432
3. **DAG Not Appearing**: Check the logs for Python import errors
4. **Task Failures**: Check individual task logs in the Airflow UI

### Log Locations

- **Airflow Logs**: `./logs/` directory
- **Container Logs**: Use `docker-compose logs` command
- **Task Logs**: Available in Airflow Web UI under each task

### Reset Airflow Database

If you need to reset the Airflow metadata database:

```bash
# Stop services
docker-compose -f docker-compose.airflow.yml down

# Remove the init container and restart
docker-compose -f docker-compose.airflow.yml up airflow-init
docker-compose -f docker-compose.airflow.yml up -d
```

## Development

### Adding New DAGs

1. Create new Python files in the `dags/` directory
2. Follow the naming convention: `XX_dag_name.py`
3. Restart the scheduler: `docker-compose -f docker-compose.airflow.yml restart airflow-scheduler`

### Modifying DAGs

1. Edit the DAG files in the `dags/` directory
2. Changes are automatically detected by the scheduler
3. Use the "Refresh" button in the Airflow UI if needed

## Integration with Main Project

The Airflow setup mounts the main project directory (`../`) to `/opt/airflow/project` in the containers, allowing the DAGs to import and use all project modules.

## Environment Variables

The Airflow setup uses the same `.env` file as the main project. Key variables:

- `POSTGRES_USER`: Database username
- `POSTGRES_PASSWORD`: Database password  
- `POSTGRES_DB`: Database name
- `BRIGHTSKY_BASE`: BrightSky API base URL
- `HTTP_CONCURRENCY`: Number of concurrent API calls
- `FORECAST_DAYS_BY`: Number of forecast days to ingest
- `DEFAULT_COUNTRY`: Target country for data processing
- `DEFAULT_PLZ3_PREFIX`: Postal code prefix filter
