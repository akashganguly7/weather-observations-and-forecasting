# Weather Observations and Forecasting - Quick Setup Guide

This guide will help you set up the complete weather data pipeline with Airflow orchestration in just a few steps.

## 🚀 Quick Start (Recommended)

### Prerequisites
- Docker and Docker Compose installed
- Python 3.11+ (for generating Fernet keys)

### One-Command Setup
```bash
./setup.sh
```

That's it! The script will:
1. ✅ Check Docker installation
2. ✅ Create environment configuration
3. ✅ Start PostgreSQL database
4. ✅ Run one-time database setup
5. ✅ Start Airflow services
6. ✅ Display access information

## 📋 Access Information

After successful setup:

- **Airflow UI**: http://localhost:8080
- **Credentials**: admin / admin
- **PostgreSQL (Main)**: localhost:5432
- **PostgreSQL (Airflow)**: localhost:5433

## 🔧 Manual Setup (Alternative)

If you prefer to run the setup manually:

### 1. Create Environment File
```bash
# Create .env file with default values
cat > .env << EOF
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
MAIN_DB=weatherdb

# Schema Configuration
RAW_SCHEMA=raw
STAGING_SCHEMA=staging
DIMENSIONS_SCHEMA=dimensions
FACT_SCHEMA=fact
BUSINESS_MART_SCHEMA=business_mart

# BrightSky API Configuration
BRIGHTSKY_BASE=https://api.brightsky.dev

# Ingestion Configuration
HTTP_CONCURRENCY=10
FORECAST_DAYS_BY=3

# Airflow Configuration
FERNET_KEY=$(python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=airflow
EOF
```

### 2. Start Main Database
```bash
docker-compose up -d postgres
```

### 3. Run One-Time Setup
```bash
docker-compose run --rm ingest python main.py
```

### 4. Start Airflow
```bash
cd airflow
docker-compose -f docker-compose.airflow.yml up -d
cd ..
```

## 🎯 Next Steps

1. **Open Airflow UI**: Navigate to http://localhost:8080
2. **Login**: Use credentials admin/admin
3. **Enable DAGs**: Toggle the DAGs to enable them
4. **Run DAGs**: 
   - First run: `weather_onetime_setup` (one-time setup)
   - Then run: `weather_hourly_ingestion` (hourly data pipeline)

## 🔍 Troubleshooting

### Common Issues

#### 1. Port Already in Use
```bash
# Check what's using the ports
lsof -i :5432  # PostgreSQL
lsof -i :8080  # Airflow
lsof -i :5433  # Airflow PostgreSQL

# Stop conflicting services or change ports in .env
```

#### 2. Docker Permission Issues
```bash
# Add user to docker group (Linux)
sudo usermod -aG docker $USER
# Logout and login again
```

#### 3. Airflow Not Starting
```bash
# Check Airflow logs
cd airflow
docker-compose -f docker-compose.airflow.yml logs

# Restart Airflow
docker-compose -f docker-compose.airflow.yml restart
```

#### 4. Database Connection Issues
```bash
# Check if PostgreSQL is running
docker-compose ps postgres

# Check database logs
docker-compose logs postgres
```

### Useful Commands

```bash
# View all logs
docker-compose logs -f

# Stop all services
docker-compose down
cd airflow && docker-compose -f docker-compose.airflow.yml down && cd ..

# Restart specific service
docker-compose restart postgres

# Clean up everything (removes volumes)
docker-compose down -v
cd airflow && docker-compose -f docker-compose.airflow.yml down -v && cd ..

# Check service status
docker-compose ps
cd airflow && docker-compose -f docker-compose.airflow.yml ps && cd ..
```

## 📊 Project Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   BrightSky     │    │   PostgreSQL    │    │    Airflow      │
│   API           │───▶│   Database      │◀───│   Orchestration │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   dbt Models    │
                       │   (Transform)   │
                       └─────────────────┘
```

### Data Flow
1. **Ingestion**: Weather data from BrightSky API
2. **Storage**: Raw data in PostgreSQL (raw schema)
3. **Transformation**: dbt models create staging, dimensions, facts, and marts
4. **Orchestration**: Airflow manages the entire pipeline

### Database Schemas
- **raw**: Raw ingested data
- **staging**: Cleaned and standardized data
- **dimensions**: Reference data (stations, postal areas)
- **fact**: Fact tables and linking tables
- **business_mart**: Aggregated business data

## 🛠️ Development

### Running Tests
```bash
# Run Python tests
python -m pytest tests/

# Run dbt tests
docker-compose run --rm dbt dbt test
```

### Adding New Data Sources
1. Create ingestion script in `src/ingest/`
2. Add dbt models in `models/`
3. Update Airflow DAGs in `airflow/dags/`

## 📚 Additional Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [BrightSky API Documentation](https://brightsky.dev/docs/)

## 🤝 Support

If you encounter any issues:
1. Check the troubleshooting section above
2. Review the logs using the commands provided
3. Ensure all prerequisites are met
4. Try the manual setup if the automated script fails

---

**Happy Weather Data Processing! 🌤️**
