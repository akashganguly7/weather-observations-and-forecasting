# Weather Observations and Forecasting - dbt Models

This directory contains dbt models for transforming weather data from raw sources into analytical tables.

## Model Structure

### Staging Models (`staging/`)
- `stg_dim_postal_area.sql` - Postal area dimension table
- `stg_dim_station.sql` - Weather station dimension table
- `stg_weather_observed_hourly_standardized.sql` - Standardized observed weather data with outlier handling
- `stg_weather_forecast_hourly_standardized.sql` - Standardized forecast weather data with outlier handling

### Intermediate Models (`intermediate/`)
- `int_link_postcode_station.sql` - Spatial links between postal codes and weather stations

### Marts Models (`marts/`)
- `mart_weather_forecast_hourly_aggregated.sql` - Aggregated weather forecast data by postal code

## Data Flow

1. **Raw Data Sources**: Weather data from external APIs stored in raw tables
2. **Staging Layer**: Clean and standardize raw data, apply outlier detection
3. **Intermediate Layer**: Create spatial relationships and business logic
4. **Marts Layer**: Create aggregated and analytical tables for end users

## Key Features

- **Incremental Processing**: Models use incremental materialization for efficient processing
- **Outlier Detection**: Built-in outlier detection and confidence scoring
- **Spatial Linking**: PostGIS-based spatial relationships between postal codes and stations
- **Data Quality**: Comprehensive data quality tests and validation
- **Documentation**: Full documentation of all models and columns

## Running the Models

```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run specific model
dbt run --models stg_weather_observed_hourly_standardized

# Run tests
dbt test

# Run comprehensive dbt test suite
dbt test --store-failures

# Generate documentation
dbt docs generate
dbt docs serve
```

## Testing

The project includes comprehensive unit tests for data quality validation:

### **Test Categories**
- **Data Quality Tests**: Range validations, null checks, format validations
- **Business Logic Tests**: Outlier detection, confidence scoring, spatial linking
- **Integration Tests**: Cross-model data integrity, referential integrity
- **Custom Tests**: Weather-specific data quality validations

### **Running Tests**
```bash
# Run all tests
dbt test

# Run tests by category
dbt test --select staging
dbt test --select marts
dbt test --select test_type:custom

# Run specific model tests
dbt test --select stg_weather_observed_hourly_standardized
```

See `tests/README.md` for detailed testing documentation.

## Configuration

- Models are configured in `dbt_project.yml`
- Database connection is configured in `profiles/profiles.yml`
- Dependencies are managed in `packages.yml`
