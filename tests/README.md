# dbt Model Tests Documentation

This directory contains comprehensive dbt model tests for the weather observations and forecasting project. These tests validate data quality, business logic, and data integrity at the SQL/database level.

**Note**: Python unit tests for the ingestion pipeline are located in the `ingestion_tests/` directory.

## Test Structure

### 1. **Custom Test Macros** (`macros/test_weather_data_quality.sql`)
Custom test macros for weather-specific data quality validation:

- `test_temperature_range()` - Validates temperature values are within reasonable range (-50°C to 50°C)
- `test_precipitation_non_negative()` - Ensures precipitation values are non-negative
- `test_wind_speed_non_negative()` - Ensures wind speed values are non-negative
- `test_wind_direction_range()` - Validates wind direction is between 0-360 degrees
- `test_humidity_range()` - Ensures humidity is between 0-100%
- `test_pressure_range()` - Validates pressure is within reasonable range (800-1100 hPa)
- `test_confidence_score_range()` - Ensures confidence scores are between 0-1
- `test_data_completeness_threshold()` - Validates data completeness above threshold
- `test_no_duplicate_records()` - Checks for duplicate records
- `test_timestamp_continuity()` - Validates timestamp continuity

### 2. **Model-Specific Tests**

#### **Staging Models**
- `test_stg_weather_observed_hourly.sql` - Tests for observed weather data
- `test_stg_weather_forecast_hourly.sql` - Tests for forecast weather data
- `test_stg_dim_station.sql` - Tests for station dimension table
- `test_stg_dim_postal_area.sql` - Tests for postal area dimension table

#### **Fact Models**
- `test_facts_link_postcode_station.sql` - Tests for spatial linking logic

#### **Marts Models**
- `test_mart_weather_forecast_hourly_aggregated.sql` - Tests for aggregated forecast weather data
- `test_mart_weather_observed_hourly_aggregated.sql` - Tests for aggregated observed weather data

#### **Integration Tests**
- `test_data_integrity.sql` - Cross-model data integrity tests

### 3. **Test Configuration** (`tests/schema.yml`)
Defines test configurations using dbt_utils expressions for:
- Data range validations
- Null value checks
- Relationship integrity
- Business rule validations

## Test Categories

### **Data Quality Tests**
- **Range Validations**: Temperature, pressure, humidity, wind direction
- **Non-negative Checks**: Precipitation, wind speed, visibility
- **Format Validations**: Postal codes, timestamps, confidence scores
- **Completeness Checks**: Data completeness thresholds

### **Business Logic Tests**
- **Outlier Detection**: Validates outlier flag logic
- **Confidence Scoring**: Ensures confidence scores are calculated correctly
- **Spatial Linking**: Validates postal code to station mapping
- **Aggregation Logic**: Tests statistical calculations in marts

### **Data Integrity Tests**
- **Referential Integrity**: Foreign key relationships
- **Uniqueness Constraints**: Primary key validations
- **Cross-model Consistency**: Data consistency across models
- **Temporal Validity**: Timestamp and data freshness checks

## Running Tests

### **Run All Tests**
```bash
dbt test
```

### **Run Tests by Category**
```bash
# Run source tests
dbt test --select source:raw

# Run staging model tests
dbt test --select staging

# Run intermediate model tests
dbt test --select intermediate

# Run marts model tests
dbt test --select marts

# Run custom tests
dbt test --select test_type:custom

# Run integration tests
dbt test --select test_type:integration
```

### **Run Specific Model Tests**
```bash
# Test specific model
dbt test --select stg_weather_observed_hourly

# Test model and its downstream dependencies
dbt test --select stg_weather_observed_hourly+
```

### **Run Tests with Different Severity**
```bash
# Run only error-level tests
dbt test --severity error

# Run warnings and errors
dbt test --severity warn
```

### **Using the Test Runner Script**
```bash
# Run comprehensive test suite with reporting
python run_dbt_tests.py
```

## Test Results

### **Success Criteria**
- ✅ All data quality tests pass
- ✅ No duplicate records
- ✅ All foreign key relationships valid
- ✅ Data ranges within expected bounds
- ✅ Business logic correctly implemented

### **Test Failures**
When tests fail:
1. Check the test output for specific failure details
2. Review the data in the failing model
3. Check for data quality issues in source systems
4. Verify business logic implementation
5. Use `dbt test --store-failures` to save failure data for analysis

### **Test Reports**
The test runner script generates JSON reports with:
- Test execution timestamps
- Pass/fail status for each test
- Detailed error messages
- Success rate statistics

## Test Configuration

### **Severity Levels**
- `error` - Test failures block deployment
- `warn` - Test failures generate warnings
- `info` - Test failures are informational

### **Test Storage**
- `store_failures: true` - Saves test failure data to database
- `fail_calc: count(*)` - Custom failure calculation
- `where: condition` - Additional test conditions

## Best Practices

### **Test Design**
1. **Test Early and Often**: Run tests during development
2. **Test Edge Cases**: Include boundary value tests
3. **Test Business Rules**: Validate core business logic
4. **Test Data Quality**: Ensure data meets quality standards
5. **Test Performance**: Monitor test execution time

### **Test Maintenance**
1. **Keep Tests Updated**: Update tests when models change
2. **Review Test Results**: Regularly review test outcomes
3. **Optimize Test Performance**: Remove redundant tests
4. **Document Test Logic**: Explain complex test scenarios
5. **Version Control**: Track test changes in git

## Troubleshooting

### **Common Issues**
1. **Test Timeouts**: Increase test timeout settings
2. **Memory Issues**: Optimize test queries
3. **False Positives**: Review test logic and data
4. **Missing Dependencies**: Ensure all required packages installed
5. **Database Permissions**: Verify test user permissions

### **Debugging Tests**
```bash
# Compile test SQL to review
dbt compile --select test_name

# Run single test with verbose output
dbt test --select test_name --debug

# Store test failures for analysis
dbt test --select test_name --store-failures
```

## Continuous Integration

### **CI/CD Integration**
- Run tests in CI pipeline before deployment
- Fail builds on test failures
- Generate test reports for stakeholders
- Monitor test performance over time

### **Test Automation**
- Schedule regular test runs
- Set up alerts for test failures
- Track test coverage metrics
- Automate test result reporting
