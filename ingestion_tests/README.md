# Ingestion Tests

This directory contains Python unit tests for the weather data ingestion pipeline components.

## Test Structure

### **Test Files**
- `test_ingestion.py` - Tests for data ingestion processes (postal, station, weather data)
- `test_utils.py` - Tests for utility functions and configuration
- `conftest.py` - Pytest configuration (minimal setup)
- `__init__.py` - Python package initialization

### **Note on Test Organization**
- **dbt model tests** are located in the `tests/` directory (see `tests/README.md`)
- This directory focuses on **unit tests for ingestion components only**

## Running Tests

### **Using Makefile (Recommended)**
```bash
# Run all ingestion tests
make test

# Run with verbose output
make test VERBOSE=1
```

### **Direct Pytest Commands**
```bash
# From project root - run all ingestion tests
python -m pytest ingestion_tests/

# With verbose output
python -m pytest ingestion_tests/ -v

# With coverage report
python -m pytest ingestion_tests/ --cov=src --cov-report=html
```

### **Run Specific Test Files**
```bash
# Test ingestion processes (postal, station, weather data)
python -m pytest ingestion_tests/test_ingestion.py

# Test utilities and configuration
python -m pytest ingestion_tests/test_utils.py
```

### **Run Specific Test Classes/Functions**
```bash
# Run specific test class
python -m pytest ingestion_tests/test_ingestion.py::TestPostalIngest

# Run specific test function
python -m pytest ingestion_tests/test_ingestion.py::TestPostalIngest::test_load_postal_topojson_success

# Run tests matching pattern
python -m pytest ingestion_tests/ -k "test_weather"
```

## Test Categories

### **Ingestion Tests** (`test_ingestion.py`)
- **Postal Data Ingestion** (`TestPostalIngest`)
  - TopoJSON loading and processing
  - Postal area data validation
- **Station Data Ingestion** (`TestStationIngest`)
  - WMO station data download
  - Station metadata processing
  - Error handling for failed downloads
- **Weather Data Ingestion** (`TestWeatherObservationIngest`, `TestWeatherForecastIngest`)
  - API data parsing and preparation
  - Data validation and error handling
  - Network error scenarios (404, network failures)

### **Utility Tests** (`test_utils.py`)
- **Weather Utils** (`TestWeatherUtils`)
  - Station ID mapping functionality
  - Data quality logging
  - Station scoping by postal codes and countries
- **Configuration Tests** (`TestConfig`)
  - Configuration value validation
  - Environment variable handling

## Test Configuration

### **Pytest Configuration** (`conftest.py`)
- **Minimal setup** - Only essential path configuration
- **Project root path** - Ensures proper module imports

### **Test Data**
- **Inline test data** - Test data is created within individual tests
- **Mock responses** - API responses are mocked using `unittest.mock`
- **No external dependencies** - Tests run without database or API connections

## Best Practices

### **Test Design**
1. **Isolated Tests**: Each test should be independent
2. **Clear Naming**: Use descriptive test names
3. **Mock External Dependencies**: Mock APIs and databases
4. **Test Edge Cases**: Include boundary conditions
5. **Fast Execution**: Keep tests quick and efficient

### **Test Maintenance**
1. **Keep Tests Updated**: Update tests when code changes
2. **Review Test Results**: Regularly review test outcomes
3. **Remove Obsolete Tests**: Clean up unused tests
4. **Document Test Logic**: Explain complex test scenarios
5. **Version Control**: Track test changes in git

## CI/CD Integration

### **Continuous Integration**
- Run tests in CI pipeline via GitHub Actions or similar
- Fail builds on test failures
- Generate test reports
- Monitor test coverage

### **Test Automation**
- Schedule regular test runs
- Set up alerts for test failures
- Track test performance
- Automate test result reporting

## Troubleshooting

### **Common Issues**
1. **Import Errors**: Check Python path and imports
2. **Database Connection**: Verify test database setup
3. **Mock Failures**: Review mock configurations
4. **Test Data**: Ensure test data is available
5. **Environment Variables**: Check test environment setup

### **Debugging Tests**
```bash
# Run with debug output
python -m pytest ingestion_tests/ -v -s

# Run single test with debug
python -m pytest ingestion_tests/test_ingestion.py::TestPostalIngest::test_load_postal_topojson_success -v -s

# Run with pdb debugger
python -m pytest ingestion_tests/ --pdb

# Run specific test class with debug
python -m pytest ingestion_tests/test_ingestion.py::TestPostalIngest -v -s
```

## Test Coverage

### **Coverage Goals**
- **Unit Tests**: 90%+ code coverage [can be tracked via Sonar]
- **Integration Tests**: Cover critical workflows
- **Edge Cases**: Test boundary conditions
- **Error Handling**: Test failure scenarios

### **Coverage Reports**
```bash
# Generate HTML coverage report
python -m pytest ingestion_tests/ --cov=src --cov-report=html

# Generate terminal coverage report
python -m pytest ingestion_tests/ --cov=src --cov-report=term-missing
```

## Current Test Status

### **Test Count**
- **Total Tests**: 16 tests
- **Test Files**: 2 files (`test_ingestion.py`, `test_utils.py`)
- **Test Classes**: 5 classes
- **All Tests Passing**: ✅

### **Test Coverage**
- **Ingestion Components**: Postal, Station, Weather data ingestion
- **Utility Functions**: Configuration, weather utils, data quality logging
- **Error Handling**: Network errors, API failures, data validation
- **Mocking**: All external dependencies are properly mocked
