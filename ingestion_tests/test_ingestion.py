#!/usr/bin/env python3
"""
Unit tests for data ingestion components.
"""

import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime, timezone

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ingest.postal_ingest import load_postal_topojson
from src.ingest.station_ingest import download_wmo_stations
from utils.weather_utils import parse_and_prepare, fetch_observations_for_station_timestamp


class TestPostalIngest:
    """Test postal area ingestion functionality."""
    
    @patch('src.ingest.postal_ingest.requests.get')
    def test_load_postal_topojson_success(self, mock_get):
        """Test successful postal data loading."""
        # Mock response
        mock_response = Mock()
        mock_response.content = b'mock_topojson_data'
        mock_get.return_value = mock_response
        
        # Mock geopandas and other dependencies
        with patch('src.ingest.postal_ingest.gpd.read_file') as mock_read_file, \
             patch('src.ingest.postal_ingest.brotli.decompress') as mock_decompress, \
             patch('src.ingest.postal_ingest.get_psycopg_conn') as mock_conn, \
             patch('src.ingest.postal_ingest.ensure_postgis_extension') as mock_ensure_postgis, \
             patch('src.ingest.postal_ingest.ensure_postal_area_schema') as mock_ensure_schema:
            
            # Setup mocks
            mock_decompress.return_value = b'decompressed_data'
            
            # Create a more realistic mock GeoDataFrame
            import pandas as pd
            mock_gdf = pd.DataFrame({
                'postcode': ['10115', '10117'],
                'geometry': [None, None]
            })
            
            mock_read_file.return_value = mock_gdf
            
            mock_cur = Mock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value = mock_cur
            
            # Test
            result = load_postal_topojson("https://github.com/yetzt/postleitzahlen/releases/download/2024.12/postleitzahlen.topojson.br")
            
            assert result is not None
            mock_get.assert_called_once_with("https://github.com/yetzt/postleitzahlen/releases/download/2024.12/postleitzahlen.topojson.br", timeout=60)
            mock_decompress.assert_called_once()


class TestStationIngest:
    """Test station ingestion functionality."""
    
    
    @patch('src.ingest.station_ingest.requests.get')
    def test_download_wmo_stations_success(self, mock_get):
        """Test successful WMO stations download."""
        # Mock response
        mock_response = Mock()
        mock_response.text = """WMO-StationID;StationName;Latitude;Longitude;Height;Country
01001;JAN MAYEN;70.93;-8.67;9.0;Norway
01002;BJORNOYA;74.52;19.02;14.0;Norway
01003;SVALBARD LUFTHAVN;78.25;15.47;28.0;Norway"""
        mock_get.return_value = mock_response
        
        result = download_wmo_stations("http://test.com")
        
        assert len(result) == 3
        assert result[0]['station_id'] == '01001'
        assert result[1]['station_id'] == '01002'
        assert result[2]['station_id'] == '01003'
    
    @patch('src.ingest.station_ingest.requests.get')
    def test_download_wmo_stations_failure(self, mock_get):
        """Test WMO stations download failure."""
        mock_get.side_effect = Exception("Network error")
        
        # The function should raise an exception, not return empty list
        with pytest.raises(Exception, match="Network error"):
            download_wmo_stations("http://test.com")


class TestWeatherObservationIngest:
    """Test weather observation ingestion functionality."""
    
    def test_parse_and_prepare_valid_data(self):
        """Test parsing valid weather observation data."""
        raw_data = {
            "weather": [
                {
                    "timestamp": "2023-08-07T12:30:00+00:00",
                    "source_id": 6007,
                    "temperature": 15.5,
                    "precipitation": 0.0,
                    "wind_speed": 5.2,
                    "wind_direction": 180,
                    "pressure_msl": 1013.25,
                    "relative_humidity": 65.0,
                    "cloud_cover": 25.0,
                    "visibility": 10000,
                    "dew_point": 8.5,
                    "solar": 0.8,
                    "sunshine": 45,
                    "wind_gust_speed": 7.5,
                    "wind_gust_direction": 175,
                    "precipitation_probability": 10,
                    "precipitation_probability_6h": 15,
                    "condition": "partly-cloudy",
                    "icon": "partly-cloudy-day"
                }
            ],
            "sources": [
                {
                    "id": 6007,
                    "wmo_station_id": "10315",
                    "station_name": "Test Station"
                }
            ]
        }
        
        result, no_data_info = parse_and_prepare(raw_data, record_source="test")
        
        assert len(result) == 1
        obs = result[0]
        # The function returns tuples: (wmo_station_id, timestamp_utc, raw_json, bright_sky_source_mapping, record_source)
        assert obs[0] == '10315'  # wmo_station_id
        assert obs[1] == datetime(2023, 8, 7, 12, 30, 0, tzinfo=timezone.utc)  # timestamp_utc
        assert obs[4] == "test"  # record_source
    
    def test_parse_and_prepare_empty_data(self):
        """Test parsing empty weather data."""
        raw_data = {"weather": [], "sources": []}
        
        result, no_data_info = parse_and_prepare(raw_data, record_source="test")
        
        assert result == []
        assert no_data_info is None
    
    def test_parse_and_prepare_missing_fields(self):
        """Test parsing data with missing fields."""
        raw_data = {
            "weather": [
                {
                    "timestamp": "2023-08-07T12:30:00+00:00",
                    "source_id": 6007,
                    "temperature": 15.5,
                    # Missing other fields
                }
            ],
            "sources": [
                {
                    "id": 6007,
                    "wmo_station_id": "10315",
                    "station_name": "Test Station"
                }
            ]
        }
        
        result, no_data_info = parse_and_prepare(raw_data, record_source="test")
        
        assert len(result) == 1
        obs = result[0]
        # The function returns tuples: (wmo_station_id, timestamp_utc, raw_json, bright_sky_source_mapping, record_source)
        assert obs[0] == '10315'  # wmo_station_id
        assert obs[1] == datetime(2023, 8, 7, 12, 30, 0, tzinfo=timezone.utc)  # timestamp_utc
        assert obs[4] == "test"  # record_source
    
    @patch('utils.weather_utils.requests.get')
    def test_fetch_observations_for_station_timestamp_success(self, mock_get):
        """Test successful API call for weather observations."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "weather": [
                {
                    "timestamp": "2023-08-07T12:30:00+00:00",
                    "temperature": 15.5,
                    "precipitation": 0.0
                }
            ],
            "sources": []
        }
        mock_get.return_value = mock_response
        
        result = fetch_observations_for_station_timestamp(["10315"], "2023-08-07T12:00+00:00", "2023-08-07T13:00+00:00")
        
        assert result is not None
        assert "weather" in result
        assert len(result["weather"]) == 1
    
    @patch('utils.weather_utils.requests.get')
    def test_fetch_observations_for_station_timestamp_404(self, mock_get):
        """Test 404 error handling for weather observations."""
        # Mock 404 response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_get.return_value = mock_response
        
        result = fetch_observations_for_station_timestamp(["10315"], "2023-08-07T12:00+00:00", "2023-08-07T13:00+00:00")
        
        assert result["_no_data"] is True
        assert result["weather"] == []
        assert result["sources"] == []
    
    @patch('utils.weather_utils.requests.get')
    def test_fetch_observations_for_station_timestamp_network_error(self, mock_get):
        """Test network error handling for weather observations."""
        mock_get.side_effect = Exception("Network error")
        
        # The function should raise an exception due to backoff decorator
        with pytest.raises(Exception, match="Network error"):
            fetch_observations_for_station_timestamp(["10315"], "2023-08-07T12:00+00:00", "2023-08-07T13:00+00:00")


class TestWeatherForecastIngest:
    """Test weather forecast ingestion functionality."""
    
    def test_parse_forecast_and_prepare_valid_data(self):
        """Test parsing valid weather forecast data."""
        raw_data = {
            "weather": [
                {
                    "timestamp": "2023-08-07T12:30:00+00:00",
                    "source_id": 6007,
                    "temperature": 18.5,
                    "precipitation": 2.5,
                    "wind_speed": 8.2,
                    "wind_direction": 270,
                    "pressure_msl": 1015.75,
                    "relative_humidity": 70.0,
                    "cloud_cover": 60.0,
                    "visibility": 8000,
                    "dew_point": 12.5,
                    "solar": 0.3,
                    "sunshine": 20,
                    "wind_gust_speed": 12.5,
                    "wind_gust_direction": 265,
                    "precipitation_probability": 80,
                    "precipitation_probability_6h": 85,
                    "condition": "rain",
                    "icon": "rain"
                }
            ],
            "sources": [
                {
                    "id": 6007,
                    "wmo_station_id": "10315",
                    "station_name": "Test Station"
                }
            ]
        }
        
        result, no_data_info = parse_and_prepare(raw_data, record_source="test_forecast")
        
        assert len(result) == 1
        obs = result[0]
        # The function returns tuples: (wmo_station_id, timestamp_utc, raw_json, bright_sky_source_mapping, record_source)
        assert obs[0] == '10315'  # wmo_station_id
        assert obs[1] == datetime(2023, 8, 7, 12, 30, 0, tzinfo=timezone.utc)  # timestamp_utc
        assert obs[4] == "test_forecast"  # record_source


if __name__ == "__main__":
    pytest.main([__file__])
