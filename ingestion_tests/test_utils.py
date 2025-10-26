#!/usr/bin/env python3
"""
Unit tests for utility functions.
"""

import os
import sys
from unittest.mock import Mock, patch

import pytest

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.weather_utils import (
    load_station_id_mapping,
    log_no_data_stations_batch,
    get_station_ids_for_scope
)
from utils.config import DEFAULT_COUNTRY, POSTAL_TOPO_URL, WMO_STATIONS_URL


class TestWeatherUtils:
    """Test weather utility functions."""
    
    @patch('utils.weather_utils.get_sqlalchemy_engine')
    def test_load_station_id_mapping_success(self, mock_engine):
        """Test successful loading of station ID mapping."""
        # Mock database response
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ('10315', 1),
            ('10317', 3)
        ]
        mock_engine.return_value.begin.return_value.__enter__.return_value = mock_conn
        
        result = load_station_id_mapping(['10315', '10317'])
        
        assert result == {'10315': 1, '10317': 3}

    @patch('utils.weather_utils.get_psycopg_conn')
    def test_load_station_id_mapping_empty(self, mock_conn):
        """Test loading station ID mapping with empty input."""
        result = load_station_id_mapping([])
        
        assert result == {}
        mock_conn.assert_not_called()
    
    @patch('utils.weather_utils.get_psycopg_conn')
    @patch('utils.weather_utils.execute_values')
    def test_log_no_data_stations_batch_success(self, mock_execute_values, mock_conn):
        """Test successful logging of no-data stations."""
        mock_cur = Mock()
        mock_conn_instance = Mock()
        mock_conn_instance.cursor.return_value = mock_cur
        mock_conn.return_value = mock_conn_instance
        
        # The function expects a list of dictionaries, not tuples
        no_data_stations = [
            {
                '_station_id': '10315',
                '_timestamp_str': '2023-08-07T12:00+00:00',
                '_api_url': 'http://api.test.com',
                '_http_status': 404,
                '_response_message': 'Not Found'
            },
            {
                '_station_id': '10316',
                '_timestamp_str': '2023-08-07T12:00+00:00',
                '_api_url': 'http://api.test.com',
                '_http_status': 404,
                '_response_message': 'Not Found'
            }
        ]
        
        result = log_no_data_stations_batch(no_data_stations)
        
        assert result is None  # Function returns None on success
        mock_execute_values.assert_called_once()
        mock_conn_instance.commit.assert_called_once()


    @patch('utils.weather_utils.get_sqlalchemy_engine')
    def test_get_station_ids_for_scope_plz3(self, mock_engine):
        """Test getting station IDs for PLZ3 scope."""
        # Mock database response
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ('10315',),
            ('10316',),
            ('10317',)
        ]
        mock_engine.return_value.begin.return_value.__enter__.return_value = mock_conn
        
        result = get_station_ids_for_scope(plz3_prefix="123")
        
        assert result == ['10315', '10316', '10317']
        mock_conn.execute.assert_called_once()
    
    @patch('utils.weather_utils.get_sqlalchemy_engine')
    def test_get_station_ids_for_scope_country(self, mock_engine):
        """Test getting station IDs for country scope."""
        # Mock database response
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ('10315',),
            ('10316',)
        ]
        mock_engine.return_value.begin.return_value.__enter__.return_value = mock_conn
        
        result = get_station_ids_for_scope(country='Germany')
        
        assert result == ['10315', '10316']
        mock_conn.execute.assert_called_once()


class TestConfig:
    """Test configuration values."""
    
    def test_config_values(self):
        """Test that configuration values are properly set."""
        assert DEFAULT_COUNTRY is not None
        assert POSTAL_TOPO_URL is not None
        assert WMO_STATIONS_URL is not None
        assert isinstance(DEFAULT_COUNTRY, str)
        assert isinstance(POSTAL_TOPO_URL, str)
        assert isinstance(WMO_STATIONS_URL, str)


if __name__ == "__main__":
    pytest.main([__file__])
