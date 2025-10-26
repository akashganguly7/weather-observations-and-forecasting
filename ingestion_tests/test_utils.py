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
    get_station_ids_for_scope
)
from utils.config import DEFAULT_STATION, POSTAL_TOPO_URL, WMO_STATIONS_URL


class TestWeatherUtils:
    """Test weather utility functions."""

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
        
        result = get_station_ids_for_scope(station_name=DEFAULT_STATION)
        
        assert result == ['10315', '10316', '10317']
        mock_conn.execute.assert_called_once()


class TestConfig:
    """Test configuration values."""
    
    def test_config_values(self):
        """Test that configuration values are properly set."""
        assert DEFAULT_STATION is not None
        assert POSTAL_TOPO_URL is not None
        assert WMO_STATIONS_URL is not None
        assert isinstance(DEFAULT_STATION, str)
        assert isinstance(POSTAL_TOPO_URL, str)
        assert isinstance(WMO_STATIONS_URL, str)


if __name__ == "__main__":
    pytest.main([__file__])
