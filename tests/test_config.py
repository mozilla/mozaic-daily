# -*- coding: utf-8 -*-
"""Tests for configuration module (date override functionality)."""

import pytest
from datetime import datetime
from mozaic_daily.config import get_runtime_config


def test_runtime_config_default():
    """Test that default runtime config calculates dates correctly."""
    config = get_runtime_config()

    # Verify all expected keys are present
    assert 'forecast_run_dt' in config
    assert 'forecast_start_date' in config
    assert 'forecast_end_date' in config
    assert 'training_end_date' in config
    assert 'countries' in config
    assert 'country_string' in config

    # Verify date formats
    assert len(config['forecast_start_date']) == 10  # YYYY-MM-DD
    assert len(config['forecast_end_date']) == 10
    assert len(config['training_end_date']) == 10

    # Verify countries are not empty
    assert len(config['countries']) > 0
    assert len(config['country_string']) > 0


def test_runtime_config_with_date_override():
    """Test that forecast_start_date override adjusts all dates correctly."""
    override_date = "2024-06-15"
    config = get_runtime_config(forecast_start_date_override=override_date)

    # Verify forecast_start_date matches override
    assert config['forecast_start_date'] == override_date

    # Verify training_end_date is one day before forecast_start_date
    assert config['training_end_date'] == "2024-06-14"

    # Verify forecast_end_date is Dec 31 of next year
    assert config['forecast_end_date'] == "2025-12-31"

    # Verify forecast_run_dt is simulated "today" (override + 1 day)
    expected_run_dt = datetime(2024, 6, 16)
    assert config['forecast_run_dt'] == expected_run_dt


def test_runtime_config_override_year_boundary():
    """Test date override near year boundary."""
    # Test date in December
    config = get_runtime_config(forecast_start_date_override="2024-12-31")
    assert config['forecast_start_date'] == "2024-12-31"
    assert config['training_end_date'] == "2024-12-30"
    assert config['forecast_end_date'] == "2025-12-31"

    # Test date in January
    config = get_runtime_config(forecast_start_date_override="2024-01-01")
    assert config['forecast_start_date'] == "2024-01-01"
    assert config['training_end_date'] == "2023-12-31"
    assert config['forecast_end_date'] == "2025-12-31"


def test_runtime_config_override_preserves_countries():
    """Test that date override doesn't affect country configuration."""
    config_default = get_runtime_config()
    config_override = get_runtime_config(forecast_start_date_override="2024-06-15")

    # Countries should be identical
    assert config_default['countries'] == config_override['countries']
    assert config_default['country_string'] == config_override['country_string']
