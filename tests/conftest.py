# -*- coding: utf-8 -*-
"""
Shared test fixtures for mozaic-daily tests.

🔒 SECURITY: All fixtures generate SYNTHETIC data only.
No real BigQuery data is used in any tests.
"""

import os
import pandas as pd
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock


# ===== SYNTHETIC DATA GENERATION =====

def generate_desktop_raw_data(
    start_date='2024-01-01',
    num_days=30,
    countries=None
):
    """Generate synthetic desktop data matching BigQuery schema.

    Schema inferred from QuerySpec.build_query() for Desktop in queries.py:
    - x: date column
    - country: string (country code or 'ROW')
    - win10, win11, winX: boolean flags
    - y: integer metric value

    🔒 SECURITY: Uses FAKE data only - no real telemetry.
    """
    if countries is None:
        countries = ['US', 'DE', 'FR']

    dates = pd.date_range(start_date, periods=num_days, freq='D')
    data = []

    # Hardcoded metric values for deterministic tests
    # Pattern: win10=1000, win11=1050, winX=950
    metric_values = {
        'win10': 1000,
        'win11': 1050,
        'winX': 950,
    }

    for i, date in enumerate(dates):
        for country in countries:
            # Create rows for each Windows version
            for win10, win11, winX, os_type in [
                (True, False, False, 'win10'),   # Windows 10
                (False, True, False, 'win11'),   # Windows 11
                (False, False, True, 'winX'),    # Other Windows
            ]:
                # Add small daily variation: base_value + day_index
                value = metric_values[os_type] + i
                data.append({
                    'x': date,
                    'country': country,
                    'win10': win10,
                    'win11': win11,
                    'winX': winX,
                    'y': value
                })

    return pd.DataFrame(data)


def generate_mobile_raw_data(
    start_date='2024-01-01',
    num_days=30,
    countries=None
):
    """Generate synthetic mobile data matching BigQuery schema.

    Schema inferred from QuerySpec.build_query() for Mobile in queries.py:
    - x: date column
    - country: string (country code or 'ROW')
    - fenix_android, firefox_ios, focus_android, focus_ios: boolean flags
    - y: integer metric value

    🔒 SECURITY: Uses FAKE data only - no real telemetry.
    """
    if countries is None:
        countries = ['US', 'DE']

    dates = pd.date_range(start_date, periods=num_days, freq='D')
    data = []

    # Hardcoded metric values for deterministic tests
    # Pattern: fenix=600, firefox_ios=580, focus_android=550, focus_ios=530
    metric_values = {
        'fenix_android': 600,
        'firefox_ios': 580,
        'focus_android': 550,
        'focus_ios': 530,
    }

    for i, date in enumerate(dates):
        for country in countries:
            # Create rows for each mobile app
            for fenix, firefox_ios, focus_a, focus_i, app_type in [
                (True, False, False, False, 'fenix_android'),   # Fenix Android
                (False, True, False, False, 'firefox_ios'),     # Firefox iOS
                (False, False, True, False, 'focus_android'),   # Focus Android
                (False, False, False, True, 'focus_ios'),       # Focus iOS
            ]:
                # Add small daily variation: base_value + day_index
                value = metric_values[app_type] + i
                data.append({
                    'x': date,
                    'country': country,
                    'fenix_android': fenix,
                    'firefox_ios': firefox_ios,
                    'focus_android': focus_a,
                    'focus_ios': focus_i,
                    'y': value
                })

    return pd.DataFrame(data)


def generate_forecast_data(
    start_date='2024-01-01',
    num_days=30,
    countries=None,
    populations=None,
    source='forecast'
):
    """Generate synthetic forecast data.

    Output schema from Mozaic.to_granular_forecast_df():
    - target_date: date
    - country: string
    - population: string
    - source: 'forecast' or 'actual'
    - value: float metric value

    🔒 SECURITY: Uses FAKE data only.
    """
    if countries is None:
        countries = ['US', 'DE']
    if populations is None:
        populations = ['win10', 'win11']

    dates = pd.date_range(start_date, periods=num_days, freq='D')
    data = []

    # Hardcoded base values for deterministic tests
    base_value = 1000.0

    for i, date in enumerate(dates):
        for j, country in enumerate(countries):
            for k, population in enumerate(populations):
                # Create deterministic but varied values:
                # base + day_index + country_offset + population_offset
                value = base_value + i + (j * 10) + (k * 5)
                data.append({
                    'target_date': date,
                    'country': country,
                    'population': population,
                    'source': source,
                    'value': float(value)
                })

    return pd.DataFrame(data)


# ===== FIXTURES: MOCK BIGQUERY CLIENT =====

@pytest.fixture
def mock_bigquery_client(mocker):
    """Mock BigQuery client that returns synthetic DataFrames.

    🔒 SECURITY: Completely mocks bigquery.Client - NO real API calls.
    Returns synthetic data matching expected schema from SQL queries.
    """
    mock_client = MagicMock()
    mock_query_result = MagicMock()

    # Default: return synthetic desktop data
    # Tests can override this return value as needed
    mock_query_result.to_dataframe.return_value = generate_desktop_raw_data()

    mock_client.query.return_value = mock_query_result

    # Patch bigquery.Client to return our mock
    mocker.patch('mozaic_daily.data.bigquery.Client', return_value=mock_client)

    return mock_client


# ===== FIXTURES: RAW DATA =====

@pytest.fixture
def sample_desktop_raw_data():
    """Generate synthetic desktop DAU data for testing.

    🔒 SECURITY: Uses FAKE data - no real telemetry values.
    """
    return generate_desktop_raw_data(
        start_date='2024-01-01',
        num_days=30,
        countries=['US', 'DE', 'FR']
    )


@pytest.fixture
def sample_mobile_raw_data():
    """Generate synthetic mobile DAU data for testing.

    🔒 SECURITY: Uses FAKE data - no real telemetry values.
    """
    return generate_mobile_raw_data(
        start_date='2024-01-01',
        num_days=30,
        countries=['US', 'DE']
    )


@pytest.fixture
def sample_datasets():
    """Return sample desktop/mobile datasets matching expected structure.

    Structure: {platform: {metric: DataFrame}}

    🔒 SECURITY: Uses synthetic data only.
    """
    return {
        'desktop': {
            'DAU': generate_desktop_raw_data(num_days=30, countries=['US', 'DE']),
            'New Profiles': generate_desktop_raw_data(num_days=30, countries=['US', 'DE']),
            'Existing Engagement DAU': generate_desktop_raw_data(num_days=30, countries=['US', 'DE']),
            'Existing Engagement MAU': generate_desktop_raw_data(num_days=30, countries=['US', 'DE']),
        },
        'mobile': {
            'DAU': generate_mobile_raw_data(num_days=30, countries=['US', 'DE']),
            'New Profiles': generate_mobile_raw_data(num_days=30, countries=['US', 'DE']),
            'Existing Engagement DAU': generate_mobile_raw_data(num_days=30, countries=['US', 'DE']),
            'Existing Engagement MAU': generate_mobile_raw_data(num_days=30, countries=['US', 'DE']),
        }
    }


# ===== FIXTURES: FORECAST DATA =====

@pytest.fixture
def sample_forecast_dataframes():
    """Return sample forecast DataFrames for each metric.

    Structure: {metric: DataFrame with columns [target_date, country, population, source, value]}

    🔒 SECURITY: Uses synthetic data only.
    """
    metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']
    result = {}

    for metric in metrics:
        result[metric] = generate_forecast_data(
            start_date='2024-02-01',
            num_days=30,
            countries=['US', 'DE', 'None'],
            populations=['win10', 'win11', 'None']
        )

    return result


@pytest.fixture
def sample_metric_dataframes():
    """Return sample DataFrames for each metric with 'value' column.

    Used for testing combine_tables().

    🔒 SECURITY: Uses synthetic data only.
    """
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    countries = ['US', 'DE']
    populations = ['win10', 'win11']
    sources = ['training', 'forecast']

    # Hardcoded base value for deterministic tests
    base_value = 1000.0

    data = []
    for i, date in enumerate(dates):
        for j, country in enumerate(countries):
            for k, population in enumerate(populations):
                for m, source in enumerate(sources):
                    # Deterministic value: base + day + country_offset + pop_offset + source_offset
                    value = base_value + i + (j * 10) + (k * 5) + (m * 2)
                    data.append({
                        'target_date': date,
                        'country': country,
                        'population': population,
                        'source': source,
                        'value': float(value)
                    })

    base_df = pd.DataFrame(data)

    return {
        'DAU': base_df.copy(),
        'New Profiles': base_df.copy(),
        'Existing Engagement DAU': base_df.copy(),
        'Existing Engagement MAU': base_df.copy(),
    }


@pytest.fixture
def sample_desktop_dataframe():
    """Return sample desktop DataFrame before formatting.

    🔒 SECURITY: Uses synthetic data only.
    """
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    countries = ['US', 'DE', 'None']
    populations = ['win10', 'win11', 'None']
    sources = ['actual', 'forecast']

    # Hardcoded base values for deterministic tests
    base_values = {
        'DAU': 1000.0,
        'New Profiles': 75.0,
        'Existing Engagement DAU': 750.0,
        'Existing Engagement MAU': 6000.0,
    }

    data = []
    for i, date in enumerate(dates):
        for j, country in enumerate(countries):
            for k, population in enumerate(populations):
                for m, source in enumerate(sources):
                    # Deterministic values with small variations
                    offset = i + (j * 10) + (k * 5) + (m * 2)
                    data.append({
                        'target_date': date,
                        'country': country,
                        'population': population,
                        'source': source,
                        'DAU': base_values['DAU'] + offset,
                        'New Profiles': base_values['New Profiles'] + (offset * 0.1),
                        'Existing Engagement DAU': base_values['Existing Engagement DAU'] + offset,
                        'Existing Engagement MAU': base_values['Existing Engagement MAU'] + (offset * 10),
                    })

    return pd.DataFrame(data)


@pytest.fixture
def sample_mobile_dataframe():
    """Return sample mobile DataFrame before formatting.

    🔒 SECURITY: Uses synthetic data only.
    """
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    countries = ['US', 'DE', 'None']
    populations = ['fenix_android', 'firefox_ios', 'None']
    sources = ['actual', 'forecast']

    # Hardcoded base values for deterministic tests
    base_values = {
        'DAU': 550.0,
        'New Profiles': 45.0,
        'Existing Engagement DAU': 400.0,
        'Existing Engagement MAU': 4000.0,
    }

    data = []
    for i, date in enumerate(dates):
        for j, country in enumerate(countries):
            for k, population in enumerate(populations):
                for m, source in enumerate(sources):
                    # Deterministic values with small variations
                    offset = i + (j * 10) + (k * 5) + (m * 2)
                    data.append({
                        'target_date': date,
                        'country': country,
                        'population': population,
                        'source': source,
                        'DAU': base_values['DAU'] + offset,
                        'New Profiles': base_values['New Profiles'] + (offset * 0.1),
                        'Existing Engagement DAU': base_values['Existing Engagement DAU'] + offset,
                        'Existing Engagement MAU': base_values['Existing Engagement MAU'] + (offset * 10),
                    })

    return pd.DataFrame(data)


# ===== FIXTURES: CHECKPOINT FILES =====

@pytest.fixture
def sample_checkpoint_files(tmp_path):
    """Create synthetic checkpoint parquet files for all metrics.

    Generates FAKE data matching schema inferred from SQL queries.
    NEVER uses real telemetry data.

    🔒 SECURITY: Uses synthetic data only. No BigQuery communication.

    Args:
        tmp_path: pytest tmp_path fixture (temporary directory)

    Returns:
        Path: temporary directory containing checkpoint files
    """
    # Change to tmp directory so checkpoints are saved there
    original_dir = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Desktop metrics
        for metric in ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']:
            df = generate_desktop_raw_data(
                start_date='2024-01-01',
                num_days=30,
                countries=['US', 'DE', 'FR']
            )
            filename = f'mozaic_parts.raw.desktop.{metric}.parquet'
            df.to_parquet(tmp_path / filename)

        # Mobile metrics
        for metric in ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']:
            df = generate_mobile_raw_data(
                start_date='2024-01-01',
                num_days=30,
                countries=['US', 'DE']
            )
            filename = f'mozaic_parts.raw.mobile.{metric}.parquet'
            df.to_parquet(tmp_path / filename)

    finally:
        # Restore original directory
        os.chdir(original_dir)

    return tmp_path


# ===== FIXTURES: TEST CONSTANTS =====

@pytest.fixture
def test_constants():
    """Return test constants (not using real constants.py values).

    🔒 SECURITY: Uses test project IDs, not production.
    """
    return {
        'forecast_start_date': '2024-02-01',
        'forecast_end_date': '2024-12-31',
        'forecast_run_dt': datetime(2024, 2, 1, 10, 30, 0),
        'training_end_date': '2024-01-30',
        'default_project': 'test-project',
        'countries': {'US', 'DE', 'FR'},
        'country_string': "'DE', 'FR', 'US'",
    }
