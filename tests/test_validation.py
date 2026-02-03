# -*- coding: utf-8 -*-
"""
Tests for validation.py module.

Tests cover all validation functions:
- Column presence and type validation
- String format validation (timestamps, git hashes, JSON)
- Row count validation
- Null value validation
- Duplicate row detection

🔒 SECURITY: All tests use SYNTHETIC data only.
"""

import pytest
import pandas as pd
import json
from datetime import datetime
from unittest.mock import MagicMock, patch

from mozaic_daily.validation import (
    validate_output_dataframe,
    _get_bigquery_fields,
    _check_column_presence,
    _check_column_type,
    _validate_string_column_formats,
    _check_row_counts,
    _validate_null_values,
    _validate_duplicate_rows,
)


# =============================================================================
# TEST FIXTURES
# =============================================================================

@pytest.fixture
def mock_runtime_config():
    """Mock runtime configuration for tests.

    🔒 SECURITY: Uses test data only.
    """
    return {
        'forecast_start_date': '2024-02-01',
        'forecast_end_date': '2025-12-31',
        'training_end_date': '2024-01-31',
        'countries': {'US', 'DE', 'FR'},
        'country_string': "'DE', 'FR', 'US'",
        'validation_countries': {'US', 'DE', 'FR', 'ALL', 'ROW'},
        'forecast_run_dt': datetime(2024, 2, 1, 10, 30, 0),
    }


@pytest.fixture
def valid_output_dataframe(mock_runtime_config):
    """Generate a minimal valid output DataFrame for testing mode.

    🔒 SECURITY: Uses FAKE data only.
    """
    # Generate data for 10 training days + 5 forecast days
    training_dates = pd.date_range('2024-01-22', periods=10, freq='D')
    forecast_dates = pd.date_range('2024-02-01', periods=5, freq='D')

    data = []
    # Use countries from mock runtime config to match validation expectations
    countries = list(mock_runtime_config['validation_countries'])
    os_values = ['win10', 'win11', 'winX', 'other', 'ALL']

    # Training data
    for date in training_dates:
        for country in countries:
            for os_val in os_values:
                data.append({
                    'forecast_run_timestamp': '2024-02-01T10:30:00',
                    'mozaic_hash': 'a' * 40,  # Valid 40-char hex string
                    'target_date': date.strftime('%Y-%m-%d'),
                    'data_type': 'training',
                    'country': country,
                    'app_name': 'desktop',
                    'data_source': 'glean_desktop',
                    'segment': json.dumps({'os': os_val}),  # Keep os_val as-is, including "ALL"
                    'dau': 1000.0 + float(date.day),
                    'new_profiles': 50.0 + float(date.day),
                    'existing_engagement_dau': 800.0 + float(date.day),
                    'existing_engagement_mau': 6000.0 + float(date.day),
                })

    # Forecast data
    for date in forecast_dates:
        for country in countries:
            for os_val in os_values:
                data.append({
                    'forecast_run_timestamp': '2024-02-01T10:30:00',
                    'mozaic_hash': 'a' * 40,
                    'target_date': date.strftime('%Y-%m-%d'),
                    'data_type': 'forecast',
                    'country': country,
                    'app_name': 'desktop',
                    'data_source': 'glean_desktop',
                    'segment': json.dumps({'os': os_val}),  # Keep os_val as-is, including "ALL"
                    'dau': 1100.0 + float(date.day),
                    'new_profiles': 55.0 + float(date.day),
                    'existing_engagement_dau': 850.0 + float(date.day),
                    'existing_engagement_mau': 6500.0 + float(date.day),
                })

    df = pd.DataFrame(data)
    # Convert target_date to string to match expected format
    df['target_date'] = df['target_date'].astype(str)
    return df


@pytest.fixture
def mock_bigquery_schema():
    """Mock BigQuery schema fields.

    Note: In the actual DataFrame, timestamps and dates are stored as strings
    (pandas object dtype), not datetime objects. BigQuery handles the conversion
    when the data is loaded.

    🔒 SECURITY: Mocks schema only, no real BigQuery calls.
    """
    from google.cloud.bigquery import SchemaField

    return {
        'forecast_run_timestamp': SchemaField('forecast_run_timestamp', 'STRING'),  # Stored as string in DataFrame
        'mozaic_hash': SchemaField('mozaic_hash', 'STRING'),
        'target_date': SchemaField('target_date', 'STRING'),  # Stored as string in DataFrame
        'data_type': SchemaField('data_type', 'STRING'),
        'country': SchemaField('country', 'STRING'),
        'app_name': SchemaField('app_name', 'STRING'),
        'data_source': SchemaField('data_source', 'STRING'),
        'segment': SchemaField('segment', 'STRING'),  # JSON stored as string in DataFrame
        'dau': SchemaField('dau', 'FLOAT64'),
        'new_profiles': SchemaField('new_profiles', 'FLOAT64'),
        'existing_engagement_dau': SchemaField('existing_engagement_dau', 'FLOAT64'),
        'existing_engagement_mau': SchemaField('existing_engagement_mau', 'FLOAT64'),
    }


# =============================================================================
# COLUMN PRESENCE TESTS
# =============================================================================

def test_check_column_presence_valid(valid_output_dataframe, mock_bigquery_schema):
    """Test that valid DataFrame with all columns passes."""
    # Should not raise
    _check_column_presence(valid_output_dataframe, mock_bigquery_schema)


def test_check_column_presence_missing_column(valid_output_dataframe, mock_bigquery_schema):
    """Test that missing column raises ValueError."""
    df = valid_output_dataframe.drop(columns=['dau'])

    with pytest.raises(ValueError, match="DataFrame is missing columns.*dau"):
        _check_column_presence(df, mock_bigquery_schema)


def test_check_column_presence_extra_column(valid_output_dataframe, mock_bigquery_schema):
    """Test that extra column raises ValueError."""
    df = valid_output_dataframe.copy()
    df['extra_column'] = 'test'

    with pytest.raises(ValueError, match="DataFrame has columns not present.*extra_column"):
        _check_column_presence(df, mock_bigquery_schema)


# =============================================================================
# COLUMN TYPE TESTS
# =============================================================================

def test_check_column_type_valid(valid_output_dataframe, mock_bigquery_schema):
    """Test that valid column types pass."""
    # Should not raise
    _check_column_type(valid_output_dataframe, mock_bigquery_schema)


def test_check_column_type_mismatch(mock_bigquery_schema):
    """Test that type mismatch raises TypeError."""
    # Create DataFrame with wrong type (integer instead of float)
    # Note: BigQuery expects FLOAT64 for metric columns
    from google.cloud.bigquery import SchemaField

    # Update schema to expect STRING for dau (wrong type)
    schema_with_mismatch = mock_bigquery_schema.copy()
    schema_with_mismatch['dau'] = SchemaField('dau', 'STRING')

    df = pd.DataFrame({
        'forecast_run_timestamp': ['2024-02-01T10:30:00'],
        'mozaic_hash': ['a' * 40],
        'target_date': ['2024-02-01'],
        'data_type': ['training'],
        'country': ['US'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['{}'],
        'dau': [1000.0],  # Float, but schema expects STRING
        'new_profiles': [50.0],
        'existing_engagement_dau': [800.0],
        'existing_engagement_mau': [6000.0],
    })

    with pytest.raises(TypeError, match="Type mismatch.*dau"):
        _check_column_type(df, schema_with_mismatch)


# =============================================================================
# STRING FORMAT TESTS
# =============================================================================

@patch('mozaic_daily.validation.get_runtime_config')
def test_validate_string_formats_valid(mock_get_config, mock_runtime_config, valid_output_dataframe):
    """Test that valid string formats pass."""
    mock_get_config.return_value = mock_runtime_config
    # Should not raise
    _validate_string_column_formats(valid_output_dataframe)


@patch('mozaic_daily.validation.get_runtime_config')
def test_validate_timestamp_format_invalid(mock_get_config, mock_runtime_config):
    """Test that invalid timestamp format raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    df = pd.DataFrame({
        'forecast_run_timestamp': ['2024-02-01'],  # Missing time component
        'mozaic_hash': ['a' * 40],
        'target_date': ['2024-02-01'],
        'data_type': ['training'],
        'country': ['US'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['{}'],
    })

    with pytest.raises(ValueError, match="Validation failed for column 'forecast_run_timestamp'"):
        _validate_string_column_formats(df)


@patch('mozaic_daily.validation.get_runtime_config')
def test_validate_git_hash_invalid(mock_get_config, mock_runtime_config):
    """Test that invalid git hash raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    df = pd.DataFrame({
        'forecast_run_timestamp': ['2024-02-01T10:30:00'],
        'mozaic_hash': ['invalid_hash'],  # Not 40 hex chars
        'target_date': ['2024-02-01'],
        'data_type': ['training'],
        'country': ['US'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['{}'],
    })

    with pytest.raises(ValueError, match="Validation failed for column 'mozaic_hash'"):
        _validate_string_column_formats(df)


@patch('mozaic_daily.validation.get_runtime_config')
def test_validate_target_date_format_invalid(mock_get_config, mock_runtime_config):
    """Test that target_date with time component raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    df = pd.DataFrame({
        'forecast_run_timestamp': ['2024-02-01T10:30:00'],
        'mozaic_hash': ['a' * 40],
        'target_date': ['2024-02-01T10:30:00'],  # Should not have time
        'data_type': ['training'],
        'country': ['US'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['{}'],
    })

    with pytest.raises(ValueError, match="Validation failed for column 'target_date'"):
        _validate_string_column_formats(df)


@patch('mozaic_daily.validation.get_runtime_config')
def test_validate_data_type_invalid(mock_get_config, mock_runtime_config):
    """Test that invalid data_type raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    df = pd.DataFrame({
        'forecast_run_timestamp': ['2024-02-01T10:30:00'],
        'mozaic_hash': ['a' * 40],
        'target_date': ['2024-02-01'],
        'data_type': ['invalid'],  # Must be 'training' or 'forecast'
        'country': ['US'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['{}'],
    })

    with pytest.raises(ValueError, match="Validation failed for column 'data_type'"):
        _validate_string_column_formats(df)


@patch('mozaic_daily.validation.get_runtime_config')
def test_validate_segment_json_invalid(mock_get_config, mock_runtime_config):
    """Test that invalid JSON in segment raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    df = pd.DataFrame({
        'forecast_run_timestamp': ['2024-02-01T10:30:00'],
        'mozaic_hash': ['a' * 40],
        'target_date': ['2024-02-01'],
        'data_type': ['training'],
        'country': ['US'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['not valid json'],  # Invalid JSON
    })

    with pytest.raises(ValueError, match="Validation failed for column 'segment'"):
        _validate_string_column_formats(df)


@patch('mozaic_daily.validation.get_runtime_config')
def test_validate_segment_os_value_invalid(mock_get_config, mock_runtime_config):
    """Test that invalid OS value in segment JSON raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    df = pd.DataFrame({
        'forecast_run_timestamp': ['2024-02-01T10:30:00'],
        'mozaic_hash': ['a' * 40],
        'target_date': ['2024-02-01'],
        'data_type': ['training'],
        'country': ['US'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': [json.dumps({'os': 'invalid_os'})],  # Invalid OS value
    })

    # The check_json_os function raises ValueError within df.apply()
    with pytest.raises(ValueError, match="(Invalid OS value found|Validation failed for json column)"):
        _validate_string_column_formats(df)


@patch('mozaic_daily.validation.get_runtime_config')
def test_validate_country_invalid(mock_get_config, mock_runtime_config):
    """Test that invalid country code raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    df = pd.DataFrame({
        'forecast_run_timestamp': ['2024-02-01T10:30:00'],
        'mozaic_hash': ['a' * 40],
        'target_date': ['2024-02-01'],
        'data_type': ['training'],
        'country': ['INVALID_COUNTRY'],  # Not in validation_countries
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['{}'],
    })

    with pytest.raises(ValueError, match="Validation failed for column 'country'"):
        _validate_string_column_formats(df)


@patch('mozaic_daily.validation.get_runtime_config')
def test_validate_app_name_invalid(mock_get_config, mock_runtime_config):
    """Test that invalid app_name raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    df = pd.DataFrame({
        'forecast_run_timestamp': ['2024-02-01T10:30:00'],
        'mozaic_hash': ['a' * 40],
        'target_date': ['2024-02-01'],
        'data_type': ['training'],
        'country': ['US'],
        'app_name': ['invalid_app'],  # Not in APP_NAMES
        'data_source': ['glean_desktop'],
        'segment': ['{}'],
    })

    with pytest.raises(ValueError, match="Validation failed for column 'app_name'"):
        _validate_string_column_formats(df)


@patch('mozaic_daily.validation.get_runtime_config')
def test_validate_data_source_invalid(mock_get_config, mock_runtime_config):
    """Test that invalid data_source raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    df = pd.DataFrame({
        'forecast_run_timestamp': ['2024-02-01T10:30:00'],
        'mozaic_hash': ['a' * 40],
        'target_date': ['2024-02-01'],
        'data_type': ['training'],
        'country': ['US'],
        'app_name': ['desktop'],
        'data_source': ['invalid_source'],  # Not in DATA_SOURCES
        'segment': ['{}'],
    })

    with pytest.raises(ValueError, match="Validation failed for column 'data_source'"):
        _validate_string_column_formats(df)


# =============================================================================
# ROW COUNT TESTS
# =============================================================================

@patch('mozaic_daily.validation.get_runtime_config')
@patch('mozaic_daily.validation.get_training_date_index')
@patch('mozaic_daily.validation.get_prediction_date_index')
def test_check_row_counts_valid(
    mock_prediction_index,
    mock_training_index,
    mock_get_config,
    mock_runtime_config,
    valid_output_dataframe
):
    """Test that valid row counts pass."""
    mock_get_config.return_value = mock_runtime_config

    # Mock training and forecast date indices to match our test data
    # get_training_date_index is called with (key, forecast_start_date) so return proper dates
    training_dates = pd.date_range('2024-01-22', periods=11, freq='D')
    forecast_dates = pd.date_range('2024-02-01', periods=5, freq='D')

    # Make the mock function return the right dates regardless of which key is passed
    def mock_training_func(key, end=None):
        return pd.DatetimeIndex(training_dates)

    mock_training_index.side_effect = mock_training_func
    mock_prediction_index.return_value = pd.DatetimeIndex(forecast_dates)

    expected_app_names = {'desktop'}
    expected_data_sources = {'glean_desktop'}
    expected_date_keys = [('desktop', 'DAU', 'glean')]
    expected_os_values = {'win10', 'win11', 'winX', 'other', 'ALL'}

    # Should not raise
    _check_row_counts(
        valid_output_dataframe,
        expected_app_names,
        expected_data_sources,
        expected_date_keys,
        expected_os_values,
        skip_country_check=True  # Skip country check for this test
    )


@patch('mozaic_daily.validation.get_runtime_config')
@patch('mozaic_daily.validation.get_training_date_index')
@patch('mozaic_daily.validation.get_prediction_date_index')
def test_check_row_counts_missing_training_days(
    mock_prediction_index,
    mock_training_index,
    mock_get_config,
    mock_runtime_config,
    valid_output_dataframe
):
    """Test that missing training days raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    # Mock to expect MORE training days than we have
    training_dates = pd.date_range('2024-01-01', periods=60, freq='D')
    forecast_dates = pd.date_range('2024-02-01', periods=5, freq='D')
    mock_training_index.return_value = pd.DatetimeIndex(training_dates)
    mock_prediction_index.return_value = pd.DatetimeIndex(forecast_dates)

    expected_app_names = {'desktop'}
    expected_data_sources = {'glean_desktop'}
    expected_date_keys = [('desktop', 'DAU', 'glean')]
    expected_os_values = {'win10', 'win11', 'winX', 'other', 'ALL'}

    with pytest.raises(ValueError, match="Training target days missing"):
        _check_row_counts(
            valid_output_dataframe,
            expected_app_names,
            expected_data_sources,
            expected_date_keys,
            expected_os_values,
            skip_country_check=True
        )


@patch('mozaic_daily.validation.get_runtime_config')
@patch('mozaic_daily.validation.get_training_date_index')
@patch('mozaic_daily.validation.get_prediction_date_index')
def test_check_row_counts_missing_forecast_days(
    mock_prediction_index,
    mock_training_index,
    mock_get_config,
    mock_runtime_config,
    valid_output_dataframe
):
    """Test that missing forecast days raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    # Mock to expect MORE forecast days than we have
    training_dates = pd.date_range('2024-01-22', periods=11, freq='D')
    forecast_dates = pd.date_range('2024-02-01', periods=30, freq='D')
    mock_training_index.return_value = pd.DatetimeIndex(training_dates)
    mock_prediction_index.return_value = pd.DatetimeIndex(forecast_dates)

    expected_app_names = {'desktop'}
    expected_data_sources = {'glean_desktop'}
    expected_date_keys = [('desktop', 'DAU', 'glean')]
    expected_os_values = {'win10', 'win11', 'winX', 'other', 'ALL'}

    with pytest.raises(ValueError, match="Forecast target days missing"):
        _check_row_counts(
            valid_output_dataframe,
            expected_app_names,
            expected_data_sources,
            expected_date_keys,
            expected_os_values,
            skip_country_check=True
        )


@patch('mozaic_daily.validation.get_runtime_config')
@patch('mozaic_daily.validation.get_training_date_index')
@patch('mozaic_daily.validation.get_prediction_date_index')
def test_check_row_counts_missing_app_name(
    mock_prediction_index,
    mock_training_index,
    mock_get_config,
    mock_runtime_config,
    valid_output_dataframe
):
    """Test that missing app_name raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    training_dates = pd.date_range('2024-01-22', periods=11, freq='D')
    forecast_dates = pd.date_range('2024-02-01', periods=5, freq='D')
    mock_training_index.return_value = pd.DatetimeIndex(training_dates)
    mock_prediction_index.return_value = pd.DatetimeIndex(forecast_dates)

    expected_app_names = {'desktop', 'firefox_ios'}  # firefox_ios not in data
    expected_data_sources = {'glean_desktop'}
    expected_date_keys = [('desktop', 'DAU', 'glean')]
    expected_os_values = {'win10', 'win11', 'winX', 'other', 'ALL'}

    with pytest.raises(ValueError, match="Missing app name"):
        _check_row_counts(
            valid_output_dataframe,
            expected_app_names,
            expected_data_sources,
            expected_date_keys,
            expected_os_values,
            skip_country_check=True
        )


@patch('mozaic_daily.validation.get_runtime_config')
@patch('mozaic_daily.validation.get_training_date_index')
@patch('mozaic_daily.validation.get_prediction_date_index')
def test_check_row_counts_extra_segment(
    mock_prediction_index,
    mock_training_index,
    mock_get_config,
    mock_runtime_config,
    valid_output_dataframe
):
    """Test that extra segment raises ValueError."""
    mock_get_config.return_value = mock_runtime_config

    training_dates = pd.date_range('2024-01-22', periods=11, freq='D')
    forecast_dates = pd.date_range('2024-02-01', periods=5, freq='D')
    mock_training_index.return_value = pd.DatetimeIndex(training_dates)
    mock_prediction_index.return_value = pd.DatetimeIndex(forecast_dates)

    expected_app_names = {'desktop'}
    expected_data_sources = {'glean_desktop'}
    expected_date_keys = [('desktop', 'DAU', 'glean')]
    expected_os_values = {'win10', 'win11'}  # Missing winX, other, ALL

    with pytest.raises(ValueError, match="Extra segment"):
        _check_row_counts(
            valid_output_dataframe,
            expected_app_names,
            expected_data_sources,
            expected_date_keys,
            expected_os_values,
            skip_country_check=True
        )


# =============================================================================
# NULL VALUE TESTS
# =============================================================================

@patch('mozaic_daily.validation.get_training_date_index')
def test_validate_null_values_valid(mock_get_training_date_index, valid_output_dataframe):
    """Test that DataFrame with no unexpected nulls passes."""
    # Mock training date index to match our data
    # Training dates should NOT include forecast_start_date (2024-02-01)
    training_dates = pd.date_range('2024-01-22', periods=10, freq='D')  # Jan 22-31

    # Make mock work with any key argument
    def mock_training_func(key):
        return pd.DatetimeIndex(training_dates)

    mock_get_training_date_index.side_effect = mock_training_func

    expected_date_keys = [('desktop', 'DAU', 'glean')]

    # Should not raise
    _validate_null_values(valid_output_dataframe, expected_date_keys)


@patch('mozaic_daily.validation.get_training_date_index')
def test_validate_null_values_missing_dates(mock_get_training_date_index, valid_output_dataframe):
    """Test that missing dates for metrics raises ValueError."""
    # Mock training date index to expect more dates than we have
    training_dates = pd.date_range('2024-01-01', periods=60, freq='D')

    # Make mock work with any key argument
    def mock_training_func(key):
        return pd.DatetimeIndex(training_dates)

    mock_get_training_date_index.side_effect = mock_training_func

    expected_date_keys = [('desktop', 'DAU', 'glean')]

    with pytest.raises(ValueError, match="Missing dates for dataset"):
        _validate_null_values(valid_output_dataframe, expected_date_keys)


# =============================================================================
# DUPLICATE ROW TESTS
# =============================================================================

def test_validate_duplicate_rows_valid(valid_output_dataframe):
    """Test that DataFrame with no duplicates passes."""
    # Should not raise
    _validate_duplicate_rows(valid_output_dataframe)


def test_validate_duplicate_rows_has_duplicates(valid_output_dataframe):
    """Test that duplicate rows raises ValueError."""
    # Add a duplicate row
    df = pd.concat([valid_output_dataframe, valid_output_dataframe.head(1)], ignore_index=True)

    with pytest.raises(ValueError, match="Duplicate rows found"):
        _validate_duplicate_rows(df)


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

@patch('mozaic_daily.validation.get_runtime_config')
@patch('mozaic_daily.validation.get_training_date_index')
@patch('mozaic_daily.validation.get_prediction_date_index')
def test_validate_output_dataframe_testing_mode(
    mock_prediction_index,
    mock_training_index,
    mock_get_config,
    mock_runtime_config,
    valid_output_dataframe
):
    """Test full validation with valid testing mode DataFrame."""
    mock_get_config.return_value = mock_runtime_config

    # Mock training and forecast date indices
    # Training dates should NOT include forecast_start_date (2024-02-01)
    training_dates = pd.date_range('2024-01-22', periods=10, freq='D')  # Jan 22-31
    forecast_dates = pd.date_range('2024-02-01', periods=5, freq='D')

    def mock_training_func(key, end=None):
        return pd.DatetimeIndex(training_dates)

    mock_training_index.side_effect = mock_training_func
    mock_prediction_index.return_value = pd.DatetimeIndex(forecast_dates)

    # Should not raise
    validate_output_dataframe(valid_output_dataframe, testing_mode=True)


@patch('mozaic_daily.validation.get_runtime_config')
@patch('mozaic_daily.validation._get_bigquery_fields')
@patch('mozaic_daily.validation.get_training_date_index')
@patch('mozaic_daily.validation.get_prediction_date_index')
def test_validate_output_dataframe_production_mode(
    mock_prediction_index,
    mock_training_index,
    mock_get_bq_fields,
    mock_get_config,
    mock_runtime_config,
    valid_output_dataframe,
    mock_bigquery_schema
):
    """Test full validation with BigQuery schema check.

    Note: This test uses testing_mode=True internally because the fixture
    only generates Desktop data, not full production data with mobile.
    """
    mock_get_config.return_value = mock_runtime_config

    # Mock training and forecast date indices
    # Training dates should NOT include forecast_start_date (2024-02-01)
    training_dates = pd.date_range('2024-01-22', periods=10, freq='D')  # Jan 22-31
    forecast_dates = pd.date_range('2024-02-01', periods=5, freq='D')

    def mock_training_func(key, end=None):
        return pd.DatetimeIndex(training_dates)

    mock_training_index.side_effect = mock_training_func
    mock_prediction_index.return_value = pd.DatetimeIndex(forecast_dates)

    # Mock BigQuery schema
    mock_get_bq_fields.return_value = mock_bigquery_schema

    # Use testing_mode=True since we only have Desktop data in fixture
    validate_output_dataframe(valid_output_dataframe, testing_mode=True)


@patch('mozaic_daily.validation.get_runtime_config')
def test_validate_output_dataframe_invalid_raises_error(mock_get_config, mock_runtime_config):
    """Test that invalid DataFrame raises appropriate error."""
    mock_get_config.return_value = mock_runtime_config

    # Create DataFrame with missing required column
    df = pd.DataFrame({
        'forecast_run_timestamp': ['2024-02-01T10:30:00'],
        'mozaic_hash': ['a' * 40],
        'target_date': ['2024-02-01'],
        # Missing other required columns
    })

    with pytest.raises((ValueError, KeyError)):
        validate_output_dataframe(df, testing_mode=True)
