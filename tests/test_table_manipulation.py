# -*- coding: utf-8 -*-
"""
Tests for table manipulation functions in mozaic_daily.py.

This is the MOST CRITICAL test suite - these transformations determine
the final output format.

🔒 SECURITY: All tests use synthetic data only.
"""

import pytest
import pandas as pd
import json
from datetime import datetime

from mozaic_daily.tables import (
    combine_tables,
    update_desktop_format,
    update_mobile_format,
    format_output_table,
)
from mozaic_daily.config import (
    get_git_commit_hash_from_pip,
    get_git_commit_hash_from_file,
    get_git_commit_hash,
)


# ===== COMBINING TABLES =====

def test_combine_tables_merges_all_metrics(sample_metric_dataframes):
    """Verify all metrics are merged into single DataFrame with outer join.

    Input: {metric_name: DataFrame} with 'value' column
    Output: DataFrame with columns renamed to metric names

    Failure indicates metrics not being combined correctly, data loss possible.
    """
    result = combine_tables(sample_metric_dataframes)

    # Check all metrics present as columns
    assert 'DAU' in result.columns, (
        f"Expected 'DAU' column in combined table. Found columns: {result.columns.tolist()}"
    )
    assert 'New Profiles' in result.columns, (
        f"Expected 'New Profiles' column in combined table. Found columns: {result.columns.tolist()}"
    )
    assert 'Existing Engagement DAU' in result.columns, (
        f"Expected 'Existing Engagement DAU' column in combined table. Found columns: {result.columns.tolist()}"
    )
    assert 'Existing Engagement MAU' in result.columns, (
        f"Expected 'Existing Engagement MAU' column in combined table. Found columns: {result.columns.tolist()}"
    )


def test_combine_tables_preserves_all_rows(sample_metric_dataframes):
    """Ensure no rows are dropped during merge (outer join behavior).

    Failure indicates data loss during merge, critical bug.
    """
    # All input DataFrames have same rows, so output should have same row count
    expected_rows = len(sample_metric_dataframes['DAU'])
    result = combine_tables(sample_metric_dataframes)

    assert len(result) == expected_rows, (
        f"Expected {expected_rows} rows after merge, got {len(result)}. "
        f"Data loss indicates wrong join type or missing data."
    )


def test_combine_tables_key_columns(sample_metric_dataframes):
    """Verify merge happens on correct keys: target_date, country, population, source.

    Failure indicates wrong join keys, produces incorrect output.
    """
    result = combine_tables(sample_metric_dataframes)

    # Check join key columns present
    expected_keys = ['target_date', 'country', 'population', 'source']
    for key in expected_keys:
        assert key in result.columns, (
            f"Expected join key '{key}' in output. Found columns: {result.columns.tolist()}"
        )


# ===== DESKTOP FORMATTING =====

def test_update_desktop_format_adds_required_columns(sample_desktop_dataframe):
    """Verify app_name='desktop', data_source='Glean_Desktop', segment JSON created.

    Failure indicates required columns missing from output.
    """
    df = sample_desktop_dataframe.copy()
    update_desktop_format(df)

    assert 'app_name' in df.columns, "Expected 'app_name' column after formatting"
    assert 'data_source' in df.columns, "Expected 'data_source' column after formatting"
    assert 'segment' in df.columns, "Expected 'segment' column after formatting"

    # Check values
    assert all(df['app_name'] == 'desktop'), "Expected app_name='desktop' for all rows"
    assert all(df['data_source'] == 'glean_desktop'), "Expected data_source='glean_desktop' for all rows"


def test_update_desktop_format_segment_json_structure(sample_desktop_dataframe):
    """Verify segment is valid JSON with 'os' key.

    Examples:
    - population='win10' → segment='{"os": "win10"}'
    - population='None' → segment='{"os": "ALL"}'

    Failure indicates invalid JSON format, validation will fail.
    """
    df = sample_desktop_dataframe.copy()
    update_desktop_format(df)

    # Check all segments are valid JSON
    for idx, segment_str in df['segment'].items():
        try:
            segment = json.loads(segment_str)
        except json.JSONDecodeError:
            pytest.fail(f"Row {idx}: segment is not valid JSON: {segment_str}")

        # Check 'os' key exists
        assert 'os' in segment, f"Row {idx}: segment missing 'os' key: {segment_str}"

    # Check specific mapping for 'None' population
    df_with_none = pd.DataFrame({
        'population': ['None', 'win10', 'win11'],
        'target_date': pd.date_range('2024-01-01', periods=3),
        'country': ['US'] * 3,
        'source': ['forecast'] * 3,
        'DAU': [1000] * 3,
    })
    update_desktop_format(df_with_none)

    segment0 = json.loads(df_with_none['segment'].iloc[0])
    assert segment0['os'] == 'ALL', f"Expected population='None' → segment={{'os': 'ALL'}}, got {segment0}"

    segment1 = json.loads(df_with_none['segment'].iloc[1])
    assert segment1['os'] == 'win10', f"Expected population='win10' → segment={{'os': 'win10'}}, got {segment1}"


def test_update_desktop_format_removes_population_column(sample_desktop_dataframe):
    """Verify population column is dropped after transformation.

    Failure indicates column not removed, output schema wrong.
    """
    df = sample_desktop_dataframe.copy()
    assert 'population' in df.columns, "Test setup error: population column should exist before formatting"

    update_desktop_format(df)

    assert 'population' not in df.columns, (
        f"Expected 'population' column to be removed. Found columns: {df.columns.tolist()}"
    )


# ===== MOBILE FORMATTING =====

def test_update_mobile_format_adds_required_columns(sample_mobile_dataframe):
    """Verify data_source='Glean_Mobile', segment='{}' added.

    Failure indicates required columns missing.
    """
    df = sample_mobile_dataframe.copy()
    update_mobile_format(df)

    assert 'app_name' in df.columns, "Expected 'app_name' column after formatting"
    assert 'data_source' in df.columns, "Expected 'data_source' column after formatting"
    assert 'segment' in df.columns, "Expected 'segment' column after formatting"

    # Check values
    assert all(df['data_source'] == 'glean_mobile'), "Expected data_source='glean_mobile' for all rows"


def test_update_mobile_format_app_name_mapping(sample_mobile_dataframe):
    """Verify population column maps to app_name correctly.

    Examples:
    - population='fenix_android' → app_name='fenix_android'
    - population='None' → app_name='ALL MOBILE'

    Failure indicates incorrect app name mapping, breaks downstream filtering.
    """
    df_test = pd.DataFrame({
        'population': ['None', 'fenix_android', 'firefox_ios'],
        'target_date': pd.date_range('2024-01-01', periods=3),
        'country': ['US'] * 3,
        'source': ['forecast'] * 3,
        'DAU': [500] * 3,
    })
    update_mobile_format(df_test)

    assert df_test['app_name'].iloc[0] == 'ALL MOBILE', (
        f"Expected population='None' → app_name='ALL MOBILE', got '{df_test['app_name'].iloc[0]}'"
    )
    assert df_test['app_name'].iloc[1] == 'fenix_android', (
        f"Expected population='fenix_android' → app_name='fenix_android', got '{df_test['app_name'].iloc[1]}'"
    )
    assert df_test['app_name'].iloc[2] == 'firefox_ios', (
        f"Expected population='firefox_ios' → app_name='firefox_ios', got '{df_test['app_name'].iloc[2]}'"
    )


def test_update_mobile_format_empty_segment(sample_mobile_dataframe):
    """Verify segment is empty JSON object: '{}'.

    Failure indicates wrong segment format for mobile.
    """
    df = sample_mobile_dataframe.copy()
    update_mobile_format(df)

    # Check all segments are '{}'
    for idx, segment_str in df['segment'].items():
        assert segment_str == '{}', (
            f"Row {idx}: Expected segment='{{}}' for mobile, got '{segment_str}'"
        )

# ===== OUTPUT FORMATTING =====

def test_format_output_table_renames_metric_columns():
    """Verify metric columns renamed to lowercase with underscores.

    Mappings:
    - 'DAU' → 'dau'
    - 'New Profiles' → 'new_profiles'
    - 'Existing Engagement DAU' → 'existing_engagement_dau'
    - 'Existing Engagement MAU' → 'existing_engagement_mau'

    Failure indicates wrong column names, BigQuery upload will fail.
    """
    df = pd.DataFrame({
        'target_date': ['2024-01-01'],
        'country': ['US'],
        'source': ['forecast'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['{"os": "ALL"}'],
        'DAU': [1000],
        'New Profiles': [50],
        'Existing Engagement DAU': [800],
        'Existing Engagement MAU': [6000],
    })

    start_date = datetime(2024, 1, 1)
    run_timestamp = datetime(2024, 1, 1, 10, 30, 0)

    result = format_output_table(df, start_date, run_timestamp)

    # Check renamed columns
    assert 'dau' in result.columns, (
        f"Expected 'dau' column after renaming. Found columns: {result.columns.tolist()}"
    )
    assert 'new_profiles' in result.columns, (
        f"Expected 'new_profiles' column after renaming. Found columns: {result.columns.tolist()}"
    )
    assert 'existing_engagement_dau' in result.columns, (
        f"Expected 'existing_engagement_dau' column after renaming. Found columns: {result.columns.tolist()}"
    )
    assert 'existing_engagement_mau' in result.columns, (
        f"Expected 'existing_engagement_mau' column after renaming. Found columns: {result.columns.tolist()}"
    )

    # Check old names removed
    assert 'DAU' not in result.columns, "Old column name 'DAU' should be removed"


def test_format_output_table_adds_metadata_columns():
    """Verify metadata columns added: forecast_start_date, forecast_run_timestamp, mozaic_hash.

    Failure indicates missing metadata, validation will fail.
    """
    df = pd.DataFrame({
        'target_date': ['2024-01-01'],
        'country': ['US'],
        'source': ['forecast'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['{"os": "ALL"}'],
        'DAU': [1000],
        'New Profiles': [50],
        'Existing Engagement DAU': [800],
        'Existing Engagement MAU': [6000],
    })

    start_date = datetime(2024, 1, 1)
    run_timestamp = datetime(2024, 1, 1, 10, 30, 0)

    result = format_output_table(df, start_date, run_timestamp)

    # Check metadata columns
    assert 'forecast_start_date' in result.columns, (
        f"Expected 'forecast_start_date' column. Found columns: {result.columns.tolist()}"
    )
    assert 'forecast_run_timestamp' in result.columns, (
        f"Expected 'forecast_run_timestamp' column. Found columns: {result.columns.tolist()}"
    )
    assert 'mozaic_hash' in result.columns, (
        f"Expected 'mozaic_hash' column. Found columns: {result.columns.tolist()}"
    )


def test_format_output_table_source_conversion():
    """Verify 'actual' source is converted to 'training' and column renamed to 'data_type'.

    Failure indicates wrong source values or column name, validation will fail.
    """
    df = pd.DataFrame({
        'target_date': ['2024-01-01', '2024-01-02'],
        'country': ['US', 'US'],
        'source': ['actual', 'forecast'],
        'app_name': ['desktop', 'desktop'],
        'data_source': ['glean_desktop', 'glean_desktop'],
        'segment': ['{"os": "ALL"}', '{"os": "ALL"}'],
        'DAU': [1000, 1100],
        'New Profiles': [50, 55],
        'Existing Engagement DAU': [800, 820],
        'Existing Engagement MAU': [6000, 6100],
    })

    start_date = datetime(2024, 1, 1)
    run_timestamp = datetime(2024, 1, 1, 10, 30, 0)

    result = format_output_table(df, start_date, run_timestamp)

    # Check 'actual' converted to 'training'
    assert result['data_type'].iloc[0] == 'training', (
        f"Expected source='actual' → data_type='training', got '{result['data_type'].iloc[0]}'"
    )
    # Check 'forecast' unchanged
    assert result['data_type'].iloc[1] == 'forecast', (
        f"Expected source='forecast' → data_type='forecast', got '{result['data_type'].iloc[1]}'"
    )


def test_format_output_table_country_all_conversion():
    """Verify 'None' country is converted to 'ALL'.

    Failure indicates wrong country values, validation will fail.
    """
    df = pd.DataFrame({
        'target_date': ['2024-01-01', '2024-01-02'],
        'country': ['None', 'US'],
        'source': ['forecast', 'forecast'],
        'app_name': ['desktop', 'desktop'],
        'data_source': ['glean_desktop', 'glean_desktop'],
        'segment': ['{"os": "ALL"}', '{"os": "ALL"}'],
        'DAU': [1000, 1100],
        'New Profiles': [50, 55],
        'Existing Engagement DAU': [800, 820],
        'Existing Engagement MAU': [6000, 6100],
    })

    start_date = datetime(2024, 1, 1)
    run_timestamp = datetime(2024, 1, 1, 10, 30, 0)

    result = format_output_table(df, start_date, run_timestamp)

    # Check 'None' converted to 'ALL'
    assert result['country'].iloc[0] == 'ALL', (
        f"Expected country='None' → 'ALL', got '{result['country'].iloc[0]}'"
    )
    # Check 'US' unchanged
    assert result['country'].iloc[1] == 'US', (
        f"Expected country='US' unchanged, got '{result['country'].iloc[1]}'"
    )


def test_format_output_table_column_types():
    """Verify string columns are explicitly cast to 'string' dtype.

    String columns: forecast_run_timestamp, target_date, mozaic_hash, data_type,
                   country, app_name, data_source, segment

    Failure indicates wrong types, BigQuery upload may fail.
    """
    df = pd.DataFrame({
        'target_date': ['2024-01-01'],
        'country': ['US'],
        'source': ['forecast'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['{"os": "ALL"}'],
        'DAU': [1000],
        'New Profiles': [50],
        'Existing Engagement DAU': [800],
        'Existing Engagement MAU': [6000],
    })

    start_date = datetime(2024, 1, 1)
    run_timestamp = datetime(2024, 1, 1, 10, 30, 0)

    result = format_output_table(df, start_date, run_timestamp)

    # Check string columns have 'string' dtype
    string_cols = [
        'forecast_run_timestamp',
        'target_date',
        'mozaic_hash',
        'data_type',
        'country',
        'app_name',
        'data_source',
        'segment',
    ]

    for col in string_cols:
        assert result[col].dtype == 'string', (
            f"Expected column '{col}' to have dtype 'string', got '{result[col].dtype}'"
        )


def test_format_output_table_column_order():
    """Verify columns are in correct order: metadata columns first, then metrics.

    Order: forecast_start_date, forecast_run_timestamp, mozaic_hash, data_source,
           target_date, data_type, country, app_name, segment, [metrics]

    Failure indicates wrong column order, affects readability and debugging.
    """
    df = pd.DataFrame({
        'target_date': ['2024-01-01'],
        'country': ['US'],
        'source': ['forecast'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['{"os": "ALL"}'],
        'DAU': [1000],
        'New Profiles': [50],
        'Existing Engagement DAU': [800],
        'Existing Engagement MAU': [6000],
    })

    start_date = datetime(2024, 1, 1)
    run_timestamp = datetime(2024, 1, 1, 10, 30, 0)

    result = format_output_table(df, start_date, run_timestamp)

    expected_prefix = [
        'forecast_start_date',
        'forecast_run_timestamp',
        'mozaic_hash',
        'data_source',
        'target_date',
        'data_type',
        'country',
        'app_name',
        'segment',
    ]

    actual_prefix = result.columns[:len(expected_prefix)].tolist()

    assert actual_prefix == expected_prefix, (
        f"Expected columns to start with {expected_prefix}, got {actual_prefix}"
    )


def test_format_output_table_date_formats():
    """Verify date formats are correct.

    - forecast_run_timestamp: 'YYYY-MM-DD HH:MM:SS'
    - target_date: 'YYYY-MM-DD'

    Failure indicates wrong date format, validation will fail.
    """
    df = pd.DataFrame({
        'target_date': ['2024-01-15'],
        'country': ['US'],
        'source': ['forecast'],
        'app_name': ['desktop'],
        'data_source': ['glean_desktop'],
        'segment': ['{"os": "ALL"}'],
        'DAU': [1000],
        'New Profiles': [50],
        'Existing Engagement DAU': [800],
        'Existing Engagement MAU': [6000],
    })

    start_date = datetime(2024, 1, 1)
    run_timestamp = datetime(2024, 1, 15, 14, 30, 45)

    result = format_output_table(df, start_date, run_timestamp)

    # Check timestamp format
    timestamp = result['forecast_run_timestamp'].iloc[0]
    assert timestamp == '2024-01-15 14:30:45', (
        f"Expected forecast_run_timestamp='2024-01-15 14:30:45', got '{timestamp}'"
    )

    # Check date format
    target_date = result['target_date'].iloc[0]
    assert target_date == '2024-01-15', (
        f"Expected target_date='2024-01-15', got '{target_date}'"
    )


# ===== GIT HASH RETRIEVAL =====

def test_get_git_commit_hash_from_pip(mocker):
    """Test retrieval of mozaic commit hash from pip freeze output.

    Failure indicates pip parsing broken, mozaic_hash will be 'unknown'.
    """
    # Mock subprocess.check_output to return fake pip freeze
    mock_output = """
numpy==1.24.0
pandas==1.5.3
-e git+https://github.com/brendanwells-moz/mozaic-forecasting@abc123def456#egg=mozaic
scipy==1.10.0
"""
    mocker.patch('subprocess.check_output', return_value=mock_output)

    result = get_git_commit_hash_from_pip('mozaic')

    assert result == 'abc123def456', (
        f"Expected hash 'abc123def456' from pip freeze, got '{result}'"
    )


def test_get_git_commit_hash_from_file(tmp_path):
    """Test retrieval from /mozaic_commit.txt file (Docker environment).

    Failure indicates file reading broken, mozaic_hash will be 'unknown'.
    """
    # Create test file
    commit_file = tmp_path / 'mozaic_commit.txt'
    commit_file.write_text('xyz789abc123\n')

    result = get_git_commit_hash_from_file(str(commit_file))

    assert result == 'xyz789abc123', (
        f"Expected hash 'xyz789abc123' from file, got '{result}'"
    )


def test_get_git_commit_hash_fallback_priority(mocker, tmp_path):
    """Verify fallback: try pip first, then file, then 'unknown'.

    Failure indicates wrong fallback order, may miss valid hash.
    """
    # Test 1: pip succeeds
    # Mock the actual functions that get_git_commit_hash() calls
    mocker.patch('mozaic_daily.config.get_git_commit_hash_from_pip', return_value='hash123')
    mocker.patch('mozaic_daily.config.get_git_commit_hash_from_file', return_value='file_hash')

    result = get_git_commit_hash()
    assert result == 'hash123', "Should use pip hash when available"

    # Test 2: pip returns 'unknown', file succeeds
    mocker.patch('mozaic_daily.config.get_git_commit_hash_from_pip', return_value='unknown')
    mocker.patch('mozaic_daily.config.get_git_commit_hash_from_file', return_value='file_hash')

    result = get_git_commit_hash()
    assert result == 'file_hash', "Should use file hash when pip returns 'unknown'"

    # Test 3: both fail (pip returns 'unknown', file returns None)
    mocker.patch('mozaic_daily.config.get_git_commit_hash_from_pip', return_value='unknown')
    mocker.patch('mozaic_daily.config.get_git_commit_hash_from_file', return_value=None)

    result = get_git_commit_hash()
    assert result == 'unknown', "Should return 'unknown' when both methods fail"
