# -*- coding: utf-8 -*-
"""
Tests for table manipulation functions in mozaic_daily.py.

This is the MOST CRITICAL test suite - these transformations determine
the final output format.

🔒 SECURITY: All tests use synthetic data only.
"""

import pytest
import pandas as pd
import numpy as np
import json
from datetime import datetime
from freezegun import freeze_time

from mozaic_daily.tables import (
    combine_tables,
    update_desktop_format,
    update_mobile_format,
    add_desktop_and_mobile_rows,
    format_output_table,
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
    """Verify app_name='desktop', app_category='Desktop', segment JSON created.

    Failure indicates required columns missing from output.
    """
    df = sample_desktop_dataframe.copy()
    update_desktop_format(df)

    assert 'app_name' in df.columns, "Expected 'app_name' column after formatting"
    assert 'app_category' in df.columns, "Expected 'app_category' column after formatting"
    assert 'segment' in df.columns, "Expected 'segment' column after formatting"

    # Check values
    assert all(df['app_name'] == 'desktop'), "Expected app_name='desktop' for all rows"
    assert all(df['app_category'] == 'Desktop'), "Expected app_category='Desktop' for all rows"


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
    """Verify app_category='Mobile', segment='{}' added.

    Failure indicates required columns missing.
    """
    df = sample_mobile_dataframe.copy()
    update_mobile_format(df)

    assert 'app_name' in df.columns, "Expected 'app_name' column after formatting"
    assert 'app_category' in df.columns, "Expected 'app_category' column after formatting"
    assert 'segment' in df.columns, "Expected 'segment' column after formatting"

    # Check values
    assert all(df['app_category'] == 'Mobile'), "Expected app_category='Mobile' for all rows"


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


# ===== AGGREGATION =====

def test_add_desktop_and_mobile_rows_creates_all_rows():
    """Verify 'ALL' category rows are created by summing Desktop and Mobile.

    Should sum:
    - Desktop rows where segment='{"os": "ALL"}'
    - Mobile rows where app_name='ALL MOBILE'

    Failure indicates ALL category missing or wrong filtering.
    """
    # Create sample data with Desktop and Mobile
    df_desktop = pd.DataFrame({
        'target_date': pd.date_range('2024-01-01', periods=2),
        'country': ['US', 'US'],
        'source': ['forecast', 'forecast'],
        'app_name': ['desktop', 'desktop'],
        'app_category': ['Desktop', 'Desktop'],
        'segment': ['{"os": "ALL"}', '{"os": "ALL"}'],
        'DAU': [1000, 1100],
        'New Profiles': [50, 60],
        'Existing Engagement DAU': [800, 850],
        'Existing Engagement MAU': [6000, 6200],
    })

    df_mobile = pd.DataFrame({
        'target_date': pd.date_range('2024-01-01', periods=2),
        'country': ['US', 'US'],
        'source': ['forecast', 'forecast'],
        'app_name': ['ALL MOBILE', 'ALL MOBILE'],
        'app_category': ['Mobile', 'Mobile'],
        'segment': ['{}', '{}'],
        'DAU': [500, 550],
        'New Profiles': [30, 35],
        'Existing Engagement DAU': [400, 420],
        'Existing Engagement MAU': [3000, 3100],
    })

    df_combined = pd.concat([df_desktop, df_mobile])
    result = add_desktop_and_mobile_rows(df_combined)

    # Check 'ALL' category exists
    all_rows = result[result['app_category'] == 'ALL']
    assert len(all_rows) > 0, "Expected 'ALL' category rows to be created"

    # Check 'ALL' rows have correct app_name
    assert all(all_rows['app_name'] == 'ALL'), (
        f"Expected app_name='ALL' for ALL category. Found: {all_rows['app_name'].unique()}"
    )


def test_add_desktop_and_mobile_rows_sums_metrics_correctly():
    """Verify metric values are summed correctly for ALL rows.

    All 4 metrics should be summed: DAU, New Profiles, Existing Engagement DAU/MAU

    Failure indicates math error, ALL category will be wrong.
    """
    # Create sample data with Desktop and Mobile
    df_desktop = pd.DataFrame({
        'target_date': ['2024-01-01'],
        'country': ['US'],
        'source': ['forecast'],
        'app_name': ['desktop'],
        'app_category': ['Desktop'],
        'segment': ['{"os": "ALL"}'],
        'DAU': [1000],
        'New Profiles': [50],
        'Existing Engagement DAU': [800],
        'Existing Engagement MAU': [6000],
    })

    df_mobile = pd.DataFrame({
        'target_date': ['2024-01-01'],
        'country': ['US'],
        'source': ['forecast'],
        'app_name': ['ALL MOBILE'],
        'app_category': ['Mobile'],
        'segment': ['{}'],
        'DAU': [500],
        'New Profiles': [30],
        'Existing Engagement DAU': [400],
        'Existing Engagement MAU': [3000],
    })

    df_combined = pd.concat([df_desktop, df_mobile])
    result = add_desktop_and_mobile_rows(df_combined)

    # Get ALL row
    all_row = result[
        (result['app_category'] == 'ALL') &
        (result['target_date'] == '2024-01-01') &
        (result['country'] == 'US')
    ]

    assert len(all_row) == 1, f"Expected exactly 1 ALL row, found {len(all_row)}"

    # Check sums
    assert all_row['DAU'].iloc[0] == 1500, (
        f"Expected DAU=1500 (1000+500), got {all_row['DAU'].iloc[0]}"
    )
    assert all_row['New Profiles'].iloc[0] == 80, (
        f"Expected New Profiles=80 (50+30), got {all_row['New Profiles'].iloc[0]}"
    )
    assert all_row['Existing Engagement DAU'].iloc[0] == 1200, (
        f"Expected Existing Engagement DAU=1200 (800+400), got {all_row['Existing Engagement DAU'].iloc[0]}"
    )
    assert all_row['Existing Engagement MAU'].iloc[0] == 9000, (
        f"Expected Existing Engagement MAU=9000 (6000+3000), got {all_row['Existing Engagement MAU'].iloc[0]}"
    )


def test_add_desktop_and_mobile_rows_groups_by_correct_keys():
    """Verify grouping happens by: target_date, country, source.

    Failure indicates wrong grouping, duplicate or missing ALL rows.
    """
    # Create data with multiple dates/countries
    dates = pd.date_range('2024-01-01', periods=2)
    countries = ['US', 'DE']

    data_desktop = []
    data_mobile = []

    for date in dates:
        for country in countries:
            data_desktop.append({
                'target_date': date,
                'country': country,
                'source': 'forecast',
                'app_name': 'desktop',
                'app_category': 'Desktop',
                'segment': '{"os": "ALL"}',
                'DAU': 1000,
                'New Profiles': 50,
                'Existing Engagement DAU': 800,
                'Existing Engagement MAU': 6000,
            })
            data_mobile.append({
                'target_date': date,
                'country': country,
                'source': 'forecast',
                'app_name': 'ALL MOBILE',
                'app_category': 'Mobile',
                'segment': '{}',
                'DAU': 500,
                'New Profiles': 30,
                'Existing Engagement DAU': 400,
                'Existing Engagement MAU': 3000,
            })

    df_combined = pd.concat([pd.DataFrame(data_desktop), pd.DataFrame(data_mobile)])
    result = add_desktop_and_mobile_rows(df_combined)

    # Should have one ALL row per date/country/source combination
    # 2 dates × 2 countries = 4 ALL rows
    all_rows = result[result['app_category'] == 'ALL']
    expected_all_rows = 2 * 2  # dates × countries

    assert len(all_rows) == expected_all_rows, (
        f"Expected {expected_all_rows} ALL rows (2 dates × 2 countries), got {len(all_rows)}"
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
        'app_category': ['Desktop'],
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
        'app_category': ['Desktop'],
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
    """Verify 'actual' source is converted to 'training'.

    Failure indicates wrong source values, validation will fail.
    """
    df = pd.DataFrame({
        'target_date': ['2024-01-01', '2024-01-02'],
        'country': ['US', 'US'],
        'source': ['actual', 'forecast'],
        'app_name': ['desktop', 'desktop'],
        'app_category': ['Desktop', 'Desktop'],
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
    assert result['source'].iloc[0] == 'training', (
        f"Expected source='actual' → 'training', got '{result['source'].iloc[0]}'"
    )
    # Check 'forecast' unchanged
    assert result['source'].iloc[1] == 'forecast', (
        f"Expected source='forecast' unchanged, got '{result['source'].iloc[1]}'"
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
        'app_category': ['Desktop', 'Desktop'],
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

    String columns: forecast_run_timestamp, target_date, mozaic_hash, source,
                   country, app_name, app_category, segment

    Failure indicates wrong types, BigQuery upload may fail.
    """
    df = pd.DataFrame({
        'target_date': ['2024-01-01'],
        'country': ['US'],
        'source': ['forecast'],
        'app_name': ['desktop'],
        'app_category': ['Desktop'],
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
        'source',
        'country',
        'app_name',
        'app_category',
        'segment',
    ]

    for col in string_cols:
        assert result[col].dtype == 'string', (
            f"Expected column '{col}' to have dtype 'string', got '{result[col].dtype}'"
        )


def test_format_output_table_column_order():
    """Verify columns are in correct order: metadata columns first, then metrics.

    Order: forecast_start_date, forecast_run_timestamp, mozaic_hash, target_date,
           source, country, app_name, app_category, segment, [metrics]

    Failure indicates wrong column order, affects readability and debugging.
    """
    df = pd.DataFrame({
        'target_date': ['2024-01-01'],
        'country': ['US'],
        'source': ['forecast'],
        'app_name': ['desktop'],
        'app_category': ['Desktop'],
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
        'target_date',
        'source',
        'country',
        'app_name',
        'app_category',
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
        'app_category': ['Desktop'],
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
    mocker.patch('mozaic_daily.tables.get_git_commit_hash_from_pip', return_value='hash123')
    mocker.patch('mozaic_daily.tables.get_git_commit_hash_from_file', return_value='file_hash')

    result = get_git_commit_hash()
    assert result == 'hash123', "Should use pip hash when available"

    # Test 2: pip returns 'unknown', file succeeds
    mocker.patch('mozaic_daily.tables.get_git_commit_hash_from_pip', return_value='unknown')
    mocker.patch('mozaic_daily.tables.get_git_commit_hash_from_file', return_value='file_hash')

    result = get_git_commit_hash()
    assert result == 'file_hash', "Should use file hash when pip returns 'unknown'"

    # Test 3: both fail (pip returns 'unknown', file returns None)
    mocker.patch('mozaic_daily.tables.get_git_commit_hash_from_pip', return_value='unknown')
    mocker.patch('mozaic_daily.tables.get_git_commit_hash_from_file', return_value=None)

    result = get_git_commit_hash()
    assert result == 'unknown', "Should return 'unknown' when both methods fail"
