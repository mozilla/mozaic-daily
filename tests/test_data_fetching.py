# -*- coding: utf-8 -*-
"""
Tests for data fetching functions in mozaic_daily.py.

🔒 SECURITY: All BigQuery interactions are MOCKED. No real BigQuery calls are made.
All test data is synthetic.
"""

import pytest
import pandas as pd
import os
from unittest.mock import MagicMock

from mozaic_daily.data import get_aggregate_data, get_queries, check_training_data_availability
from tests.conftest import generate_desktop_raw_data, generate_mobile_raw_data


# ===== BIGQUERY INTEGRATION (100% MOCKED) =====

def test_get_aggregate_data_executes_all_queries(mocker):
    """Verify that all platform/metric/source queries are executed.

    BigQuery client is MOCKED - no actual queries sent to BigQuery.
    Returns synthetic DataFrames matching expected schema from SQL queries.

    Should execute:
    - 4 desktop glean queries (DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU)
    - 4 desktop legacy queries (DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU)
    - 4 mobile glean queries (DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU)

    Total: 12 queries

    Failure indicates missing query execution or wrong query count.
    """
    # Mock BigQuery client
    mock_client = MagicMock()
    mock_query_result = MagicMock()

    # Return synthetic data for any query
    mock_query_result.to_dataframe.return_value = generate_desktop_raw_data(num_days=5)
    mock_client.query.return_value = mock_query_result

    mocker.patch('mozaic_daily.data.bigquery.Client', return_value=mock_client)

    # Get queries
    queries = get_queries("'US', 'DE', 'FR'")

    # Run get_aggregate_data
    result = get_aggregate_data(queries, 'test-project', checkpoints=False)

    # Verify queries were executed
    # Should have called query() 12 times (4 desktop glean + 4 desktop legacy + 4 mobile glean)
    assert mock_client.query.call_count == 12, (
        f"Expected 12 queries to be executed (4 desktop glean + 4 desktop legacy + 4 mobile glean), got {mock_client.query.call_count}"
    )


def test_get_aggregate_data_returns_correct_structure(mocker):
    """Verify returned data structure is nested dict: {platform: {source: {metric: DataFrame}}}.

    BigQuery client is MOCKED - returns synthetic data only.

    Failure indicates structure change that would break downstream code.
    """
    # Mock BigQuery client
    mock_client = MagicMock()
    mock_query_result = MagicMock()
    mock_query_result.to_dataframe.return_value = generate_desktop_raw_data(num_days=5)
    mock_client.query.return_value = mock_query_result

    mocker.patch('mozaic_daily.data.bigquery.Client', return_value=mock_client)

    queries = get_queries("'US', 'DE'")
    result = get_aggregate_data(queries, 'test-project', checkpoints=False)

    # Check structure: {platform: {source: {metric: DataFrame}}}
    assert 'desktop' in result, "Expected 'desktop' key in result"
    assert 'mobile' in result, "Expected 'mobile' key in result"

    # Check desktop has both glean and legacy sources
    assert 'glean' in result['desktop'], "Expected 'glean' source in desktop results"
    assert 'legacy' in result['desktop'], "Expected 'legacy' source in desktop results"

    # Check desktop glean metrics
    desktop_metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']
    for metric in desktop_metrics:
        assert metric in result['desktop']['glean'], f"Expected metric '{metric}' in desktop glean results"
        assert isinstance(result['desktop']['glean'][metric], pd.DataFrame), (
            f"Expected desktop glean['{metric}'] to be a DataFrame"
        )
        assert metric in result['desktop']['legacy'], f"Expected metric '{metric}' in desktop legacy results"
        assert isinstance(result['desktop']['legacy'][metric], pd.DataFrame), (
            f"Expected desktop legacy['{metric}'] to be a DataFrame"
        )

    # Check mobile has glean source
    assert 'glean' in result['mobile'], "Expected 'glean' source in mobile results"

    # Check mobile glean metrics
    mobile_metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']
    for metric in mobile_metrics:
        assert metric in result['mobile']['glean'], f"Expected metric '{metric}' in mobile glean results"
        assert isinstance(result['mobile']['glean'][metric], pd.DataFrame), (
            f"Expected mobile glean['{metric}'] to be a DataFrame"
        )


def test_get_aggregate_data_handles_bigquery_errors(mocker):
    """Test graceful handling of BigQuery failures (timeout, auth, etc).

    Mock the BigQuery client to raise exceptions, verify error handling.
    No actual BigQuery communication occurs.

    Should raise appropriate error with helpful message, not crash silently.
    """
    # Mock BigQuery client to raise exception
    mock_client = MagicMock()
    mock_client.query.side_effect = Exception("BigQuery timeout")

    mocker.patch('mozaic_daily.data.bigquery.Client', return_value=mock_client)

    queries = get_queries("'US', 'DE'")

    # Should raise exception (not catch silently)
    with pytest.raises(Exception) as exc_info:
        get_aggregate_data(queries, 'test-project', checkpoints=False)

    assert "BigQuery timeout" in str(exc_info.value), (
        "Expected BigQuery error to propagate"
    )


# ===== CHECKPOINTING =====

def test_checkpointing_saves_parquet_files(tmp_path, mocker):
    """Verify checkpoint files are created in expected format.

    Uses synthetic data from mocked BigQuery client.
    Expected files: mozaic_parts.raw.{source}.{platform}.{metric}.parquet

    Failure indicates checkpoint filenames changed or files not created.
    """
    # Change to tmp directory
    original_dir = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Mock BigQuery client
        mock_client = MagicMock()
        mock_query_result = MagicMock()
        mock_query_result.to_dataframe.return_value = generate_desktop_raw_data(num_days=5)
        mock_client.query.return_value = mock_query_result

        mocker.patch('mozaic_daily.data.bigquery.Client', return_value=mock_client)

        queries = get_queries("'US', 'DE'")
        result = get_aggregate_data(queries, 'test-project', checkpoints=True)

        # Verify checkpoint files exist
        expected_files = [
            # Desktop Glean
            'mozaic_parts.raw.glean.desktop.DAU.parquet',
            'mozaic_parts.raw.glean.desktop.New Profiles.parquet',
            'mozaic_parts.raw.glean.desktop.Existing Engagement DAU.parquet',
            'mozaic_parts.raw.glean.desktop.Existing Engagement MAU.parquet',
            # Desktop Legacy
            'mozaic_parts.raw.legacy.desktop.DAU.parquet',
            'mozaic_parts.raw.legacy.desktop.New Profiles.parquet',
            'mozaic_parts.raw.legacy.desktop.Existing Engagement DAU.parquet',
            'mozaic_parts.raw.legacy.desktop.Existing Engagement MAU.parquet',
            # Mobile Glean
            'mozaic_parts.raw.glean.mobile.DAU.parquet',
            'mozaic_parts.raw.glean.mobile.New Profiles.parquet',
            'mozaic_parts.raw.glean.mobile.Existing Engagement DAU.parquet',
            'mozaic_parts.raw.glean.mobile.Existing Engagement MAU.parquet',
        ]

        for filename in expected_files:
            filepath = tmp_path / filename
            assert filepath.exists(), (
                f"Expected checkpoint file '{filename}' to be created. "
                f"Files in {tmp_path}: {os.listdir(tmp_path)}"
            )

    finally:
        os.chdir(original_dir)


def test_checkpointing_loads_existing_files_without_querying(tmp_path, mocker):
    """When checkpoints exist, should load from files without calling BigQuery.

    Create synthetic checkpoint files, verify they're loaded.
    BigQuery client should never be called.

    Failure indicates checkpoint loading broken or BigQuery called unnecessarily.
    """
    # Change to tmp directory
    original_dir = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Create checkpoint files with new naming scheme
        metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']

        for metric in metrics:
            # Desktop Glean
            df_desktop = generate_desktop_raw_data(num_days=5)
            df_desktop.to_parquet(tmp_path / f'mozaic_parts.raw.glean.desktop.{metric}.parquet')

            # Desktop Legacy
            df_desktop = generate_desktop_raw_data(num_days=5)
            df_desktop.to_parquet(tmp_path / f'mozaic_parts.raw.legacy.desktop.{metric}.parquet')

            # Mobile Glean
            df_mobile = generate_mobile_raw_data(num_days=5)
            df_mobile.to_parquet(tmp_path / f'mozaic_parts.raw.glean.mobile.{metric}.parquet')

        # Mock BigQuery client (should NOT be called)
        mock_client = MagicMock()
        mocker.patch('mozaic_daily.data.bigquery.Client', return_value=mock_client)

        queries = get_queries("'US', 'DE'")
        result = get_aggregate_data(queries, 'test-project', checkpoints=True)

        # Verify BigQuery client was NOT called
        assert mock_client.query.call_count == 0, (
            f"Expected BigQuery to NOT be called when checkpoints exist, "
            f"but it was called {mock_client.query.call_count} times"
        )

        # Verify data was loaded with correct structure
        assert 'desktop' in result, "Expected 'desktop' key in result"
        assert 'mobile' in result, "Expected 'mobile' key in result"
        assert 'glean' in result['desktop'], "Expected 'glean' source in desktop results"
        assert 'legacy' in result['desktop'], "Expected 'legacy' source in desktop results"
        assert 'DAU' in result['desktop']['glean'], "Expected 'DAU' in desktop glean results"

    finally:
        os.chdir(original_dir)


def test_checkpointing_skips_bigquery_when_files_exist(tmp_path, mocker):
    """Verify BigQuery client query method is not called when checkpoint files exist.

    Create synthetic checkpoint files first, then run get_aggregate_data.
    Mock should verify BigQuery client.query() was never called.

    Failure indicates expensive BigQuery calls happening when they shouldn't.
    """
    # Change to tmp directory
    original_dir = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Create ALL checkpoint files first with new naming scheme
        metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']

        for metric in metrics:
            # Desktop Glean
            df_desktop = generate_desktop_raw_data(num_days=10, countries=['US', 'DE'])
            df_desktop.to_parquet(tmp_path / f'mozaic_parts.raw.glean.desktop.{metric}.parquet')

            # Desktop Legacy
            df_desktop = generate_desktop_raw_data(num_days=10, countries=['US', 'DE'])
            df_desktop.to_parquet(tmp_path / f'mozaic_parts.raw.legacy.desktop.{metric}.parquet')

            # Mobile Glean
            df_mobile = generate_mobile_raw_data(num_days=10, countries=['US', 'DE'])
            df_mobile.to_parquet(tmp_path / f'mozaic_parts.raw.glean.mobile.{metric}.parquet')

        # Mock BigQuery client
        mock_client = MagicMock()
        mock_query_result = MagicMock()
        mock_query_result.to_dataframe.return_value = generate_desktop_raw_data(num_days=5)
        mock_client.query.return_value = mock_query_result

        mocker.patch('mozaic_daily.data.bigquery.Client', return_value=mock_client)

        # Run get_aggregate_data
        queries = get_queries("'US', 'DE'")
        result = get_aggregate_data(queries, 'test-project', checkpoints=True)

        # Verify BigQuery query method was NEVER called
        assert mock_client.query.call_count == 0, (
            f"Expected BigQuery.query() to NOT be called when checkpoints exist. "
            f"Called {mock_client.query.call_count} times. This is inefficient!"
        )

        # Verify we got data from checkpoints with correct structure
        assert 'desktop' in result
        assert 'mobile' in result
        assert 'glean' in result['desktop']
        assert 'legacy' in result['desktop']
        assert len(result['desktop']['glean']['DAU']) > 0, "Should have loaded data from checkpoint"

    finally:
        os.chdir(original_dir)


# ===== check_training_data_availability() TESTS =====

def _make_mock_client_with_max_date(mocker, max_date_value):
    """Helper to mock bigquery.Client returning a fixed max_date for all queries."""
    mock_client = MagicMock()
    mock_result = MagicMock()
    mock_result.to_dataframe.return_value = pd.DataFrame({'max_date': [max_date_value]})
    mock_client.query.return_value = mock_result
    mocker.patch('mozaic_daily.data.bigquery.Client', return_value=mock_client)
    return mock_client


def test_check_training_data_availability_passes_when_data_is_current(mocker):
    """Verify no exception raised when all tables have data on training_end_date.

    BigQuery client is MOCKED to return max_date == training_end_date.

    Failure indicates the availability check incorrectly blocks valid runs.
    """
    training_end_date = '2026-02-16'
    _make_mock_client_with_max_date(mocker, pd.Timestamp('2026-02-16'))

    # Should not raise
    check_training_data_availability('test-project', training_end_date)


def test_check_training_data_availability_passes_when_data_is_ahead(mocker):
    """Verify no exception raised when tables have data beyond training_end_date.

    BigQuery client is MOCKED to return max_date > training_end_date.

    Failure indicates the check incorrectly fails when tables are ahead.
    """
    training_end_date = '2026-02-16'
    _make_mock_client_with_max_date(mocker, pd.Timestamp('2026-02-17'))

    # Should not raise
    check_training_data_availability('test-project', training_end_date)


def test_check_training_data_availability_raises_when_data_is_behind(mocker):
    """Verify ValueError raised when a table's max date is before training_end_date.

    BigQuery client is MOCKED to return max_date one day before required.

    Failure indicates the pre-flight check is not catching the problem.
    """
    training_end_date = '2026-02-16'
    _make_mock_client_with_max_date(mocker, pd.Timestamp('2026-02-15'))

    with pytest.raises(ValueError):
        check_training_data_availability('test-project', training_end_date)


def test_check_training_data_availability_error_includes_dates(mocker):
    """Verify the ValueError message includes the required and available dates.

    BigQuery client is MOCKED to return max_date one day before required.

    Failure indicates unhelpful error messages that make debugging harder.
    """
    training_end_date = '2026-02-16'
    _make_mock_client_with_max_date(mocker, pd.Timestamp('2026-02-15'))

    with pytest.raises(ValueError) as exc_info:
        check_training_data_availability('test-project', training_end_date)

    error_message = str(exc_info.value)
    assert '2026-02-16' in error_message, (
        f"Expected required date '2026-02-16' in error message: {error_message}"
    )
    assert '2026-02-15' in error_message, (
        f"Expected available date '2026-02-15' in error message: {error_message}"
    )


def test_check_training_data_availability_error_suggests_forecast_start_date(mocker):
    """Verify the ValueError message suggests an actionable --forecast_start_date.

    When data is available through 2026-02-15, the suggested start date is 2026-02-16
    (available date + 1 day), so the pipeline uses T-1 data that actually exists.

    Failure indicates missing actionable guidance in the error message.
    """
    training_end_date = '2026-02-16'
    _make_mock_client_with_max_date(mocker, pd.Timestamp('2026-02-15'))

    with pytest.raises(ValueError) as exc_info:
        check_training_data_availability('test-project', training_end_date)

    error_message = str(exc_info.value)
    # Suggested date = max_date + 1 day = 2026-02-16
    assert '--forecast_start_date 2026-02-16' in error_message, (
        f"Expected '--forecast_start_date 2026-02-16' in error message: {error_message}"
    )


def test_check_training_data_availability_error_includes_table_name(mocker):
    """Verify the ValueError message includes the name of the unavailable table.

    BigQuery client is MOCKED to return max_date one day before required.

    Failure indicates the error message doesn't help identify which table is behind.
    """
    training_end_date = '2026-02-16'
    mock_client = _make_mock_client_with_max_date(mocker, pd.Timestamp('2026-02-15'))

    with pytest.raises(ValueError) as exc_info:
        check_training_data_availability('test-project', training_end_date)

    error_message = str(exc_info.value)
    # The error should mention a BigQuery table name (contains project.dataset.table format)
    assert 'moz-fx-data-shared-prod' in error_message, (
        f"Expected BigQuery project name in error message: {error_message}"
    )