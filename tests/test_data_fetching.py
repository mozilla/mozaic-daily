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

from mozaic_daily.data import get_aggregate_data, get_queries
from tests.conftest import generate_desktop_raw_data, generate_mobile_raw_data


# ===== BIGQUERY INTEGRATION (100% MOCKED) =====

def test_get_aggregate_data_executes_all_queries(mocker):
    """Verify that all platform/metric queries are executed.

    BigQuery client is MOCKED - no actual queries sent to BigQuery.
    Returns synthetic DataFrames matching expected schema from SQL queries.

    Should execute:
    - 4 desktop queries (DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU)
    - 4 mobile queries (DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU)

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
    # Should have called query() 8 times (4 desktop + 4 mobile)
    assert mock_client.query.call_count == 8, (
        f"Expected 8 queries to be executed (4 desktop + 4 mobile), got {mock_client.query.call_count}"
    )


def test_get_aggregate_data_returns_correct_structure(mocker):
    """Verify returned data structure is nested dict: {platform: {metric: DataFrame}}.

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

    # Check structure: {platform: {metric: DataFrame}}
    assert 'desktop' in result, "Expected 'desktop' key in result"
    assert 'mobile' in result, "Expected 'mobile' key in result"

    # Check desktop metrics
    desktop_metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']
    for metric in desktop_metrics:
        assert metric in result['desktop'], f"Expected metric '{metric}' in desktop results"
        assert isinstance(result['desktop'][metric], pd.DataFrame), (
            f"Expected desktop['{metric}'] to be a DataFrame"
        )

    # Check mobile metrics
    mobile_metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']
    for metric in mobile_metrics:
        assert metric in result['mobile'], f"Expected metric '{metric}' in mobile results"
        assert isinstance(result['mobile'][metric], pd.DataFrame), (
            f"Expected mobile['{metric}'] to be a DataFrame"
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
    Expected files: mozaic_parts.raw.{platform}.{metric}.parquet

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
            'mozaic_parts.raw.desktop.DAU.parquet',
            'mozaic_parts.raw.desktop.New Profiles.parquet',
            'mozaic_parts.raw.desktop.Existing Engagement DAU.parquet',
            'mozaic_parts.raw.desktop.Existing Engagement MAU.parquet',
            'mozaic_parts.raw.mobile.DAU.parquet',
            'mozaic_parts.raw.mobile.New Profiles.parquet',
            'mozaic_parts.raw.mobile.Existing Engagement DAU.parquet',
            'mozaic_parts.raw.mobile.Existing Engagement MAU.parquet',
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
        # Create checkpoint files
        metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']

        for metric in metrics:
            df_desktop = generate_desktop_raw_data(num_days=5)
            df_desktop.to_parquet(tmp_path / f'mozaic_parts.raw.desktop.{metric}.parquet')

            df_mobile = generate_mobile_raw_data(num_days=5)
            df_mobile.to_parquet(tmp_path / f'mozaic_parts.raw.mobile.{metric}.parquet')

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

        # Verify data was loaded
        assert 'desktop' in result, "Expected 'desktop' key in result"
        assert 'mobile' in result, "Expected 'mobile' key in result"
        assert 'DAU' in result['desktop'], "Expected 'DAU' in desktop results"

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
        # Create ALL checkpoint files first
        metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']

        for metric in metrics:
            # Desktop
            df_desktop = generate_desktop_raw_data(num_days=10, countries=['US', 'DE'])
            df_desktop.to_parquet(tmp_path / f'mozaic_parts.raw.desktop.{metric}.parquet')

            # Mobile
            df_mobile = generate_mobile_raw_data(num_days=10, countries=['US', 'DE'])
            df_mobile.to_parquet(tmp_path / f'mozaic_parts.raw.mobile.{metric}.parquet')

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

        # Verify we got data from checkpoints
        assert 'desktop' in result
        assert 'mobile' in result
        assert len(result['desktop']['DAU']) > 0, "Should have loaded data from checkpoint"

    finally:
        os.chdir(original_dir)
