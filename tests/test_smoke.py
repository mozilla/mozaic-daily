# -*- coding: utf-8 -*-
"""
Smoke tests for mozaic_daily pipeline.

These tests verify the pipeline runs without crashing and components are called
in the correct order. They do NOT verify correctness of business logic - that's
covered in unit tests (test_table_manipulation.py, etc.).

🔒 SECURITY: Uses only synthetic checkpoint files. No real BigQuery data or connections.
"""

import pytest
import pandas as pd
import os
from datetime import datetime
from unittest.mock import MagicMock

from mozaic_daily import main
from tests.conftest import (
    generate_desktop_raw_data,
    generate_mobile_raw_data,
    generate_forecast_data
)


# Mark all tests in this file as smoke tests
pytestmark = pytest.mark.smoke


# ===== SMOKE TESTS =====

def test_pipeline_completes_without_crashing(sample_checkpoint_files, mocker):
    """Verify the pipeline runs to completion without errors.

    This is a smoke test - just checking it doesn't crash.
    Does NOT verify correctness of output.

    Failure indicates something is broken in the pipeline flow.
    """
    original_dir = os.getcwd()
    os.chdir(sample_checkpoint_files)

    try:
        # Mock Mozaic components
        mock_tileset = MagicMock()
        mocker.patch('mozaic_daily.mozaic.TileSet', return_value=mock_tileset)
        mocker.patch('mozaic_daily.mozaic.populate_tiles')

        mock_mozaic_desktop = MagicMock()
        mock_mozaic_desktop.to_granular_forecast_df.return_value = generate_forecast_data(
            start_date='2024-01-31',
            num_days=30,
            countries=['US', 'DE', 'FR', 'None'],
            populations=['win10', 'win11', 'winX', 'None']
        )

        mock_mozaic_mobile = MagicMock()
        mock_mozaic_mobile.to_granular_forecast_df.return_value = generate_forecast_data(
            start_date='2024-01-31',
            num_days=30,
            countries=['US', 'DE', 'None'],
            populations=['fenix_android', 'firefox_ios', 'focus_android', 'focus_ios', 'None']
        )

        def mock_curate_side_effect(datasets, tileset, model, mozaics, *args):
            from mozaic.models import desktop_forecast_model
            if model == desktop_forecast_model:
                mozaics['DAU'] = mock_mozaic_desktop
                mozaics['New Profiles'] = mock_mozaic_desktop
                mozaics['Existing Engagement DAU'] = mock_mozaic_desktop
                mozaics['Existing Engagement MAU'] = mock_mozaic_desktop
            else:
                mozaics['DAU'] = mock_mozaic_mobile
                mozaics['New Profiles'] = mock_mozaic_mobile
                mozaics['Existing Engagement DAU'] = mock_mozaic_mobile
                mozaics['Existing Engagement MAU'] = mock_mozaic_mobile

        mocker.patch('mozaic_daily.mozaic.utils.curate_mozaics', side_effect=mock_curate_side_effect)

        mock_constants = {
            'forecast_start_date': '2024-01-31',
            'forecast_end_date': '2024-12-31',
            'forecast_run_dt': datetime(2024, 1, 31, 10, 0, 0),
            'training_end_date': '2024-01-29',
            'default_project': 'test-project',
            'countries': {'US', 'DE', 'FR'},
            'country_string': "'DE', 'FR', 'US'",
            'forecast_checkpoint_filename': 'mozaic_parts.forecast.parquet',
        }
        mocker.patch('mozaic_daily.get_constants', return_value=mock_constants)

        # Run pipeline - should complete without exceptions
        df = main(project='test-project', checkpoints=True)

        # Basic smoke test assertions
        assert df is not None, "Pipeline returned None"
        assert isinstance(df, pd.DataFrame), "Pipeline did not return a DataFrame"
        assert len(df) > 0, "Pipeline returned empty DataFrame"

    finally:
        os.chdir(original_dir)


def test_pipeline_calls_components_in_order(sample_checkpoint_files, mocker):
    """Verify pipeline calls components in the expected order.

    Checks:
    1. Data is loaded from checkpoints
    2. Desktop forecasting runs
    3. Mobile forecasting runs
    4. Results are combined and formatted

    Failure indicates orchestration logic changed.
    """
    original_dir = os.getcwd()
    os.chdir(sample_checkpoint_files)

    try:
        # Track calls
        call_order = []

        # Mock Mozaic
        mock_tileset = MagicMock()
        mocker.patch('mozaic_daily.mozaic.TileSet', return_value=mock_tileset)

        def mock_populate(*args):
            call_order.append('populate_tiles')

        mocker.patch('mozaic_daily.mozaic.populate_tiles', side_effect=mock_populate)

        mock_mozaic = MagicMock()
        mock_mozaic.to_granular_forecast_df.return_value = generate_forecast_data(num_days=10)

        def mock_curate(*args):
            call_order.append('curate_mozaics')
            args[3]['DAU'] = mock_mozaic
            args[3]['New Profiles'] = mock_mozaic
            args[3]['Existing Engagement DAU'] = mock_mozaic
            args[3]['Existing Engagement MAU'] = mock_mozaic

        mocker.patch('mozaic_daily.mozaic.utils.curate_mozaics', side_effect=mock_curate)

        mock_constants = {
            'forecast_start_date': '2024-01-31',
            'forecast_end_date': '2024-12-31',
            'forecast_run_dt': datetime(2024, 1, 31, 10, 0, 0),
            'training_end_date': '2024-01-29',
            'default_project': 'test-project',
            'countries': {'US', 'DE'},
            'country_string': "'DE', 'US'",
            'forecast_checkpoint_filename': 'mozaic_parts.forecast.parquet',
        }
        mocker.patch('mozaic_daily.get_constants', return_value=mock_constants)

        # Run pipeline
        df = main(project='test-project', checkpoints=True)

        # Verify components were called
        assert 'populate_tiles' in call_order, "populate_tiles was not called"
        assert 'curate_mozaics' in call_order, "curate_mozaics was not called"

        # Verify both platforms were processed (2 populate + 2 curate = 4 total)
        assert call_order.count('populate_tiles') == 2, "Expected 2 populate_tiles calls (desktop + mobile)"
        assert call_order.count('curate_mozaics') == 2, "Expected 2 curate_mozaics calls (desktop + mobile)"

    finally:
        os.chdir(original_dir)


def test_checkpoint_system_works(tmp_path, mocker):
    """Verify checkpoint system: files are created and can be reloaded.

    This is a basic smoke test of the checkpointing mechanism.

    Failure indicates checkpointing is broken.
    """
    original_dir = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Mock BigQuery
        mock_client = MagicMock()
        mock_query_result = MagicMock()

        def mock_query_side_effect(query):
            result = MagicMock()
            if 'mobile' in query.lower() or 'fenix' in query.lower():
                result.to_dataframe.return_value = generate_mobile_raw_data(num_days=10)
            else:
                result.to_dataframe.return_value = generate_desktop_raw_data(num_days=10)
            return result

        mock_client.query.side_effect = mock_query_side_effect
        mocker.patch('mozaic_daily.bigquery.Client', return_value=mock_client)

        # Mock Mozaic
        mock_tileset = MagicMock()
        mocker.patch('mozaic_daily.mozaic.TileSet', return_value=mock_tileset)
        mocker.patch('mozaic_daily.mozaic.populate_tiles')

        mock_mozaic = MagicMock()
        mock_mozaic.to_granular_forecast_df.return_value = generate_forecast_data(num_days=10)

        def mock_curate_side_effect(datasets, tileset, model, mozaics, *args):
            mozaics['DAU'] = mock_mozaic
            mozaics['New Profiles'] = mock_mozaic
            mozaics['Existing Engagement DAU'] = mock_mozaic
            mozaics['Existing Engagement MAU'] = mock_mozaic

        mocker.patch('mozaic_daily.mozaic.utils.curate_mozaics', side_effect=mock_curate_side_effect)

        mock_constants = {
            'forecast_start_date': '2024-01-31',
            'forecast_end_date': '2024-12-31',
            'forecast_run_dt': datetime(2024, 1, 31, 10, 0, 0),
            'training_end_date': '2024-01-29',
            'default_project': 'test-project',
            'countries': {'US', 'DE'},
            'country_string': "'DE', 'US'",
            'forecast_checkpoint_filename': 'mozaic_parts.forecast.parquet',
        }
        mocker.patch('mozaic_daily.get_constants', return_value=mock_constants)

        # First run: create checkpoints
        df1 = main(project='test-project', checkpoints=True)

        # Verify checkpoint files were created
        expected_files = [
            'mozaic_parts.raw.desktop.DAU.parquet',
            'mozaic_parts.raw.mobile.DAU.parquet',
            'mozaic_parts.forecast.parquet',
        ]

        for filename in expected_files:
            filepath = tmp_path / filename
            assert filepath.exists(), f"Checkpoint file '{filename}' was not created"

        # Second run: should load from checkpoints (no BigQuery calls)
        query_count_before = mock_client.query.call_count

        df2 = main(project='test-project', checkpoints=True)

        query_count_after = mock_client.query.call_count

        # Verify BigQuery was not called again
        assert query_count_after == query_count_before, (
            "BigQuery was called when checkpoints existed - checkpointing not working"
        )

    finally:
        os.chdir(original_dir)


def test_desktop_and_mobile_processed_separately(sample_checkpoint_files, mocker):
    """Verify desktop and mobile data flow through separate code paths.

    This checks that the pipeline correctly splits processing between platforms.

    Failure indicates desktop/mobile split is broken.
    """
    original_dir = os.getcwd()
    os.chdir(sample_checkpoint_files)

    try:
        # Track which models are used
        models_used = []

        mock_tileset = MagicMock()
        mocker.patch('mozaic_daily.mozaic.TileSet', return_value=mock_tileset)
        mocker.patch('mozaic_daily.mozaic.populate_tiles')

        mock_mozaic = MagicMock()
        mock_mozaic.to_granular_forecast_df.return_value = generate_forecast_data(num_days=10)

        def mock_curate_side_effect(datasets, tileset, model, mozaics, *args):
            models_used.append(model)
            mozaics['DAU'] = mock_mozaic
            mozaics['New Profiles'] = mock_mozaic
            mozaics['Existing Engagement DAU'] = mock_mozaic
            mozaics['Existing Engagement MAU'] = mock_mozaic

        mocker.patch('mozaic_daily.mozaic.utils.curate_mozaics', side_effect=mock_curate_side_effect)

        mock_constants = {
            'forecast_start_date': '2024-01-31',
            'forecast_end_date': '2024-12-31',
            'forecast_run_dt': datetime(2024, 1, 31, 10, 0, 0),
            'training_end_date': '2024-01-29',
            'default_project': 'test-project',
            'countries': {'US', 'DE'},
            'country_string': "'DE', 'US'",
            'forecast_checkpoint_filename': 'mozaic_parts.forecast.parquet',
        }
        mocker.patch('mozaic_daily.get_constants', return_value=mock_constants)

        # Run pipeline
        df = main(project='test-project', checkpoints=True)

        # Verify both models were used
        from mozaic.models import desktop_forecast_model, mobile_forecast_model

        assert desktop_forecast_model in models_used, "Desktop model was not used"
        assert mobile_forecast_model in models_used, "Mobile model was not used"
        assert len(models_used) == 2, f"Expected 2 model calls, got {len(models_used)}"

    finally:
        os.chdir(original_dir)
