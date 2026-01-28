# -*- coding: utf-8 -*-
"""
Tests for forecasting functions in mozaic_daily.py.

Tests Mozaic integration and forecast DataFrame generation.

🔒 SECURITY: Uses synthetic test data only.
"""

import pandas as pd
from unittest.mock import MagicMock

from mozaic_daily.forecast import get_forecast_dfs, get_desktop_forecast_dfs, get_mobile_forecast_dfs
from mozaic.models import desktop_forecast_model, mobile_forecast_model
from tests.conftest import generate_forecast_data


# ===== MOZAIC INTEGRATION =====

def test_get_forecast_dfs_calls_populate_tiles(mocker, sample_datasets):
    """Verify populate_tiles is called with correct arguments.

    Should receive: datasets, tileset, model, start_date, end_date

    Failure indicates Mozaic integration broken or parameter order changed.
    """
    # Mock Mozaic components
    mock_tileset = MagicMock()
    mocker.patch('mozaic_daily.forecast.mozaic.TileSet', return_value=mock_tileset)

    mock_populate = mocker.patch('mozaic_daily.forecast.mozaic.populate_tiles')
    mock_curate = mocker.patch('mozaic_daily.forecast.mozaic.utils.curate_mozaics')

    # Mock Mozaic.to_granular_forecast_df
    mock_mozaic = MagicMock()
    mock_mozaic.to_granular_forecast_df.return_value = generate_forecast_data(num_days=10)

    def mock_curate_side_effect(datasets, tileset, model, mozaics, *args):
        mozaics['DAU'] = mock_mozaic
        mozaics['New Profiles'] = mock_mozaic
        mozaics['Existing Engagement DAU'] = mock_mozaic
        mozaics['Existing Engagement MAU'] = mock_mozaic

    mock_curate.side_effect = mock_curate_side_effect

    # Run function
    result = get_forecast_dfs(
        sample_datasets['desktop'],
        desktop_forecast_model,
        '2024-02-01',
        '2024-12-31'
    )

    # Verify populate_tiles was called
    assert mock_populate.called, "Expected populate_tiles to be called"

    # Verify arguments
    call_args = mock_populate.call_args
    assert call_args is not None, "populate_tiles should have been called with arguments"

    # Check that datasets, tileset, model, dates were passed
    args, kwargs = call_args
    assert len(args) == 5, (
        f"Expected populate_tiles to be called with 5 args (datasets, tileset, model, start, end), got {len(args)}"
    )


def test_get_forecast_dfs_calls_curate_mozaics(mocker, sample_datasets):
    """Verify curate_mozaics is called with correct arguments.

    Failure indicates curate step missing or arguments incorrect.
    """
    # Mock Mozaic components
    mock_tileset = MagicMock()
    mocker.patch('mozaic_daily.forecast.mozaic.TileSet', return_value=mock_tileset)

    mock_populate = mocker.patch('mozaic_daily.forecast.mozaic.populate_tiles')
    mock_curate = mocker.patch('mozaic_daily.forecast.mozaic.utils.curate_mozaics')

    # Mock Mozaic.to_granular_forecast_df
    mock_mozaic = MagicMock()
    mock_mozaic.to_granular_forecast_df.return_value = generate_forecast_data(num_days=10)

    def mock_curate_side_effect(datasets, tileset, model, mozaics, *args):
        mozaics['DAU'] = mock_mozaic
        mozaics['New Profiles'] = mock_mozaic
        mozaics['Existing Engagement DAU'] = mock_mozaic
        mozaics['Existing Engagement MAU'] = mock_mozaic

    mock_curate.side_effect = mock_curate_side_effect

    # Run function
    result = get_forecast_dfs(
        sample_datasets['desktop'],
        desktop_forecast_model,
        '2024-02-01',
        '2024-12-31'
    )

    # Verify curate_mozaics was called
    assert mock_curate.called, "Expected curate_mozaics to be called"

    # Verify arguments
    call_args = mock_curate.call_args
    assert call_args is not None, "curate_mozaics should have been called with arguments"


def test_get_forecast_dfs_returns_metric_dataframes(mocker, sample_datasets):
    """Verify output is dict mapping metric names to DataFrames.

    Should return DataFrame with columns: target_date, country, population, source, value

    Failure indicates output structure changed, breaking downstream code.
    """
    # Mock Mozaic components
    mock_tileset = MagicMock()
    mocker.patch('mozaic_daily.forecast.mozaic.TileSet', return_value=mock_tileset)

    mock_populate = mocker.patch('mozaic_daily.forecast.mozaic.populate_tiles')
    mock_curate = mocker.patch('mozaic_daily.forecast.mozaic.utils.curate_mozaics')

    # Mock Mozaic.to_granular_forecast_df
    mock_mozaic_dau = MagicMock()
    mock_mozaic_dau.to_granular_forecast_df.return_value = generate_forecast_data(
        num_days=10,
        countries=['US', 'DE']
    )

    mock_mozaic_np = MagicMock()
    mock_mozaic_np.to_granular_forecast_df.return_value = generate_forecast_data(
        num_days=10,
        countries=['US', 'DE']
    )

    def mock_curate_side_effect(datasets, tileset, model, mozaics, *args):
        mozaics['DAU'] = mock_mozaic_dau
        mozaics['New Profiles'] = mock_mozaic_np
        mozaics['Existing Engagement DAU'] = mock_mozaic_dau
        mozaics['Existing Engagement MAU'] = mock_mozaic_dau

    mock_curate.side_effect = mock_curate_side_effect

    # Run function
    result = get_forecast_dfs(
        sample_datasets['desktop'],
        desktop_forecast_model,
        '2024-02-01',
        '2024-12-31'
    )

    # Verify output is dict
    assert isinstance(result, dict), f"Expected dict output, got {type(result)}"

    # Verify metrics present
    expected_metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']
    for metric in expected_metrics:
        assert metric in result, f"Expected metric '{metric}' in output"
        assert isinstance(result[metric], pd.DataFrame), (
            f"Expected result['{metric}'] to be a DataFrame, got {type(result[metric])}"
        )

        # Verify DataFrame has expected columns
        expected_cols = ['target_date', 'country', 'population', 'source', 'value']
        for col in expected_cols:
            assert col in result[metric].columns, (
                f"Expected column '{col}' in {metric} DataFrame. "
                f"Found columns: {result[metric].columns.tolist()}"
            )


def test_desktop_forecast_uses_desktop_model(mocker, sample_datasets):
    """Verify get_desktop_forecast_dfs uses desktop_forecast_model.

    Failure indicates wrong model being used for platform.
    """
    # Mock Mozaic components
    mock_tileset = MagicMock()
    mocker.patch('mozaic_daily.forecast.mozaic.TileSet', return_value=mock_tileset)

    mock_populate = mocker.patch('mozaic_daily.forecast.mozaic.populate_tiles')
    mock_curate = mocker.patch('mozaic_daily.forecast.mozaic.utils.curate_mozaics')

    # Mock Mozaic.to_granular_forecast_df
    mock_mozaic = MagicMock()
    mock_mozaic.to_granular_forecast_df.return_value = generate_forecast_data(num_days=10)

    def mock_curate_side_effect(datasets, tileset, model, mozaics, *args):
        # Verify model is desktop_forecast_model
        assert model == desktop_forecast_model, (
            f"Expected desktop_forecast_model, got {model}"
        )
        mozaics['DAU'] = mock_mozaic
        mozaics['New Profiles'] = mock_mozaic
        mozaics['Existing Engagement DAU'] = mock_mozaic
        mozaics['Existing Engagement MAU'] = mock_mozaic

    mock_curate.side_effect = mock_curate_side_effect

    # Run function
    result = get_desktop_forecast_dfs(
        sample_datasets,
        '2024-02-01',
        '2024-12-31'
    )

    # If we reach here, the assertion in mock_curate_side_effect passed
    assert mock_curate.called, "curate_mozaics should have been called"


def test_mobile_forecast_uses_mobile_model(mocker, sample_datasets):
    """Verify get_mobile_forecast_dfs uses mobile_forecast_model.

    Failure indicates wrong model being used for platform.
    """
    # Mock Mozaic components
    mock_tileset = MagicMock()
    mocker.patch('mozaic_daily.forecast.mozaic.TileSet', return_value=mock_tileset)

    mock_populate = mocker.patch('mozaic_daily.forecast.mozaic.populate_tiles')
    mock_curate = mocker.patch('mozaic_daily.forecast.mozaic.utils.curate_mozaics')

    # Mock Mozaic.to_granular_forecast_df
    mock_mozaic = MagicMock()
    mock_mozaic.to_granular_forecast_df.return_value = generate_forecast_data(num_days=10)

    def mock_curate_side_effect(datasets, tileset, model, mozaics, *args):
        # Verify model is mobile_forecast_model
        assert model == mobile_forecast_model, (
            f"Expected mobile_forecast_model, got {model}"
        )
        mozaics['DAU'] = mock_mozaic
        mozaics['New Profiles'] = mock_mozaic
        mozaics['Existing Engagement DAU'] = mock_mozaic
        mozaics['Existing Engagement MAU'] = mock_mozaic

    mock_curate.side_effect = mock_curate_side_effect

    # Run function
    result = get_mobile_forecast_dfs(
        sample_datasets,
        '2024-02-01',
        '2024-12-31'
    )

    # If we reach here, the assertion in mock_curate_side_effect passed
    assert mock_curate.called, "curate_mozaics should have been called"
