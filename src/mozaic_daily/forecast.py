# -*- coding: utf-8 -*-
"""Mozaic-based forecasting for Desktop and Mobile metrics.

This module uses the Mozaic package to generate forecasts:
1. Creates TileSet and populates tiles for each metric/country/segment
2. Curates mozaics by aggregating tiles
3. Applies platform-specific forecast models
4. Returns DataFrames with granular forecasts

Functions:
- get_forecast_dfs(): Generic forecast generation
- get_desktop_forecast_dfs(): Desktop-specific wrapper
- get_mobile_forecast_dfs(): Mobile-specific wrapper
"""

from typing import Dict, Any
import pandas as pd
import warnings
from collections import defaultdict
import mozaic
from mozaic.models import desktop_forecast_model, mobile_forecast_model
from mozaic import Mozaic


# Do the forecasting
def get_forecast_dfs(
    datasets: Dict[str, pd.DataFrame],
    forecast_model: Any,
    forecast_start_date: str,
    forecast_end_date: str,
    quantile: float = None,
) -> Dict[str, pd.DataFrame]:
    """Generate forecasts using Mozaic.

    Args:
        datasets: Dict of metric -> DataFrame with historical data
        forecast_model: Mozaic forecast model (desktop or mobile)
        forecast_start_date: Start date for forecast period
        forecast_end_date: End date for forecast period
        quantile: Quantile for point forecast (default: 0.5 from FORECAST_CONFIG)

    Returns:
        Dict of metric -> DataFrame with forecast results

    Example - Iterating over quantiles:
        # Compare forecasts at different quantiles
        for q in [0.25, 0.5, 0.75]:
            dfs = get_desktop_forecast_dfs(datasets, start, end, quantile=q)
            # Analyze sensitivity to quantile choice
    """
    from .config import FORECAST_CONFIG

    if quantile is None:
        quantile = FORECAST_CONFIG['quantile']
    tileset = mozaic.TileSet()

    print('\n--- Populate tiles\n')
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=RuntimeWarning,
            message=".*divide by zero.*|.*overflow.*|.*invalid value.*"
        )
        mozaic.populate_tiles(
            datasets,
            tileset,
            forecast_model,
            forecast_start_date,
            forecast_end_date,
        )

    mozaics: Dict[str, Mozaic] = {}
    _ctry = defaultdict(lambda: defaultdict(mozaic.Mozaic))
    _pop = defaultdict(lambda: defaultdict(mozaic.Mozaic))

    print('\n--- Curate Mozaics\n')
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=RuntimeWarning,
            message=".*divide by zero.*|.*overflow.*|.*invalid value.*"
        )

        mozaic.utils.curate_mozaics(
            datasets,
            tileset,
            forecast_model,
            mozaics,
            _ctry,
            _pop,
        )

    dfs = {}
    for metric, moz in mozaics.items():
        print(metric)
        dfs[metric] = moz.to_granular_forecast_df(quantile=quantile)

    return dfs


def get_desktop_forecast_dfs(
    metric_data: Dict[str, pd.DataFrame],
    forecast_start_date: str,
    forecast_end_date: str,
    quantile: float = None,
) -> Dict[str, pd.DataFrame]:
    """Generate Desktop forecasts using Mozaic.

    Args:
        metric_data: Dict of metric -> DataFrame (already source-specific)
        forecast_start_date: Start date for forecast period
        forecast_end_date: End date for forecast period
        quantile: Quantile for point forecast (default: 0.5)

    Returns:
        Dict of metric -> DataFrame with forecast results
    """
    return get_forecast_dfs(
        metric_data,
        desktop_forecast_model,
        forecast_start_date,
        forecast_end_date,
        quantile=quantile,
    )


def get_mobile_forecast_dfs(
    metric_data: Dict[str, pd.DataFrame],
    forecast_start_date: str,
    forecast_end_date: str,
    quantile: float = None,
) -> Dict[str, pd.DataFrame]:
    """Generate Mobile forecasts using Mozaic.

    Args:
        metric_data: Dict of metric -> DataFrame (already source-specific)
        forecast_start_date: Start date for forecast period
        forecast_end_date: End date for forecast period
        quantile: Quantile for point forecast (default: 0.5)

    Returns:
        Dict of metric -> DataFrame with forecast results
    """
    return get_forecast_dfs(
        metric_data,
        mobile_forecast_model,
        forecast_start_date,
        forecast_end_date,
        quantile=quantile,
    )
