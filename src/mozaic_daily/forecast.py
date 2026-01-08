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
from mozaic import TileSet, Tile, Mozaic, populate_tiles, curate_mozaics, mozaic_divide


# Do the forecasting
def get_forecast_dfs(
    datasets: Dict[str, pd.DataFrame],
    forecast_model: Any,
    forecast_start_date: str,
    forecast_end_date: str
) -> Dict[str, pd.DataFrame]:
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

    mozaics: dict[str, Mozaic] = {}
    _ctry = defaultdict(lambda: defaultdict(mozaic.Mozaic))
    _pop = defaultdict(lambda: defaultdict(mozaic.Mozaic))

    print ('\n--- Curate Mozaics\n')
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
        dfs[metric] = moz.to_granular_forecast_df()

    return dfs


def get_desktop_forecast_dfs(
    datasets: Dict[str, Dict[str, pd.DataFrame]],
    forecast_start_date: str,
    forecast_end_date: str,
) -> Dict[str, pd.DataFrame]:
    return get_forecast_dfs(
        datasets["desktop"],
        desktop_forecast_model,
        forecast_start_date,
        forecast_end_date,
    )


def get_mobile_forecast_dfs(
    datasets: Dict[str, Dict[str, pd.DataFrame]],
    forecast_start_date: str,
    forecast_end_date: str,
) -> Dict[str, pd.DataFrame]:
    return get_forecast_dfs(
        datasets["mobile"],
        mobile_forecast_model,
        forecast_start_date,
        forecast_end_date,
    )
