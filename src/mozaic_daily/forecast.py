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

from dataclasses import dataclass
from typing import Dict, Any, List, Type
import holidays
import pandas as pd
import warnings
from collections import defaultdict
import mozaic
from mozaic.models import (
    desktop_forecast_model,
    mobile_forecast_model,
    ModelConfig,
    DesktopModelConfig,
    MobileModelConfig,
    make_desktop_model,
    make_mobile_model,
)
from mozaic import Mozaic


@dataclass
class ForecastResult:
    """Holds the outputs of a Mozaic forecast run."""
    dfs: Dict[str, pd.DataFrame]
    mozaics: Dict[str, Mozaic]
    config: ModelConfig = None


# Do the forecasting
def _check_data_health(datasets: Dict[str, pd.DataFrame]) -> None:
    """Check input data for conditions that may cause Mozaic failures.

    Prints warnings for:
    - Empty datasets
    - Zero-variance data (all values identical)
    - All-zero data

    Args:
        datasets: Dict of metric -> DataFrame with historical data
    """
    for metric, df in datasets.items():
        # Skip if not a DataFrame (shouldn't happen in production, but handles test mocks)
        if not isinstance(df, pd.DataFrame):
            continue

        if df.empty:
            print(f'WARNING: Empty data for metric "{metric}"')
            continue

        if 'y' in df.columns:
            if df['y'].std() == 0:
                print(f'WARNING: Zero variance in metric "{metric}" - all values are {df["y"].iloc[0]}')
            if (df['y'] == 0).all():
                print(f'WARNING: All-zero data for metric "{metric}"')


def get_forecast_dfs(
    datasets: Dict[str, pd.DataFrame],
    forecast_model: Any,
    forecast_start_date: str,
    forecast_end_date: str,
    quantile: float = None,
    additional_holidays: List[Type[holidays.HolidayBase]] = None,
    config: ModelConfig = None,
) -> ForecastResult:
    """Generate forecasts using Mozaic.

    Args:
        datasets: Dict of metric -> DataFrame with historical data
        forecast_model: Mozaic forecast model (desktop or mobile)
        forecast_start_date: Start date for forecast period
        forecast_end_date: End date for forecast period
        quantile: Quantile for point forecast (default: 0.5 from FORECAST_CONFIG)
        additional_holidays: Custom holiday calendars passed to populate_tiles
            (default: empty list)

    Returns:
        ForecastResult with dfs (metric -> DataFrame) and mozaics (metric -> Mozaic)

    Example - Iterating over quantiles:
        # Compare forecasts at different quantiles
        for q in [0.25, 0.5, 0.75]:
            result = get_desktop_forecast_dfs(datasets, start, end, quantile=q)
            # Analyze sensitivity to quantile choice
    """
    from .config import FORECAST_CONFIG

    if quantile is None:
        quantile = FORECAST_CONFIG['quantile']
    if additional_holidays is None:
        additional_holidays = []

    # Check data health before forecasting
    _check_data_health(datasets)

    tileset = mozaic.TileSet()

    print('\n--- Populate tiles\n')
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=RuntimeWarning,
            message=".*divide by zero.*|.*overflow.*|.*invalid value.*"
        )
        try:
            tile_kwargs = {}
            if config is not None:
                tile_kwargs['holiday_threshold'] = config.holiday_threshold
                tile_kwargs['holiday_max_radius'] = config.holiday_max_radius
                tile_kwargs['holiday_min_radius'] = config.holiday_min_radius
            mozaic.populate_tiles(
                datasets,
                tileset,
                forecast_model,
                forecast_start_date,
                forecast_end_date,
                additional_holidays=additional_holidays,
                **tile_kwargs,
            )
        except Exception as e:
            print(f'\nERROR: Mozaic populate_tiles failed')
            print(f'Processing metrics: {list(datasets.keys())}')
            print(f'Forecast period: {forecast_start_date} to {forecast_end_date}')
            print(f'Original error: {e}')
            raise

    mozaics: Dict[str, Mozaic] = {}
    country_mozaics = defaultdict(lambda: defaultdict(mozaic.Mozaic))
    population_mozaics = defaultdict(lambda: defaultdict(mozaic.Mozaic))

    print('\n--- Curate Mozaics\n')
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=RuntimeWarning,
            message=".*divide by zero.*|.*overflow.*|.*invalid value.*"
        )

        try:
            mozaic_kwargs = {}
            if config is not None:
                mozaic_kwargs['holiday_effect_floor'] = config.holiday_effect_floor
            mozaic.utils.curate_mozaics(
                datasets,
                tileset,
                forecast_model,
                mozaics,
                country_mozaics,
                population_mozaics,
                **mozaic_kwargs,
            )
        except Exception as e:
            print(f'\nERROR: Mozaic curate_mozaics failed')
            print(f'Processing metrics: {list(datasets.keys())}')
            print(f'Original error: {e}')
            raise

    print(f'\n--- Extracting forecasts ({len(mozaics)} metrics)')
    dfs = {}
    for i, (metric, moz) in enumerate(mozaics.items(), 1):
        print(f'  [{i}/{len(mozaics)}] {metric}')
        dfs[metric] = moz.to_granular_forecast_df(quantile=quantile)

    return ForecastResult(dfs=dfs, mozaics=mozaics, config=config)


def get_desktop_forecast_dfs(
    metric_data: Dict[str, pd.DataFrame],
    forecast_start_date: str,
    forecast_end_date: str,
    quantile: float = None,
    additional_holidays: List[Type[holidays.HolidayBase]] = None,
    config: DesktopModelConfig = None,
) -> ForecastResult:
    """Generate Desktop forecasts using Mozaic.

    Args:
        metric_data: Dict of metric -> DataFrame (already source-specific)
        forecast_start_date: Start date for forecast period
        forecast_end_date: End date for forecast period
        quantile: Quantile for point forecast (default: 0.5)
        additional_holidays: Custom holiday calendars passed to populate_tiles
        config: DesktopModelConfig with prophet/holiday params (default: None uses hardcoded defaults)

    Returns:
        ForecastResult with dfs and mozaics
    """
    model = make_desktop_model(config) if config is not None else desktop_forecast_model
    return get_forecast_dfs(
        metric_data,
        model,
        forecast_start_date,
        forecast_end_date,
        quantile=quantile,
        additional_holidays=additional_holidays,
        config=config,
    )


def get_mobile_forecast_dfs(
    metric_data: Dict[str, pd.DataFrame],
    forecast_start_date: str,
    forecast_end_date: str,
    quantile: float = None,
    additional_holidays: List[Type[holidays.HolidayBase]] = None,
    config: MobileModelConfig = None,
) -> ForecastResult:
    """Generate Mobile forecasts using Mozaic.

    Args:
        metric_data: Dict of metric -> DataFrame (already source-specific)
        forecast_start_date: Start date for forecast period
        forecast_end_date: End date for forecast period
        quantile: Quantile for point forecast (default: 0.5)
        additional_holidays: Custom holiday calendars passed to populate_tiles
        config: MobileModelConfig with prophet/holiday params (default: None uses hardcoded defaults)

    Returns:
        ForecastResult with dfs and mozaics
    """
    model = make_mobile_model(config) if config is not None else mobile_forecast_model
    return get_forecast_dfs(
        metric_data,
        model,
        forecast_start_date,
        forecast_end_date,
        quantile=quantile,
        additional_holidays=additional_holidays,
        config=config,
    )
