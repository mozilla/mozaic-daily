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
import numpy as np
import warnings
from collections import defaultdict
import mozaic
from mozaic.models import desktop_forecast_model, mobile_forecast_model
from mozaic import Mozaic


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
            mozaic.populate_tiles(
                datasets,
                tileset,
                forecast_model,
                forecast_start_date,
                forecast_end_date,
            )
        except Exception as e:
            print(f'\nERROR: Mozaic populate_tiles failed')
            print(f'Processing metrics: {list(datasets.keys())}')
            print(f'Forecast period: {forecast_start_date} to {forecast_end_date}')
            print(f'Original error: {e}')
            raise

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

        try:
            mozaic.utils.curate_mozaics(
                datasets,
                tileset,
                forecast_model,
                mozaics,
                _ctry,
                _pop,
            )
        except Exception as e:
            print(f'\nERROR: Mozaic curate_mozaics failed')
            print(f'Processing metrics: {list(datasets.keys())}')
            print(f'Original error: {e}')
            raise

    # --- DEBUG: Dump tileset contents ---
    print('\n--- DEBUG: Tileset tile inventory ---')
    for metric_key, countries in tileset.tiles.items():
        for country_key, populations in countries.items():
            pop_names = sorted(populations.keys())
            print(f'  {metric_key} | {country_key}: {pop_names}')

    # --- DEBUG: Dump mozaic contents ---
    print('\n--- DEBUG: Mozaic inventory ---')
    for metric_key, moz in mozaics.items():
        print(f'  {metric_key}: {len(moz.tiles)} tiles, '
              f'countries={sorted(moz.get_countries())}, '
              f'populations={sorted(moz.get_populations())}')

    print(f'\n--- Extracting forecasts ({len(mozaics)} metrics)')
    dfs = {}
    for i, (metric, moz) in enumerate(mozaics.items(), 1):
        print(f'  [{i}/{len(mozaics)}] {metric}')
        granular_df = moz.to_granular_forecast_df(quantile=quantile)
        dfs[metric] = granular_df

        # --- DEBUG: Per-metric forecast completeness ---
        _debug_granular_forecast(metric, granular_df, moz)

    # --- DEBUG: Cross-metric date coverage comparison ---
    _debug_cross_metric_coverage(dfs)

    return dfs


# =============================================================================
# DEBUG HELPERS (temporary - debug branch only)
# =============================================================================

def _debug_granular_forecast(metric: str, granular_df: pd.DataFrame, moz: Mozaic) -> None:
    """Print debug info about a single metric's granular forecast DataFrame."""
    forecast_rows = granular_df[granular_df['source'] == 'forecast']
    actual_rows = granular_df[granular_df['source'] == 'actual']

    forecast_dates = sorted(forecast_rows['target_date'].unique())
    actual_dates = sorted(actual_rows['target_date'].unique())

    # Count unique (country, population) combos per source type
    forecast_combos = forecast_rows.groupby(['country', 'population']).size()
    actual_combos = actual_rows.groupby(['country', 'population']).size()

    print(f'\n    DEBUG [{metric}] granular_forecast_df:')
    print(f'      Total rows: {len(granular_df):,}  '
          f'(actual={len(actual_rows):,}, forecast={len(forecast_rows):,})')
    print(f'      Forecast date range: {forecast_dates[0]} to {forecast_dates[-1]}  '
          f'({len(forecast_dates)} dates)')
    print(f'      Actual date range: {actual_dates[0]} to {actual_dates[-1]}  '
          f'({len(actual_dates)} dates)')
    print(f'      Forecast (country, population) combos: {len(forecast_combos)}')
    print(f'      Actual (country, population) combos: {len(actual_combos)}')

    # Check for null values in the value column (should be none after _standard_df_to_forecast_df)
    null_values = granular_df['value'].isna().sum()
    if null_values > 0:
        print(f'      WARNING: {null_values} null values in "value" column!')

    # Check if any (country, population) combos have fewer forecast dates than expected
    expected_forecast_count = len(forecast_dates)
    short_combos = forecast_combos[forecast_combos < expected_forecast_count]
    if len(short_combos) > 0:
        print(f'      WARNING: {len(short_combos)} combos have fewer forecast dates '
              f'than the max ({expected_forecast_count}):')
        for (country, population), count in short_combos.items():
            missing_count = expected_forecast_count - count
            # Find which dates are missing for this combo
            combo_dates = set(
                forecast_rows[
                    (forecast_rows['country'] == country) &
                    (forecast_rows['population'] == population)
                ]['target_date'].values
            )
            all_forecast_dates = set(forecast_rows['target_date'].unique())
            missing_dates = sorted(all_forecast_dates - combo_dates)
            # Show up to 5 missing dates
            date_preview = ', '.join(str(d)[:10] for d in missing_dates[:5])
            if len(missing_dates) > 5:
                date_preview += f', ... (+{len(missing_dates) - 5} more)'
            print(f'        {country}/{population}: {count} dates '
                  f'(missing {missing_count}: {date_preview})')

    # --- DEBUG: Inspect individual tile forecasts for this metric ---
    # Check each tile's raw forecast DataFrame for NaN in the 'forecast' column
    print(f'      Tile-level forecast NaN check:')
    for tile in moz.tiles:
        tile_df = tile.to_df(quantile=0.5)
        if 'forecast' not in tile_df.columns:
            print(f'        {tile.country}/{tile.population}: '
                  f'NO "forecast" column! Columns: {list(tile_df.columns)}')
            continue
        forecast_slice = tile_df[tile_df['submission_date'] >= pd.to_datetime(tile.forecast_start_date)]
        nan_forecast = forecast_slice['forecast'].isna()
        nan_count = nan_forecast.sum()
        if nan_count > 0:
            nan_dates = forecast_slice.loc[nan_forecast, 'submission_date']
            date_preview = ', '.join(str(d)[:10] for d in sorted(nan_dates.values)[:5])
            if len(nan_dates) > 5:
                date_preview += f', ... (+{len(nan_dates) - 5} more)'
            print(f'        {tile.country}/{tile.population}: '
                  f'{nan_count} NaN forecast dates: {date_preview}')
        else:
            print(f'        {tile.country}/{tile.population}: OK '
                  f'({len(forecast_slice)} forecast dates, 0 NaN)')


def _debug_cross_metric_coverage(dfs: Dict[str, pd.DataFrame]) -> None:
    """Compare date x country x population coverage across metrics."""
    print('\n--- DEBUG: Cross-metric forecast date coverage ---')

    # Build sets of (target_date, country, population) for forecast rows per metric
    metric_keys = {}
    for metric, df in dfs.items():
        forecast_rows = df[df['source'] == 'forecast']
        keys = set(
            zip(forecast_rows['target_date'], forecast_rows['country'], forecast_rows['population'])
        )
        metric_keys[metric] = keys
        print(f'  {metric}: {len(keys):,} forecast (date, country, population) keys')

    # Find keys present in ANY metric
    all_keys = set()
    for keys in metric_keys.values():
        all_keys |= keys

    # For each metric, find missing keys and report
    for metric, keys in metric_keys.items():
        missing = all_keys - keys
        if missing:
            # Group missing keys by (country, population) to show which combos are affected
            from collections import Counter
            combo_counts = Counter((c, p) for _, c, p in missing)
            print(f'  {metric}: MISSING {len(missing)} keys present in other metrics')
            for (country, pop), count in combo_counts.most_common(10):
                # Show which dates are missing for this combo
                missing_dates = sorted(d for d, c, p in missing if c == country and p == pop)
                date_preview = ', '.join(str(d)[:10] for d in missing_dates[:3])
                if len(missing_dates) > 3:
                    date_preview += f', ... (+{len(missing_dates) - 3} more)'
                print(f'    {country}/{pop}: missing {count} dates ({date_preview})')
        else:
            print(f'  {metric}: complete coverage (no missing keys)')


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
