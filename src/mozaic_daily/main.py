# -*- coding: utf-8 -*-
"""Main orchestration for mozaic-daily forecasting pipeline.

This module ties together all components to run the full forecast pipeline:
1. Load configuration and constants
2. Fetch data from BigQuery (with checkpoint support)
3. Generate forecasts using Mozaic
4. Format output for BigQuery upload
5. Return validated DataFrame

Functions:
- main(): Entry point for forecast generation

Usage:
    python -m mozaic_daily.main
"""

from typing import Optional
import pandas as pd
import os
from .config import get_runtime_config, STATIC_CONFIG
from .data import get_queries, get_aggregate_data, check_training_data_availability
from .forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs
from .tables import (
    combine_tables, update_desktop_format, update_mobile_format,
    format_output_table
)
from .queries import Platform, DataSource


# =============================================================================
# CONSTANTS
# =============================================================================

DATA_SOURCES_TO_PROCESS = [
    DataSource.GLEAN_DESKTOP,
    DataSource.LEGACY_DESKTOP,
    DataSource.GLEAN_MOBILE,
]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def print_testing_mode_banner():
    """Print a loud banner indicating testing mode is active."""
    width = 60
    char = '='
    print('\n' + char * width)
    print('TESTING MODE ENABLED - Desktop Glean only')
    print(char * width + '\n')


def get_format_function(platform: Platform):
    """Return the appropriate format function for a platform."""
    if platform == Platform.DESKTOP:
        return update_desktop_format
    return update_mobile_format


def get_forecast_function(platform: Platform):
    """Return the appropriate forecast function for a platform."""
    if platform == Platform.DESKTOP:
        return get_desktop_forecast_dfs
    return get_mobile_forecast_dfs


def get_checkpoint_filename(is_testing: bool) -> str:
    """Return appropriate checkpoint filename based on testing mode."""
    if is_testing:
        return STATIC_CONFIG['testing_mode_checkpoint_filename']
    return STATIC_CONFIG['forecast_checkpoint_filename']


def load_checkpoint_if_exists(filename: str) -> Optional[pd.DataFrame]:
    """Load checkpoint if file exists, return None otherwise."""
    if os.path.exists(filename):
        print('Forecast already generated. Loading existing data.')
        return pd.read_parquet(filename)
    return None


def save_checkpoint(df: pd.DataFrame, filename: str) -> None:
    """Save DataFrame to checkpoint file."""
    df.to_parquet(filename)


def should_process_in_testing_mode(data_source: DataSource) -> bool:
    """Return True if this data source should be processed in testing mode.

    Testing mode only processes Desktop Glean to speed up iteration.
    """
    return data_source == DataSource.GLEAN_DESKTOP


def process_data_source(
    data_source: DataSource,
    datasets: dict,
    forecast_start: str,
    forecast_end: str
) -> pd.DataFrame:
    """Process a single data source through the forecast pipeline.

    Args:
        data_source: DataSource enum identifying which data to process
        datasets: Nested dict of data by platform/source/metric
        forecast_start: Start date for forecast period
        forecast_end: End date for forecast period

    Returns:
        DataFrame with forecasts for this data source, properly formatted
    """
    # Get platform-specific data and functions
    platform = data_source.platform
    source = data_source.telemetry_source
    source_data = datasets[platform.value][source.value]

    # Generate forecasts
    forecast_func = get_forecast_function(platform)
    forecast_dfs = forecast_func(source_data, forecast_start, forecast_end)

    # Combine and format
    df_combined = combine_tables(forecast_dfs)
    format_func = get_format_function(platform)
    format_func(df_combined, data_source=data_source.value)

    return df_combined


def generate_forecasts(
    datasets: dict,
    runtime_config: dict,
    is_testing: bool
) -> pd.DataFrame:
    """Generate forecasts for all data sources and combine them.

    Args:
        datasets: Nested dict of data by platform/source/metric
        runtime_config: Runtime configuration with dates
        is_testing: Whether testing mode is enabled

    Returns:
        Combined DataFrame with all forecasts
    """
    all_dfs = []

    # Calculate total sources for progress tracking
    total_sources = len([ds for ds in DATA_SOURCES_TO_PROCESS
                         if not is_testing or should_process_in_testing_mode(ds)])
    source_num = 0

    for data_source in DATA_SOURCES_TO_PROCESS:
        # In testing mode, only process Desktop Glean
        if is_testing and not should_process_in_testing_mode(data_source):
            continue

        source_num += 1
        print(f'\n[{source_num}/{total_sources}] Forecasting {data_source.display_name}')

        df = process_data_source(
            data_source,
            datasets,
            runtime_config['forecast_start_date'],
            runtime_config['forecast_end_date']
        )
        all_dfs.append(df)

    print('\n\nDone with forecasts')

    # Combine all data sources and format for output
    df = pd.concat(all_dfs, ignore_index=True)
    df = format_output_table(
        df,
        runtime_config['forecast_start_date'],
        runtime_config['forecast_run_dt']
    )

    return df


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main(
    project: Optional[str] = None,
    checkpoints: Optional[bool] = False,
    testing_mode: Optional[str] = None,
    forecast_start_date: Optional[str] = None
) -> pd.DataFrame:
    """Run the full forecasting pipeline.

    Args:
        project: GCP project ID for BigQuery (defaults to config value)
        checkpoints: Enable file-based checkpointing for faster iteration
        testing_mode: String flag to enable testing mode (must match exact value)
        forecast_start_date: Override date (YYYY-MM-DD) for historical forecast runs.
            Simulates running the forecast on this date.

    Returns:
        DataFrame with forecasts
    """
    # Load configuration with optional date override
    config = get_runtime_config(forecast_start_date_override=forecast_start_date)
    if not project:
        project = STATIC_CONFIG['default_project']

    # Enable testing only with exact string match (prevents accidents)
    is_testing = (testing_mode == STATIC_CONFIG['testing_mode_enable_string'])
    if is_testing:
        print_testing_mode_banner()

    print(f'Running forecast from {config["forecast_start_date"]} through {config["forecast_end_date"]}')
    print(f'Other config:\n{config}')

    # Set up checkpointing
    checkpoint_filename = get_checkpoint_filename(is_testing)

    # Run pre-flight data availability check unless forecast checkpoint already exists.
    # Skipping when the checkpoint exists avoids unnecessary BQ calls during iteration.
    forecast_checkpoint_exists = checkpoints and os.path.exists(checkpoint_filename)
    if not forecast_checkpoint_exists:
        check_training_data_availability(project, config['training_end_date'])

    # Fetch data from BigQuery (with internal checkpointing)
    datasets = get_aggregate_data(
        get_queries(config['country_string'], testing_mode=is_testing),
        project,
        checkpoints=checkpoints
    )

    # Load checkpoint OR generate forecasts
    df = None
    if checkpoints:
        df = load_checkpoint_if_exists(checkpoint_filename)

    if df is None:
        df = generate_forecasts(datasets, config, is_testing)
        if checkpoints:
            save_checkpoint(df, checkpoint_filename)

    # Return result
    return df


if __name__ == '__main__':
    main(checkpoints=True)
