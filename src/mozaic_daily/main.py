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

from typing import Optional, Set
import pandas as pd
import os
from .config import get_runtime_config, STATIC_CONFIG, build_filter_code
from .data import get_queries, get_aggregate_data, check_training_data_availability
from .forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs
from .tables import (
    combine_tables, update_desktop_format, update_mobile_format,
    format_output_table
)
from .queries import Platform, Metric, DataSource, ADDITIONAL_HOLIDAYS


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

def print_filter_banner(
    data_source_filter: Optional[Set[DataSource]],
    metric_filter: Optional[Set[Metric]]
):
    """Print a banner showing active filters."""
    width = 60
    char = '='
    print('\n' + char * width)
    print('FILTERED MODE ENABLED')
    if data_source_filter is not None:
        sources = ', '.join(sorted(ds.value for ds in data_source_filter))
        print(f'  Data sources: {sources}')
    if metric_filter is not None:
        metrics = ', '.join(sorted(m.value for m in metric_filter))
        print(f'  Metrics: {metrics}')
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


def get_checkpoint_filename(
    forecast_start_date: str,
    output_dir: str = ".",
    data_source_filter: Optional[Set[DataSource]] = None,
    metric_filter: Optional[Set[Metric]] = None,
) -> str:
    """Return appropriate checkpoint filename based on filters and output directory."""
    filter_code = build_filter_code(data_source_filter, metric_filter)
    if filter_code:
        filename = STATIC_CONFIG['forecast_checkpoint_filename_filtered_template'].format(
            date=forecast_start_date, filter_code=filter_code
        )
    else:
        filename = STATIC_CONFIG['forecast_checkpoint_filename_template'].format(date=forecast_start_date)
    return os.path.join(output_dir, filename)


def load_checkpoint_if_exists(filename: str) -> Optional[pd.DataFrame]:
    """Load checkpoint if file exists, return None otherwise."""
    if os.path.exists(filename):
        print('Forecast already generated. Loading existing data.')
        return pd.read_parquet(filename)
    return None


def save_checkpoint(df: pd.DataFrame, filename: str) -> None:
    """Save DataFrame to checkpoint file."""
    df.to_parquet(filename)


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
    additional_holidays = ADDITIONAL_HOLIDAYS.get(data_source, [])
    forecast_dfs = forecast_func(
        source_data, forecast_start, forecast_end,
        additional_holidays=additional_holidays,
    )

    # Combine and format
    df_combined = combine_tables(forecast_dfs)
    format_func = get_format_function(platform)
    format_func(df_combined, data_source=data_source.value)

    return df_combined


def generate_forecasts(
    datasets: dict,
    runtime_config: dict,
    data_source_filter: Optional[Set[DataSource]] = None,
) -> pd.DataFrame:
    """Generate forecasts for all data sources and combine them.

    Args:
        datasets: Nested dict of data by platform/source/metric
        runtime_config: Runtime configuration with dates
        data_source_filter: If set, only process these data sources

    Returns:
        Combined DataFrame with all forecasts
    """
    all_dfs = []

    sources_to_process = [
        ds for ds in DATA_SOURCES_TO_PROCESS
        if data_source_filter is None or ds in data_source_filter
    ]
    total_sources = len(sources_to_process)

    for source_num, data_source in enumerate(sources_to_process, start=1):
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
    checkpoints: bool = False,
    data_source_filter: Optional[Set[DataSource]] = None,
    metric_filter: Optional[Set[Metric]] = None,
    forecast_start_date: Optional[str] = None,
    output_dir: Optional[str] = None
) -> pd.DataFrame:
    """Run the full forecasting pipeline.

    Args:
        project: GCP project ID for BigQuery (defaults to config value)
        checkpoints: Enable file-based checkpointing for faster iteration
        data_source_filter: If set, only process these data sources (e.g., {DataSource.GLEAN_MOBILE})
        metric_filter: If set, only process these metrics (e.g., {Metric.DAU})
        forecast_start_date: Override date (YYYY-MM-DD) for historical forecast runs.
            Simulates running the forecast on this date.
        output_dir: Directory to write checkpoint files to (defaults to current directory).
            Created automatically if it doesn't exist.

    Returns:
        DataFrame with forecasts
    """
    # Resolve output directory and create it if needed
    resolved_output_dir = output_dir if output_dir is not None else "."
    os.makedirs(resolved_output_dir, exist_ok=True)

    # Load configuration with optional date override
    config = get_runtime_config(forecast_start_date_override=forecast_start_date)
    if not project:
        project = STATIC_CONFIG['default_project']

    is_filtered = data_source_filter is not None or metric_filter is not None
    if is_filtered:
        print_filter_banner(data_source_filter, metric_filter)

    print(f'Running forecast from {config["forecast_start_date"]} through {config["forecast_end_date"]}')
    print(f'Other config:\n{config}')

    # Set up checkpointing
    checkpoint_filename = get_checkpoint_filename(
        config['forecast_start_date'], resolved_output_dir,
        data_source_filter=data_source_filter, metric_filter=metric_filter
    )

    # Run pre-flight data availability check unless forecast checkpoint already exists.
    # Skipping when the checkpoint exists avoids unnecessary BQ calls during iteration.
    forecast_checkpoint_exists = checkpoints and os.path.exists(checkpoint_filename)
    if not forecast_checkpoint_exists:
        check_training_data_availability(project, config['training_end_date'])

    # Fetch data from BigQuery (with internal checkpointing)
    datasets = get_aggregate_data(
        get_queries(
            config['country_string'],
            data_source_filter=data_source_filter,
            metric_filter=metric_filter,
        ),
        project,
        checkpoints=checkpoints,
        output_dir=resolved_output_dir
    )

    # Load checkpoint OR generate forecasts
    df = None
    if checkpoints:
        df = load_checkpoint_if_exists(checkpoint_filename)

    if df is None:
        df = generate_forecasts(datasets, config, data_source_filter=data_source_filter)
        if checkpoints:
            save_checkpoint(df, checkpoint_filename)

    # Return result
    return df


if __name__ == '__main__':
    main(checkpoints=True)
