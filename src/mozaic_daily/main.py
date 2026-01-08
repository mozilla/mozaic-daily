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
from .config import get_constants, TESTING_MODE_ENABLE_STRING, TESTING_MODE_CHECKPOINT_FILENAME
from .data import get_queries, get_aggregate_data
from .forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs
from .tables import (
    combine_tables, update_desktop_format, update_mobile_format,
    add_desktop_and_mobile_rows, format_output_table
)


def print_testing_mode_banner():
    """Print a loud banner indicating testing mode is active."""
    width = 60
    char = '='
    print('\n' + char * width)
    print('TESTING MODE ENABLED - Desktop/DAU only')
    print(char * width + '\n')


def main(
    project: Optional[str] = None,
    checkpoints: Optional[bool] = False,
    testing_mode: Optional[str] = None
) -> pd.DataFrame:
    # Establish constants
    constants = get_constants()
    if not project:
        project = constants['default_project']

    # Enable testing only with exact string match (prevents accidents)
    is_testing = (testing_mode == TESTING_MODE_ENABLE_STRING)

    if is_testing:
        print_testing_mode_banner()
        checkpoint_filename = TESTING_MODE_CHECKPOINT_FILENAME
    else:
        checkpoint_filename = constants['forecast_checkpoint_filename']

    print(f'Running forecast from {constants["forecast_start_date"]} through {constants["forecast_end_date"]}')
    print(f'Other constants:\n{constants}')

    # Get the data
    # This method does internal file checkpointing
    datasets = get_aggregate_data(
        get_queries(constants['country_string'], testing_mode=is_testing),
        project,
        checkpoints = checkpoints
    )

    df = None
    if checkpoints and os.path.exists(checkpoint_filename):
        print('Forecast already generated. Loading existing data.')
        df = pd.read_parquet(checkpoint_filename)
    else:
        # Process the data
        print('Desktop Forecasting\n')
        df_desktop = combine_tables(get_desktop_forecast_dfs(
                datasets,
                constants['forecast_start_date'],
                constants['forecast_end_date']
            )
        )

        if is_testing:
            # Skip mobile, format desktop only
            print('\n\nDone with forecasts (testing mode)')
            update_desktop_format(df_desktop)
            df = format_output_table(df_desktop, constants['forecast_start_date'], constants['forecast_run_dt'])
        else:
            # Normal path: process both platforms
            print('Mobile Forecasting\n')
            df_mobile = combine_tables(get_mobile_forecast_dfs(
                    datasets,
                    constants['forecast_start_date'],
                    constants['forecast_end_date']
                )
            )
            print('\n\nDone with forecasts')

            # Format data
            update_desktop_format(df_desktop)
            update_mobile_format(df_mobile)

            df = add_desktop_and_mobile_rows(pd.concat([df_desktop, df_mobile]))
            df = format_output_table(df, constants['forecast_start_date'], constants['forecast_run_dt'])

        if checkpoints:
            df.to_parquet(checkpoint_filename)

    return df


if __name__ == '__main__':
    main(checkpoints=True)
