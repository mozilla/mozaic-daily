#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run validation on the mozaic-daily forecast checkpoint file.

This script reads the checkpoint parquet file and validates it against BigQuery schema.
It can be run from anywhere in the project.

Usage:
    python scripts/run_validation.py                                    # Validate today's normal checkpoint
    python scripts/run_validation.py --forecast-start-date 2026-02-24  # Validate checkpoint for a specific date
    python scripts/run_validation.py --testing                          # Validate testing/filtered checkpoint
    python scripts/run_validation.py --data-sources glean_mobile       # Validate filtered checkpoint
    python scripts/run_validation.py --output-dir /tmp/my-run          # Validate checkpoint in custom directory
"""

import sys
import argparse
from pathlib import Path

# Add src directory to path so we can import the package
repo_root = Path(__file__).parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

import pandas as pd
from mozaic_daily.config import STATIC_CONFIG, get_runtime_config, build_filter_code
from mozaic_daily.validation import validate_output_dataframe
from mozaic_daily.queries import DataSource, Metric

VALID_DATA_SOURCES = [ds.value for ds in DataSource]
VALID_METRICS = [m.value for m in Metric]

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run validation on mozaic-daily forecast checkpoint'
    )
    parser.add_argument(
        '--testing',
        action='store_true',
        help='Convenience alias: validate as glean_desktop/DAU filtered run'
    )
    parser.add_argument(
        '--data-sources',
        action='append',
        choices=VALID_DATA_SOURCES,
        metavar='SOURCE',
        help=f'Filter validation to specific data source(s). Valid: {", ".join(VALID_DATA_SOURCES)}'
    )
    parser.add_argument(
        '--metrics',
        action='append',
        choices=VALID_METRICS,
        metavar='METRIC',
        help=f'Filter validation to specific metric(s). Valid: {", ".join(VALID_METRICS)}'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Directory containing checkpoint files (default: current directory)'
    )
    parser.add_argument(
        '--forecast-start-date',
        type=str,
        default=None,
        help='Forecast start date (YYYY-MM-DD) to select the checkpoint file (default: yesterday)'
    )
    args = parser.parse_args()

    # Validate that --testing is not combined with explicit filters
    if args.testing and (args.data_sources or args.metrics):
        parser.error('--testing cannot be combined with --data-sources or --metrics')

    # Build filter sets
    if args.testing:
        data_source_filter = {DataSource.GLEAN_DESKTOP}
        metric_filter = {Metric.DAU}
    else:
        data_source_filter = {DataSource(v) for v in args.data_sources} if args.data_sources else None
        metric_filter = {Metric(v) for v in args.metrics} if args.metrics else None

    is_filtered = data_source_filter is not None or metric_filter is not None

    # Determine checkpoint filename
    if args.forecast_start_date:
        date = args.forecast_start_date
    else:
        runtime_config = get_runtime_config()
        date = runtime_config['forecast_start_date']

    filter_code = build_filter_code(data_source_filter, metric_filter)
    if filter_code:
        checkpoint_filename = STATIC_CONFIG['forecast_checkpoint_filename_filtered_template'].format(
            date=date, filter_code=filter_code
        )
    else:
        checkpoint_filename = STATIC_CONFIG['forecast_checkpoint_filename_template'].format(date=date)

    output_dir = args.output_dir if args.output_dir is not None else "."
    checkpoint_file = str(Path(output_dir) / checkpoint_filename)

    # Fail if expected checkpoint doesn't exist
    if not Path(checkpoint_file).exists():
        raise FileNotFoundError(f"Checkpoint file not found: {checkpoint_file}")

    print(f"Validating checkpoint: {checkpoint_file}")
    df = pd.read_parquet(checkpoint_file)
    validate_output_dataframe(
        df,
        data_source_filter=data_source_filter,
        metric_filter=metric_filter,
        forecast_start_date=args.forecast_start_date,
    )
