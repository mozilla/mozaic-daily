#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run the mozaic-daily main pipeline with checkpoints enabled.

This script mimics the old behavior of running `python mozaic_daily.py`.
It can be run from anywhere in the project.

Usage:
    # Normal mode (all platforms/metrics)
    python scripts/run_main.py

    # Testing mode (desktop glean/DAU only, convenience alias)
    python scripts/run_main.py --testing

    # Filter to specific data source(s) (repeat flag for multiple)
    python scripts/run_main.py --data-sources glean_mobile
    python scripts/run_main.py --data-sources glean_desktop --data-sources legacy_desktop

    # Filter to specific metric(s) (repeat flag for multiple)
    python scripts/run_main.py --metrics DAU
    python scripts/run_main.py --metrics DAU --metrics "New Profiles"

    # Combine filters (intersection, both flags repeatable)
    python scripts/run_main.py --data-sources glean_mobile --metrics DAU

    # Historical forecast
    python scripts/run_main.py \\
      --forecast-start-date 2024-06-15

    # Write checkpoints to a specific directory
    python scripts/run_main.py --output-dir /tmp/my-run
"""

import sys
import argparse
from pathlib import Path

# Add src directory to path so we can import the package
repo_root = Path(__file__).parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from mozaic_daily import main
from mozaic_daily.queries import DataSource, Metric

VALID_DATA_SOURCES = [ds.value for ds in DataSource]
VALID_METRICS = [m.value for m in Metric]

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run the mozaic-daily forecasting pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--testing',
        action='store_true',
        help='Convenience alias: filter to glean_desktop/DAU only'
    )
    parser.add_argument(
        '--data-sources',
        action='append',
        choices=VALID_DATA_SOURCES,
        metavar='SOURCE',
        help=f'Filter to specific data source(s). Valid: {", ".join(VALID_DATA_SOURCES)}'
    )
    parser.add_argument(
        '--metrics',
        action='append',
        choices=VALID_METRICS,
        metavar='METRIC',
        help=f'Filter to specific metric(s). Valid: {", ".join(VALID_METRICS)}'
    )
    parser.add_argument(
        '--forecast-start-date',
        type=str,
        help='Override forecast start date (YYYY-MM-DD) for historical runs'
    )
    parser.add_argument(
        '--no-checkpoints',
        action='store_true',
        help='Disable checkpoint loading (required for local batch historical processing)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Directory to write checkpoint files to (default: current directory)'
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

    # Disable checkpoints if flag is set
    use_checkpoints = not args.no_checkpoints

    main(
        checkpoints=use_checkpoints,
        data_source_filter=data_source_filter,
        metric_filter=metric_filter,
        forecast_start_date=args.forecast_start_date,
        output_dir=args.output_dir
    )
