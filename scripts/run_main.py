#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run the mozaic-daily main pipeline with checkpoints enabled.

This script mimics the old behavior of running `python mozaic_daily.py`.
It can be run from anywhere in the project.

Usage:
    # Normal mode (all platforms/metrics)
    python scripts/run_main.py

    # Testing mode (desktop/DAU only)
    python scripts/run_main.py --testing

    # Historical forecast with debug flags
    python scripts/run_main.py \
      --forecast-start-date 2024-06-15 \
      --dau-only \
      --forecast-only \
      --output-dir ./forecasts
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
from mozaic_daily.config import STATIC_CONFIG

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run the mozaic-daily forecasting pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--testing',
        action='store_true',
        help='Run in testing mode (desktop/DAU only)'
    )
    parser.add_argument(
        '--forecast-start-date',
        type=str,
        help='Override forecast start date (YYYY-MM-DD) for historical runs'
    )
    parser.add_argument(
        '--dau-only',
        action='store_true',
        help='Only query DAU metrics (reduces from 12 to 3 queries)'
    )
    parser.add_argument(
        '--forecast-only',
        action='store_true',
        help='Return only forecast rows (exclude training data)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Save output to directory as dau_forecast_{date}.parquet'
    )
    args = parser.parse_args()

    testing_mode = STATIC_CONFIG['testing_mode_enable_string'] if args.testing else None
    main(
        checkpoints=True,
        testing_mode=testing_mode,
        forecast_start_date=args.forecast_start_date,
        forecast_only=args.forecast_only,
        dau_only=args.dau_only,
        output_dir=args.output_dir
    )
