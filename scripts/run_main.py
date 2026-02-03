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
        '--no-checkpoints',
        action='store_true',
        help='Disable checkpoint loading (required for batch historical processing)'
    )
    args = parser.parse_args()

    testing_mode = STATIC_CONFIG['testing_mode_enable_string'] if args.testing else None

    # Disable checkpoints if flag is set
    use_checkpoints = not args.no_checkpoints

    main(
        checkpoints=use_checkpoints,
        testing_mode=testing_mode,
        forecast_start_date=args.forecast_start_date
    )
