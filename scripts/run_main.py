#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run the mozaic-daily main pipeline with checkpoints enabled.

This script mimics the old behavior of running `python mozaic_daily.py`.
It can be run from anywhere in the project.

Usage:
    python scripts/run_main.py           # Normal mode (all platforms/metrics)
    python scripts/run_main.py --testing # Testing mode (desktop/DAU only)
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
        description='Run the mozaic-daily forecasting pipeline'
    )
    parser.add_argument(
        '--testing',
        action='store_true',
        help='Run in testing mode (desktop/DAU only)'
    )
    args = parser.parse_args()

    testing_mode = STATIC_CONFIG['testing_mode_enable_string'] if args.testing else None
    main(checkpoints=True, testing_mode=testing_mode)
