#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run validation on the mozaic-daily forecast checkpoint file.

This script mimics the old behavior of running `python mozaic_daily_validation.py`.
It reads the checkpoint parquet file and validates it against BigQuery schema.
It can be run from anywhere in the project.

Usage:
    python scripts/run_validation.py           # Validate normal checkpoint
    python scripts/run_validation.py --testing # Validate testing checkpoint
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
from mozaic_daily.config import get_constants, TESTING_MODE_CHECKPOINT_FILENAME
from mozaic_daily.validation import validate_output_dataframe

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run validation on mozaic-daily forecast checkpoint'
    )
    parser.add_argument(
        '--testing',
        action='store_true',
        help='Validate testing mode checkpoint (desktop/DAU only)'
    )
    args = parser.parse_args()

    constants = get_constants()

    if args.testing:
        checkpoint_file = TESTING_MODE_CHECKPOINT_FILENAME
        testing_mode = True
    else:
        checkpoint_file = constants['forecast_checkpoint_filename']
        testing_mode = False

    # Fail if expected checkpoint doesn't exist
    if not Path(checkpoint_file).exists():
        raise FileNotFoundError(f"Checkpoint file not found: {checkpoint_file}")

    print(f"Validating checkpoint: {checkpoint_file}")
    df = pd.read_parquet(checkpoint_file)
    validate_output_dataframe(df, testing_mode=testing_mode)
