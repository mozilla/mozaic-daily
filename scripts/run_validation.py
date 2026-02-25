#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run validation on the mozaic-daily forecast checkpoint file.

This script reads the checkpoint parquet file and validates it against BigQuery schema.
It can be run from anywhere in the project.

Usage:
    python scripts/run_validation.py                          # Validate normal checkpoint
    python scripts/run_validation.py --testing                # Validate testing checkpoint
    python scripts/run_validation.py --output-dir /tmp/my-run # Validate checkpoint in custom directory
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
from mozaic_daily.config import STATIC_CONFIG
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
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Directory containing checkpoint files (default: current directory)'
    )
    args = parser.parse_args()

    if args.testing:
        checkpoint_filename = STATIC_CONFIG['testing_mode_checkpoint_filename']
        testing_mode = True
    else:
        checkpoint_filename = STATIC_CONFIG['forecast_checkpoint_filename']
        testing_mode = False

    output_dir = args.output_dir if args.output_dir is not None else "."
    checkpoint_file = str(Path(output_dir) / checkpoint_filename)

    # Fail if expected checkpoint doesn't exist
    if not Path(checkpoint_file).exists():
        raise FileNotFoundError(f"Checkpoint file not found: {checkpoint_file}")

    print(f"Validating checkpoint: {checkpoint_file}")
    df = pd.read_parquet(checkpoint_file)
    validate_output_dataframe(df, testing_mode=testing_mode)
