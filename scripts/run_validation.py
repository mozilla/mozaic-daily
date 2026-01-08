#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run validation on the mozaic-daily forecast checkpoint file.

This script mimics the old behavior of running `python mozaic_daily_validation.py`.
It reads the checkpoint parquet file and validates it against BigQuery schema.
It can be run from anywhere in the project.

Usage:
    python scripts/run_validation.py
"""

import sys
from pathlib import Path

# Add src directory to path so we can import the package
repo_root = Path(__file__).parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

import pandas as pd
from mozaic_daily.config import get_constants
from mozaic_daily.validation import validate_output_dataframe

if __name__ == '__main__':
    df = pd.read_parquet(get_constants()['forecast_checkpoint_filename'])
    validate_output_dataframe(df)
