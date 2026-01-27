# -*- coding: utf-8 -*-
"""Configuration and constants for mozaic-daily forecasting.

This module defines:
- Static configuration (true constants)
- Runtime configuration (dates, projects, BigQuery tables)
- Country/market lists
- Date constraints for each metric (start dates, excluded ranges)
- SQL time clause generation
- Date index generation for validation
- Git hash retrieval for version tracking
"""

from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import subprocess
import re
from pathlib import Path

import pandas as pd
from .queries import get_date_keys, get_training_date_index

# Static configuration (true constants)
STATIC_CONFIG = {
    'default_project': 'moz-fx-data-bq-data-science',
    'default_table': 'moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v1',
    'forecast_checkpoint_filename': 'mozaic_parts.forecast.parquet',
    'raw_checkpoint_filename_template': 'mozaic_parts.raw.{platform}.{metric}.parquet',
    'testing_mode_enable_string': 'ENABLE_TESTING_MODE',
    'testing_mode_checkpoint_filename': 'mozaic_parts.forecast.TESTING.parquet',
}

# Forecast configuration
FORECAST_CONFIG = {
    'quantile': 0.5,  # Default quantile for to_granular_forecast_df()
}

def get_runtime_config() -> Dict[str, Any]:
    """Calculate runtime configuration based on current datetime.

    Returns dates, markets, and derived values. Does not include static config.
    """
    config = {}

    # Dates (calculated at runtime)
    forecast_run_dt = datetime.now()
    config['forecast_run_dt'] = forecast_run_dt
    config['forecast_start_date'] = (forecast_run_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    config['forecast_end_date'] = datetime(forecast_run_dt.year + 1, 12, 31).strftime("%Y-%m-%d")
    config['training_end_date'] = (forecast_run_dt - timedelta(days=2)).strftime("%Y-%m-%d")

    # Markets
    top_DAU_markets = set(
        ["US", "BR", "CA", "MX", "AR", "IN", "ID", "JP", "IR", "CN", "DE", "FR", "PL", "RU", "IT"]
    )
    top_google_markets = set(
        ["US", "DE", "FR", "GB", "PL", "CA", "CH", "IT", "AU", "NL", "ES", "JP", "AT"]
    )
    nonmonetized_google = set(["RU", "UA", "TR", "BY", "KZ", "CN"])
    config['countries'] = top_DAU_markets | top_google_markets | nonmonetized_google
    config['country_string'] = ", ".join(f"'{i}'" for i in sorted(config['countries']))
    config['validation_countries'] = config['countries'] | set(['ALL', 'ROW'])

    return config


def get_prediction_date_index(start: str, end: str) -> pd.DatetimeIndex:
    start_dt = pd.to_datetime(start).normalize()
    end_dt = pd.to_datetime(end).normalize()

    return pd.date_range(start=start_dt, end=end_dt, freq='D')


# Git hash retrieval functions

def get_git_commit_hash_from_pip(package_name: str = "mozaic") -> str:
    try:
        output = subprocess.check_output(["pip", "freeze"], text=True)
        for line in output.splitlines():
            if line.startswith("-e git+") and f"#egg={package_name}" in line:
                match = re.search(r"git\+(.+?)@([a-f0-9]+)#egg", line)
                if match:
                    base_url, sha = match.groups()
                    return sha
    except Exception:
        pass
    return "unknown"

def get_git_commit_hash_from_file(path: str = '/mozaic_commit.txt') -> str:
    """Return the commit/version string if the file exists, else None."""
    p = Path(path)
    if not p.exists():
        return None

    text = p.read_text().strip()
    return text or None

def get_git_commit_hash() -> str:
    pip_version = get_git_commit_hash_from_pip()
    if pip_version == 'unknown':
        file_version = get_git_commit_hash_from_file()
        if file_version:
            return file_version

    return pip_version
