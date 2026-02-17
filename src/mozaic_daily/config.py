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
from typing import Dict, Any, Optional
import subprocess
import re
from pathlib import Path

import pandas as pd

# Static configuration (true constants)
STATIC_CONFIG = {
    'default_project': 'moz-fx-data-bq-data-science',
    'default_table': 'moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v2',
    'forecast_checkpoint_filename': 'mozaic_parts.forecast.parquet',
    'raw_checkpoint_filename_template': 'mozaic_parts.raw.{source}.{platform}.{metric}.parquet',
    'testing_mode_enable_string': 'ENABLE_TESTING_MODE',
    'testing_mode_checkpoint_filename': 'mozaic_parts.forecast.TESTING.parquet',
}

# Forecast configuration
FORECAST_CONFIG = {
    'quantile': 0.5,  # Default quantile for to_granular_forecast_df()
}

def get_runtime_config(forecast_start_date_override: Optional[str] = None) -> Dict[str, Any]:
    """Calculate runtime configuration based on current datetime or override.

    Args:
        forecast_start_date_override: Optional date string (YYYY-MM-DD) to simulate
            running the forecast on a historical date. When provided, treats this as
            the forecast start date (T-0) and adjusts all other dates accordingly:
            - training_end_date = override - 1 day (T-1)
            - forecast_end_date = Dec 31 of (override year + 1)

    Returns:
        Dict with dates, markets, and derived values. Does not include static config.

    Raises:
        ValueError: If forecast_start_date_override is in the future
    """
    config = {}


    config['forecast_run_dt'] = datetime.now()

    # Dates (calculated at runtime or from override)
    if forecast_start_date_override:
        # Parse override as the forecast start date (simulated "yesterday")
        forecast_start_dt = datetime.strptime(forecast_start_date_override, "%Y-%m-%d")

        # Validate that override date is not in the future
        today = datetime.now().date()
        if forecast_start_dt.date() > today:
            raise ValueError(f"forecast_start_date_override ({forecast_start_date_override}) cannot be in the future")
    else:
        # Default behavior: use yesterday
        forecast_start_dt = datetime.now() - timedelta(days=1)

    config['forecast_start_date'] = (forecast_start_dt).strftime("%Y-%m-%d")
    config['forecast_end_date'] = datetime(forecast_start_dt.year + 1, 12, 31).strftime("%Y-%m-%d")
    config['training_end_date'] = (forecast_start_dt - timedelta(days=1)).strftime("%Y-%m-%d")

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
    except (subprocess.CalledProcessError, FileNotFoundError):
        # pip command failed or pip not found
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
