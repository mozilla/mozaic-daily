# -*- coding: utf-8 -*-
"""Configuration and constants for mozaic-daily forecasting.

This module defines:
- Static configuration (true constants)
- Runtime configuration (dates, projects, BigQuery tables)
- Country/market lists
- Date constraints for each metric (start dates, excluded ranges)
- SQL time clause generation
- Date index generation for validation
"""

from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any

import pandas as pd

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

def get_date_constraints() -> Dict[Tuple[str, str], Dict]:
    return {
        ("desktop", "DAU"): {
            "date_field": "submission_date",
            "start": '2023-04-17',
            "excludes": [],
        },
        ("desktop", "New Profiles"): {
            "date_field": "first_seen_date",
            "start": '2023-07-01',
            "excludes": [('2023-07-18', '2023-07-19')],
        },
        ("desktop", "Existing Engagement DAU"): {
            "date_field": "submission_date",
            "start": '2023-06-07',
            "excludes": [],
        },
        ("desktop", "Existing Engagement MAU"): {
            "date_field": "submission_date",
            "start": '2023-06-07',
            "excludes": [],
        },
        ("mobile", "DAU"): {
            "date_field": "submission_date",
            "start": '2020-12-31',
            "excludes": [],
        },
        ("mobile", "New Profiles"): {
            "date_field": "first_seen_date",
            "start": '2023-07-01',
            "excludes": [('2023-07-18', '2023-07-19')],
        },
        ("mobile", "Existing Engagement DAU"): {
            "date_field": "submission_date",
            "start": '2023-07-01',
            "excludes": [],
        },
        ("mobile", "Existing Engagement MAU"): {
            "date_field": "submission_date",
            "start": '2023-07-01',
            "excludes": [],
        },
    }

def get_date_keys():
    return get_date_constraints().keys()




def get_sql_time_clause(
    key: Tuple[str, str],
    quote: str = '"',
) -> str:
    constraints = get_date_constraints()
    if key not in constraints:
        raise KeyError(f"Unknown key: {key}")

    entry = constraints[key]
    field = entry["date_field"]
    start = entry["start"]
    excludes: List[Tuple[str, str]] = entry.get("excludes", [])

    parts = [f'{field} >= {quote}{start}{quote}']
    for ex_start, ex_end in excludes:
        parts.append(
            f'{field} NOT BETWEEN {quote}{ex_start}{quote} AND {quote}{ex_end}{quote}'
        )

    return " AND ".join(parts)

def get_training_date_index(
    key: Tuple[str, str],
    end: Optional[str] = None,
) -> pd.DatetimeIndex:
    constraints = get_date_constraints()
    if key not in constraints:
        raise KeyError(f"Unknown key: {key}")

    entry = constraints[key]
    start = pd.to_datetime(entry["start"]).normalize()
    if end:
        end_dt = pd.to_datetime(end).normalize()
    else:
        end_dt = pd.to_datetime(get_runtime_config()['training_end_date']).normalize()

    full = pd.date_range(start=start, end=end_dt, freq='D')

    excludes = entry.get("excludes", [])
    if not excludes:
        return full

    mask = pd.Series(True, index=full)

    for ex_start, ex_end in excludes:
        ex_s = pd.to_datetime(ex_start).normalize()
        ex_e = pd.to_datetime(ex_end).normalize()
        mask.loc[(full >= ex_s) & (full <= ex_e)] = False

    return full[mask.values]

def get_prediction_date_index(start: str, end: str) -> pd.DatetimeIndex:
    start_dt = pd.to_datetime(start).normalize()
    end_dt = pd.to_datetime(end).normalize()

    return pd.date_range(start=start_dt, end=end_dt, freq='D')
