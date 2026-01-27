# -*- coding: utf-8 -*-
"""Table formatting and manipulation.

This module transforms raw forecast DataFrames into the final output format
for BigQuery upload. Key operations:
- Combining metric tables into single DataFrame
- Formatting Desktop/Mobile segments (app names, OS JSON, data_source)
- Column renaming and type conversion

Note: This module does NOT create cross-platform aggregate rows.
Each row belongs to exactly one data_source (Glean_Desktop, Legacy_Desktop, or Glean_Mobile).

Functions:
- combine_tables(): Merges metric-specific DataFrames
- update_desktop_format(): Formats Desktop segment columns
- update_mobile_format(): Formats Mobile segment columns
- format_output_table(): Final formatting for BigQuery
"""

import pandas as pd
import numpy as np
import json
from typing import Dict, Optional
from datetime import datetime

from .config import get_git_commit_hash


# Table manipulation functions


def combine_tables(table_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    base_df = None
    for metric, df in table_dict.items():
        tmp_df = df.rename(columns={"value": metric})

        if base_df is None:
            base_df = tmp_df
        else:
            base_df = base_df.merge(
                tmp_df,
                how="outer",
                on=["target_date", "country", "population", "source"],
            )

    return base_df


def update_desktop_format(df: pd.DataFrame) -> None:
    """Format Desktop forecast DataFrame.

    Adds:
    - app_name: "desktop"
    - data_source: "Glean_Desktop" (all current Desktop queries use Glean)
    - segment: JSON string with os field
    """
    df["app_name"] = "desktop"
    df["data_source"] = "Glean_Desktop"
    df["segment"] = df["population"].apply(
        lambda x: json.dumps({"os": "ALL" if x == "None" else x})
    )
    df.drop("population", axis=1, inplace=True)


def update_mobile_format(df: pd.DataFrame) -> None:
    """Format Mobile forecast DataFrame.

    Adds:
    - app_name: specific app name or "ALL MOBILE" for aggregates
    - data_source: "Glean_Mobile" (all Mobile queries use Glean)
    - segment: Empty JSON object (Mobile doesn't segment by OS)
    """
    df["app_name"] = df["population"].where(df["population"] != "None", "ALL MOBILE")
    df["data_source"] = "Glean_Mobile"
    df["segment"] = "{}"
    df.drop("population", axis=1, inplace=True)


def format_output_table(
    df: pd.DataFrame, start_date: datetime, run_timestamp: datetime
) -> pd.DataFrame:

    df.rename(columns={
        'DAU': 'dau',
        'New Profiles': 'new_profiles',
        'Existing Engagement DAU': 'existing_engagement_dau',
        'Existing Engagement MAU': 'existing_engagement_mau',
    }, inplace=True)

    non_metric_cols = [
        "forecast_start_date",
        "forecast_run_timestamp",
        "mozaic_hash",
        "target_date",
        "source",
        "data_source",
        "country",
        "app_name",
        "segment",
    ]
    metric_cols = [
        c for c in df.columns if c not in non_metric_cols
    ]
    full_col_order = non_metric_cols + metric_cols

    df["country"] = df["country"].where(df["country"] != "None", "ALL")
    df["forecast_start_date"] = start_date
    df['forecast_start_date'] = pd.to_datetime(df['forecast_start_date'])
    df["forecast_run_timestamp"] = run_timestamp
    df['forecast_run_timestamp'] = pd.to_datetime(df['forecast_run_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['target_date'] = pd.to_datetime(df['target_date']).dt.strftime('%Y-%m-%d')
    df["mozaic_hash"] = get_git_commit_hash()
    df["source"] = np.where(df["source"] == "actual", "training", df["source"])

    string_cols = [
        "forecast_run_timestamp",
        "target_date",
        "mozaic_hash",
        "source",
        "data_source",
        "country",
        "app_name",
        "segment",
    ]

    df[string_cols] = df[string_cols].astype("string")
    df = df[full_col_order]
    df.sort_values(non_metric_cols)

    return df
