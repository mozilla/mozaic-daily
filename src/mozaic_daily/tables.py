# -*- coding: utf-8 -*-
"""Table formatting and manipulation.

This module transforms raw forecast DataFrames into the final output format
for BigQuery upload. Key operations:
- Combining metric tables into single DataFrame
- Formatting Desktop/Mobile segments (app names, OS JSON, data_source)
- Column renaming and type conversion

Note: This module does NOT create cross-platform aggregate rows.
Each row belongs to exactly one data_source (glean_desktop, legacy_desktop, or glean_mobile).

Functions:
- combine_tables(): Merges metric-specific DataFrames
- update_desktop_format(): Formats Desktop segment columns
- update_mobile_format(): Formats Mobile segment columns
- format_output_table(): Final formatting for BigQuery
"""

import pandas as pd
import numpy as np
import json
from typing import Dict
from datetime import datetime

from .config import get_git_commit_hash


# Table manipulation functions


def combine_tables(table_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Combine multiple metric-specific DataFrames into a single wide DataFrame.

    Performs outer joins on common index columns (target_date, country, population, source)
    to merge metric values from separate DataFrames into columns of a single DataFrame.

    Args:
        table_dict: Dictionary mapping metric names to DataFrames. Each DataFrame must have
                   a 'value' column and common index columns (target_date, country,
                   population, source).

    Returns:
        Combined DataFrame with metrics as separate columns. The 'value' column from each
        input DataFrame is renamed to the corresponding metric name.
    """
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


def update_desktop_format(df: pd.DataFrame, data_source: str = "glean_desktop") -> None:
    """Format Desktop forecast DataFrame.

    Args:
        df: DataFrame to format (modified in place)
        data_source: Data source value (glean_desktop or legacy_desktop)

    Adds:
    - app_name: "desktop"
    - data_source: Provided data_source parameter
    - segment: JSON string with os field
    """
    df["app_name"] = "desktop"
    df["data_source"] = data_source
    df["segment"] = df["population"].apply(
        lambda x: json.dumps({"os": "ALL" if x == "None" else x})
    )
    df.drop("population", axis=1, inplace=True)


def update_mobile_format(df: pd.DataFrame, data_source: str = "glean_mobile") -> None:
    """Format Mobile forecast DataFrame.

    Args:
        df: DataFrame to format (modified in place)
        data_source: Data source value (glean_mobile)

    Adds:
    - app_name: specific app name or "ALL MOBILE" for aggregates
    - data_source: Provided data_source parameter (always glean_mobile)
    - segment: Empty JSON object (Mobile doesn't segment by OS)
    """
    df["app_name"] = df["population"].where(df["population"] != "None", "ALL MOBILE")
    df["data_source"] = data_source
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

    df["country"] = df["country"].where(df["country"] != "None", "ALL")
    df["forecast_start_date"] = start_date
    df['forecast_start_date'] = pd.to_datetime(df['forecast_start_date'])
    df["forecast_run_timestamp"] = run_timestamp
    df['forecast_run_timestamp'] = pd.to_datetime(df['forecast_run_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['target_date'] = pd.to_datetime(df['target_date']).dt.strftime('%Y-%m-%d')
    df["mozaic_hash"] = get_git_commit_hash()
    df["source"] = np.where(df["source"] == "actual", "training", df["source"])
    df.rename(columns={"source": "data_type"}, inplace=True)

    non_metric_cols = [
        "forecast_start_date",
        "forecast_run_timestamp",
        "mozaic_hash",
        "data_source",
        "target_date",
        "data_type",
        "country",
        "app_name",
        "segment",
    ]
    metric_cols = [
        c for c in df.columns if c not in non_metric_cols
    ]
    full_col_order = non_metric_cols + metric_cols

    string_cols = [
        "forecast_run_timestamp",
        "target_date",
        "mozaic_hash",
        "data_type",
        "data_source",
        "country",
        "app_name",
        "segment",
    ]

    df[string_cols] = df[string_cols].astype("string")
    df = df[full_col_order]
    df = df.sort_values(non_metric_cols)

    return df
