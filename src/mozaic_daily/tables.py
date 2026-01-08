# -*- coding: utf-8 -*-
"""Table formatting, manipulation, and version tracking.

This module transforms raw forecast DataFrames into the final output format
for BigQuery upload. Key operations:
- Combining metric tables into single DataFrame
- Formatting Desktop/Mobile segments (app names, OS JSON)
- Creating aggregate "ALL" rows (Desktop+Mobile combined)
- Column renaming and type conversion
- Git hash retrieval for version tracking

Functions:
- combine_tables(): Merges metric-specific DataFrames
- update_desktop_format(): Formats Desktop segment columns
- update_mobile_format(): Formats Mobile segment columns
- add_desktop_and_mobile_rows(): Creates aggregate rows
- format_output_table(): Final formatting for BigQuery
- get_git_commit_hash*(): Version tracking utilities
"""

import pandas as pd
import numpy as np
import json
import re
import subprocess
from pathlib import Path
from typing import Optional
from datetime import datetime


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


# Table manipulation functions


def combine_tables(table_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
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
    df["app_name"] = "desktop"
    df["app_category"] = "Desktop"
    df["segment"] = df["population"].apply(
        lambda x: json.dumps({"os": "ALL" if x == "None" else x})
    )
    df.drop("population", axis=1, inplace=True)


def update_mobile_format(df: pd.DataFrame) -> None:
    df["app_name"] = df["population"].where(df["population"] != "None", "ALL MOBILE")
    df["app_category"] = "Mobile"
    df["segment"] = "{}"
    df.drop("population", axis=1, inplace=True)


def add_desktop_and_mobile_rows(df: pd.DataFrame) -> pd.DataFrame:
    tmp_df = (
        df[
            ((df["app_category"] == "Mobile") & (df["app_name"] == "ALL MOBILE"))
            | ((df["app_category"] == "Desktop") & (df["segment"] == '{"os": "ALL"}'))
        ]
        .groupby(["target_date", "country", "source"])
        .agg(
            {
                "DAU": "sum",
                "New Profiles": "sum",
                "Existing Engagement DAU": "sum",
                "Existing Engagement MAU": "sum",
            }
        )
    )
    tmp_df["app_category"] = "ALL"
    tmp_df["app_name"] = "ALL"
    tmp_df["segment"] = '{"os": "ALL"}'
    tmp_df = tmp_df.reset_index()
    return pd.concat([df, tmp_df[df.columns]])


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
        "country",
        "app_name",
        "app_category",
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
        "country",
        "app_name",
        "app_category",
        "segment",
    ]

    df[string_cols] = df[string_cols].astype("string")
    df = df[full_col_order]
    df.sort_values(non_metric_cols)

    return df
