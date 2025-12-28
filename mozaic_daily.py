# -*- coding: utf-8 -*-

# Setup
import numpy as np
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
import json, os, re, subprocess
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
import warnings

import mozaic
import prophet
from mozaic.models import desktop_forecast_model, mobile_forecast_model
from mozaic import TileSet, Tile, Mozaic, populate_tiles, curate_mozaics, mozaic_divide
from constants import *

from google.cloud import bigquery




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


def desktop_query(
        x: str,
        y: str,
        countries: str,
        table: str,
        windows_version_column: str,
        where: str,
    ) -> str:
    return f"""
    SELECT {x} AS x,
           IF(country IN ({countries}), country, 'ROW') AS country,
           IFNULL(LOWER({windows_version_column}) LIKE '%windows 10%', FALSE) AS win10,
           IFNULL(LOWER({windows_version_column}) LIKE '%windows 11%', FALSE) AS win11,
           IFNULL(LOWER({windows_version_column}) LIKE '%windows%' AND LOWER({windows_version_column}) NOT LIKE '%windows 10%' AND LOWER({windows_version_column}) NOT LIKE '%windows 11%', FALSE) AS winX,
           SUM({y}) AS y,
     FROM `{table}`
    WHERE {where}
    GROUP BY ALL
    ORDER BY 1, 2 ASC
    """


def mobile_query(
        x: str,
        y: str,
        countries: str,
        table: str,
        app_name_column: str,
        where: str,
    ) -> str:
    return f"""
    SELECT {x} AS x,
           IF(country IN ({countries}), country, 'ROW') AS country,
           IFNULL(LOWER({app_name_column}) LIKE '%fenix%', FALSE) AS fenix_android,
           IFNULL(LOWER({app_name_column}) LIKE '%firefox ios%', FALSE) AS firefox_ios,
           IFNULL(LOWER({app_name_column}) LIKE '%focus android%', FALSE) AS focus_android,
           IFNULL(LOWER({app_name_column}) LIKE '%focus ios%', FALSE) AS focus_ios,
           SUM({y}) AS y,
     FROM `{table}`
    WHERE {where}
    GROUP BY ALL
    ORDER BY 1, 2 ASC
    """

def get_queries(
    countries: str
) -> Dict[str, Dict[str, str]]:
    queries = {"desktop": {}, "mobile": {}}
    queries["desktop"]["DAU"] = desktop_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates",
        windows_version_column="os_version",
        where=f'app_name = "Firefox Desktop" AND {get_sql_time_clause(("desktop", "DAU"))}',
    )

    queries["desktop"]["New Profiles"] = desktop_query(
        x="first_seen_date",
        y="new_profiles",
        countries=countries,
        table="moz-fx-data-shared-prod.firefox_desktop.new_profiles_aggregates",
        windows_version_column="windows_version",
        where=f'is_desktop AND {get_sql_time_clause(("desktop", "New Profiles"))}',
    )

    queries["desktop"]["Existing Engagement DAU"] = desktop_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.firefox_desktop.desktop_engagement_aggregates",
        windows_version_column="normalized_os_version",
        where=f'is_desktop AND lifecycle_stage = "existing_user" AND {get_sql_time_clause(("desktop", "Existing Engagement DAU"))}',
    )

    queries["desktop"]["Existing Engagement MAU"] = desktop_query(
        x="submission_date",
        y="mau",
        countries=countries,
        table="moz-fx-data-shared-prod.firefox_desktop.desktop_engagement_aggregates",
        windows_version_column="normalized_os_version",
        where=f'is_desktop AND lifecycle_stage = "existing_user" AND {get_sql_time_clause(("desktop", "Existing Engagement MAU"))}',
    )

    # Mobile
    queries["mobile"]["DAU"] = mobile_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates",
        app_name_column="app_name",
        where=f'app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS") AND {get_sql_time_clause(("mobile", "DAU"))}',
    )

    queries["mobile"]["New Profiles"] = mobile_query(
        x="first_seen_date",
        y="new_profiles",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.mobile_new_profiles",
        app_name_column="app_name",
        where=f'is_mobile AND {get_sql_time_clause(("mobile", "New Profiles"))}',
    )

    queries["mobile"]["Existing Engagement DAU"] = mobile_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.mobile_engagement",
        app_name_column="app_name",
        where=f'is_mobile AND lifecycle_stage = "existing_user" AND {get_sql_time_clause(("mobile", "Existing Engagement DAU"))}',
    )

    queries["mobile"]["Existing Engagement MAU"] = mobile_query(
        x="submission_date",
        y="mau",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.mobile_engagement",
        app_name_column="app_name",
        where=f'is_mobile AND lifecycle_stage = "existing_user" AND {get_sql_time_clause(("mobile", "Existing Engagement MAU"))}',
    )
    return queries

# Get data
def get_aggregate_data(
    queries: Dict[str, Dict[str, str]], 
    project: str,
    checkpoints: Optional[bool] = False,
) -> Dict[str, Dict[str, pd.DataFrame]]:
    datasets = {"desktop": {}, "mobile": {}}

    make_filename = lambda platform, metric: f'mozaic_parts.raw.{platform}.{metric}.parquet'

    # fetch query results and store the raw data
    for metric, query in queries["desktop"].items():
        checkpoint_filename = make_filename("desktop", metric)
        df = None
        if checkpoints and os.path.exists(checkpoint_filename):
            print(f'Desktop {metric} exists, loading')
            df = pd.read_parquet(checkpoint_filename)
        else:
            print(f"Querying Desktop {metric}")
            print (query)
            df = bigquery.Client(project).query(query).to_dataframe()
            if checkpoints:
                df.to_parquet(checkpoint_filename)
        datasets['desktop'][metric] = df        

    for metric, query in queries["mobile"].items():
        checkpoint_filename = make_filename("mobile", metric)
        df = None
        if checkpoints and os.path.exists(checkpoint_filename):
            print(f'Mobile {metric} exists, loading')
            df = pd.read_parquet(checkpoint_filename)
        else:
            print(f"Querying Mobile {metric}")
            print(query)
            df = bigquery.Client(project).query(query).to_dataframe()
            if checkpoints:
                df.to_parquet(checkpoint_filename)
        datasets['mobile'][metric] = df

    return datasets

# Do the forecasting
def get_forecast_dfs(
    datasets: Dict[str, pd.DataFrame],
    forecast_model: Any,
    forecast_start_date: str,
    forecast_end_date: str
) -> Dict[str, pd.DataFrame]:
    tileset = mozaic.TileSet()

    print('\n--- Populate tiles\n')
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=RuntimeWarning,
            message=".*divide by zero.*|.*overflow.*|.*invalid value.*"
        )
        mozaic.populate_tiles(
            datasets,
            tileset,
            forecast_model,
            forecast_start_date,
            forecast_end_date,
        )

    mozaics: dict[str, Mozaic] = {}
    _ctry = defaultdict(lambda: defaultdict(mozaic.Mozaic))
    _pop = defaultdict(lambda: defaultdict(mozaic.Mozaic))

    print ('\n--- Curate Mozaics\n')
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=RuntimeWarning,
            message=".*divide by zero.*|.*overflow.*|.*invalid value.*"
        )

        mozaic.utils.curate_mozaics(
            datasets,
            tileset,
            forecast_model,
            mozaics,
            _ctry,
            _pop,
        )

    dfs = {}
    for metric, moz in mozaics.items():
        print(metric)
        dfs[metric] = moz.to_granular_forecast_df()

    return dfs


def get_desktop_forecast_dfs(
    datasets: Dict[str, Dict[str, pd.DataFrame]],
    forecast_start_date: str,
    forecast_end_date: str,
) -> Dict[str, pd.DataFrame]:
    return get_forecast_dfs(
        datasets["desktop"],
        desktop_forecast_model,
        forecast_start_date,
        forecast_end_date,
    )


def get_mobile_forecast_dfs(
    datasets: Dict[str, Dict[str, pd.DataFrame]],
    forecast_start_date: str,
    forecast_end_date: str,
) -> Dict[str, pd.DataFrame]:
    return get_forecast_dfs(
        datasets["mobile"],
        mobile_forecast_model,
        forecast_start_date,
        forecast_end_date,
    )


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


def main(
    project: Optional[str] = None,
    checkpoints: Optional[bool] = False
) -> pd.DataFrame:
    # Establish constants
    constants = get_constants()
    if not project:
        project = constants['default_project']
    print(f'Running forecast from {constants["forecast_start_date"]} through {constants["forecast_end_date"]}')

    # Get the data
    # This method does internal file checkpointing
    datasets = get_aggregate_data(
        get_queries(constants['country_string']),
        project,
        checkpoints = checkpoints
    )

    checkpoint_filename = constants['forecast_checkpoint_filename']
    df = None
    if checkpoints and os.path.exists(checkpoint_filename):
        print('Forecast already generated. Loading existing data.')
        df = pd.read_parquet(checkpoint_filename)
    else:
        # Process the data
        print('Desktop Forecasting\n')
        df_desktop = combine_tables(get_desktop_forecast_dfs(
                datasets,
                constants['forecast_start_date'], 
                constants['forecast_end_date']
            )
        )
        print('Mobile Forecasting\n')
        df_mobile = combine_tables(get_mobile_forecast_dfs(
                datasets,
                constants['forecast_start_date'], 
                constants['forecast_end_date']
            )
        )
        print('\n\nDone with forecasts')

        # Format data
        update_desktop_format(df_desktop)
        update_mobile_format(df_mobile)

        df = add_desktop_and_mobile_rows(pd.concat([df_desktop, df_mobile]))
        df = format_output_table(df, constants['forecast_start_date'], constants['forecast_run_dt'])
        if checkpoints:
            df.to_parquet(checkpoint_filename)
        

    return df
    

if __name__ == '__main__':
    main(checkpoints=True)
