# -*- coding: utf-8 -*-

# Setup
import numpy as np
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
import json, os, re, subprocess
from typing import Dict, Any


import mozaic
import prophet
from mozaic.models import desktop_forecast_model, mobile_forecast_model
from mozaic import TileSet, Tile, Mozaic, populate_tiles, curate_mozaics, mozaic_divide


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
           IFNULL(LOWER({app_name_column}) LIKE '%fenix%', FALSE) AS fenix,
           IFNULL(LOWER({app_name_column}) LIKE '%firefox ios%', FALSE) AS firefox_ios,
           IFNULL(LOWER({app_name_column}) LIKE '%focus android%', FALSE) AS focus_android,
           SUM({y}) AS y,
     FROM `{table}`
    WHERE {where}
    GROUP BY ALL
    ORDER BY 1, 2 ASC
    """


# Desktop
def get_queries(start_date_training: str, start_date_mobile_clean_training: str, countries: str) -> Dict[str, Dict[str, str]]:
    queries = {"desktop": {}, "mobile": {}}
    queries["desktop"]["DAU"] = desktop_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.active_users_aggregates",
        windows_version_column="os_version",
        where=f'app_name = "Firefox Desktop" AND submission_date >= "{start_date_training}"',
    )

    queries["desktop"]["New Profiles"] = desktop_query(
        x="first_seen_date",
        y="new_profiles",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.desktop_new_profiles",
        windows_version_column="windows_version",
        where=f'is_desktop AND first_seen_date >= "{start_date_training}" AND first_seen_date NOT BETWEEN "2023-07-18" AND "2023-07-19" # anomaly',
    )

    queries["desktop"]["Existing Engagement DAU"] = desktop_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.desktop_engagement",
        windows_version_column="normalized_os_version",
        where=f'is_desktop AND lifecycle_stage = "existing_user" AND submission_date >= "{start_date_training}"',
    )

    queries["desktop"]["Existing Engagement MAU"] = desktop_query(
        x="submission_date",
        y="mau",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.desktop_engagement",
        windows_version_column="normalized_os_version",
        where=f'is_desktop AND lifecycle_stage = "existing_user" AND submission_date >= "{start_date_training}"',
    )

    # Mobile
    queries["mobile"]["DAU"] = mobile_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.active_users_aggregates",
        app_name_column="app_name",
        where=f'app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS") AND submission_date >= "{start_date_training}"',
    )

    queries["mobile"]["New Profiles"] = mobile_query(
        x="first_seen_date",
        y="new_profiles",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.mobile_new_profiles",
        app_name_column="app_name",
        where=f'is_mobile AND first_seen_date >= "{start_date_mobile_clean_training}" AND first_seen_date NOT BETWEEN "2023-07-18" AND "2023-07-19" # anomaly',
    )

    queries["mobile"]["Existing Engagement DAU"] = mobile_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.mobile_engagement",
        app_name_column="app_name",
        where=f'is_mobile AND lifecycle_stage = "existing_user" AND submission_date >= "{start_date_mobile_clean_training}"',
    )

    queries["mobile"]["Existing Engagement MAU"] = mobile_query(
        x="submission_date",
        y="mau",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.mobile_engagement",
        app_name_column="app_name",
        where=f'is_mobile AND lifecycle_stage = "existing_user" AND submission_date >= "{start_date_training}"',
    )
    return queries

# Get data
def get_aggregate_data(queries: Dict[str, Dict[str, str]], project: str) -> Dict[str, Dict[str, pd.DataFrame]]:
    datasets = {"desktop": {}, "mobile": {}}

    # fetch query results and store the raw data
    for metric, query in queries["desktop"].items():
        print(f"Querying Desktop {metric}")
        datasets["desktop"][metric] = bigquery.Client(project).query(query).to_dataframe()

    for metric, query in queries["mobile"].items():
        print(f"Querying Mobile {metric}")
        datasets["mobile"][metric] = bigquery.Client(project).query(query).to_dataframe()

    return datasets

# Do the forecasting
def get_forecast_dfs(
    datasets: Dict[str, pd.DataFrame],
    forecast_model: Any,
    forecast_start_date: str,
    forecast_end_date: str,
) -> Dict[str, pd.DataFrame]:
    tileset = mozaic.TileSet()
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
    df["app"] = "Desktop"
    df["app_category"] = "Desktop"
    df["segment"] = df["population"].apply(
        lambda x: json.dumps({"os": "ALL" if x == "None" else x})
    )
    df.drop("population", axis=1, inplace=True)


def update_mobile_format(df: pd.DataFrame) -> None:
    df["app"] = df["population"].where(df["population"] != "None", "ALL MOBILE")
    df["app_category"] = "Mobile"
    df["segment"] = "{}"
    df.drop("population", axis=1, inplace=True)


def add_desktop_and_mobile_rows(df: pd.DataFrame) -> pd.DataFrame:
    tmp_df = (
        df[
            ((df["app_category"] == "Mobile") & (df["app"] == "ALL MOBILE"))
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
    tmp_df["app"] = "ALL"
    tmp_df["segment"] = '{"os": "ALL"}'
    tmp_df = tmp_df.reset_index()
    return pd.concat([df, tmp_df[df.columns]])


def format_output_table(
    df: pd.DataFrame, start_date: datetime, run_timestamp: datetime
) -> pd.DataFrame:
    non_metric_cols = [
        "forecast_start_date",
        "forecast_run_timestamp",
        "mozaic_hash",
        "target_date",
        "source",
        "country",
        "app",
        "app_category",
        "segment",
    ]
    metric_cols = [
        c for c in df.columns if c not in non_metric_cols
    ]
    full_col_order = non_metric_cols + metric_cols

    df["country"] = df["country"].where(df["country"] != "None", "ALL")
    df["forecast_start_date"] = start_date
    df["forecast_run_timestamp"] = run_timestamp
    df["mozaic_hash"] = get_git_commit_hash_from_pip()
    df["source"] = np.where(df["source"] == "actual", "training", df["source"])
    metric = "DAU"
    df.rename(columns={"value": metric.lower()}, inplace=True)
    df = df[full_col_order]
    df.sort_values(non_metric_cols)

    return df


def main() -> pd.DataFrame:
    # Establish constants

    # Dates
    forecast_run_dt = datetime.now()
    forecast_start_date = (forecast_run_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    forecast_end_date = datetime(forecast_run_dt.year + 1, 12, 31).strftime("%Y-%m-%d")

    start_date_training = "2020-01-01"
    start_date_mobile_clean_training = "2023-07-01"

    # Markets
    top_DAU_markets = set(
        ["US", "BR", "CA", "MX", "AR", "IN", "ID", "JP", "IR", "CN", "DE", "FR", "PL", "RU", "IT"]
    )
    top_google_markets = set(
        ["US", "DE", "FR", "GB", "PL", "CA", "CH", "IT", "AU", "NL", "ES", "JP", "AT"]
    )
    nonmonetized_google = set(["RU", "UA", "TR", "BY", "KZ", "CN"])
    all_markets = top_DAU_markets | top_google_markets | nonmonetized_google
    countries = ", ".join(f"'{i}'" for i in sorted(all_markets))

    # Other
    project = "moz-fx-data-bq-data-science"


    # Get data and process it
    datasets = get_aggregate_data(get_queries(
            start_date_training, 
            start_date_mobile_clean_training,
            countries
        ),
        project
    )

    df_desktop = combine_tables(get_desktop_forecast_dfs(
            datasets,
            forecast_start_date, 
            forecast_end_date
        )
    )
    df_mobile = combine_tables(get_mobile_forecast_dfs(
            datasets,
            forecast_start_date, 
            forecast_end_date
        )
    )

    # Format data
    update_desktop_format(df_desktop)
    update_mobile_format(df_mobile)

    df = add_desktop_and_mobile_rows(pd.concat([df_desktop, df_mobile]))
    df = format_output_table(df, forecast_start_date, forecast_run_dt)

    return df
    

if __name__ == '__main__':
    main()