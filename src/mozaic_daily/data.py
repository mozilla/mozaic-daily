# -*- coding: utf-8 -*-
"""BigQuery data fetching and SQL query builders.

This module executes SQL queries against BigQuery and returns DataFrames.
Supports checkpoint-based caching to disk (parquet files) to avoid
re-querying during development/testing.

Functions:
- desktop_query(): SQL builder for Desktop metrics
- mobile_query(): SQL builder for Mobile metrics
- get_queries(): Returns all query functions
- get_aggregate_data(): Fetches all Desktop and Mobile metrics
"""

from typing import Dict, Optional
import pandas as pd
from google.cloud import bigquery
import os
from .config import get_sql_time_clause


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
    countries: str,
    testing_mode: bool = False
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

    if testing_mode:
        return queries  # Early return with only desktop/DAU

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
