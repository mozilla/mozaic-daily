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

from typing import Dict, Optional, Tuple, List
import pandas as pd
from google.cloud import bigquery
import os
from .config import STATIC_CONFIG

# Consolidated query configuration: all metadata needed to build SQL queries
QUERY_CONFIGS = {
    ("desktop", "DAU"): {
        'table': 'moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates',
        'os_column': 'os_version',
        'where_clause': 'app_name = "Firefox Desktop"',
        'date_field': 'submission_date',
        'date_start': '2023-04-17',
        'date_excludes': [],
        'x_column': 'submission_date',
        'y_column': 'dau',
    },
    ("desktop", "New Profiles"): {
        'table': 'moz-fx-data-shared-prod.firefox_desktop.new_profiles_aggregates',
        'os_column': 'windows_version',
        'where_clause': 'is_desktop',
        'date_field': 'first_seen_date',
        'date_start': '2023-07-01',
        'date_excludes': [('2023-07-18', '2023-07-19')],
        'x_column': 'first_seen_date',
        'y_column': 'new_profiles',
    },
    ("desktop", "Existing Engagement DAU"): {
        'table': 'moz-fx-data-shared-prod.firefox_desktop.desktop_engagement_aggregates',
        'os_column': 'normalized_os_version',
        'where_clause': 'is_desktop AND lifecycle_stage = "existing_user"',
        'date_field': 'submission_date',
        'date_start': '2023-06-07',
        'date_excludes': [],
        'x_column': 'submission_date',
        'y_column': 'dau',
    },
    ("desktop", "Existing Engagement MAU"): {
        'table': 'moz-fx-data-shared-prod.firefox_desktop.desktop_engagement_aggregates',
        'os_column': 'normalized_os_version',
        'where_clause': 'is_desktop AND lifecycle_stage = "existing_user"',
        'date_field': 'submission_date',
        'date_start': '2023-06-07',
        'date_excludes': [],
        'x_column': 'submission_date',
        'y_column': 'mau',
    },
    ("mobile", "DAU"): {
        'table': 'moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates',
        'app_column': 'app_name',
        'where_clause': 'app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS")',
        'date_field': 'submission_date',
        'date_start': '2020-12-31',
        'date_excludes': [],
        'x_column': 'submission_date',
        'y_column': 'dau',
    },
    ("mobile", "New Profiles"): {
        'table': 'moz-fx-data-shared-prod.telemetry.mobile_new_profiles',
        'app_column': 'app_name',
        'where_clause': 'is_mobile',
        'date_field': 'first_seen_date',
        'date_start': '2023-07-01',
        'date_excludes': [('2023-07-18', '2023-07-19')],
        'x_column': 'first_seen_date',
        'y_column': 'new_profiles',
    },
    ("mobile", "Existing Engagement DAU"): {
        'table': 'moz-fx-data-shared-prod.telemetry.mobile_engagement',
        'app_column': 'app_name',
        'where_clause': 'is_mobile AND lifecycle_stage = "existing_user"',
        'date_field': 'submission_date',
        'date_start': '2023-07-01',
        'date_excludes': [],
        'x_column': 'submission_date',
        'y_column': 'dau',
    },
    ("mobile", "Existing Engagement MAU"): {
        'table': 'moz-fx-data-shared-prod.telemetry.mobile_engagement',
        'app_column': 'app_name',
        'where_clause': 'is_mobile AND lifecycle_stage = "existing_user"',
        'date_field': 'submission_date',
        'date_start': '2023-07-01',
        'date_excludes': [],
        'x_column': 'submission_date',
        'y_column': 'mau',
    },
}

def build_sql_time_clause(key: Tuple[str, str], quote: str = '"') -> str:
    """Build SQL time clause from QUERY_CONFIGS."""
    cfg = QUERY_CONFIGS[key]
    field = cfg['date_field']
    start = cfg['date_start']
    excludes: List[Tuple[str, str]] = cfg['date_excludes']

    parts = [f'{field} >= {quote}{start}{quote}']
    for ex_start, ex_end in excludes:
        parts.append(
            f'{field} NOT BETWEEN {quote}{ex_start}{quote} AND {quote}{ex_end}{quote}'
        )
    return " AND ".join(parts)


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
    """Build SQL queries for all platform/metric combinations.

    Args:
        countries: SQL-formatted country list string
        testing_mode: If True, return only desktop/DAU query

    Returns:
        Nested dict: {platform: {metric: sql_query}}
    """
    queries = {"desktop": {}, "mobile": {}}

    # Build queries from QUERY_CONFIGS
    for (platform, metric), cfg in QUERY_CONFIGS.items():
        # Build WHERE clause with date constraints
        where_clause = f'{cfg["where_clause"]} AND {build_sql_time_clause((platform, metric))}'

        # Use appropriate query builder based on platform
        if platform == "desktop":
            query = desktop_query(
                x=cfg['x_column'],
                y=cfg['y_column'],
                countries=countries,
                table=cfg['table'],
                windows_version_column=cfg['os_column'],
                where=where_clause,
            )
        else:  # mobile
            query = mobile_query(
                x=cfg['x_column'],
                y=cfg['y_column'],
                countries=countries,
                table=cfg['table'],
                app_name_column=cfg['app_column'],
                where=where_clause,
            )

        queries[platform][metric] = query

        # Early return for testing mode (only desktop/DAU)
        if testing_mode and platform == "desktop" and metric == "DAU":
            return queries

    return queries

# Get data
def get_aggregate_data(
    queries: Dict[str, Dict[str, str]],
    project: str,
    checkpoints: Optional[bool] = False,
) -> Dict[str, Dict[str, pd.DataFrame]]:
    datasets = {"desktop": {}, "mobile": {}}

    # Use template from STATIC_CONFIG for checkpoint filenames
    filename_template = STATIC_CONFIG['raw_checkpoint_filename_template']

    # fetch query results and store the raw data
    for metric, query in queries["desktop"].items():
        checkpoint_filename = filename_template.format(platform="desktop", metric=metric)
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
        checkpoint_filename = filename_template.format(platform="mobile", metric=metric)
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
