# -*- coding: utf-8 -*-

# Setup
import numpy as np
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
import json, os, re, subprocess
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path


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
           IFNULL(LOWER({app_name_column}) LIKE '%fenix%', FALSE) AS fenix,
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
    constraints: Dict[Tuple[str, str], Dict], 
    countries: str
) -> Dict[str, Dict[str, str]]:
    queries = {"desktop": {}, "mobile": {}}
    queries["desktop"]["DAU"] = desktop_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates",
        windows_version_column="os_version",
        where=f'app_name = "Firefox Desktop" AND {get_sql_time_clause(("desktop", "DAU"), constraints)}',
    )

    queries["desktop"]["New Profiles"] = desktop_query(
        x="first_seen_date",
        y="new_profiles",
        countries=countries,
        table="moz-fx-data-shared-prod.firefox_desktop.new_profiles_aggregates",
        windows_version_column="windows_version",
        where=f'is_desktop AND {get_sql_time_clause(("desktop", "New Profiles"), constraints)}',
    )

    queries["desktop"]["Existing Engagement DAU"] = desktop_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.firefox_desktop.desktop_engagement_aggregates",
        windows_version_column="normalized_os_version",
        where=f'is_desktop AND lifecycle_stage = "existing_user" AND {get_sql_time_clause(("desktop", "Existing Engagement DAU"), constraints)}',
    )

    queries["desktop"]["Existing Engagement MAU"] = desktop_query(
        x="submission_date",
        y="mau",
        countries=countries,
        table="moz-fx-data-shared-prod.firefox_desktop.desktop_engagement_aggregates",
        windows_version_column="normalized_os_version",
        where=f'is_desktop AND lifecycle_stage = "existing_user" AND {get_sql_time_clause(("desktop", "Existing Engagement MAU"), constraints)}',
    )

    # Mobile
    queries["mobile"]["DAU"] = mobile_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates",
        app_name_column="app_name",
        where=f'app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS") AND {get_sql_time_clause(("mobile", "DAU"), constraints)}',
    )

    queries["mobile"]["New Profiles"] = mobile_query(
        x="first_seen_date",
        y="new_profiles",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.mobile_new_profiles",
        app_name_column="app_name",
        where=f'is_mobile AND {get_sql_time_clause(("mobile", "New Profiles"), constraints)}',
    )

    queries["mobile"]["Existing Engagement DAU"] = mobile_query(
        x="submission_date",
        y="dau",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.mobile_engagement",
        app_name_column="app_name",
        where=f'is_mobile AND lifecycle_stage = "existing_user" AND {get_sql_time_clause(("mobile", "Existing Engagement DAU"), constraints)}',
    )

    queries["mobile"]["Existing Engagement MAU"] = mobile_query(
        x="submission_date",
        y="mau",
        countries=countries,
        table="moz-fx-data-shared-prod.telemetry.mobile_engagement",
        app_name_column="app_name",
        where=f'is_mobile AND lifecycle_stage = "existing_user" AND {get_sql_time_clause(("mobile", "Existing Engagement MAU"), constraints)}',
    )
    return queries

# Get data
def get_aggregate_data(queries: Dict[str, Dict[str, str]], project: str) -> Dict[str, Dict[str, pd.DataFrame]]:
    datasets = {"desktop": {}, "mobile": {}}

    make_filename = lambda platform, metric: f'mozaic_parts.raw.{platform}.{metric}.parquet'

    # fetch query results and store the raw data
    for metric, query in queries["desktop"].items():
        checkpoint_filename = make_filename("desktop", metric)
        df = None
        if os.path.exists(checkpoint_filename):
            print(f'Desktop {metric} exists, loading')
            df = pd.read_parquet(checkpoint_filename)
        else:
            print(f"Querying Desktop {metric}")
            print (query)
            datasets["desktop"][metric] = bigquery.Client(project).query(query).to_dataframe()
            datasets["desktop"][metric].to_parquet(checkpoint_filename)

    for metric, query in queries["mobile"].items():
        checkpoint_filename = make_filename("mobile", metric)
        df = None
        if os.path.exists(checkpoint_filename):
            print(f'Mobile {metric} exists, loading')
            df = pd.read_parquet(checkpoint_filename)
        else:
            print(f"Querying Mobile {metric}")
            print(query)
            datasets["mobile"][metric] = bigquery.Client(project).query(query).to_dataframe()
            datasets["mobile"][metric].to_parquet(checkpoint_filename)

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
    df["app_name"] = "Desktop"
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



def validate_df_against_table(
    df: pd.DataFrame,
    table_id: str,
    project: str,

) -> None:
    """
    Utility method for the flow

    Validate that:
      - DataFrame columns match BigQuery table columns (no missing/extra).
      - Pandas dtypes are roughly compatible with BigQuery field types.
    Raises on problems.
    """
    table = bigquery.Client(project).get_table(table_id)
    bq_fields = {field.name: field for field in table.schema}

    # --- 1. Column presence checks ---
    df_cols = set(df.columns)
    bq_cols = set(bq_fields.keys())

    missing_in_df = bq_cols - df_cols
    extra_in_df = df_cols - bq_cols

    if missing_in_df:
        raise ValueError(
            f"DataFrame is missing columns present in BigQuery table: {sorted(missing_in_df)}"
        )

    if extra_in_df:
        raise ValueError(
            f"DataFrame has columns not present in BigQuery table: {sorted(extra_in_df)}"
        )

    # --- 2. Rough dtype ↔ BigQuery type compatibility ---
    # This is intentionally coarse; BigQuery does some coercion, but we want to catch obvious mismatches.
    pandas_to_bq_rough = {
        "int64": {"INTEGER", "INT64", "NUMERIC"},
        "Int64": {"INTEGER", "INT64", "NUMERIC"},      # pandas nullable integer
        "float64": {"FLOAT", "FLOAT64", "NUMERIC"},
        "boolean": {"BOOL", "BOOLEAN"},                # pandas BooleanDtype
        "bool": {"BOOL", "BOOLEAN"},
        "datetime64[ns]": {"TIMESTAMP", "DATETIME", "DATE"},
        "object": {"STRING", "BYTES", "GEOGRAPHY", "JSON"},
        "string": {"STRING"},
    }

    for name in df.columns:
        field = bq_fields[name]
        bq_type = field.field_type.upper()
        pd_dtype = df[name].dtype

        pd_dtype_str = str(pd_dtype)

        allowed_bq_types = None
        for key, types in pandas_to_bq_rough.items():
            if pd_dtype_str.startswith(key):
                allowed_bq_types = types
                break

        # If we don't recognize the dtype bucket, let BigQuery try to coerce.
        if allowed_bq_types is None:
            continue

        if bq_type not in allowed_bq_types:
            raise TypeError(
                f"Type mismatch for column '{name}': "
                f"pandas dtype '{pd_dtype_str}' may not be compatible with BigQuery type '{bq_type}'."
            )

    # --- 3. Check for rows ---
    # We can coarsly check that there's a least one row per target date. (There should be more with various countries and segments)
    constants = get_constants()
    start_date_training = datetime.strptime(constants['start_date_training'],'%Y-%m-%d')
    forecast_start_date = datetime.strptime(constants['forecast_start_date'],'%Y-%m-%d')
    forecast_end_date = datetime.strptime(constants['forecast_end_date'],'%Y-%m-%d')

    training_days = (
        df.loc[df["source"] == "training", 'target_date']
        .unique()
    )
    required_training_days = pd.date_range(start_date_training, forecast_start_date, freq='D').strftime('%Y-%m-%d')[:-1]

    missing_training_days = [d for d in required_training_days if d not in training_days]
    if len (missing_training_days) > 0:
        raise ValueError(
            f"Training target days missing: {missing_training_days}' "
        )


    forecast_days = (
        df.loc[df["source"] == "forecast", 'target_date']
        .unique()
    )
    required_forecast_days = pd.date_range(forecast_start_date, forecast_end_date, freq='D').strftime('%Y-%m-%d')

    missing_forecast_days = [d for d in required_forecast_days if d not in forecast_days]
    if len (missing_forecast_days) > 0:
        raise ValueError(
            f"Forecast target days missing: {missing_forecast_days}' "
        )

    # If we get here, validation passed
    return

def get_constants() -> Dict[str, str]:
    constants = {}

    # Dates
    forecast_run_dt = datetime.now()
    constants['forecast_run_dt'] = forecast_run_dt
    constants['forecast_start_date'] = (forecast_run_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    constants['forecast_end_date'] = datetime(forecast_run_dt.year + 1, 12, 31).strftime("%Y-%m-%d")

    # Markets
    top_DAU_markets = set(
        ["US", "BR", "CA", "MX", "AR", "IN", "ID", "JP", "IR", "CN", "DE", "FR", "PL", "RU", "IT"]
    )
    top_google_markets = set(
        ["US", "DE", "FR", "GB", "PL", "CA", "CH", "IT", "AU", "NL", "ES", "JP", "AT"]
    )
    nonmonetized_google = set(["RU", "UA", "TR", "BY", "KZ", "CN"])
    constants['countries'] = top_DAU_markets | top_google_markets | nonmonetized_google
    constants['country_string'] = ", ".join(f"'{i}'" for i in sorted(constants['countries']))

    return constants

def get_date_constraints() -> Dict[Tuple[str, str], Dict]:
    return {
        ("desktop", "DAU"): {
            "date_field": "submission_date",
            "start": '2023-04-17',
            "excludes": [],
        },
        ("desktop", "New Profiles"): {
            "date_field": "first_seen_date",
            "start": '2023-06-07',
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
            "start": "2023-07-01",
            "excludes": [('2023-07-18', '2023-07-19')],
        },
        ("mobile", "Existing Engagement DAU"): {
            "date_field": "submission_date",
            "start": "2023-07-01",
            "excludes": [],
        },
        ("mobile", "Existing Engagement MAU"): {
            "date_field": "submission_date",
            "start": "2023-07-01",
            "excludes": [],
        },
    }

def get_date_keys():
    return get_date_constraints.keys()



def get_sql_time_clause(
    key: Tuple[str, str],
    constraints: Dict[Tuple[str, str], Dict],
    quote: str = '"',
) -> str:
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
    constraints: Dict[Tuple[str, str], Dict],
    end: Optional[str] = None,
) -> pd.DatetimeIndex:
    if key not in constraints:
        raise KeyError(f"Unknown key: {key}")

    entry = constraints[key]
    start = pd.to_datetime(entry["start"]).normalize()
    end_dt = pd.to_datetime(end).normalize() if end else pd.Timestamp.now().normalize()

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
    start_dt = pd.to_datetime(end).normalize()
    end_dt = pd.to_datetime(end).normalize()

    full = pd.date_range(start=start_dt, end=end_dt, freq='D')

def main(project: str = "moz-fx-data-bq-data-science") -> pd.DataFrame:
    # Establish constants
    constants = get_constants()
    date_constraints = get_date_constraints()

    # Get the data
    # This method does internal file checkpointing
    datasets = get_aggregate_data(get_queries(
            date_constraints,
            constants['country_string']
            ),
            project
        )

    # debug
    return

    checkpoint_filename = 'mozaic_parts.forecast.parquet'
    df = None
    if os.path.exists(checkpoint_filename):
        df = pd.read_parquet(checkpoint_filename)
    else:
        # Process the data
        df_desktop = combine_tables(get_desktop_forecast_dfs(
                datasets,
                constants['forecast_start_date'], 
                constants['forecast_end_date']
            )
        )
        df_mobile = combine_tables(get_mobile_forecast_dfs(
                datasets,
                constants['forecast_start_date'], 
                constants['forecast_end_date']
            )
        )

        # Format data
        update_desktop_format(df_desktop)
        update_mobile_format(df_mobile)

        df = add_desktop_and_mobile_rows(pd.concat([df_desktop, df_mobile]))
        df = format_output_table(df, constants['forecast_start_date'], constants['forecast_run_dt'])

        df.to_parquet(checkpoint_filename)
        

    return df
    

if __name__ == '__main__':
    main()
