# -*- coding: utf-8 -*-
"""Simple validation methods for the mozaic daily output table.

The general pattern is that a check will raise an exception to
announce problems. It will succeed silently."""

# Imports
import pandas as pd
import json
from datetime import datetime
from functools import reduce
from typing import Dict, List, Tuple, Optional, Any
import re

from google.cloud import bigquery

from constants import *

# Allowed value constants

APP_NAMES = set([
    'firefox_ios', 'focus_ios', 'fenix_android', 'focus_android', 
    'desktop', 'ALL MOBILE', 'ALL'
])
OPTIONAL_APP_NAMES = set(['other_mobile'])

APP_CATEGORIES = set([
    'Mobile', 'Desktop', 'ALL'
])

OS_VALUES = set([
    'win10', 'win11', 'winX', 'other', 'ALL', None
])

# Validation

def _get_bigquery_fields(
    project: str, 
    table_id: str
) -> Dict[str, bigquery.schema.SchemaField]:
    print('\t Validating fields')
    table = bigquery.Client(project).get_table(table_id)
    bq_fields = {field.name: field for field in table.schema}

    return bq_fields

def _check_column_presence(
    df: pd.DataFrame, 
    bq_fields: Dict[str, bigquery.schema.SchemaField]
) -> None:
    """There should be a column in the output dataframe corresponding to
    every column in the table schema and vice versa."""
    print('\t Validating column presence')
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

def _check_column_type(
    df: pd.DataFrame, 
    bq_fields: Dict[str, bigquery.schema.SchemaField]
) -> None:
    """The column types in the output table should roughly correspond
    to the columns in the schema. This is intentionally coarse; 
    BigQuery does some coercion, but we want to catch obvious mismatches."""
    print('\t Validating column types')

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

def _validate_string_column_formats(df: pd.DataFrame) -> None:
    print('\t Validating string column formats')
    def validate_column(col, validator):
        column_series = df[col].drop_duplicates()
        mask = column_series.map(validator)
        if not mask.all():
            print(column_series)
            print(mask)
            bad = column_series[~mask]
            raise ValueError(
                f"Validation failed for column '{col}'. "
                f"{len(bad)} invalid value(s), examples: {bad.head().tolist()}"
            )

    def make_allowed_string_validator(allowed):
        allowed_set = set(allowed)
        def validator(x):
            return x in allowed_set
        return validator

    def is_full_timestamp(x):
        try:
            datetime.fromisoformat(x)
            # Make sure it actually includes time
            return "T" in x or " " in x
        except Exception:
            return False

    def is_date_only(x):
        try:
            dt = datetime.fromisoformat(x)
            # Reject if time information is present
            return dt.time() == datetime.min.time()
        except Exception:
            return False

    SHA1_RE = re.compile(r"^[0-9a-fA-F]{40}$")
    def is_git_hash(x):
        return bool(SHA1_RE.match(x))

    def is_json_string(x):
        if not isinstance(x, str):
            return False
        try:
            json.loads(x)
            return True
        except Exception:
            return False

    validate_column('forecast_run_timestamp', is_full_timestamp)

    validate_column('mozaic_hash', is_git_hash)

    validate_column('target_date', is_date_only)

    validate_column('source', 
        make_allowed_string_validator(('training', 'forecast'))
    )

    validate_column('country', 
        make_allowed_string_validator(get_constants()['validation_countries'])
    )

    validate_column('app_name', 
        make_allowed_string_validator(APP_NAMES | OPTIONAL_APP_NAMES)
    )

    validate_column('app_category', 
        make_allowed_string_validator(APP_CATEGORIES)
    )

    validate_column('segment', is_json_string)

    def check_json_os(val):
        validator = make_allowed_string_validator(OS_VALUES)
        try:
            obj = json.loads(val)
            value = obj.get("os")
            if not validator(value):
                raise ValueError(
                    f"Validation failed for json column 'segment'. "
                    f"Invalid OS value found: '{value}'"
                )
        except Exception:
            raise ValueError(
                f"Validation failed for json column 'segment'."
                f"OS value not found."
            )

    df['segment'].apply(check_json_os)
    
def _check_row_counts(df: pd.DataFrame) -> None:
    print('\t Validating row counts')
    constants = get_constants()
    date_constraints = get_date_constraints()
    training_date_index_for = lambda key: get_training_date_index(key, constants['forecast_start_date'])

    # Overall date checks, training
    date_keys = get_date_keys()
    joint_training_index = reduce(lambda a, b: a.union(b), map(training_date_index_for, get_date_keys()))
    training_days = (
        df.loc[df["source"] == "training", 'target_date']
        .unique()
    )
    required_training_days = joint_training_index.strftime('%Y-%m-%d')[:-1] # Don't include the final day

    missing_training_days = [d for d in required_training_days if d not in training_days]
    if len (missing_training_days) > 0:
        raise ValueError(
            f"Training target days missing: {missing_training_days}' "
        )

    # Overall date checks, forecast
    forecast_days = (
        df.loc[df["source"] == "forecast", 'target_date']
        .unique()
    )
    required_forecast_days = get_prediction_date_index(constants['forecast_start_date'], constants['forecast_end_date']).strftime('%Y-%m-%d')

    missing_forecast_days = [d for d in required_forecast_days if d not in forecast_days]
    if len (missing_forecast_days) > 0:
        raise ValueError(
            f"Forecast target days missing: {missing_forecast_days}' "
        )

    # Country row checks
    country_count_df = df.groupby('target_date')['country'].nunique().reset_index()
    country_count_check = country_count_df['country'] == len(constants['countries'])
    if country_count_check.any():
        raise ValueError(
            f'Countries incorrect for dates: {list(country_count_df[~country_count_check]["target_date"])}'
        )

    # Segment row checks
    def row_check(col, comparison, human_readable_name):
        present = list(df[col].drop_duplicates())
        missing = [n for n in comparison if n not in present]
        if len(missing) > 0:
            raise ValueError(
                f'Missing {human_readable_name}: {missing}\n'
                f'Values present: {present}'
            )
        extra = [n for n in present if n not in comparison]
        if len(extra) > 0:
            raise ValueError(
                f'Extra {human_readable_name}: {extra_app_names}'
            )

    row_check('app_name', APP_NAMES, 'app name(s)')
    row_check('app_category', APP_CATEGORIES, 'app category')
    row_check('segment', 
        [f'{{"os": "{x}"}}' if x is not None else '{}' for x in OS_VALUES ],
        'segment(s)'
    )

    # No more rows than all possible combinations
    max_rows = len(constants['countries']) * len(APP_NAMES) * len(APP_CATEGORIES) * len(OS_VALUES)
    max_count_df = df.groupby('target_date').size().reset_index()
    max_count_mask = max_count_df.iloc[:,1] > max_rows
    if max_count_mask.any():
        raise ValueError(
            f'These dates have too many rows: {max_count_df[max_count_mask]["target_date"]}'
        )

def _validate_null_values(df: pd.DataFrame) -> None:
    print('\t Validating null values')
    target_cols = {
        'DAU': 'dau',
        'New Profiles': 'new_profiles',
        'Existing Engagement DAU': 'existing_engagement_dau',
        'Existing Engagement MAU': 'existing_engagement_mau'
    }
    max_columns = pd.get_option('display.max_columns')
    pd.set_option('display.max_columns', None)

    for key in get_date_keys():
        index = get_training_date_index(key).strftime('%Y-%m-%d')
        test_col_name = f'{key[0]}_{key[1]}_expected'
        expected_df = pd.Series(True, index=index, name=test_col_name).to_frame().reset_index()

        target_col = target_cols[key[1]]

        validation_df = (
            df[(df['app_category'] == key[0].capitalize()) & (df['source'] == 'training') & (df[target_col].notna())]
            .groupby('target_date')[target_col].sum().reset_index()
            .merge(
                expected_df,
                left_on = 'target_date',
                right_on = 'index',
                how = 'outer'
            )
        )

        missing = validation_df[(validation_df[target_col].isnull()) & (validation_df[test_col_name].notnull())]
        if len(missing) > 0:
            raise ValueError(
                f"""Missing dates for dataset {key}. Example rows:

                {missing.head(5)}
                """
            )
        extra = validation_df[(validation_df[target_col].notnull()) & (validation_df[test_col_name].isnull())]
        if len(extra) > 0:
            raise ValueError(
                f"""Extra dates for dataset {key}. Example rows:

                {extra.head(5)}
                """
            )
    pd.set_option('display.max_columns', max_columns)

def _validate_duplicate_rows(df: pd.DataFrame) -> None:
    print('\t Validating duplicate rows')
    key_cols = [x for x in df.columns if x not in ('dau', 'new_profiles', 'existing_engagement_dau', 'existing_engagement_mau')]
    duplicates = df[df.duplicated(subset=key_cols, keep=False)]

    if len(duplicates) > 0:
        raise ValueError(
            f"""Duplicate rows found. Example rows:

            {extra.head(6)}
            """
        )

# Validation entrypoint
def validate_output_dataframe(df: pd.DataFrame):
    constants = get_constants()

    bq_fields = _get_bigquery_fields(constants['default_project'], constants['default_table'])
    _check_column_presence(df, bq_fields)
    _check_column_type(df, bq_fields)
    _validate_string_column_formats(df)
    _check_row_counts(df)
    _validate_null_values(df)
    _validate_duplicate_rows(df)


if __name__ == '__main__':
    df = pd.read_parquet(get_constants()['forecast_checkpoint_filename'])
    validate_output_dataframe(df)