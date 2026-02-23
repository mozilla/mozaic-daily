#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Inspect rows with null DAU values in a pipeline output parquet file or BigQuery.

Useful for investigating forecast quality issues. Reads a parquet file (or
queries BigQuery directly) and prints every row where DAU is null, untruncated.
Optionally saves null rows to a new parquet file for further analysis.

Usage:
    # Pick from files in ./debug_output interactively
    python scripts/inspect_nulls.py

    # Inspect a specific file
    python scripts/inspect_nulls.py ./debug_output/forecast_output.parquet

    # Inspect and save null rows (auto-named)
    python scripts/inspect_nulls.py ./debug_output/forecast_output.parquet --save

    # Inspect and save null rows to a specific path
    python scripts/inspect_nulls.py ./debug_output/forecast_output.parquet --save ./nulls.parquet

    # Scan a different directory for parquet files
    python scripts/inspect_nulls.py --output-dir ./my_investigation

    # Query today's forecast from BigQuery
    python scripts/inspect_nulls.py --bigquery

    # Query a specific date from BigQuery
    python scripts/inspect_nulls.py --bigquery --date 2026-02-20

    # Query BigQuery and save null rows
    python scripts/inspect_nulls.py --bigquery --date 2026-02-20 --save
"""

import sys
import argparse
import datetime
from pathlib import Path

BIGQUERY_PROJECT = 'moz-fx-data-bq-data-science'
BIGQUERY_TABLE = 'moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v2'
BIGQUERY_FALLBACK_TABLE = 'moz-fx-data-shared-prod.forecasts.mart_mozaic_daily_forecast'


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the inspect_nulls script."""
    parser = argparse.ArgumentParser(
        description='Inspect rows with null DAU values in a parquet file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        'file',
        nargs='?',
        metavar='FILE',
        help='Path to a parquet file. If omitted, scan --output-dir for parquet files.',
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./debug_output',
        help='Directory to scan for parquet files when FILE is omitted (default: ./debug_output)',
    )
    parser.add_argument(
        '--save',
        nargs='?',
        const=True,
        metavar='FILENAME',
        help=(
            'Save null rows to a parquet file. '
            'Without a value, saves to {input_stem}_dau_nulls.parquet in the same directory '
            '(file mode) or bq_nulls_{date}.parquet in the current directory (BigQuery mode). '
            'With a value, saves to that path.'
        ),
    )
    parser.add_argument(
        '--bigquery',
        action='store_true',
        help='Query BigQuery directly instead of reading a local parquet file.',
    )
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        metavar='YYYY-MM-DD',
        help=(
            'forecast_start_date to query in BigQuery mode (default: today). '
            'Only meaningful with --bigquery.'
        ),
    )

    return parser


def pick_parquet_file_interactively(output_dir: Path) -> Path:
    """List parquet files in output_dir and prompt the user to pick one.

    Returns the selected Path, or exits if the user cancels or the
    directory contains no parquet files.
    """
    if not output_dir.exists():
        print(f"Error: directory not found: {output_dir}", file=sys.stderr)
        sys.exit(1)

    parquet_files = sorted(output_dir.glob('*.parquet'))

    if not parquet_files:
        print(f"No parquet files found in {output_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Parquet files in {output_dir}:")
    for index, path in enumerate(parquet_files, start=1):
        print(f"  {index}. {path.name}")

    raw_input = input("\nEnter number (or q to quit): ").strip()

    if raw_input.lower() == 'q':
        print("Cancelled.")
        sys.exit(0)

    try:
        selection = int(raw_input)
    except ValueError:
        print(f"Error: expected a number, got '{raw_input}'", file=sys.stderr)
        sys.exit(1)

    if selection < 1 or selection > len(parquet_files):
        print(f"Error: selection {selection} out of range (1–{len(parquet_files)})", file=sys.stderr)
        sys.exit(1)

    return parquet_files[selection - 1]


def detect_dau_column(dataframe) -> str:
    """Return the name of the DAU column ('dau' or 'DAU').

    Raises SystemExit if neither column is present.
    """
    if 'dau' in dataframe.columns:
        return 'dau'
    if 'DAU' in dataframe.columns:
        return 'DAU'

    print("Error: no DAU column found (checked 'dau' and 'DAU')", file=sys.stderr)
    print(f"Columns present: {list(dataframe.columns)}", file=sys.stderr)
    sys.exit(1)


def resolve_save_path(save_arg, input_file: Path = None, date_str: str = None) -> Path:
    """Resolve the output path for --save.

    If save_arg is True (flag given with no value), use auto-naming:
      - File mode: {input_stem}_dau_nulls.parquet in the same directory as input_file
      - BigQuery mode: bq_nulls_{date_str}.parquet in the current directory
    If save_arg is a string, use it as the path directly.
    """
    if save_arg is True:
        if date_str is not None:
            return Path(f"bq_nulls_{date_str}.parquet")
        return input_file.parent / f"{input_file.stem}_dau_nulls.parquet"
    return Path(save_arg)


def query_bigquery_null_counts(client, table: str, date_str: str) -> tuple[int, int]:
    """Query BigQuery for the total row count and null DAU count for a given date.

    Returns (total_rows, null_dau_count).
    Raises google.api_core.exceptions.Forbidden or NotFound on access errors.
    """
    from google.cloud import bigquery

    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(dau IS NULL) AS null_dau
        FROM `{table}`
        WHERE forecast_start_date = @date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('date', 'DATE', date_str),
        ]
    )
    result = client.query(sql, job_config=job_config).result()
    row = next(iter(result))
    return int(row.total), int(row.null_dau)


def query_bigquery_null_rows(client, table: str, date_str: str):
    """Query BigQuery for all rows with null DAU for a given date.

    Returns a pandas DataFrame.
    """
    from google.cloud import bigquery
    import pandas as pd

    sql = f"""
        SELECT *
        FROM `{table}`
        WHERE forecast_start_date = @date
          AND dau IS NULL
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('date', 'DATE', date_str),
        ]
    )
    return client.query(sql, job_config=job_config).to_dataframe()


def run_bigquery_mode(date_str: str, save_arg) -> None:
    """Orchestrate the BigQuery null inspection workflow.

    Tries the primary table first, falls back to the public view on
    permission or not-found errors. Prints null counts and row details,
    then optionally saves null rows to parquet.
    """
    from google.cloud import bigquery
    from google.api_core import exceptions as google_exceptions

    client = bigquery.Client(project=BIGQUERY_PROJECT)

    def try_query_counts(table: str) -> tuple[int, int]:
        return query_bigquery_null_counts(client, table, date_str)

    table = BIGQUERY_TABLE
    print(f"Querying {table} for forecast_start_date = {date_str} ...")

    try:
        total_rows, null_count = try_query_counts(table)
    except (google_exceptions.Forbidden, google_exceptions.NotFound) as primary_error:
        print(f"  Could not access primary table ({primary_error}), trying fallback ...")
        table = BIGQUERY_FALLBACK_TABLE
        try:
            total_rows, null_count = try_query_counts(table)
        except (google_exceptions.Forbidden, google_exceptions.NotFound) as fallback_error:
            print(f"\nError: could not access either BigQuery table.", file=sys.stderr)
            print(f"  Primary:  {BIGQUERY_TABLE}", file=sys.stderr)
            print(f"  Fallback: {BIGQUERY_FALLBACK_TABLE}", file=sys.stderr)
            print(f"  Last error: {fallback_error}", file=sys.stderr)
            print("\nPlease check your BigQuery credentials and table access.", file=sys.stderr)
            sys.exit(1)

    print(f"\nNull dau rows: {null_count:,} of {total_rows:,} total\n")

    if total_rows == 0:
        print(f"No rows found for forecast_start_date = {date_str}.")
        print("The forecast may not have run for this date yet.")
        sys.exit(0)

    if null_count == 0:
        print("No null DAU rows found.")
        sys.exit(0)

    null_rows = query_bigquery_null_rows(client, table, date_str)
    print_null_rows(null_rows)

    if save_arg is not None:
        save_path = resolve_save_path(save_arg, date_str=date_str)
        null_rows.to_parquet(save_path)
        print(f"Saved {null_count:,} null rows to {save_path}")


def print_null_rows(null_rows) -> None:
    """Print null DAU rows untruncated to stdout."""
    import pandas as pd

    with pd.option_context(
        'display.max_columns', None,
        'display.width', None,
        'display.max_colwidth', None,
    ):
        for row_index, row in null_rows.iterrows():
            print(f"--- Row {row_index} ---")
            print(row.to_string())
            print()


if __name__ == '__main__':
    import pandas as pd

    parser = build_arg_parser()
    args = parser.parse_args()

    # Validate flag combinations
    if args.bigquery and args.file:
        print("Error: --bigquery and FILE are mutually exclusive.", file=sys.stderr)
        sys.exit(1)

    if args.date and not args.bigquery:
        print("Warning: --date has no effect without --bigquery.", file=sys.stderr)

    # BigQuery mode
    if args.bigquery:
        date_str = args.date or datetime.date.today().isoformat()
        run_bigquery_mode(date_str, args.save)
        sys.exit(0)

    # File mode: resolve the input file either from the positional argument or interactively
    if args.file:
        input_file = Path(args.file)
        if not input_file.exists():
            print(f"Error: file not found: {input_file}", file=sys.stderr)
            sys.exit(1)
    else:
        output_dir = Path(args.output_dir)
        input_file = pick_parquet_file_interactively(output_dir)

    print(f"Reading {input_file} ...")
    dataframe = pd.read_parquet(input_file)

    if dataframe.empty:
        print("DataFrame is empty — no rows to inspect.")
        sys.exit(0)

    dau_column = detect_dau_column(dataframe)
    null_rows = dataframe[dataframe[dau_column].isnull()]
    total_rows = len(dataframe)
    null_count = len(null_rows)

    print(f"\nNull {dau_column} rows: {null_count:,} of {total_rows:,} total\n")

    if null_count == 0:
        print("No null DAU rows found.")
        sys.exit(0)

    print_null_rows(null_rows)

    if args.save is not None:
        save_path = resolve_save_path(args.save, input_file=input_file)
        null_rows.to_parquet(save_path)
        print(f"Saved {null_count:,} null rows to {save_path}")
