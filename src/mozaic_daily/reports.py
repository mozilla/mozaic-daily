# -*- coding: utf-8 -*-
"""Debug and research reporting utilities.

This module provides functions for inspecting forecast output quality,
primarily to help diagnose issues like unexpected null values in the
forecast pipeline output.

Functions:
- print_null_report(df): Print a structured null-count breakdown by date,
  country/segment, and data source.
"""

import pandas as pd


METRIC_COLUMNS = [
    'dau',
    'new_profiles',
    'existing_engagement_dau',
    'existing_engagement_mau',
]

GROUP_BY_COLUMNS = [
    'target_date',
    'data_type',
    'country',
    'segment',
    'data_source',
]


def print_null_report(df: pd.DataFrame) -> None:
    """Print null counts broken down by date, country/segment, and data source.

    Produces four sections:
      1. Summary: total null count and percentage per metric column
      2. By date: top 20 dates with the most nulls (per metric)
      3. By country x segment: null counts per country/segment combination
      4. By data_source: null counts per data source

    Args:
        df: Output DataFrame from the forecast pipeline, after format_output_table()
    """
    available_metrics = [col for col in METRIC_COLUMNS if col in df.columns]

    if not available_metrics:
        print("No metric columns found in DataFrame. Expected:", METRIC_COLUMNS)
        return

    _print_summary_section(df, available_metrics)
    _print_by_date_section(df, available_metrics)
    _print_by_country_segment_section(df, available_metrics)
    _print_by_data_source_section(df, available_metrics)


def _print_summary_section(df: pd.DataFrame, metric_columns: list) -> None:
    """Print total null counts and percentages per metric column."""
    total_rows = len(df)
    print("\n" + "=" * 60)
    print("NULL REPORT — SUMMARY")
    print("=" * 60)
    print(f"Total rows: {total_rows:,}")
    print()

    for metric in metric_columns:
        null_count = df[metric].isna().sum()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0.0
        print(f"  {metric}: {null_count:,} nulls ({null_pct:.1f}%)")


def _print_by_date_section(df: pd.DataFrame, metric_columns: list) -> None:
    """Print top 20 dates ranked by total null count across all metrics."""
    print("\n" + "=" * 60)
    print("NULL REPORT — BY DATE (top 20)")
    print("=" * 60)

    if 'target_date' not in df.columns:
        print("  'target_date' column not found, skipping.")
        return

    null_flags = df[metric_columns].isna()
    by_date = null_flags.groupby(df['target_date']).sum()
    by_date['total_nulls'] = by_date.sum(axis=1)
    by_date = by_date.sort_values('total_nulls', ascending=False).head(20)

    if by_date['total_nulls'].sum() == 0:
        print("  No nulls found.")
        return

    print(by_date.to_string())


def _print_by_country_segment_section(df: pd.DataFrame, metric_columns: list) -> None:
    """Print null counts grouped by country and segment."""
    print("\n" + "=" * 60)
    print("NULL REPORT — BY COUNTRY x SEGMENT")
    print("=" * 60)

    group_cols = [col for col in ['country', 'segment'] if col in df.columns]
    if not group_cols:
        print("  Neither 'country' nor 'segment' columns found, skipping.")
        return

    null_flags = df[metric_columns].isna()
    groups = df[group_cols].copy()
    combined = pd.concat([groups, null_flags], axis=1)
    by_group = combined.groupby(group_cols)[metric_columns].sum()
    by_group['total_nulls'] = by_group.sum(axis=1)
    by_group = by_group[by_group['total_nulls'] > 0].sort_values('total_nulls', ascending=False)

    if by_group.empty:
        print("  No nulls found.")
        return

    print(by_group.to_string())


def _print_by_data_source_section(df: pd.DataFrame, metric_columns: list) -> None:
    """Print null counts grouped by data_source."""
    print("\n" + "=" * 60)
    print("NULL REPORT — BY DATA SOURCE")
    print("=" * 60)

    if 'data_source' not in df.columns:
        print("  'data_source' column not found, skipping.")
        return

    null_flags = df[metric_columns].isna()
    combined = pd.concat([df[['data_source']], null_flags], axis=1)
    by_source = combined.groupby('data_source')[metric_columns].sum()
    by_source['total_nulls'] = by_source.sum(axis=1)
    by_source = by_source.sort_values('total_nulls', ascending=False)

    if by_source['total_nulls'].sum() == 0:
        print("  No nulls found.")
        return

    print(by_source.to_string())
    print()
