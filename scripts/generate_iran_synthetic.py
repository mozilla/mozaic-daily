#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Generate ALL-level synthetic Iran data (historical + forecast) for the plus-Iran workflow.

Iran's internet has been shut down since approximately 2026-02-28. This script
generates a complete Iran-only forecast using the mozaic pipeline and saves the
ALL-level totals (summed across segments) as a parquet file. The output includes
both historical training data (from BigQuery) and forecast data (from mozaic),
giving a complete Iran time series.

The downstream script ``add_iran_to_forecast.py`` reads this parquet and adds
Iran's values to a no-Iran forecast via simple summation:

    world_total = forecast(world_minus_iran) + forecast(iran)

Usage:
    python scripts/generate_iran_synthetic.py [--project PROJECT] [--output-dir DIR]
    python scripts/generate_iran_synthetic.py --data-sources legacy_desktop glean_mobile --metrics DAU

Output:
    <output-dir>/iran_synthetic.parquet  (default: data/iran_synthetic/)
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from google.cloud import bigquery

# Add src/ to path (same pattern as scripts/run_main.py)
repo_root = Path(__file__).parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from mozaic_daily.config import STATIC_CONFIG
from mozaic_daily.forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs
from mozaic_daily.queries import (
    ADDITIONAL_HOLIDAYS,
    DataSource,
    Metric,
    Platform,
    QUERY_SPECS,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FORECAST_START = "2026-02-27"
FORECAST_END = "2027-12-31"
DEFAULT_OUTPUT_DIR = os.path.join(repo_root, "data", "iran_synthetic")
OUTPUT_FILENAME = "iran_synthetic.parquet"

ALL_DATA_SOURCES = {DataSource.GLEAN_DESKTOP, DataSource.LEGACY_DESKTOP, DataSource.GLEAN_MOBILE}
ALL_METRICS = {m for m in Metric}

FORECAST_FN_BY_PLATFORM = {
    Platform.DESKTOP: get_desktop_forecast_dfs,
    Platform.MOBILE: get_mobile_forecast_dfs,
}


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def extract_iran_all_totals(
    forecast_df: pd.DataFrame,
    data_source_name: str,
    metric_name: str,
) -> pd.DataFrame:
    """Extract ALL-level Iran totals from mozaic forecast output.

    Mozaic's ``curate_mozaics()`` computes an ``ALL`` population that sums
    across individual segment populations. This function selects those
    pre-aggregated rows for country='IR', giving one value per date.

    Both training (actual) and forecast rows are kept, providing a complete
    time series from the earliest training date through the forecast horizon.

    Args:
        forecast_df: DataFrame from ``Mozaic.to_granular_forecast_df()`` with
            columns: target_date, country, population, source, value.
        data_source_name: Output data_source string (e.g., "legacy_desktop").
        metric_name: Metric name string (e.g., "DAU").

    Returns:
        DataFrame with columns: data_source, target_date, metric, value.
    """
    df = forecast_df[
        (forecast_df["country"] == "IR") & (forecast_df["population"] == "ALL")
    ].copy()

    if df.empty:
        print(f"  WARNING: No IR/ALL rows for {data_source_name} {metric_name}")
        return pd.DataFrame(columns=["data_source", "target_date", "metric", "value"])

    df["data_source"] = data_source_name
    df["metric"] = metric_name
    df["target_date"] = pd.to_datetime(df["target_date"]).dt.strftime("%Y-%m-%d")
    df = df.rename(columns={"value": "iran_value"})

    return df[["data_source", "target_date", "metric", "iran_value"]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# BQ queries
# ---------------------------------------------------------------------------

def query_iran_data(
    project: str,
    data_sources: set,
    metrics: set,
) -> Dict[str, Dict[str, Dict[str, pd.DataFrame]]]:
    """Query BigQuery for Iran-only historical data.

    Args:
        project: BigQuery project ID.
        data_sources: Set of DataSource enums to query.
        metrics: Set of Metric enums to query.

    Returns:
        Nested dict ``{platform: {source: {metric: DataFrame}}}``.
    """
    datasets: Dict[str, Dict[str, Dict[str, pd.DataFrame]]] = {
        "desktop": {"glean": {}, "legacy": {}},
        "mobile": {"glean": {}},
    }

    specs_to_run = [
        spec for spec in QUERY_SPECS.values()
        if spec.data_source in data_sources and spec.metric in metrics
    ]

    for i, spec in enumerate(specs_to_run, 1):
        platform = spec.platform.value
        source = spec.telemetry_source.value
        metric = spec.metric.value

        print(f"[{i}/{len(specs_to_run)}] Querying {spec.data_source.display_name} {metric}")
        start = time.time()
        sql = spec.build_query("'IR'")
        # build_query() excludes Iran (AND country != 'IR') for the main pipeline.
        # Strip it here since this script specifically needs Iran data.
        sql = sql.replace("AND country != 'IR'", "")
        client = bigquery.Client(project)
        df = client.query(sql).to_dataframe()
        elapsed = time.time() - start
        print(f"  -> {len(df)} rows in {elapsed:.1f}s")

        if df.empty:
            print(f"  WARNING: No data returned for {spec.data_source.display_name} {metric}")

        datasets[platform][source][metric] = df

    return datasets


# ---------------------------------------------------------------------------
# Forecasting
# ---------------------------------------------------------------------------

def generate_forecasts(
    datasets: Dict[str, Dict[str, Dict[str, pd.DataFrame]]],
    data_sources: set,
) -> Dict[DataSource, Dict[str, pd.DataFrame]]:
    """Run mozaic forecasts for each data source.

    Returns:
        Dict mapping ``DataSource`` to forecast result dfs ``{metric: DataFrame}``.
    """
    forecasts: Dict[DataSource, Dict[str, pd.DataFrame]] = {}

    sources_to_process = [
        (ds, datasets[ds.platform.value][ds.telemetry_source.value])
        for ds in data_sources
    ]

    for i, (data_source, metric_data) in enumerate(sources_to_process, 1):
        print(f"\n{'='*60}")
        print(f"[{i}/{len(sources_to_process)}] Forecasting {data_source.display_name}")
        print(f"{'='*60}")

        if not metric_data:
            print(f"  Skipping — no data available")
            continue

        forecast_fn = FORECAST_FN_BY_PLATFORM[data_source.platform]
        additional_holidays = ADDITIONAL_HOLIDAYS.get(data_source, [])

        result = forecast_fn(
            metric_data,
            FORECAST_START,
            FORECAST_END,
            additional_holidays=additional_holidays,
        )
        forecasts[data_source] = result.dfs

    return forecasts


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def build_output_dataframe(
    forecasts: Dict[DataSource, Dict[str, pd.DataFrame]],
) -> pd.DataFrame:
    """Extract ALL-level Iran totals from all forecast outputs and combine.

    Returns:
        Combined DataFrame with columns: data_source, target_date, metric, iran_value.
    """
    all_dfs: List[pd.DataFrame] = []

    for data_source, metric_dfs in forecasts.items():
        for metric, forecast_df in metric_dfs.items():
            extracted = extract_iran_all_totals(
                forecast_df, data_source.value, metric
            )
            if not extracted.empty:
                all_dfs.append(extracted)
                print(f"  {data_source.display_name} {metric}: {len(extracted)} rows")

    if not all_dfs:
        raise ValueError("No forecast data produced — check BQ queries and mozaic output")

    return pd.concat(all_dfs, ignore_index=True)


def print_summary(df: pd.DataFrame) -> None:
    """Print a summary of the generated synthetic data."""
    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")
    print(f"Total rows: {len(df)}")
    print(f"Date range: {df['target_date'].min()} to {df['target_date'].max()}")
    print(f"\nData source / metric combinations:")
    combos = df.groupby(["data_source", "metric"]).agg(
        rows=("iran_value", "size"),
        mean_value=("iran_value", "mean"),
        min_date=("target_date", "min"),
        max_date=("target_date", "max"),
    )
    for (ds, met), row in combos.iterrows():
        print(f"  {ds} / {met}: {row['rows']} rows, "
              f"mean={row['mean_value']:,.0f}, "
              f"dates {row['min_date']} to {row['max_date']}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate ALL-level synthetic Iran data (historical + forecast).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--project",
        default=STATIC_CONFIG["default_project"],
        help=f"BigQuery project ID (default: {STATIC_CONFIG['default_project']})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for output parquet file (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--data-sources",
        nargs="+",
        choices=[ds.value for ds in ALL_DATA_SOURCES],
        default=None,
        help="Filter to specific data sources (default: all)",
    )
    parser.add_argument(
        "--metrics",
        nargs="+",
        choices=[m.value for m in ALL_METRICS],
        default=None,
        help="Filter to specific metrics (default: all)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Resolve filters
    if args.data_sources:
        data_sources = {DataSource(ds) for ds in args.data_sources}
    else:
        data_sources = ALL_DATA_SOURCES

    if args.metrics:
        metrics = {Metric(m) for m in args.metrics}
    else:
        metrics = ALL_METRICS

    output_path = os.path.join(args.output_dir, OUTPUT_FILENAME)

    print(f"Generating ALL-level synthetic Iran data")
    print(f"  Forecast period: {FORECAST_START} to {FORECAST_END}")
    print(f"  Data sources: {', '.join(ds.value for ds in data_sources)}")
    print(f"  Metrics: {', '.join(m.value for m in metrics)}")
    print(f"  Output: {output_path}")
    print(f"  BQ project: {args.project}")

    overall_start = time.time()

    # Step 1: Query BigQuery
    print(f"\n{'='*60}")
    print("Step 1: Querying BigQuery for Iran data")
    print(f"{'='*60}")
    datasets = query_iran_data(args.project, data_sources, metrics)

    # Step 2: Run mozaic forecasts
    print(f"\n{'='*60}")
    print("Step 2: Running mozaic forecasts")
    print(f"{'='*60}")
    forecasts = generate_forecasts(datasets, data_sources)

    # Step 3: Extract ALL-level totals
    print(f"\n{'='*60}")
    print("Step 3: Extracting ALL-level Iran totals")
    print(f"{'='*60}")
    combined = build_output_dataframe(forecasts)

    # Step 4: Save
    os.makedirs(args.output_dir, exist_ok=True)
    combined.to_parquet(output_path, index=False)
    print(f"\nSaved to {output_path}")

    elapsed = time.time() - overall_start
    print(f"Total elapsed: {elapsed / 60:.1f} minutes")

    print_summary(combined)


if __name__ == "__main__":
    main()
