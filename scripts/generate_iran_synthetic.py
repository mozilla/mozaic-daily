#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Generate synthetic Iran forecast data for the "World w/ fake Iran" pipeline.

Iran's internet has been shut down since approximately 2026-02-28.  This script
generates a complete Iran-only forecast using the mozaic pipeline and saves the
forecast values (with holiday effects included) as a parquet file.  The parquet
file is later used by Branch 2 (``world-with-fake-iran``) to splice synthetic
Iran data into the real training datasets.

The synthetic data deliberately includes holiday effects so that the downstream
world-level pipeline's holiday detrending will correctly identify and remove
them, producing a smooth detrended series.

Usage:
    python scripts/generate_iran_synthetic.py [--project PROJECT]

Output:
    data/iran_synthetic/iran_synthetic.parquet
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd
from google.cloud import bigquery

# Add src/ to path (same pattern as scripts/run_main.py)
repo_root = Path(__file__).parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from mozaic_daily.config import STATIC_CONFIG
from mozaic_daily.forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs
from mozaic_daily.iran_utils import (
    DESKTOP_SEGMENT_COLUMNS,
    MOBILE_SEGMENT_COLUMNS,
    population_to_segment_bools,
)
from mozaic_daily.queries import (
    ADDITIONAL_HOLIDAYS,
    DataSource,
    Platform,
    QUERY_SPECS,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FORECAST_START = "2026-02-27"
FORECAST_END = "2027-12-31"
OUTPUT_PATH = os.path.join(repo_root, "data", "iran_synthetic", "iran_synthetic.parquet")

# Mapping from DataSource to the segment columns used by that platform
SEGMENT_COLUMNS_BY_PLATFORM = {
    Platform.DESKTOP: DESKTOP_SEGMENT_COLUMNS,
    Platform.MOBILE: MOBILE_SEGMENT_COLUMNS,
}


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------

def convert_forecast_to_bq_format(
    forecast_df: pd.DataFrame,
    platform: str,
    telemetry_source: str,
    metric: str,
    segment_columns: List[str],
) -> pd.DataFrame:
    """Convert a single mozaic forecast DataFrame to BigQuery-output format.

    Filters to forecast-only rows for Iran (excluding ALL aggregates), expands
    the ``population`` column into boolean segment columns, and adds identifier
    columns for later joining.

    Args:
        forecast_df: DataFrame from ``Mozaic.to_granular_forecast_df()`` with
            columns ``target_date``, ``country``, ``population``, ``source``,
            ``value``.
        platform: Platform string (``"desktop"`` or ``"mobile"``).
        telemetry_source: Telemetry source string (``"glean"`` or ``"legacy"``).
        metric: Metric name string (e.g., ``"DAU"``).
        segment_columns: Boolean column names for this platform.

    Returns:
        DataFrame with columns: ``platform``, ``telemetry_source``, ``metric``,
        ``x``, ``country``, ``y``, and one boolean column per segment.
    """
    df = forecast_df.copy()

    # Keep only forecast rows for Iran, excluding aggregate populations
    df = df[
        (df["source"] == "forecast")
        & (df["country"] == "IR")
        & (df["population"] != "ALL")
    ]

    if df.empty:
        return pd.DataFrame()

    # Expand population into boolean segment columns
    bools = df["population"].apply(
        lambda pop: population_to_segment_bools(pop, segment_columns)
    )
    bool_df = pd.DataFrame(bools.tolist(), index=df.index)
    df = pd.concat([df, bool_df], axis=1)

    # Rename to BQ-output column names
    df = df.rename(columns={"target_date": "x", "value": "y"})

    # Drop intermediate columns
    df = df.drop(columns=["source", "population"])

    # Add identifier columns
    df["platform"] = platform
    df["telemetry_source"] = telemetry_source
    df["metric"] = metric

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def query_iran_data(project: str) -> Dict[str, Dict[str, Dict[str, pd.DataFrame]]]:
    """Query BigQuery for Iran-only historical data across all data sources.

    Returns:
        Nested dict ``{platform: {source: {metric: DataFrame}}}``.
    """
    datasets: Dict[str, Dict[str, Dict[str, pd.DataFrame]]] = {
        "desktop": {"glean": {}, "legacy": {}},
        "mobile": {"glean": {}},
    }

    client = bigquery.Client(project)
    total_specs = len(QUERY_SPECS)

    for i, spec in enumerate(QUERY_SPECS.values(), 1):
        platform = spec.platform.value
        source = spec.telemetry_source.value
        metric = spec.metric.value

        print(f"[{i}/{total_specs}] Querying {spec.data_source.display_name} {metric}")
        start = time.time()
        sql = spec.build_query("'IR'")
        df = client.query(sql).to_dataframe()
        elapsed = time.time() - start
        print(f"  -> {len(df)} rows in {elapsed:.1f}s")

        if df.empty:
            print(f"  WARNING: No data returned for {spec.data_source.display_name} {metric}")

        datasets[platform][source][metric] = df

    return datasets


def generate_forecasts(
    datasets: Dict[str, Dict[str, Dict[str, pd.DataFrame]]],
) -> Dict[DataSource, Dict[str, pd.DataFrame]]:
    """Run mozaic forecasts for each data source.

    Returns:
        Dict mapping ``DataSource`` to ``{metric: forecast_DataFrame}``.
    """
    forecasts: Dict[DataSource, Dict[str, pd.DataFrame]] = {}

    sources_to_process = [
        (DataSource.GLEAN_DESKTOP, datasets["desktop"]["glean"], get_desktop_forecast_dfs),
        (DataSource.LEGACY_DESKTOP, datasets["desktop"]["legacy"], get_desktop_forecast_dfs),
        (DataSource.GLEAN_MOBILE, datasets["mobile"]["glean"], get_mobile_forecast_dfs),
    ]

    for i, (data_source, metric_data, forecast_fn) in enumerate(sources_to_process, 1):
        print(f"\n{'='*60}")
        print(f"[{i}/{len(sources_to_process)}] Forecasting {data_source.display_name}")
        print(f"{'='*60}")

        if not metric_data:
            print(f"  Skipping — no data available")
            continue

        additional_holidays = ADDITIONAL_HOLIDAYS.get(data_source, [])
        dfs = forecast_fn(
            metric_data,
            FORECAST_START,
            FORECAST_END,
            additional_holidays=additional_holidays,
        )
        forecasts[data_source] = dfs

    return forecasts


def build_output_dataframe(
    forecasts: Dict[DataSource, Dict[str, pd.DataFrame]],
) -> pd.DataFrame:
    """Convert all forecast outputs to BQ format and combine.

    Returns:
        Combined DataFrame with all data sources, metrics, and segment columns.
    """
    all_dfs: List[pd.DataFrame] = []

    for data_source, metric_dfs in forecasts.items():
        platform = data_source.platform.value
        telemetry_source = data_source.telemetry_source.value
        segment_columns = SEGMENT_COLUMNS_BY_PLATFORM[data_source.platform]

        for metric, forecast_df in metric_dfs.items():
            converted = convert_forecast_to_bq_format(
                forecast_df, platform, telemetry_source, metric, segment_columns
            )
            if not converted.empty:
                all_dfs.append(converted)
                print(f"  {data_source.display_name} {metric}: {len(converted)} rows")

    if not all_dfs:
        raise ValueError("No forecast data produced — check BQ queries and mozaic output")

    combined = pd.concat(all_dfs, ignore_index=True)

    # Fill null booleans: desktop rows get False for mobile columns and vice versa
    all_bool_cols = DESKTOP_SEGMENT_COLUMNS + MOBILE_SEGMENT_COLUMNS
    for col in all_bool_cols:
        if col in combined.columns:
            combined[col] = combined[col].fillna(False)

    return combined


def print_summary(df: pd.DataFrame) -> None:
    """Print a summary of the generated synthetic data."""
    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")
    print(f"Total rows: {len(df)}")
    print(f"Date range: {df['x'].min()} to {df['x'].max()}")
    print(f"Country: {df['country'].unique().tolist()}")
    print(f"\nData source / metric combinations:")
    combos = df.groupby(["platform", "telemetry_source", "metric"]).size()
    for (plat, src, met), count in combos.items():
        print(f"  {plat}/{src}/{met}: {count} rows")


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic Iran forecast data for the world-with-fake-iran pipeline."
    )
    parser.add_argument(
        "--project",
        default=STATIC_CONFIG["default_project"],
        help=f"BigQuery project ID (default: {STATIC_CONFIG['default_project']})",
    )
    args = parser.parse_args()

    print(f"Generating synthetic Iran forecast data")
    print(f"  Forecast period: {FORECAST_START} to {FORECAST_END}")
    print(f"  Output: {OUTPUT_PATH}")
    print(f"  BQ project: {args.project}")

    # Step 1: Query BigQuery for Iran-only data
    print(f"\n{'='*60}")
    print("Step 1: Querying BigQuery for Iran data")
    print(f"{'='*60}")
    datasets = query_iran_data(args.project)

    # Step 2: Run mozaic forecasts
    print(f"\n{'='*60}")
    print("Step 2: Running mozaic forecasts")
    print(f"{'='*60}")
    forecasts = generate_forecasts(datasets)

    # Step 3: Convert and combine
    print(f"\n{'='*60}")
    print("Step 3: Converting to BQ format")
    print(f"{'='*60}")
    combined = build_output_dataframe(forecasts)

    # Step 4: Save
    output_dir = os.path.dirname(OUTPUT_PATH)
    os.makedirs(output_dir, exist_ok=True)
    combined.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nSaved to {OUTPUT_PATH}")

    print_summary(combined)


if __name__ == "__main__":
    main()
