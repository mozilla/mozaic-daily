#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Add synthetic Iran DAU values to a no-Iran forecast output via summation.

Takes a no-Iran forecast parquet (produced by the world-without-iran pipeline)
and a synthetic Iran parquet (produced by generate_iran_synthetic.py), and
creates a new "plus-Iran" forecast where:

    ALL_new(date) = ALL_old(date) + Iran(date)

This is deliberately simple: we treat the two forecasts as independent and
sum them. forecast(world-iran) + forecast(iran) is a defensible approximation
that's much easier to explain than splicing Iran data into training sets.

The output keeps only ALL-level aggregation rows (segment={"os":"ALL"} for
desktop, app_name="ALL MOBILE" for mobile) and adds country="IR" rows to
show Iran's standalone contribution.

Usage:
    python scripts/add_iran_to_forecast.py --input forecast.parquet
    python scripts/add_iran_to_forecast.py --input forecast.parquet --output out.parquet
"""

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

METRIC_COL = "dau"

SUPPORTED_DATA_SOURCES = {
    "legacy_desktop": {
        "all_filter_col": "segment",
        "all_filter_val": '{"os": "ALL"}',
        "ir_template": {"app_name": "desktop", "segment": '{"os": "ALL"}'},
    },
    "glean_mobile": {
        "all_filter_col": "app_name",
        "all_filter_val": "ALL MOBILE",
        "ir_template": {"app_name": "ALL MOBILE", "segment": "{}"},
    },
}

DEFAULT_SYNTHETIC_PATH = Path(__file__).parent.parent / "data" / "iran_synthetic" / "iran_synthetic.parquet"


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_inputs(input_path: str, synthetic_path: str) -> tuple:
    """Load and validate both input parquet files.

    Returns:
        (forecast_df, synthetic_df) tuple.

    Raises:
        FileNotFoundError: If either file doesn't exist.
        SystemExit: If no supported data_sources are found in the input.
    """
    forecast_df = pd.read_parquet(input_path)
    print(f"Loaded forecast: {len(forecast_df)} rows, "
          f"data_sources: {sorted(forecast_df['data_source'].unique())}")

    synthetic_df = pd.read_parquet(synthetic_path)
    print(f"Loaded synthetic: {len(synthetic_df)} rows, "
          f"data_sources: {sorted(synthetic_df['data_source'].unique())}")

    # Check that at least one supported data_source exists in both files
    forecast_sources = set(forecast_df["data_source"].unique())
    synthetic_sources = set(synthetic_df["data_source"].unique())
    supported = set(SUPPORTED_DATA_SOURCES.keys())

    usable = forecast_sources & synthetic_sources & supported
    if not usable:
        print(f"ERROR: No supported data_source found in both files.", file=sys.stderr)
        print(f"  Forecast has: {forecast_sources}", file=sys.stderr)
        print(f"  Synthetic has: {synthetic_sources}", file=sys.stderr)
        print(f"  Supported: {supported}", file=sys.stderr)
        sys.exit(1)

    # Check that dau column exists
    if METRIC_COL not in forecast_df.columns:
        print(f"ERROR: Forecast file missing '{METRIC_COL}' column.", file=sys.stderr)
        sys.exit(1)

    # Check that DAU metric exists in synthetic data
    if "DAU" not in synthetic_df["metric"].unique():
        print(f"ERROR: Synthetic file missing DAU metric.", file=sys.stderr)
        sys.exit(1)

    return forecast_df, synthetic_df


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def get_iran_dau_by_date(synthetic_df: pd.DataFrame, data_source: str) -> pd.DataFrame:
    """Extract Iran DAU values indexed by target_date for one data_source.

    Args:
        synthetic_df: Synthetic Iran parquet with columns:
            data_source, target_date, metric, iran_value.
        data_source: Data source string (e.g., "legacy_desktop").

    Returns:
        DataFrame with columns: target_date, iran_dau.
    """
    mask = (synthetic_df["data_source"] == data_source) & (synthetic_df["metric"] == "DAU")
    iran = synthetic_df[mask][["target_date", "iran_value"]].copy()
    iran = iran.rename(columns={"iran_value": "iran_dau"})
    return iran


def filter_to_all_level(
    forecast_df: pd.DataFrame,
    data_source: str,
    config: dict,
) -> pd.DataFrame:
    """Filter forecast to ALL-level rows for one data_source.

    Keeps all countries but only the aggregate segment
    (segment={"os":"ALL"} for desktop, app_name="ALL MOBILE" for mobile).

    Args:
        forecast_df: Full forecast DataFrame.
        data_source: Data source string.
        config: Entry from SUPPORTED_DATA_SOURCES.

    Returns:
        Filtered DataFrame (copy).
    """
    filter_col = config["all_filter_col"]
    filter_val = config["all_filter_val"]

    mask = (forecast_df["data_source"] == data_source) & (forecast_df[filter_col] == filter_val)
    return forecast_df[mask].copy()


def add_iran_to_all_country(
    all_level_df: pd.DataFrame,
    iran_dau: pd.DataFrame,
) -> pd.DataFrame:
    """Add Iran DAU to country='ALL' rows where dates match.

    Non-ALL country rows pass through unchanged. For country='ALL' rows,
    the dau column is increased by Iran's value for dates where synthetic
    data exists. Dates without Iran data are left as-is.

    Args:
        all_level_df: ALL-level forecast rows (all countries).
        iran_dau: DataFrame with target_date and iran_dau columns.

    Returns:
        Modified DataFrame with Iran added to country='ALL' dau values.
    """
    df = all_level_df.copy()
    is_all_country = df["country"] == "ALL"

    # Merge Iran values onto country='ALL' rows
    all_rows = df[is_all_country].merge(iran_dau, on="target_date", how="left")
    all_rows["iran_dau"] = all_rows["iran_dau"].fillna(0)
    all_rows[METRIC_COL] = all_rows[METRIC_COL] + all_rows["iran_dau"]
    all_rows = all_rows.drop(columns=["iran_dau"])

    # Recombine: modified ALL rows + unchanged non-ALL rows
    non_all_rows = df[~is_all_country]
    return pd.concat([all_rows, non_all_rows], ignore_index=True)


def create_ir_rows(
    all_level_df: pd.DataFrame,
    iran_dau: pd.DataFrame,
    config: dict,
    forecast_start_date: pd.Timestamp,
) -> pd.DataFrame:
    """Create country='IR' rows showing Iran's standalone contribution.

    Args:
        all_level_df: ALL-level forecast rows (used to copy metadata).
        iran_dau: DataFrame with target_date and iran_dau columns.
        config: Entry from SUPPORTED_DATA_SOURCES with ir_template.
        forecast_start_date: The forecast_start_date from the input file,
            used to determine data_type (training vs forecast).

    Returns:
        New DataFrame with country='IR' rows.
    """
    # Get metadata from an existing row (any row will do — they all share these values)
    sample_row = all_level_df.iloc[0]
    forecast_run_timestamp = sample_row["forecast_run_timestamp"]
    mozaic_hash = sample_row["mozaic_hash"]
    data_source = sample_row["data_source"]

    forecast_start_str = forecast_start_date.strftime("%Y-%m-%d")

    ir_rows = iran_dau.copy()
    ir_rows["forecast_start_date"] = forecast_start_date
    ir_rows["forecast_run_timestamp"] = forecast_run_timestamp
    ir_rows["mozaic_hash"] = mozaic_hash
    ir_rows["data_source"] = data_source
    ir_rows["country"] = pd.array(["IR"] * len(ir_rows), dtype="string")
    ir_rows["data_type"] = np.where(
        ir_rows["target_date"] < forecast_start_str, "training", "forecast"
    )

    # Apply template (app_name, segment)
    for col, val in config["ir_template"].items():
        ir_rows[col] = val

    # Rename iran_dau to the metric column
    ir_rows = ir_rows.rename(columns={"iran_dau": METRIC_COL})

    # Add any other metric columns as NaN (the input may have only dau, but be safe)
    for col in all_level_df.columns:
        if col not in ir_rows.columns:
            ir_rows[col] = np.nan

    # Cast string columns to match input dtypes
    string_cols = ["forecast_run_timestamp", "mozaic_hash", "data_source",
                   "target_date", "data_type", "country", "app_name", "segment"]
    for col in string_cols:
        if col in ir_rows.columns:
            ir_rows[col] = ir_rows[col].astype("string")

    return ir_rows


# ---------------------------------------------------------------------------
# Spot checks
# ---------------------------------------------------------------------------

def print_spot_check(
    data_source: str,
    original_all_df: pd.DataFrame,
    modified_all_df: pd.DataFrame,
    iran_dau: pd.DataFrame,
) -> None:
    """Print spot-check comparison for a few sample dates."""
    # Get country='ALL' rows from original and modified
    orig = original_all_df[original_all_df["country"] == "ALL"].set_index("target_date")[METRIC_COL]
    mod = modified_all_df[modified_all_df["country"] == "ALL"].set_index("target_date")[METRIC_COL]
    iran = iran_dau.set_index("target_date")["iran_dau"]

    # Pick sample dates: first with Iran data, a few middle ones, last
    iran_dates = sorted(iran.index)
    if not iran_dates:
        print(f"\n  No Iran dates to spot-check for {data_source}")
        return

    sample_dates = []
    # First date before Iran data (if exists)
    all_dates = sorted(orig.index)
    pre_iran = [d for d in all_dates if d < iran_dates[0]]
    if pre_iran:
        sample_dates.append(pre_iran[-1])
    # First, middle, last Iran dates
    sample_dates.append(iran_dates[0])
    if len(iran_dates) > 2:
        sample_dates.append(iran_dates[len(iran_dates) // 2])
    sample_dates.append(iran_dates[-1])

    print(f"\n=== {data_source} DAU spot check ===")
    print(f"{'target_date':<14} {'old_all':>14} {'iran':>14} {'new_all':>14}")
    print("-" * 58)
    for date in sample_dates:
        old_val = orig.get(date, float("nan"))
        iran_val = iran.get(date, None)
        new_val = mod.get(date, float("nan"))
        iran_str = f"{iran_val:>14,.0f}" if iran_val is not None else "  (no IR data)"
        print(f"{date:<14} {old_val:>14,.0f} {iran_str} {new_val:>14,.0f}")


# ---------------------------------------------------------------------------
# CLI and main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Add synthetic Iran DAU values to a no-Iran forecast output.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input", required=True,
        help="Path to the no-Iran forecast parquet file",
    )
    parser.add_argument(
        "--output", default=None,
        help="Output parquet path (default: {input_stem}.plus_iran.parquet)",
    )
    parser.add_argument(
        "--synthetic", default=str(DEFAULT_SYNTHETIC_PATH),
        help=f"Path to synthetic Iran parquet (default: {DEFAULT_SYNTHETIC_PATH})",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    start_time = time.time()

    # Resolve output path
    input_path = Path(args.input)
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.with_suffix(".plus_iran.parquet")

    print(f"Adding Iran to forecast")
    print(f"  Input:     {input_path}")
    print(f"  Synthetic: {args.synthetic}")
    print(f"  Output:    {output_path}")

    # Load
    forecast_df, synthetic_df = load_inputs(str(input_path), args.synthetic)
    forecast_start_date = forecast_df["forecast_start_date"].iloc[0]

    # Process each supported data_source found in the input
    forecast_sources = set(forecast_df["data_source"].unique())
    result_frames = []

    for data_source, config in SUPPORTED_DATA_SOURCES.items():
        if data_source not in forecast_sources:
            continue
        if data_source not in set(synthetic_df["data_source"].unique()):
            print(f"\nWARNING: {data_source} in forecast but not in synthetic data, skipping")
            continue

        print(f"\n--- Processing {data_source} ---")

        # Get Iran DAU values
        iran_dau = get_iran_dau_by_date(synthetic_df, data_source)
        print(f"  Iran dates: {iran_dau['target_date'].min()} to {iran_dau['target_date'].max()} "
              f"({len(iran_dau)} dates)")

        # Filter to ALL-level rows
        all_level = filter_to_all_level(forecast_df, data_source, config)
        print(f"  ALL-level rows: {len(all_level)} "
              f"(countries: {sorted(all_level['country'].unique())})")

        # Save original for spot-check
        original_all_level = all_level.copy()

        # Add Iran to country='ALL' rows
        modified = add_iran_to_all_country(all_level, iran_dau)

        # Create country='IR' rows
        ir_rows = create_ir_rows(all_level, iran_dau, config, forecast_start_date)
        print(f"  Created {len(ir_rows)} IR rows")

        # Spot check
        print_spot_check(data_source, original_all_level, modified, iran_dau)

        result_frames.append(modified)
        result_frames.append(ir_rows)

    if not result_frames:
        print("ERROR: No data_sources could be processed.", file=sys.stderr)
        sys.exit(1)

    # Combine and sort
    result = pd.concat(result_frames, ignore_index=True)

    # Ensure column order and dtypes match input
    col_order = [c for c in forecast_df.columns if c in result.columns]
    result = result[col_order]
    for col in col_order:
        if result[col].dtype != forecast_df[col].dtype:
            result[col] = result[col].astype(forecast_df[col].dtype)
    result = result.sort_values(
        ["data_source", "country", "target_date"]
    ).reset_index(drop=True)

    # Write output
    result.to_parquet(str(output_path), index=False)

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Output: {output_path}")
    print(f"  Rows: {len(result)}")
    print(f"  Countries: {sorted(result['country'].unique())}")
    print(f"  Data sources: {sorted(result['data_source'].unique())}")
    print(f"  Date range: {result['target_date'].min()} to {result['target_date'].max()}")
    print(f"  Elapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
