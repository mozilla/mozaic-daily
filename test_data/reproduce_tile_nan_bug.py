"""
Reproduce: Tile.to_df() produces NaN on specific forecast dates near holidays.

BUG SUMMARY
===========
Tile.to_df(quantile=0.5) returns NaN in the 'forecast' column for specific dates
near holidays. Every tile across all countries/populations is affected equally.
The affected dates vary by metric but are consistent across tiles.

The Mozaic.to_df() (aggregate-level) does NOT have this issue -- only individual
tile-level calls via Tile.to_df() and, by extension,
Mozaic.to_granular_forecast_df().

When to_granular_forecast_df() calls _standard_df_to_forecast_df(), it drops NaN
rows (line ~372: df = df[~df['value'].isna()]). These dropped rows become nulls
downstream after an outer join across metrics.

WHERE THE NaN IS CREATED
========================
tile.py:105-107 in Tile.to_df():

    forecast_df["forecast"] = (
        self.forecast_reconciled + self.forecasted_holiday_impacts
    ).quantile(quantile, axis=1)

forecast_reconciled is a DataFrame of forecast samples (rows=dates, cols=samples).
forecasted_holiday_impacts is the same shape. If either has NaN for a date, the
sum has NaN, and .quantile() returns NaN.

MOST LIKELY ROOT CAUSE: aggregate_holiday_impacts_upward (core.py:277-300)
==========================================================================
For leaf tiles (not sub-mozaics), forecasted_holiday_impacts is computed at
core.py:290:

    tile.forecasted_holiday_impacts = getattr(tile, attr).multiply(
        tile.proportional_holiday_effects.reset_index(drop=True), axis=0
    )

This multiplies forecast_reconciled (DataFrame, rows=dates) by
proportional_holiday_effects (Series, indexed by date, then reset to positional).

proportional_holiday_effects is set in _predict_holiday_effects() at core.py:250-253:

    self.proportional_holiday_effects = (
        df.groupby("date")["average_effect"].sum()
        .reindex(self.forecast_dates, fill_value=0)
    )

The .reindex(self.forecast_dates, fill_value=0) should cover all dates. But this
is set on the country-level Mozaic, then assigned to child tiles at core.py:269:

    tile.proportional_holiday_effects = self.proportional_holiday_effects

Possible issues:
1. Index alignment: reset_index(drop=True) at line 291 converts to positional
   index. If forecast_reconciled has a different number of rows than
   proportional_holiday_effects, positional alignment breaks.
2. forecast_dates mismatch: The Mozaic's forecast_dates (used in reindex) might
   differ from a tile's forecast_reconciled date range after reconciliation.
3. NaN in forecast_reconciled itself: If reconciliation introduces NaN for
   certain dates (e.g. division by near-zero in _reconcile_top_down_by_rescaling),
   multiplying by holiday effects preserves it.

SECONDARY SUSPECT: _reconcile_top_down_by_rescaling (core.py:128-154)
=====================================================================
This rescales tiles to match the topline. If the topline has holiday impacts but
individual tiles are being rescaled before holiday effects are applied, dates where
holiday effects are large could create numerical issues (division by near-zero,
etc.).

EXPECTED NaN DATES FOR DAU (forecast_start_date=2026-02-17)
============================================================
2026-12-27, 2027-08-07, 2027-08-14, 2027-08-15, 2027-08-22

WHAT NOT TO INVESTIGATE
=======================
- The outer join in the caller (combine_tables) -- that's just where nulls become
  visible, not where they're created.
- Tile skipping / sparse data -- all tiles are affected equally.
- Prophet/Stan nondeterminism -- same dates fail every run.
- The Mozaic.to_df() path -- that works fine; only Tile.to_df() is broken.

USAGE
=====
Place legacy_desktop_dau.parquet alongside this script, then run:

    python reproduce_tile_nan_bug.py

The script will:
1. Load the test data (real Desktop Legacy DAU from BigQuery)
2. Run populate_tiles and curate_mozaics using desktop_forecast_model
3. Check every tile's to_df() output for NaN in the 'forecast' column
4. Report which dates have NaN and whether the bug reproduced

Expected runtime: ~5-6 minutes (26 countries x 4 populations = 104 Prophet fits).
"""

import sys
import time
import warnings
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

import mozaic
from mozaic import Mozaic, TileSet
from mozaic.models import desktop_forecast_model


# ---- Configuration matching the original bug report ----
FORECAST_START_DATE = "2026-02-17"
FORECAST_END_DATE = "2027-12-31"
QUANTILE = 0.5

# Expected NaN dates from the bug report (DAU metric)
EXPECTED_NAN_DATES = {
    "2026-12-27",
    "2027-08-07",
    "2027-08-14",
    "2027-08-15",
    "2027-08-22",
}


def load_data(parquet_path: Path) -> dict:
    """Load test data and return datasets dict for populate_tiles.

    The parquet file contains Desktop Legacy DAU data with columns:
        x (datetime64): date
        country (str): country code (26 countries + 'ROW')
        win10 (bool): Windows 10 segment flag
        win11 (bool): Windows 11 segment flag
        winX (bool): other Windows versions segment flag
        y (int64): DAU value

    Returns:
        {"DAU": DataFrame} ready for mozaic.populate_tiles()
    """
    df = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df):,} rows from {parquet_path}")
    print(f"  Date range: {df['x'].min().date()} to {df['x'].max().date()}")
    print(f"  Countries: {sorted(df['country'].unique())}")
    return {"DAU": df}


def run_pipeline(datasets: dict):
    """Run the full mozaic pipeline: populate_tiles -> curate_mozaics.

    Returns:
        (metric_mozaics, country_mozaics, population_mozaics, tileset)
    """
    tileset = TileSet()

    print("\n--- Populate tiles ---\n")
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", category=RuntimeWarning,
            message=".*divide by zero.*|.*overflow.*|.*invalid value.*"
        )
        mozaic.populate_tiles(
            datasets, tileset, desktop_forecast_model,
            FORECAST_START_DATE, FORECAST_END_DATE,
        )

    metric_mozaics = {}
    country_mozaics = defaultdict(lambda: defaultdict(Mozaic))
    population_mozaics = defaultdict(lambda: defaultdict(Mozaic))

    print("\n--- Curate mozaics ---\n")
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", category=RuntimeWarning,
            message=".*divide by zero.*|.*overflow.*|.*invalid value.*"
        )
        mozaic.utils.curate_mozaics(
            datasets, tileset, desktop_forecast_model,
            metric_mozaics, country_mozaics, population_mozaics,
        )

    return metric_mozaics, country_mozaics, population_mozaics, tileset


def check_tiles_for_nan(metric_mozaics: dict) -> dict:
    """Check each tile's to_df() output for NaN in the 'forecast' column.

    Returns:
        Dict mapping metric -> list of (tile_name, nan_dates) tuples
    """
    results = {}
    for metric, moz in metric_mozaics.items():
        total_tiles = len(moz.tiles)
        print(f"\n--- Checking {total_tiles} tiles for metric: {metric} ---")

        tile_results = []
        clean_count = 0
        nan_date_signature = None

        for tile in moz.tiles:
            tile_df = tile.to_df(quantile=QUANTILE)

            # Only look at forecast period rows
            forecast_rows = tile_df[
                tile_df["submission_date"] >= pd.to_datetime(FORECAST_START_DATE)
            ]

            nan_mask = forecast_rows["forecast"].isna()
            nan_count = nan_mask.sum()

            if nan_count > 0:
                nan_dates = sorted(
                    forecast_rows.loc[nan_mask, "submission_date"].dt.date.tolist()
                )
                tile_results.append((tile.name, nan_dates))

                # Track whether all affected tiles share the same NaN dates
                date_key = tuple(str(d) for d in nan_dates)
                if nan_date_signature is None:
                    nan_date_signature = date_key
                elif date_key != nan_date_signature:
                    # Different tile has different NaN dates -- noteworthy
                    print(f"  NOTE: {tile.name} has DIFFERENT NaN dates: "
                          f"{[str(d) for d in nan_dates]}")
            else:
                clean_count += 1

        # Summarize instead of printing 104 identical lines
        affected_count = len(tile_results)
        if affected_count > 0:
            all_same = all(
                tuple(str(d) for d in dates) == nan_date_signature
                for _, dates in tile_results
            )
            nan_dates_str = [str(d) for d in tile_results[0][1]]

            if all_same:
                print(f"  All {affected_count}/{total_tiles} affected tiles share "
                      f"the SAME NaN dates: {nan_dates_str}")
                # Show a few example tile names
                example_tiles = [name for name, _ in tile_results[:3]]
                print(f"  Example tiles: {example_tiles}")
            else:
                print(f"  {affected_count}/{total_tiles} tiles have NaN "
                      f"(dates vary across tiles -- see NOTE lines above)")

        if clean_count > 0:
            print(f"  {clean_count}/{total_tiles} tiles are clean (no NaN)")

        results[metric] = tile_results
    return results


def check_mozaic_level(metric_mozaics: dict) -> None:
    """Verify the aggregate Mozaic.to_df() does NOT have NaN.

    This confirms the bug is tile-level only: Mozaic.to_df() computes its own
    forecast_reconciled + forecasted_holiday_impacts at the aggregate level,
    which does not have the alignment issue that individual tiles do.
    """
    print("\n--- Sanity check: Mozaic.to_df() (aggregate level) ---")
    for metric, moz in metric_mozaics.items():
        moz_df = moz.to_df(quantile=QUANTILE)
        forecast_rows = moz_df[
            moz_df["submission_date"] >= pd.to_datetime(FORECAST_START_DATE)
        ]
        nan_count = forecast_rows["forecast"].isna().sum()
        total_dates = len(forecast_rows)
        if nan_count == 0:
            print(f"  {metric}: CLEAN -- 0 NaN across {total_dates} forecast dates")
            print(f"  (This confirms the bug is tile-level only, not aggregate-level)")
        else:
            print(f"  {metric}: UNEXPECTED -- {nan_count} NaN across {total_dates} "
                  f"forecast dates (aggregate should be clean)")


def report_results(results: dict) -> bool:
    """Print summary and return True if bug was reproduced."""
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    bug_reproduced = False
    for metric, tile_results in results.items():
        affected_tiles = len(tile_results)
        if affected_tiles == 0:
            print(f"\n{metric}: No NaN found in any tile.")
            print(f"  Bug NOT reproduced for this metric.")
            continue

        bug_reproduced = True
        all_nan_dates = set()
        for tile_name, nan_dates in tile_results:
            all_nan_dates.update(str(d) for d in nan_dates)

        print(f"\n{metric}:")
        print(f"  Affected tiles: {affected_tiles}")
        print(f"  NaN dates:      {sorted(all_nan_dates)}")

        # Check against expected dates from the bug report
        if EXPECTED_NAN_DATES:
            matched = all_nan_dates & EXPECTED_NAN_DATES
            extra = all_nan_dates - EXPECTED_NAN_DATES
            missing = EXPECTED_NAN_DATES - all_nan_dates
            if matched:
                print(f"  Matched expected:     {sorted(matched)}")
            if extra:
                print(f"  Unexpected NaN dates: {sorted(extra)}")
            if missing:
                print(f"  Expected but missing: {sorted(missing)}")
            if matched == EXPECTED_NAN_DATES and not extra:
                print(f"  --> Exact match with expected NaN dates")

    print()
    if bug_reproduced:
        print("VERDICT: BUG REPRODUCED")
        print("  Tile.to_df(quantile) returns NaN on holiday-adjacent forecast dates.")
        print("  Mozaic.to_df(quantile) does NOT -- the bug is tile-level only.")
        print("  See docstring at top of this file for root cause analysis.")
    else:
        print("VERDICT: Bug NOT reproduced with this data/configuration.")
    print("=" * 60)

    return bug_reproduced


def main():
    data_path = Path(__file__).parent / "legacy_desktop_dau.parquet"
    if not data_path.exists():
        print(f"ERROR: Data file not found: {data_path}")
        print("Place legacy_desktop_dau.parquet alongside this script.")
        sys.exit(1)

    wall_start = time.time()

    datasets = load_data(data_path)

    pipeline_start = time.time()
    metric_mozaics, country_mozaics, population_mozaics, tileset = run_pipeline(datasets)
    pipeline_elapsed = time.time() - pipeline_start
    print(f"\nPipeline completed in {pipeline_elapsed:.0f}s "
          f"({pipeline_elapsed/60:.1f} min)")

    # Check tiles for NaN (the bug)
    results = check_tiles_for_nan(metric_mozaics)

    # Verify Mozaic-level is clean (confirms bug is tile-level only)
    check_mozaic_level(metric_mozaics)

    # Report
    bug_reproduced = report_results(results)

    wall_elapsed = time.time() - wall_start
    print(f"\nTotal wall time: {wall_elapsed:.0f}s ({wall_elapsed/60:.1f} min)")

    sys.exit(0 if bug_reproduced else 1)


if __name__ == "__main__":
    main()
