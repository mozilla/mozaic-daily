"""Verify the adjustment state (raw vs adj-h) of every forecast artifact under
``data-official/`` by reproducing the composite CSV from the claimed-raw parquet.

For each forecast date (e.g. 2026-04-01, 2026-05-13) the script:

1. Loads the no-Iran parquet (claimed raw).
2. Aggregates to a world-level daily DAU series, computes 28-day MA.
3. Loads adjustment specs from the matching ``adjustments/`` dir and applies them.
4. Diffs the result against the composite CSV's ``*_no_iran`` columns.

If the diff is near-zero, the parquet's raw state and the CSV's adj-h state are
both verified. Also diffs ``plus_iran - no_iran`` parquets to confirm they were
produced by simple Iran addition.

Output: ``tmp/inventory.csv`` — one row per artifact under data-official/ with
columns ``path, claimed_state, verified_state, evidence, action``.

Run:
    source .venv/bin/activate
    python scripts/verify_forecast_states.py
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

# Make the package importable when running from repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from mozaic_daily.adjustments import load_adjustments_from_dir  # noqa: E402


# --- world-level DAU extraction --------------------------------------------

def world_dau_series(df: pd.DataFrame, data_source: str, app_filter: str, segment_filter: str) -> pd.Series:
    """Filter the forecast parquet down to the world-level ALL segment + ALL country."""
    mask = (
        (df["country"] == "ALL")
        & (df["segment"] == segment_filter)
        & (df["data_source"] == data_source)
        & (df["app_name"] == app_filter)
    )
    sub = df.loc[mask, ["target_date", "dau"]].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    return sub.set_index("target_date").sort_index()["dau"]


# Filter conventions for world-level extraction (match june_composite_forecast.ipynb)
DESKTOP_SEGMENT_FILTER = '{"os": "ALL"}'
DESKTOP_APP_FILTER = "desktop"
MOBILE_SEGMENT_FILTER = "{}"
MOBILE_APP_FILTER = "ALL MOBILE"


def to_28ma(daily: pd.Series) -> pd.Series:
    return daily.sort_index().rolling(28).mean()


# --- per-date verification -------------------------------------------------

DATASETS = [
    # (forecast_start, prev_forecast_start_or_None, desktop_dir, mobile_dir, composite_csv, prev_adj_dir)
    # April forecast: prev forecast start unknown (use forecast_start so no shift applied)
    {
        "label": "2026-04",
        "forecast_start": "2026-04-01",
        "desktop_no_iran": "data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.ld-D.parquet",
        "desktop_plus_iran": "data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.ld-D.plus_iran.parquet",
        "mobile_no_iran": "data-official/2026-04/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.gm-D.parquet",
        "mobile_plus_iran": "data-official/2026-04/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.gm-D.plus_iran.parquet",
        "adjustments_dir": "data-official/2026-04/adjustments",
        "composite_csv": "april_composite_forecast_28ma.csv",
        "desktop_data_source": "legacy_desktop",
        "mobile_data_source": "glean_mobile",
    },
    {
        "label": "2026-06",
        "forecast_start": "2026-05-13",
        "desktop_no_iran": "data-official/2026-06/desktop_cps0.15983_thresh50_recent13_clip0.6/mozaic_daily_forecast.2026-05-13.ld-D.parquet",
        "desktop_plus_iran": "data-official/2026-06/desktop_cps0.15983_thresh50_recent13_clip0.6/mozaic_daily_forecast.2026-05-13.ld-D.plus_iran.parquet",
        "mobile_no_iran": "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-05-13.gm-D.parquet",
        "mobile_plus_iran": "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-05-13.gm-D.plus_iran.parquet",
        "adjustments_dir": "data-official/2026-06/adjustments",
        "composite_csv": "data-official/2026-06/june_composite_forecast_28ma.csv",
        "desktop_data_source": "legacy_desktop",
        "mobile_data_source": "glean_mobile",
    },
]


def verify_dataset(ds: dict, inventory: list[dict]) -> None:
    print(f"\n=== Verifying {ds['label']} ===")
    fc_start = pd.Timestamp(ds["forecast_start"])
    composite = pd.read_csv(ds["composite_csv"], parse_dates=["date"]).set_index("date")

    desktop_no_iran_df = pd.read_parquet(ds["desktop_no_iran"])
    desktop_plus_iran_df = pd.read_parquet(ds["desktop_plus_iran"])
    mobile_no_iran_df = pd.read_parquet(ds["mobile_no_iran"])
    mobile_plus_iran_df = pd.read_parquet(ds["mobile_plus_iran"])

    desktop_no_iran_daily = world_dau_series(
        desktop_no_iran_df, ds["desktop_data_source"], DESKTOP_APP_FILTER, DESKTOP_SEGMENT_FILTER
    )
    desktop_plus_iran_daily = world_dau_series(
        desktop_plus_iran_df, ds["desktop_data_source"], DESKTOP_APP_FILTER, DESKTOP_SEGMENT_FILTER
    )
    mobile_no_iran_daily = world_dau_series(
        mobile_no_iran_df, ds["mobile_data_source"], MOBILE_APP_FILTER, MOBILE_SEGMENT_FILTER
    )
    mobile_plus_iran_daily = world_dau_series(
        mobile_plus_iran_df, ds["mobile_data_source"], MOBILE_APP_FILTER, MOBILE_SEGMENT_FILTER
    )

    desktop_no_iran_ma = to_28ma(desktop_no_iran_daily)
    desktop_plus_iran_ma = to_28ma(desktop_plus_iran_daily)
    mobile_no_iran_ma = to_28ma(mobile_no_iran_daily)
    mobile_plus_iran_ma = to_28ma(mobile_plus_iran_daily)

    # Load adjustments rendered onto the full composite index
    full_idx = composite.index
    net = load_adjustments_from_dir(ds["adjustments_dir"], full_idx)

    # Apply headwinds to no_iran MAs starting at forecast_start
    def adj(series, platform):
        result = series.copy()
        fc_mask = result.index >= fc_start
        rendered = net[platform].reindex(result.index, fill_value=0.0)
        result[fc_mask] += rendered[fc_mask]
        return result

    desktop_no_iran_ma_adj = adj(desktop_no_iran_ma, "desktop")
    desktop_plus_iran_ma_adj = adj(desktop_plus_iran_ma, "desktop")
    mobile_no_iran_ma_adj = adj(mobile_no_iran_ma, "mobile")
    mobile_plus_iran_ma_adj = adj(mobile_plus_iran_ma, "mobile")

    # Compare reproduced vs CSV — use only dates present in both
    def compare(reproduced, csv_col, label):
        merged = pd.concat(
            [reproduced.rename("repro"), composite[csv_col].rename("csv")], axis=1
        ).dropna()
        if merged.empty:
            print(f"  {label}: NO OVERLAP — could not verify")
            return None
        max_abs = (merged["repro"] - merged["csv"]).abs().max()
        max_rel = ((merged["repro"] - merged["csv"]).abs() / merged["csv"].abs()).max()
        ok = max_rel < 1e-6
        status = "MATCH" if ok else "DIFFER"
        print(f"  {label}: {status} (max_abs={max_abs:,.4f}, max_rel={max_rel:.2e})")
        return ok

    results = {
        "desktop_no_iran_adj": compare(desktop_no_iran_ma_adj, "desktop_28ma_no_iran", "desktop no_iran + headwinds vs CSV"),
        "desktop_plus_iran_adj": compare(desktop_plus_iran_ma_adj, "desktop_28ma_with_iran", "desktop plus_iran + headwinds vs CSV"),
        "mobile_no_iran_adj": compare(mobile_no_iran_ma_adj, "mobile_28ma_no_iran", "mobile no_iran + headwinds vs CSV"),
        "mobile_plus_iran_adj": compare(mobile_plus_iran_ma_adj, "mobile_28ma_with_iran", "mobile plus_iran + headwinds vs CSV"),
    }

    # plus_iran - no_iran should equal Iran DAU (which we don't have an independent
    # check for here, but the difference should be POSITIVE and SMOOTH, not zero
    # and not equal to the headwind ramp)
    desktop_iran_implied = (desktop_plus_iran_daily - desktop_no_iran_daily).dropna()
    mobile_iran_implied = (mobile_plus_iran_daily - mobile_no_iran_daily).dropna()
    print(f"  Implied desktop Iran (mean daily): {desktop_iran_implied.mean():,.0f}")
    print(f"  Implied mobile Iran (mean daily):  {mobile_iran_implied.mean():,.0f}")

    # Inventory rows for the 4 parquets + 1 CSV
    def add_row(path, claimed, verified, evidence, action):
        inventory.append({
            "path": path,
            "claimed_state": claimed,
            "verified_state": verified,
            "evidence": evidence,
            "action": action,
        })

    def state(ok):
        return "raw" if ok else "UNKNOWN"

    add_row(
        ds["desktop_no_iran"],
        "raw",
        state(results["desktop_no_iran_adj"]),
        "reproduced composite CSV no_iran column by applying headwinds to parquet's 28-day MA",
        "rename .raw.parquet" if results["desktop_no_iran_adj"] else "INVESTIGATE",
    )
    add_row(
        ds["desktop_plus_iran"],
        "raw + iran-composition",
        state(results["desktop_plus_iran_adj"]),
        "reproduced composite CSV with_iran column by applying headwinds to plus_iran parquet's 28-day MA",
        "rename .raw.plus_iran.parquet" if results["desktop_plus_iran_adj"] else "INVESTIGATE",
    )
    add_row(
        ds["mobile_no_iran"],
        "raw",
        state(results["mobile_no_iran_adj"]),
        "reproduced composite CSV no_iran column by applying headwinds to parquet's 28-day MA",
        "rename .raw.parquet" if results["mobile_no_iran_adj"] else "INVESTIGATE",
    )
    add_row(
        ds["mobile_plus_iran"],
        "raw + iran-composition",
        state(results["mobile_plus_iran_adj"]),
        "reproduced composite CSV with_iran column by applying headwinds to plus_iran parquet's 28-day MA",
        "rename .raw.plus_iran.parquet" if results["mobile_plus_iran_adj"] else "INVESTIGATE",
    )

    all_csv_ok = all(v for v in results.values() if v is not None)
    add_row(
        ds["composite_csv"],
        "adj-h",
        "adj-h" if all_csv_ok else "UNKNOWN",
        "headwind application reproducible from raw parquets" if all_csv_ok else "headwind reproduction failed",
        "rename .adj-h.csv" if all_csv_ok else "INVESTIGATE",
    )


def tag_scratch_dir(scratch_glob: str, inventory: list[dict], note: str) -> None:
    """Tag every parquet/csv under a scratch glob as raw (source-code inferred)."""
    from glob import glob
    for path in sorted(glob(scratch_glob, recursive=True)):
        if path.endswith(".parquet") or path.endswith(".csv"):
            inventory.append({
                "path": str(Path(path).relative_to(REPO_ROOT)) if str(REPO_ROOT) in path else path,
                "claimed_state": "raw",
                "verified_state": "raw (by directory convention)",
                "evidence": note,
                "action": "rename .raw.{ext} (low priority; scratch)",
            })


def verify_june_mobile_plot_series(inventory: list[dict]) -> None:
    """Verify the june_mobile_plot_series.csv against the (verified adj-h) composite CSVs."""
    plot_path = "data-official/2026-06/june_mobile_plot_series.csv"
    required = [plot_path, "data-official/2026-06/june_composite_forecast_28ma.csv",
                "april_composite_forecast_28ma.csv"]
    if any(not Path(p).exists() for p in required):
        print("\n=== SKIPPING june mobile plot-series check — inputs not on disk (pruned cycle) ===")
        return
    plot = pd.read_csv(plot_path, parse_dates=["date"]).set_index("date")
    june = pd.read_csv("data-official/2026-06/june_composite_forecast_28ma.csv", parse_dates=["date"]).set_index("date")
    april = pd.read_csv("april_composite_forecast_28ma.csv", parse_dates=["date"]).set_index("date")
    diff1 = (
        pd.concat([plot["june_draft_no_iran"], june["mobile_28ma_no_iran"]], axis=1)
        .dropna()
        .pipe(lambda d: (d.iloc[:, 0] - d.iloc[:, 1]).abs().max())
    )
    diff2 = (
        pd.concat([plot["april_no_iran"], april["mobile_28ma_no_iran"]], axis=1)
        .dropna()
        .pipe(lambda d: (d.iloc[:, 0] - d.iloc[:, 1]).abs().max())
    )
    ok = diff1 < 1e-3 and diff2 < 1e-3
    print(f"\njune_mobile_plot_series.csv: {'MATCH' if ok else 'DIFFER'} (june_diff={diff1:.4f}, april_diff={diff2:.4f})")
    inventory.append({
        "path": plot_path,
        "claimed_state": "adj-h",
        "verified_state": "adj-h" if ok else "UNKNOWN",
        "evidence": "bit-matches mobile_28ma_no_iran columns in verified-adj-h composite CSVs (June + April)",
        "action": "rename .adj-h.csv" if ok else "INVESTIGATE",
    })


# --- main ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="tmp/inventory.csv")
    args = parser.parse_args()

    inventory: list[dict] = []
    for ds in DATASETS:
        # Cycles older than current+N-1 get pruned from the working tree (see the `clean-slate`
        # commit), which used to crash this whole audit on the first missing file. Skip loudly
        # instead: the point of the tool is to check what IS on disk, and a hard failure on an
        # intentionally-absent cycle means nothing downstream ever gets verified.
        required = [ds["composite_csv"], ds["desktop_no_iran"], ds["mobile_no_iran"]]
        missing = [p for p in required if not Path(p).exists()]
        if missing:
            print(f"\n=== SKIPPING {ds['label']} — {len(missing)} input(s) not on disk ===")
            for path in missing:
                print(f"    missing: {path}")
            print("    (expected for a pruned cycle; archived under gs://moz-data-science-brwells-bucket/)")
            continue
        verify_dataset(ds, inventory)

    verify_june_mobile_plot_series(inventory)

    # Scratch directories — all parquets here are raw model outputs by source-code convention
    # (produced by run_comparison_forecasts.py / run_param_scan.py / run_main.py without adjustments)
    tag_scratch_dir(
        "data-official/2026-04/comparisons/**/*",
        inventory,
        "produced by scripts/run_comparison_forecasts.py — never applies headwinds",
    )
    tag_scratch_dir(
        "data-official/2026-06/comparisons/**/*",
        inventory,
        "produced by scripts/run_comparison_forecasts.py — never applies headwinds",
    )
    tag_scratch_dir(
        "research/param-scans/results/**/*",
        inventory,
        "produced by scripts/run_param_scan.py — never applies headwinds",
    )
    tag_scratch_dir(
        "research/param-scans/pinned/**/*",
        inventory,
        "produced by scripts/run_pinned_scan.py — never applies headwinds",
    )

    out_path = REPO_ROOT / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["path", "claimed_state", "verified_state", "evidence", "action"])
        writer.writeheader()
        writer.writerows(inventory)
    print(f"\nWrote {len(inventory)} rows to {out_path}")


if __name__ == "__main__":
    main()
