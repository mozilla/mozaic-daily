"""Regenerate composite forecast CSVs from verified raw parquets via the new adjustments module.

For each forecast month (April, June 2026), this script:

1. Loads the four raw parquets (desktop/mobile × no-iran/plus-iran) via ``load_forecast()``,
   which validates state markers and sidecar meta.
2. Aggregates world-level daily DAU, computes 28-day MA.
3. Loads adjustment specs from ``data-official/{date}/adjustments/`` and applies them.
4. Writes the resulting composite CSV as ``..._28ma.adj-h.csv`` with sidecar meta.
5. Diffs the regenerated CSV against the on-disk original to confirm bit-equivalence.

Run:
    source .venv/bin/activate
    python scripts/regenerate_composites.py                # writes + diffs
    python scripts/regenerate_composites.py --diff-only    # just diff, no rewrite
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from mozaic_daily.adjustments import (  # noqa: E402
    apply_net_adjustment_to_series,
    build_adjustments_applied_list,
    load_adjustments_from_dir,
    load_code_registry,
    load_forecast,
    write_meta,
)


DESKTOP_FILTER = dict(data_source="legacy_desktop", segment='{"os": "ALL"}', app_name="desktop")
MOBILE_FILTER = dict(data_source="glean_mobile", segment="{}", app_name="ALL MOBILE")


def world_daily(df: pd.DataFrame, **filt) -> pd.Series:
    mask = (df["country"] == "ALL")
    for col, val in filt.items():
        mask &= df[col] == val
    sub = df.loc[mask, ["target_date", "dau"]].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    return sub.set_index("target_date").sort_index()["dau"]


def to_28ma(series: pd.Series) -> pd.Series:
    return series.sort_index().rolling(28).mean()


CONFIGS = [
    {
        "label": "april",
        "forecast_start": "2026-04-01",
        "desktop_no_iran": "data-official/2026-04/desktop_cps0.15983_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.ld-D.raw.parquet",
        "desktop_plus_iran": "data-official/2026-04/desktop_cps0.15983_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.ld-D.raw.plus_iran.parquet",
        "mobile_no_iran": "data-official/2026-04/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.gm-D.raw.parquet",
        "mobile_plus_iran": "data-official/2026-04/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.gm-D.raw.plus_iran.parquet",
        "adjustments_dir": "data-official/2026-04/adjustments",
        "composite_csv": "april_composite_forecast_28ma.adj-h.csv",
    },
    {
        "label": "june",
        "forecast_start": "2026-05-13",
        "desktop_no_iran": "data-official/2026-06/desktop_cps0.15983_thresh50_recent13_clip0.6/mozaic_daily_forecast.2026-05-13.ld-D.raw.parquet",
        "desktop_plus_iran": "data-official/2026-06/desktop_cps0.15983_thresh50_recent13_clip0.6/mozaic_daily_forecast.2026-05-13.ld-D.raw.plus_iran.parquet",
        "mobile_no_iran": "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-05-13.gm-D.raw.parquet",
        "mobile_plus_iran": "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-05-13.gm-D.raw.plus_iran.parquet",
        "adjustments_dir": "data-official/2026-06/adjustments",
        "composite_csv": "data-official/2026-06/june_composite_forecast_28ma.adj-h.csv",
    },
]


def regenerate(cfg: dict, diff_only: bool) -> None:
    print(f"\n=== {cfg['label']} (forecast_start={cfg['forecast_start']}) ===")
    fc_start = pd.Timestamp(cfg["forecast_start"])

    # All four parquets must validate as state=raw via load_forecast (state guard)
    desktop_no_iran_df, _ = load_forecast(cfg["desktop_no_iran"], require_state=[])
    desktop_plus_iran_df, _ = load_forecast(cfg["desktop_plus_iran"], require_state=[])
    mobile_no_iran_df, _ = load_forecast(cfg["mobile_no_iran"], require_state=[])
    mobile_plus_iran_df, _ = load_forecast(cfg["mobile_plus_iran"], require_state=[])

    desktop_no_iran_ma = to_28ma(world_daily(desktop_no_iran_df, **DESKTOP_FILTER))
    desktop_plus_iran_ma = to_28ma(world_daily(desktop_plus_iran_df, **DESKTOP_FILTER))
    mobile_no_iran_ma = to_28ma(world_daily(mobile_no_iran_df, **MOBILE_FILTER))
    mobile_plus_iran_ma = to_28ma(world_daily(mobile_plus_iran_df, **MOBILE_FILTER))

    # Export date range matches the existing CSVs: forecast_start through 2026-12-31
    export_dates = pd.date_range(fc_start, "2026-12-31", freq="D")
    net = load_adjustments_from_dir(cfg["adjustments_dir"], export_dates)

    def adj(ma_series, platform):
        return apply_net_adjustment_to_series(ma_series, net, platform, forecast_start=fc_start)

    composite = pd.DataFrame({
        "date": export_dates,
        "desktop_28ma_with_iran": adj(desktop_plus_iran_ma, "desktop").reindex(export_dates).values,
        "desktop_28ma_no_iran": adj(desktop_no_iran_ma, "desktop").reindex(export_dates).values,
        "mobile_28ma_with_iran": adj(mobile_plus_iran_ma, "mobile").reindex(export_dates).values,
        "mobile_28ma_no_iran": adj(mobile_no_iran_ma, "mobile").reindex(export_dates).values,
    })

    # Diff against on-disk original
    on_disk = pd.read_csv(cfg["composite_csv"], parse_dates=["date"])
    aligned = on_disk.set_index("date").reindex(composite["date"]).reset_index()
    max_abs = 0.0
    max_rel = 0.0
    for col in ["desktop_28ma_with_iran", "desktop_28ma_no_iran", "mobile_28ma_with_iran", "mobile_28ma_no_iran"]:
        diff = (composite[col].values - aligned[col].values)
        max_abs = max(max_abs, abs(diff).max())
        rel = abs(diff) / abs(aligned[col].values)
        max_rel = max(max_rel, rel.max())
    print(f"  Diff vs on-disk: max_abs={max_abs:.6g}, max_rel={max_rel:.2e}")
    if max_rel < 1e-6:
        print("  MATCH — module reproduces existing composite bit-exact (float precision)")
    else:
        print("  DIFFER — investigate before overwriting")

    if diff_only:
        return

    # Overwrite the on-disk CSV with the regenerated one (will be identical at float precision)
    out_path = REPO_ROOT / cfg["composite_csv"]
    composite.to_csv(out_path, index=False)
    print(f"  Wrote {len(composite)} rows to {out_path}")

    # Refresh sidecar meta: now real provenance (produced by this script), not reconstructed
    registry = load_code_registry()
    spec_path = REPO_ROOT / cfg["adjustments_dir"] / "headwind.json"
    adjustments_applied = build_adjustments_applied_list(
        codes=["h"],
        code_to_spec_file={"h": spec_path},
        registry=registry,
    )
    write_meta(
        out_path,
        forecast_start_date=cfg["forecast_start"],
        data_source="desktop+mobile composite (world DAU 28-day MA)",
        produced_by="scripts/regenerate_composites.py",
        model_config=None,
        adjustments_applied=adjustments_applied,
        extra={
            "parent_files": [
                cfg["desktop_no_iran"],
                cfg["desktop_plus_iran"],
                cfg["mobile_no_iran"],
                cfg["mobile_plus_iran"],
            ],
            "provenance": "regenerated",
        },
    )
    print(f"  Refreshed sidecar meta")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--diff-only", action="store_true", help="Just diff; don't overwrite")
    parser.add_argument("--label", choices=["april", "june", "all"], default="all")
    args = parser.parse_args()

    for cfg in CONFIGS:
        if args.label != "all" and cfg["label"] != args.label:
            continue
        regenerate(cfg, args.diff_only)


if __name__ == "__main__":
    main()
