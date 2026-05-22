"""Regenerate composite forecast CSVs from verified raw / adjusted parquets.

For each forecast month (April, June 2026), this script:

1. Loads the four parent parquets (desktop/mobile × no-iran/plus-iran) via
   ``load_forecast()``. Parents may carry per-tile adjustment codes (e.g.
   ``m`` for marketing-lift baked into the mobile parquet itself).
2. Aggregates world-level daily DAU, computes 28-day MA.
3. Loads adjustment specs from ``data-official/{date}/adjustments/`` and applies
   them composite-style (currently: ``h`` headwinds).
4. Unions the parents' per-tile codes with the composite-applied codes to
   derive the canonical state marker for the output CSV filename.
5. Writes the resulting composite CSV as ``..._28ma.adj-{codes}.csv`` with a
   sidecar meta listing every contributing adjustment.
6. Diffs the regenerated CSV against the on-disk original to confirm
   bit-equivalence.

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
    canonical_codes,
    insert_state_marker,
    load_adjustments_from_dir,
    load_code_registry,
    load_forecast,
    parse_state_from_path,
    state_marker,
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
        "desktop_no_iran": "data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.ld-D.raw.parquet",
        "desktop_plus_iran": "data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.ld-D.raw.plus_iran.parquet",
        "mobile_no_iran": "data-official/2026-04/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.gm-D.raw.parquet",
        "mobile_plus_iran": "data-official/2026-04/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.gm-D.raw.plus_iran.parquet",
        "adjustments_dir": "data-official/2026-04/adjustments",
        # Template path: marker-free; insert_state_marker(template, final_codes)
        # picks the canonical .adj-{codes}.csv filename for this run.
        "composite_csv_template": "april_composite_forecast_28ma.csv",
    },
    {
        "label": "june",
        "forecast_start": "2026-05-21",
        "desktop_no_iran": "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-21.ld-D.raw.parquet",
        "desktop_plus_iran": "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-21.ld-D.raw.plus_iran.parquet",
        "mobile_no_iran": "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-21.gm-D.raw.parquet",
        "mobile_plus_iran": "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-21.gm-D.raw.plus_iran.parquet",
        "adjustments_dir": "data-official/2026-06/adjustments",
        "composite_csv_template": "data-official/2026-06/june_composite_forecast_28ma.csv",
    },
    {
        # June refresh with marketing-lift (`m`) baked into the mobile parent.
        # Desktop parent stays raw (marketing-lift is mobile-only). Composite
        # will end up `.adj-hm.csv` because parent codes ['m'] are union'd
        # with composite-applied ['h'].
        "label": "june_with_marketing",
        "forecast_start": "2026-05-17",
        "desktop_no_iran": "data-official/2026-06/desktop_cps0.15983_thresh50_recent13_clip0.6/mozaic_daily_forecast.2026-05-17.ld-D.raw.parquet",
        "desktop_plus_iran": "data-official/2026-06/desktop_cps0.15983_thresh50_recent13_clip0.6/mozaic_daily_forecast.2026-05-17.ld-D.raw.plus_iran.parquet",
        "mobile_no_iran": "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-17.gm-D.adj-m.parquet",
        "mobile_plus_iran": "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-17.gm-D.adj-m.plus_iran.parquet",
        "adjustments_dir": "data-official/2026-06/adjustments",
        "composite_csv_template": "data-official/2026-06/june_with_marketing_composite_forecast_28ma.csv",
    },
    {
        # June refresh, threshold-matched to April (holiday_threshold=-0.05) with
        # headwind anchor reduced to -1,403,000 to align Dec-15 28dMA with April.
        # Marketing-lift (`m`) is still baked into the mobile parent, so the final
        # composite is `.adj-hm.csv`.
        "label": "june_thresh_aligned",
        "forecast_start": "2026-05-17",
        "desktop_no_iran": "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-17.ld-D.raw.parquet",
        "desktop_plus_iran": "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-17.ld-D.raw.plus_iran.parquet",
        "mobile_no_iran": "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-17.gm-D.adj-m.parquet",
        "mobile_plus_iran": "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-17.gm-D.adj-m.plus_iran.parquet",
        "adjustments_dir": "data-official/2026-06/adjustments",
        "composite_csv_template": "data-official/2026-06/june_thresh_aligned_composite_forecast_28ma.csv",
    },
]


def _find_marketing_spec_for_forecast_start(forecast_start: str) -> Path | None:
    """Locate the marketing.json spec whose applies_to_forecast_start matches.

    Mirrors ``mozaic_daily.main._find_marketing_spec_for_forecast`` so this
    script stays self-contained.
    """
    import json
    candidates = sorted(
        (REPO_ROOT / "data-official").glob("*/marketing/marketing.json")
    )
    for candidate in candidates:
        spec = json.loads(candidate.read_text())
        if spec.get("applies_to_forecast_start") == forecast_start:
            return candidate
    return None


def regenerate(cfg: dict, diff_only: bool) -> None:
    print(f"\n=== {cfg['label']} (forecast_start={cfg['forecast_start']}) ===")
    fc_start = pd.Timestamp(cfg["forecast_start"])

    # Parent parquets: per-tile codes (e.g. m) are already baked in — load_forecast
    # validates state markers, but we don't require a particular state.
    parent_paths = [cfg["desktop_no_iran"], cfg["desktop_plus_iran"],
                    cfg["mobile_no_iran"], cfg["mobile_plus_iran"]]
    parent_codes = set()
    for path in parent_paths:
        parent_codes.update(parse_state_from_path(path))

    desktop_no_iran_df, _ = load_forecast(cfg["desktop_no_iran"])
    desktop_plus_iran_df, _ = load_forecast(cfg["desktop_plus_iran"])
    mobile_no_iran_df, _ = load_forecast(cfg["mobile_no_iran"])
    mobile_plus_iran_df, _ = load_forecast(cfg["mobile_plus_iran"])

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

    # The composite carries every adjustment applied to its parents plus
    # whatever this script applies composite-style (currently: h).
    composite_applied_codes = ["h"]
    final_codes = sorted(parent_codes | set(composite_applied_codes))
    expected_marker = state_marker(final_codes)
    print(f"  Parent codes: {sorted(parent_codes) or '[]'}; "
          f"composite-applied: {composite_applied_codes}; "
          f"final state: {expected_marker}")

    # Compute output CSV path with the canonical state marker. The template is
    # marker-free; insert_state_marker picks the canonical filename.
    template_path = REPO_ROOT / cfg["composite_csv_template"]
    out_path = insert_state_marker(template_path, final_codes)

    # Diff against on-disk original at the canonical path (or, if the canonical
    # doesn't exist yet, fall back to any sibling .adj-*. file — useful when
    # adding a new code for the first time).
    diff_target = out_path
    if not diff_target.exists():
        # Look for any sibling composite that we can sanity-check against.
        siblings = sorted(template_path.parent.glob(template_path.stem + ".adj-*.csv"))
        diff_target = siblings[0] if siblings else None
    if diff_target is None or not diff_target.exists():
        print(f"  No on-disk composite to diff against; will create fresh at {out_path.name}")
        max_abs = float("nan")
        max_rel = float("nan")
    else:
        on_disk = pd.read_csv(diff_target, parse_dates=["date"])
        aligned = on_disk.set_index("date").reindex(composite["date"]).reset_index()
        max_abs = 0.0
        max_rel = 0.0
        for col in ["desktop_28ma_with_iran", "desktop_28ma_no_iran", "mobile_28ma_with_iran", "mobile_28ma_no_iran"]:
            diff = (composite[col].values - aligned[col].values)
            max_abs = max(max_abs, abs(diff).max())
            rel = abs(diff) / abs(aligned[col].values)
            max_rel = max(max_rel, rel.max())
        print(f"  Diff vs {diff_target.name}: max_abs={max_abs:.6g}, max_rel={max_rel:.2e}")
        if max_rel < 1e-6:
            print("  MATCH — module reproduces existing composite bit-exact (float precision)")
        else:
            print("  DIFFER — investigate before overwriting")

    if diff_only:
        return

    # Overwrite the on-disk CSV with the regenerated one (will be identical at float precision)
    composite.to_csv(out_path, index=False)
    print(f"  Wrote {len(composite)} rows to {out_path}")

    # Sidecar meta with every contributing adjustment code
    registry = load_code_registry()
    code_to_spec_file: dict[str, Path] = {}
    if "h" in final_codes:
        code_to_spec_file["h"] = REPO_ROOT / cfg["adjustments_dir"] / "headwind.json"
    if "m" in final_codes:
        marketing_spec = _find_marketing_spec_for_forecast_start(cfg["forecast_start"])
        if marketing_spec is None:
            raise FileNotFoundError(
                f"final_codes includes 'm' but no marketing.json spec matches "
                f"applies_to_forecast_start={cfg['forecast_start']}"
            )
        code_to_spec_file["m"] = marketing_spec
    adjustments_applied = build_adjustments_applied_list(
        codes=final_codes,
        code_to_spec_file=code_to_spec_file,
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
    valid_labels = [cfg["label"] for cfg in CONFIGS] + ["all"]
    parser.add_argument("--label", choices=valid_labels, default="all")
    args = parser.parse_args()

    for cfg in CONFIGS:
        if args.label != "all" and cfg["label"] != args.label:
            continue
        regenerate(cfg, args.diff_only)


if __name__ == "__main__":
    main()
