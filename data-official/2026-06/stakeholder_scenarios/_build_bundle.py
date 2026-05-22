"""Build the stakeholder-scenarios data bundle.

Reads production forecasts and adjustments from this repo, extracts the
mobile world-rollup series we need, and writes them to ./data/ as small
single-series parquets that the standalone notebook consumes.

Run once at bundle creation time; the script is kept here for traceability
but is not advertised in the bundle README. Do not call from the notebook.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

BUNDLE_DIR = Path(__file__).parent
DATA_DIR = BUNDLE_DIR / "data"
REPO_ROOT = BUNDLE_DIR.parent.parent.parent  # data-official/2026-06/stakeholder_scenarios → repo root

JUNE_RAW_PATH = (
    REPO_ROOT
    / "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426"
    / "mozaic_daily_forecast.2026-05-17.gm-D.raw.parquet"
)
APRIL_RAW_PATH = (
    REPO_ROOT
    / "data-official/2026-04/mobile_cps0.02_thresh32_recent13_clip0.6"
    / "mozaic_daily_forecast.2026-04-01.gm-D.raw.parquet"
)
# The "marketing-off" baseline is derived as (anchor_adj_m − anchor_lift) at world
# rollup. We use the v2 hybrid trial as the anchor because its lift_at_training_end
# equals the empirical Fenix gap by construction, so subtracting the lift back out
# leaves a Prophet "no-marketing" trajectory that joins smoothly to actuals at
# forecast start. The convolution-model adj-m would leave a ~85k boundary residual.
V2_HYBRID_ADJM_PATH = (
    REPO_ROOT
    / "tmp/hybrid_v2_trial_2026-05-17"
    / "mozaic_daily_forecast.2026-05-17.gm-D.parquet"
)
V2_HYBRID_LIFT_PATH = (
    REPO_ROOT
    / "data-official/2026-06/marketing"
    / "marketing_lift_model.real_data_v2.hybrid.2026-05-20.parquet"
)
HEADWIND_SPEC_PATH = REPO_ROOT / "data-official/2026-06/adjustments/headwind.json"
V2_CSV_PATH = (
    REPO_ROOT
    / ".claude/worktrees/marketing-real-data/marketing_real_data_model/data"
    / "paid_dau_weekly_forecast.v2_model.20260520.csv"
)

TRAINING_END = pd.Timestamp("2026-05-16")  # June forecast's training end


def _aggregate_world_mobile(forecast_path: Path) -> pd.Series:
    """Filter a gm-D forecast parquet to the mobile world rollup."""
    df = pd.read_parquet(forecast_path)
    df["target_date"] = pd.to_datetime(df["target_date"])
    mask = (
        (df["data_source"] == "glean_mobile")
        & (df["country"] == "ALL")
        & (df["app_name"] == "ALL MOBILE")
        & (df["segment"] == "{}")
    )
    return df.loc[mask].set_index("target_date").sort_index()["dau"].astype("float64")


def _aggregate_world_fenix(forecast_path: Path, data_type_filter: str | None = None) -> pd.Series:
    """Filter a gm-D forecast parquet to the Fenix Android world rollup."""
    df = pd.read_parquet(forecast_path)
    df["target_date"] = pd.to_datetime(df["target_date"])
    mask = (
        (df["data_source"] == "glean_mobile")
        & (df["country"] == "ALL")
        & (df["app_name"] == "fenix_android")
        & (df["segment"] == "{}")
    )
    if data_type_filter is not None:
        mask &= df["data_type"] == data_type_filter
    return df.loc[mask].set_index("target_date").sort_index()["dau"].astype("float64")


def build_no_mktg_baseline() -> None:
    """Derive the marketing-off baseline as anchor_adj_m − anchor_lift at world rollup.

    The anchor is the v2 hybrid trial forecast (adj-m). Its m-applier subtracted
    the v2 hybrid lift from training before mozaic fit, so Prophet learned a
    no-marketing trajectory. Subtracting the lift back out reconstructs that
    trajectory at the world rollup. The result joins smoothly to actuals at
    forecast start (lift_at_training_end equals the empirical Fenix gap by
    construction in the v2 hybrid recipe).
    """
    adj_m = _aggregate_world_mobile(V2_HYBRID_ADJM_PATH)
    lift_df = pd.read_parquet(V2_HYBRID_LIFT_PATH)
    lift = lift_df["marketing_lift_daily"]
    lift.index = pd.to_datetime(lift.index)
    lift = lift.reindex(adj_m.index, fill_value=0.0)
    series = adj_m - lift
    out = pd.DataFrame({"date": series.index, "dau": series.values})
    out_path = DATA_DIR / "no_mktg_baseline_world_daily.parquet"
    out.to_parquet(out_path, index=False)
    print(f"Wrote {out_path}  ({len(out)} rows, {out['date'].min().date()} → {out['date'].max().date()})")
    # Diagnostic: confirm the boundary continuity
    te = pd.Timestamp("2026-05-16")
    fs = pd.Timestamp("2026-05-17")
    print(f"  derived marketing_off @ training_end ({te.date()}):    {series.loc[te]:>+15,.0f}")
    print(f"  derived marketing_off @ forecast_start ({fs.date()}):  {series.loc[fs]:>+15,.0f}")
    print(f"  derived marketing_off @ 2026-12-15:                    {series.loc[pd.Timestamp('2026-12-15')]:>+15,.0f}")


def build_april_n1() -> None:
    """World mobile DAU from the April raw forecast (N-1 reference)."""
    series = _aggregate_world_mobile(APRIL_RAW_PATH)
    out = pd.DataFrame({"date": series.index, "dau": series.values})
    out_path = DATA_DIR / "april_n1_world_daily.parquet"
    out.to_parquet(out_path, index=False)
    print(f"Wrote {out_path}  ({len(out)} rows, {out['date'].min().date()} → {out['date'].max().date()})")


def build_fenix_gap() -> None:
    """Empirical Fenix gap = (June Fenix actuals) − (April Fenix forecast), historical only."""
    april_fenix = _aggregate_world_fenix(APRIL_RAW_PATH)
    actuals_fenix = _aggregate_world_fenix(JUNE_RAW_PATH, data_type_filter="training")
    gap_index = april_fenix.index.intersection(actuals_fenix.index)
    gap = (actuals_fenix.reindex(gap_index) - april_fenix.reindex(gap_index))
    gap = gap.loc[: actuals_fenix.dropna().index.max()]
    out = pd.DataFrame({"date": gap.index, "fenix_gap_daily": gap.values})
    out_path = DATA_DIR / "fenix_gap_daily.parquet"
    out.to_parquet(out_path, index=False)
    print(f"Wrote {out_path}  ({len(out)} rows, {out['date'].min().date()} → {out['date'].max().date()})")


def build_headwind_spec() -> None:
    """Copy the production headwind spec verbatim."""
    spec = json.loads(HEADWIND_SPEC_PATH.read_text())
    out_path = DATA_DIR / "headwind_spec.json"
    out_path.write_text(json.dumps(spec, indent=2))
    print(f"Wrote {out_path}  ({spec['type']} {spec['start_date']} → {spec['anchor_date']}, mobile={spec.get('mobile_dau')})")


def build_stakeholder_targets() -> None:
    """The three Dec-15 stakeholder marker values, same as 05's plot."""
    targets = {
        "stretch": 17_742_615,
        "base": 17_522_795,
        "low": 17_019_424,
        "measurement_date": "2026-12-15",
    }
    out_path = DATA_DIR / "stakeholder_targets.json"
    out_path.write_text(json.dumps(targets, indent=2))
    print(f"Wrote {out_path}")


def copy_example_csv() -> None:
    """The v2 marketing CSV serves as a working template stakeholders can fork."""
    out_path = BUNDLE_DIR / "example_csv.csv"
    shutil.copyfile(V2_CSV_PATH, out_path)
    print(f"Wrote {out_path}  ({out_path.stat().st_size} bytes)")


def write_snapshot_manifest() -> None:
    """Record what we snapshotted, so the README can reference it."""
    manifest = {
        "snapshot_date_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "forecast_cycle": "2026-06",
        "training_end": str(TRAINING_END.date()),
        "sources": {
            "no_mktg_baseline_anchor_adj_m": str(V2_HYBRID_ADJM_PATH.relative_to(REPO_ROOT)),
            "no_mktg_baseline_anchor_lift": str(V2_HYBRID_LIFT_PATH.relative_to(REPO_ROOT)),
            "april_raw": str(APRIL_RAW_PATH.relative_to(REPO_ROOT)),
            "june_raw_for_fenix_gap": str(JUNE_RAW_PATH.relative_to(REPO_ROOT)),
            "headwind_spec": str(HEADWIND_SPEC_PATH.relative_to(REPO_ROOT)),
            "example_csv": str(V2_CSV_PATH.relative_to(REPO_ROOT)),
        },
    }
    out_path = DATA_DIR / "snapshot_manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2))
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    DATA_DIR.mkdir(exist_ok=True)
    build_no_mktg_baseline()
    build_april_n1()
    build_fenix_gap()
    build_headwind_spec()
    build_stakeholder_targets()
    copy_example_csv()
    write_snapshot_manifest()
    print("\nBundle data files built. Mobile actuals are pre-fetched separately via the BQ step.")
