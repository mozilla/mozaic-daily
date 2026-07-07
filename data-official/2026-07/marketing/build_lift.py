"""Build the July 2026 marketing-lift series, June-anchored.

Construction (references June, per user directive 2026-06-30/07-01):

    L_july(d) = L_june(d) + [ csv_jul_total(d) - csv_jun_total(d) ]

i.e. carry June's delivered marketing-lift plot forward and move it only by the
change in the marketing team's own Total-Paid-DAU forecast between the June CSV
(2026-05-20) and the July CSV (2026-06-29). The year-end outlook grew only
~+139k, so the July lift ~= June lift + ~139k -> the mobile+marketing KPI stays
close to June's, with any difference traceable to the CSV-to-CSV delta.

This deliberately does NOT use the April baseline or any empirical gap (those
carried the Iran asymmetry). Allocation stays telemetry-based downstream.

Writes the parquet + sidecar meta into this directory.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]

JULY_CSV = HERE / "source_data" / "paid_dau_from_uac_campaigns.20260629.csv"
JUNE_CSV = Path("/Users/brendanwells/Downloads/paid_dau_analysis_20260520 - weekly_dau_forecast.csv")
JUNE_LIFT_PARQUET = REPO / "data-official/2026-06/marketing/marketing_lift_model.real_data_v2.hybrid.2026-05-22.parquet"

OUT_PARQUET = HERE / "marketing_lift_model.june_anchored.2026-06-29.parquet"
OUT_META = HERE / "marketing_lift_model.june_anchored.2026-06-29.meta.json"

PARQUET_START = pd.Timestamp("2026-02-01")
PARQUET_END = pd.Timestamp("2026-12-31")
WEEK1_MONDAY = pd.Timestamp("2026-01-05")
CAMPAIGN_LAUNCH = pd.Timestamp("2026-04-06")


def _daily_from_weekly(week_index: pd.DatetimeIndex, values, end=PARQUET_END) -> pd.Series:
    """Linearly interpolate weekly Monday anchors to a daily series through `end`."""
    s = pd.Series(values, index=pd.DatetimeIndex(week_index)).sort_index().astype("float64")
    daily = pd.date_range(s.index.min(), end, freq="D")
    return s.reindex(daily).interpolate(method="linear", limit_area="inside").ffill()


def load_july_total_daily() -> pd.Series:
    df = pd.read_csv(JULY_CSV, thousands=",")
    wk = df["week"].str.extract(r"(\d+)")[0].astype(int)
    dates = WEEK1_MONDAY + pd.to_timedelta((wk - 1) * 7, unit="D")
    return _daily_from_weekly(dates, df["Total Paid DAU"].values)


def load_june_total_daily() -> pd.Series:
    df = pd.read_csv(JUNE_CSV, thousands=",")
    dates = pd.to_datetime(df["week"])
    return _daily_from_weekly(dates, df["total_paid_dau"].values)


def load_june_lift_daily() -> pd.Series:
    df = pd.read_parquet(JUNE_LIFT_PARQUET)
    s = df["marketing_lift_daily"].copy()
    s.index = pd.DatetimeIndex(s.index).normalize()
    return s.sort_index()


def main() -> None:
    full_index = pd.date_range(PARQUET_START, PARQUET_END, freq="D")

    june_lift = load_june_lift_daily().reindex(full_index).fillna(0.0)
    july_total = load_july_total_daily().reindex(full_index).ffill().bfill()
    june_total = load_june_total_daily().reindex(full_index).ffill().bfill()

    csv_delta = july_total - june_total
    july_lift = (june_lift + csv_delta).rename("marketing_lift_daily")

    # Keep the pre-launch convention: no marketing lift before the campaign.
    july_lift.loc[july_lift.index < CAMPAIGN_LAUNCH] = 0.0

    export_df = pd.DataFrame({
        "marketing_lift_daily": july_lift.astype("float64"),
        "marketing_lift_ma": july_lift.rolling(28, min_periods=14).mean().astype("float64"),
    })
    export_df.index.name = "target_date"
    export_df.index = pd.DatetimeIndex(export_df.index).normalize()

    # Schema contract must match the June parquet (promoted).
    ref = pd.read_parquet(JUNE_LIFT_PARQUET)
    assert list(export_df.columns) == list(ref.columns)
    assert export_df.dtypes.to_dict() == ref.dtypes.to_dict()
    assert export_df.index.is_unique and export_df.index.is_monotonic_increasing
    assert export_df.index.min() == PARQUET_START and export_df.index.max() == PARQUET_END

    export_df.to_parquet(OUT_PARQUET)

    def _sha1(p: Path) -> str:
        return hashlib.sha1(Path(p).read_bytes()).hexdigest()

    git_hash = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO).decode().strip()
    meta = {
        "model_name": "fenix_marketing_lift_june_anchored",
        "description": (
            "July 2026 marketing-lift series, June-anchored. "
            "L_july(d) = L_june(d) + (csv_jul_total(d) - csv_jun_total(d)), where csv_*_total is the "
            "marketing team's Total-Paid-DAU forecast interpolated to daily. Carries June's delivered "
            "lift plot forward, moved only by the +~139k change in the year-end marketing outlook "
            "between the 2026-05-20 (June) and 2026-06-29 (July) CSVs. No April baseline / empirical "
            "gap is used (those carried the Iran asymmetry). Pre-launch (< 2026-04-06) = 0."
        ),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mozaic_daily_git_hash": git_hash,
        "coverage": {
            "start_date": str(PARQUET_START.date()),
            "end_date": str(PARQUET_END.date()),
            "campaign_launch": str(CAMPAIGN_LAUNCH.date()),
            "training_end": "2026-06-28",
            "forecast_end": str(PARQUET_END.date()),
        },
        "methodology": {
            "framing": "june-anchored: prior-cycle lift plot + change in marketing-team CSV outlook",
            "rule": "L_july(d) = L_june(d) + (csv_jul_total(d) - csv_jun_total(d)); 0 pre-launch",
            "reference_cycle": "June 2026 (marketing_lift_model.real_data_v2.hybrid.2026-05-22)",
        },
        "source_data": {
            "july_csv": str(JULY_CSV),
            "july_csv_sha1": _sha1(JULY_CSV),
            "june_csv": str(JUNE_CSV),
            "june_lift_parquet": str(JUNE_LIFT_PARQUET),
        },
        "key_values": {
            "june_lift_year_end": float(june_lift.loc[PARQUET_END]),
            "csv_delta_year_end": float(csv_delta.loc[PARQUET_END]),
            "july_lift_year_end": float(july_lift.loc[PARQUET_END]),
            "july_lift_training_end": float(july_lift.loc[pd.Timestamp("2026-06-28")]),
        },
        "artifact_sha1": _sha1(OUT_PARQUET),
    }
    OUT_META.write_text(json.dumps(meta, indent=2))

    print(f"Wrote {OUT_PARQUET.name}  sha1={meta['artifact_sha1'][:12]}")
    print(f"Wrote {OUT_META.name}")
    print("\nKey values:")
    for k, v in meta["key_values"].items():
        print(f"  {k:>26}: {v:>+12,.0f}")


if __name__ == "__main__":
    main()
