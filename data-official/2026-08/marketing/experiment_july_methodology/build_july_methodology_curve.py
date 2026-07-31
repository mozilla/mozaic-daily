"""Build an August lift curve using JULY's methodology on AUGUST's source values.

Counterfactual experiment: what would the August mobile forecast have been if we had kept the
prior cycle's construction instead of switching to anchor-and-subtract?

July's rule (`data-official/2026-07/marketing/build_lift.py`) carried the previous cycle's
delivered curve forward, moved only by the change in the marketing team's own Total-Paid-DAU
outlook between the two source deliveries:

    L_july(d) = L_june(d) + [csv_jul_total(d) - csv_jun_total(d)]

Applying that same rule one cycle later, with August's source in place of July's:

    L_aug_JM(d) = L_july(d) + [aug_total(d) - csv_jul_total(d)],   0 before campaign launch

where `aug_total` is the UAC + Meta combined Total Paid DAU from the new query. Note that July's
source predates Meta entirely, so under this rule the **whole Meta contribution enters as
incremental delta** — a property of the methodology, not a modelling choice made here.

This is an EXPERIMENT. It is not wired and must not be. The canonical August curve is
`../marketing_lift_model.uac_meta_total.2026-07-28.parquet` (anchor-and-subtract).

Run: python3 data-official/2026-08/marketing/experiment_july_methodology/build_july_methodology_curve.py
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
MARKETING = HERE.parent
REPO = HERE.parents[3]

JULY_DELIVERED_LIFT = REPO / "data-official/2026-07/marketing/marketing_lift_model.total.2026-06-29.parquet"
JULY_SOURCE_CSV = REPO / "data-official/2026-07/marketing/source_data/paid_dau_from_uac_campaigns.20260629.csv"
AUGUST_SOURCE_CSV = MARKETING / "source_data/uac_meta_paid_dau.20260730.csv"

OUT_PARQUET = HERE / "marketing_lift_model.july_methodology.2026-07-28.parquet"
OUT_META = HERE / "marketing_lift_model.july_methodology.2026-07-28.meta.json"

PARQUET_START = pd.Timestamp("2026-02-01")
PARQUET_END = pd.Timestamp("2026-12-31")
WEEK1_MONDAY = pd.Timestamp("2026-01-05")
CAMPAIGN_LAUNCH = pd.Timestamp("2026-04-06")
KPI_DATE = pd.Timestamp("2026-12-15")
MA_WINDOW = 28


def _daily_from_weekly(week_index, values, end: pd.Timestamp = PARQUET_END) -> pd.Series:
    """Linearly interpolate weekly Monday anchors to daily — same helper July's builder used."""
    weekly = pd.Series(values, index=pd.DatetimeIndex(week_index)).sort_index().astype("float64")
    daily = pd.date_range(weekly.index.min(), end, freq="D")
    return weekly.reindex(daily).interpolate(method="linear", limit_area="inside").ffill()


def load_july_source_total() -> pd.Series:
    """July's `Total Paid DAU` column, weeks labelled 'Week N' from 2026-01-05."""
    df = pd.read_csv(JULY_SOURCE_CSV, thousands=",")
    week_number = df["week"].str.extract(r"(\d+)")[0].astype(int)
    dates = WEEK1_MONDAY + pd.to_timedelta((week_number - 1) * 7, unit="D")
    return _daily_from_weekly(dates, df["Total Paid DAU"].values)


def load_august_source_total() -> pd.Series:
    """August's combined UAC + Meta Total Paid DAU. Meta is already cumulative in the query."""
    df = pd.read_csv(AUGUST_SOURCE_CSV, parse_dates=["date"])
    combined = df["uac_raw"].fillna(0) + df["meta_raw"].fillna(0)
    return _daily_from_weekly(df["date"], combined.values)


def load_july_delivered_lift() -> pd.Series:
    series = pd.read_parquet(JULY_DELIVERED_LIFT)["marketing_lift_daily"]
    series.index = pd.DatetimeIndex(series.index).normalize()
    return series.sort_index()


def main() -> None:
    full_index = pd.date_range(PARQUET_START, PARQUET_END, freq="D")

    july_lift = load_july_delivered_lift().reindex(full_index).fillna(0.0)
    august_total = load_august_source_total().reindex(full_index).ffill().bfill()
    july_total = load_july_source_total().reindex(full_index).ffill().bfill()

    source_delta = august_total - july_total
    lift = (july_lift + source_delta).rename("marketing_lift_daily")

    # Same pre-launch convention as July's builder: no marketing lift before the campaign.
    lift.loc[lift.index < CAMPAIGN_LAUNCH] = 0.0

    export_df = pd.DataFrame({
        "marketing_lift_daily": lift.astype("float64"),
        "marketing_lift_ma": lift.rolling(MA_WINDOW, min_periods=14).mean().astype("float64"),
    })
    export_df.index.name = "target_date"

    reference = pd.read_parquet(JULY_DELIVERED_LIFT)
    assert list(export_df.columns) == list(reference.columns), "column mismatch vs July's curve"
    assert export_df.dtypes.to_dict() == reference.dtypes.to_dict(), "dtype mismatch vs July's curve"
    assert export_df.index.is_unique and export_df.index.is_monotonic_increasing
    assert export_df.index.min() == PARQUET_START and export_df.index.max() == PARQUET_END
    export_df.to_parquet(OUT_PARQUET)

    def _sha1(path: Path) -> str:
        return hashlib.sha1(Path(path).read_bytes()).hexdigest()

    meta = {
        "model_name": "fenix_marketing_lift_july_methodology_august_values",
        "description": (
            "EXPERIMENT — NOT WIRED. August lift curve built with July's methodology: "
            "L(d) = L_july_delivered(d) + [aug_source_total(d) - jul_source_total(d)], 0 pre-launch. "
            "Counterfactual for 'what if we had not switched to anchor-and-subtract'. The canonical "
            "August curve is ../marketing_lift_model.uac_meta_total.2026-07-28.parquet."
        ),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mozaic_daily_git_hash": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO).decode().strip(),
        "coverage": {
            "start_date": str(PARQUET_START.date()),
            "end_date": str(PARQUET_END.date()),
            "campaign_launch": str(CAMPAIGN_LAUNCH.date()),
        },
        "methodology": {
            "framing": "prior-cycle delivered curve + change in the marketing team's source outlook",
            "rule": "L(d) = L_july(d) + (aug_total(d) - jul_total(d)); 0 pre-launch",
            "inherits": "July's curve, which itself inherits June's empirical 45-day gap measurement",
            "meta_channel": (
                "July's source has no Meta, so the entire Meta contribution enters as incremental "
                "delta under this rule"
            ),
        },
        "source_data": {
            "july_delivered_lift": str(JULY_DELIVERED_LIFT.relative_to(REPO)),
            "july_delivered_lift_sha1": _sha1(JULY_DELIVERED_LIFT),
            "july_source_csv": str(JULY_SOURCE_CSV.relative_to(REPO)),
            "august_source_csv": str(AUGUST_SOURCE_CSV.relative_to(REPO)),
            "august_source_csv_sha1": _sha1(AUGUST_SOURCE_CSV),
        },
        "key_values": {
            "july_lift_dec15": float(july_lift.loc[KPI_DATE]),
            "source_delta_dec15": float(source_delta.loc[KPI_DATE]),
            "lift_dec15": float(lift.loc[KPI_DATE]),
            "lift_year_end": float(lift.loc[PARQUET_END]),
        },
        "artifact_sha1": _sha1(OUT_PARQUET),
    }
    OUT_META.write_text(json.dumps(meta, indent=2))

    print(f"Wrote {OUT_PARQUET.name}  sha1={meta['artifact_sha1'][:12]}")
    for key, value in meta["key_values"].items():
        print(f"  {key:>22}: {value:>+14,.0f}")
    print("\nSanity: negative days (pre-launch convention should leave none after 04-06):",
          int((lift.loc[CAMPAIGN_LAUNCH:] < 0).sum()))


if __name__ == "__main__":
    main()
