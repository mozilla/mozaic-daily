"""Build two PARALLEL July marketing-lift curves for head-to-head comparison.

Both are June-anchored and share an identical historical portion (the shipped
june_anchored lift through training_end 2026-06-28). They differ ONLY in the
forward growth driver, so the comparison isolates the cohort choice:

    forward(d) = L(training_end) + [ csv_col(d) - csv_col(training_end) ]

    - variant "total"     : csv_col = July "Total Paid DAU"  (incl declining Pre-2026)
    - variant "cohort2026" : csv_col = July "2026 DAU"        (campaign cohort only)

Writes marketing_lift_model.total.2026-06-29.parquet and
       marketing_lift_model.cohort2026.2026-06-29.parquet (+ sidecar metas).
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
BASE_PARQUET = HERE / "marketing_lift_model.june_anchored.2026-06-29.parquet"  # shared history + anchor

TRAINING_END = pd.Timestamp("2026-06-28")
CAMPAIGN_LAUNCH = pd.Timestamp("2026-04-06")
PARQUET_START = pd.Timestamp("2026-02-01")
PARQUET_END = pd.Timestamp("2026-12-31")
WEEK1_MONDAY = pd.Timestamp("2026-01-05")


def july_daily(column: str) -> pd.Series:
    df = pd.read_csv(JULY_CSV, thousands=",")
    wk = df["week"].str.extract(r"(\d+)")[0].astype(int)
    dates = WEEK1_MONDAY + pd.to_timedelta((wk - 1) * 7, unit="D")
    s = pd.Series(df[column].values, index=pd.DatetimeIndex(dates)).sort_index().astype("float64")
    daily = pd.date_range(s.index.min(), PARQUET_END, freq="D")
    return s.reindex(daily).interpolate(method="linear", limit_area="inside").ffill()


def build_variant(name: str, csv_column: str) -> dict:
    base = pd.read_parquet(BASE_PARQUET)["marketing_lift_daily"].copy()
    base.index = pd.DatetimeIndex(base.index).normalize()
    anchor = float(base.loc[TRAINING_END])

    csv = july_daily(csv_column)
    csv_at_te = float(csv.loc[TRAINING_END])

    full = pd.date_range(PARQUET_START, PARQUET_END, freq="D")
    lift = pd.Series(index=full, dtype="float64", name="marketing_lift_daily")
    hist = full <= TRAINING_END
    lift.loc[full[hist]] = base.reindex(full[hist]).values                       # shared history
    fwd = full[full > TRAINING_END]
    lift.loc[fwd] = anchor + (csv.reindex(fwd).values - csv_at_te)               # cohort-specific forward
    lift.loc[lift.index < CAMPAIGN_LAUNCH] = 0.0

    out = pd.DataFrame({
        "marketing_lift_daily": lift.astype("float64"),
        "marketing_lift_ma": lift.rolling(28, min_periods=14).mean().astype("float64"),
    })
    out.index.name = "target_date"
    out.index = pd.DatetimeIndex(out.index).normalize()

    ref = pd.read_parquet(BASE_PARQUET)
    assert list(out.columns) == list(ref.columns) and out.dtypes.to_dict() == ref.dtypes.to_dict()
    assert out.index.min() == PARQUET_START and out.index.max() == PARQUET_END

    pq = HERE / f"marketing_lift_model.{name}.2026-06-29.parquet"
    mj = HERE / f"marketing_lift_model.{name}.2026-06-29.meta.json"
    out.to_parquet(pq)
    git_hash = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO).decode().strip()
    meta = {
        "model_name": f"fenix_marketing_lift_june_anchored_{name}",
        "description": (
            f"July 2026 marketing-lift, June-anchored, forward driven by July CSV '{csv_column}'. "
            "Shared history = june_anchored through 2026-06-28; forward = L(training_end) + "
            f"(csv(d) - csv(training_end)). One of a parallel pair (total vs cohort2026) built to "
            "isolate the Pre-2026-inclusion question to the forward slope."
        ),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mozaic_daily_git_hash": git_hash,
        "coverage": {"start_date": str(PARQUET_START.date()), "end_date": str(PARQUET_END.date()),
                     "campaign_launch": str(CAMPAIGN_LAUNCH.date()), "training_end": str(TRAINING_END.date())},
        "methodology": {"csv_column": csv_column, "anchor_at_training_end": anchor,
                        "base_parquet": str(BASE_PARQUET)},
        "key_values": {"anchor_training_end": anchor,
                       "lift_year_end": float(lift.loc[PARQUET_END]),
                       "forward_growth_to_eoy": float(lift.loc[PARQUET_END] - anchor)},
        "artifact_sha1": hashlib.sha1(pq.read_bytes()).hexdigest(),
    }
    mj.write_text(json.dumps(meta, indent=2))
    return {"name": name, "parquet": pq.name, "anchor": anchor,
            "eoy": float(lift.loc[PARQUET_END]), "fwd": float(lift.loc[PARQUET_END] - anchor)}


def main() -> None:
    for name, col in [("total", "Total Paid DAU"), ("cohort2026", "2026 DAU")]:
        r = build_variant(name, col)
        print(f"{r['name']:11}  anchor(06-28)={r['anchor']:>+11,.0f}  fwd_growth={r['fwd']:>+9,.0f}  EOY={r['eoy']:>+11,.0f}  -> {r['parquet']}")


if __name__ == "__main__":
    main()
