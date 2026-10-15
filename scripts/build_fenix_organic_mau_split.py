#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Turn the Fenix paid/organic MAU pull into the measured split the ORGANIC mobile MAU forecast consumes.

Input: ``bq_query.py --machine`` JSON of ``scripts/sql/fenix_paid_organic_mau_split.sql`` (submission_date x country x
growth_source, 28-day active clients; same paid definition as adjustment `p`, ``is_mau`` instead of ``is_dau``).

Output (``data-official/2027-glean-draft/organic_mau/``):
    fenix_paid_organic_mau.<end>.parquet   submission_date, country, organic_mau, paid_mau, total_mau, organic_share
    fenix_paid_organic_mau.<end>.meta.json provenance + the numbers (paid share at the start / end, per country at the end)
    plots/fenix_paid_mau_share.png         paid share of Fenix MAU over time, world + the five most-paid countries

Reuses the DAU producer's pure helpers (``organic_source.build_split_frame`` / ``check_split_coverage``) on the MAU counts,
so the partition and coverage rules are the same ones `p` enforces. Nothing here runs the model.

Usage:
    python scripts/build_fenix_organic_mau_split.py --json tmp/mobile_mau_pull/fenix_paid_organic_mau_split.json --training-end 2026-10-09
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
if str(repo_root / "src") not in sys.path:
    sys.path.insert(0, str(repo_root / "src"))

from mozaic_daily.config import get_runtime_config  # noqa: E402
from mozaic_daily.organic_source import build_split_frame, check_split_coverage  # noqa: E402

OUT = repo_root / "data-official/2027-glean-draft/organic_mau"


def load_pull(path: Path) -> pd.DataFrame:
    payload = json.loads(path.read_text())
    rows = payload["rows"] if isinstance(payload, dict) and "rows" in payload else payload
    df = pd.DataFrame(rows)
    required = {"submission_date", "country", "growth_source", "mau"}
    if not required <= set(df.columns):
        raise ValueError(f"pull is missing {sorted(required - set(df.columns))}; columns {sorted(df.columns)}")
    df["submission_date"] = pd.to_datetime(df["submission_date"])
    df["mau"] = pd.to_numeric(df["mau"]).astype("int64")
    return df


def mau_split_frame(pull: pd.DataFrame) -> pd.DataFrame:
    """The DAU builder on MAU counts, columns renamed to say what they are."""
    split = build_split_frame(pull.rename(columns={"mau": "dau"}))
    return split.rename(columns={"organic_dau": "organic_mau", "paid_dau": "paid_mau", "total_dau": "total_mau"})


def plot_share(split: pd.DataFrame, out_path: Path) -> None:
    world = split.groupby("submission_date")[["organic_mau", "paid_mau", "total_mau"]].sum()
    paid_share = world["paid_mau"] / world["total_mau"]
    last = split[split["submission_date"] == split["submission_date"].max()].set_index("country")
    top = (last["paid_mau"] / last["total_mau"]).sort_values(ascending=False).head(5).index.tolist()
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(paid_share, color="black", lw=2, label="world (ex nothing; IR included here, excluded at apply time)")
    for c in top:
        s = split[split["country"] == c].set_index("submission_date")
        ax.plot(s["paid_mau"] / s["total_mau"], lw=1, label=c)
    ax.set_title("Paid share of Fenix MAU (28-day active clients), measured from client-level data")
    ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=1))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3)); ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.grid(True, alpha=0.3); ax.legend(fontsize=8)
    ax.text(0.5, 0.5, "PRE-DRAFT", transform=ax.transAxes, fontsize=72, color="#6a1b9a", alpha=0.14, ha="center", va="center", rotation=28, fontweight="bold")
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--json", type=Path, required=True)
    p.add_argument("--training-end", required=True, help="last training day the split must cover, YYYY-MM-DD")
    p.add_argument("--out-dir", type=Path, default=OUT)
    p.add_argument("--start", default=None,
                   help="drop split days before this date. The live client tables lose old paid cohorts, and the join is visibly broken in the "
                        "view's first weeks (Aug-Sep 2024: ~0 paid MAU against 216K paid DAU in the mirror); the runner holds the first kept day flat backwards")
    args = p.parse_args()
    pull = load_pull(args.json)
    dropped_days = 0
    if args.start:
        keep = pull["submission_date"] >= pd.Timestamp(args.start)
        dropped_days = int(pull.loc[~keep, "submission_date"].nunique())
        pull = pull[keep]
    split = mau_split_frame(pull)
    expected = set(get_runtime_config(forecast_start_date_override=args.training_end)["countries"]) | {"ROW"}
    # the coverage check reads organic_share only, so the renamed columns are fine
    check_split_coverage(split, expected_countries=expected, training_end=pd.Timestamp(args.training_end))

    end = split["submission_date"].max().date().isoformat()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    target = args.out_dir / f"fenix_paid_organic_mau.{end}.parquet"
    split.to_parquet(target, index=False)

    world = split.groupby("submission_date")[["organic_mau", "paid_mau", "total_mau"]].sum()
    first, last = world.iloc[0], world.iloc[-1]
    last_by_country = split[split["submission_date"] == split["submission_date"].max()].set_index("country")
    meta = {
        "produced_by": "scripts/build_fenix_organic_mau_split.py", "written_at": datetime.now().isoformat(timespec="seconds"),
        "sql": "scripts/sql/fenix_paid_organic_mau_split.sql", "source_json": str(args.json), "source_sha1": hashlib.sha1(args.json.read_bytes()).hexdigest(),
        "paid_definition": "mozdata.fenix.new_profile_clients paid_vs_organic_gclid='Paid' AND release AND com.android.vending (same as `p`); is_mau",
        "date_range": [str(split["submission_date"].min().date()), end], "rows": int(len(split)), "countries": sorted(split["country"].unique()),
        "start_override": args.start, "days_dropped_before_start": dropped_days,
        "world_paid_share_first_day": float(first["paid_mau"] / first["total_mau"]), "world_paid_share_last_day": float(last["paid_mau"] / last["total_mau"]),
        "world_paid_mau_last_day": int(last["paid_mau"]), "world_total_mau_last_day": int(last["total_mau"]),
        "paid_share_by_country_last_day": {c: round(float(r["paid_mau"] / r["total_mau"]), 4) for c, r in last_by_country.iterrows()},
    }
    (args.out_dir / f"fenix_paid_organic_mau.{end}.meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    plot_share(split, args.out_dir / "plots" / "fenix_paid_mau_share.png")
    print(f"wrote {target}: {meta['date_range']}, world paid share {meta['world_paid_share_first_day']:.2%} -> {meta['world_paid_share_last_day']:.2%} "
          f"(paid MAU {meta['world_paid_mau_last_day']:,} of {meta['world_total_mau_last_day']:,} on {end})")
    print("paid share by country on the last day:", meta["paid_share_by_country_last_day"])
    print(f"plot  {args.out_dir / 'plots' / 'fenix_paid_mau_share.png'}")


if __name__ == "__main__":
    main()
