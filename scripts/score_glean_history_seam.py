#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Seam and shape scorecard for one or more Glean-history builds (world and per OS, DAU and MAU).

Per build: the seam step (last history day -> first forecast day on 7-day means, and a 1-day step that for DAU compares the
first forecast day with the same weekday a week earlier; DAU on the daily series because the trailing MA hides the step), the autumn shape (Oct 9 -> Nov 15 / Dec 15 of the seam year)
against the same stretch in prior years, the Dec-15 values, and the soft constraint Brendan set on 2026-10-12:
MAU 2027-12-15 must be below MAU 2026-12-15 (no MAU growth expected next year).

Usage:
    python scripts/score_glean_history_seam.py label=path.parquet [label=path.parquet ...] [--seam 2026-10-10]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
if str(repo_root / "src") not in sys.path:
    sys.path.insert(0, str(repo_root / "src"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402

OS_ORDER = ["ALL", "windows", "mac", "linux"]


def world_series(df: pd.DataFrame, metric: str, os_name: str) -> pd.Series:
    seg = json.dumps({"os": os_name})
    return df[(df["country"] == "ALL") & (df["segment"] == seg)].set_index("target_date").sort_index()[metric]


def score(df: pd.DataFrame, seam: pd.Timestamp) -> list[dict]:
    rows = []
    last = seam - pd.Timedelta(days=1)
    y = seam.year
    for metric in [m for m in ("dau", "mau") if m in df.columns and df[m].notna().any()]:   # MAU-only builds have no dau
        for os_name in OS_ORDER:
            s = world_series(df, metric, os_name)
            a7 = s[last - pd.Timedelta(days=6):last].mean()
            f7 = s[seam:seam + pd.Timedelta(days=6)].mean()
            ref = s if metric == "mau" else s.rolling(28).mean()
            oct9 = pd.Timestamp(f"{y}-10-09")
            prior = []
            for py in (y - 3, y - 2, y - 1):
                a = ref.get(pd.Timestamp(f"{py}-10-09"))
                if a is not None and pd.notna(a):
                    prior.append(ref[pd.Timestamp(f"{py}-12-15")] / a - 1)
            d26, d27 = ref[pd.Timestamp(f"{y}-12-15")], ref[pd.Timestamp(f"{y + 1}-12-15")]
            rows.append({
                "metric": metric, "os": os_name,
                # DAU has a weekly cycle: compare the first forecast day with the same weekday a week earlier.
                "seam_1d": (s[seam] / s[seam - pd.Timedelta(days=7)] - 1) if metric == "dau" else (s[seam] / s[last] - 1),
                "seam_7d": f7 / a7 - 1,
                "oct9_to_dec15": ref[pd.Timestamp(f"{y}-12-15")] / ref[oct9] - 1,
                "prior_years_oct9_to_dec15": f"{min(prior):+.1%}..{max(prior):+.1%}" if prior else "n/a",
                f"dec15_{y}": d26, f"dec15_{y + 1}": d27, "dec_to_dec": d27 / d26 - 1,
            })
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("builds", nargs="+", help="label=path.parquet")
    p.add_argument("--seam", default=None, help="defaults to the first build's sidecar")
    args = p.parse_args()
    pd.set_option("display.width", 220)
    for item in args.builds:
        label, path = item.split("=", 1)
        df, meta = load_forecast(path)
        df["target_date"] = pd.to_datetime(df["target_date"])
        seam = pd.Timestamp(args.seam or meta["forecast_start_date"])
        table = pd.DataFrame(score(df, seam))
        y = seam.year
        fmt = {"seam_1d": "{:+.2%}".format, "seam_7d": "{:+.2%}".format, "oct9_to_dec15": "{:+.1%}".format,
               f"dec15_{y}": "{:,.0f}".format, f"dec15_{y + 1}": "{:,.0f}".format, "dec_to_dec": "{:+.1%}".format}
        print(f"\n=== {label} (seam {seam.date()}) ===")
        print(table.to_string(index=False, formatters=fmt))
        mau_world = table[(table.metric == "mau") & (table.os == "ALL")].iloc[0]
        verdict = "PASS" if mau_world["dec_to_dec"] < 0 else "FAIL"
        print(f"soft constraint MAU {y + 1}-12-15 < MAU {y}-12-15: {verdict} ({mau_world['dec_to_dec']:+.2%})")


if __name__ == "__main__":
    main()
