#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Side-by-side comparison of Glean-history builds sharing a seam: world and per-country values at the headline dates.

Usage:
    python scripts/compare_glean_history_builds.py --out data-official/2027-glean-draft/csv/glean2027_build_comparison.csv \\
        plain=data-official/2027-glean-draft/desktop_glean_history_g01_2026-10-10/<slug>/....raw.parquet \\
        o_dau_only=.../desktop_glean_history_g01_o-dauonly_2026-10-10/<slug>/....adj-o.parquet \\
        o_both=.../desktop_glean_history_g01_o_2026-10-10/<slug>/....adj-o.parquet

Prints world DAU (28d MA) and MAU (daily) at 2026-12-15 and 2027-12-15 per build, plus per-country MAU 2027-12-15,
and writes the long table. Read-only.
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

DATES = ["2026-12-15", "2027-12-15"]
ALL_OS = json.dumps({"os": "ALL"})


def values_for(parquet: Path) -> pd.DataFrame:
    df, _ = load_forecast(str(parquet))
    df["target_date"] = pd.to_datetime(df["target_date"])
    allos = df[df["segment"] == ALL_OS]
    dau = allos.pivot_table(index="target_date", columns="country", values="dau").rolling(28).mean()
    mau = allos.pivot_table(index="target_date", columns="country", values="mau")
    rows = []
    for on in DATES:
        for country in dau.columns:
            rows.append({"date": on, "country": country, "dau_28ma": dau.loc[on, country], "mau": mau.loc[on, country]})
    return pd.DataFrame(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("builds", nargs="+", help="label=path.parquet")
    p.add_argument("--out", type=Path, default=None)
    args = p.parse_args()
    frames = []
    for item in args.builds:
        label, path = item.split("=", 1)
        frames.append(values_for(Path(path)).assign(build=label))
    long = pd.concat(frames, ignore_index=True)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        long.to_csv(args.out, index=False)
    labels = [b.split("=", 1)[0] for b in args.builds]
    world = long[long["country"] == "ALL"].pivot_table(index=["date"], columns="build", values=["mau", "dau_28ma"])[[("mau", l) for l in labels] + [("dau_28ma", l) for l in labels]]
    pd.set_option("display.width", 200)
    print("WORLD (all OS)\n", world.round(0).to_string())
    by_country = long[long["date"] == "2027-12-15"].pivot_table(index="country", columns="build", values="mau")[labels]
    ref = labels[0]
    for l in labels[1:]:
        by_country[f"{l} vs {ref} %"] = ((by_country[l] / by_country[ref] - 1) * 100).round(2)
    print("\nMAU 2027-12-15 by country\n", by_country.round(0).to_string())


if __name__ == "__main__":
    main()
