#!/usr/bin/env python3
"""Report the per-tile level/volatility correlation behind desktop's regime switch.

Desktop's ``seasonality_regime="auto"`` decides *per tile*: a tile runs
multiplicative (and linear-growth, log-space) when
``corr(|y|, |dy|) > seasonality_corr_threshold``, computed on the tile's
holiday-detrended history. Because that decision is per tile, the threshold is a
continuous dial between all-additive and all-multiplicative -- but only if you
know where the tiles actually sit.

This script reads a fitted mozaic pickle and reports, for each candidate
threshold, how many tiles and (more importantly) how much *DAU weight* would run
multiplicative. The tile mix is heavily skewed, so the tile count badly
misrepresents headline impact: on 2026-08 desktop the legacy 0.0 cutoff puts
37.5% of tiles but only 7.6% of DAU on the multiplicative side.

Use it to place ``--seasonality-corr-threshold`` grid points on real structure
rather than spreading them evenly over [-1, 1].

Usage
-----
    source .venv/bin/activate
    python scripts/tile_corr_distribution.py \\
        data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/mozaic_objects.legacy_desktop.2026-07-28.pkl
"""

from __future__ import annotations

import argparse
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_THRESHOLDS = [-1.0, -0.6, -0.45, -0.35, -0.30, -0.26, -0.25, -0.22,
                      -0.20, -0.15, -0.14, -0.13, -0.105, -0.05, 0.0, 0.10,
                      0.20, 0.35, 0.50]
WEIGHT_TAIL_DAYS = 90


def tile_frame(pkl_path: Path, metric: str = "DAU") -> pd.DataFrame:
    """One row per tile: country, population, switch corr, recent-mean DAU weight."""
    with open(pkl_path, "rb") as fh:
        mozaics = pickle.load(fh)
    if metric not in mozaics:
        raise SystemExit(f"metric {metric!r} not in pickle; found {list(mozaics)}")

    rows = []
    for tile in mozaics[metric].tiles:
        detrended = pd.Series(
            np.asarray(tile.holiday_detrended_historical_data, dtype=float)).dropna()
        # Mirrors desktop_forecast_model: `corr or 0` treats NaN as 0.0.
        corr = (detrended.abs().corr(detrended.diff().abs())
                if len(detrended) >= 10 else np.nan)
        raw = pd.Series(np.asarray(tile.raw_historical_data, dtype=float)).dropna()
        rows.append({
            "country": tile.country,
            "population": tile.population,
            "corr": 0.0 if pd.isna(corr) else float(corr),
            "weight": float(raw.tail(WEIGHT_TAIL_DAYS).mean()) if len(raw) else 0.0,
        })
    return pd.DataFrame(rows)


def threshold_table(df: pd.DataFrame, thresholds: list[float]) -> pd.DataFrame:
    total = df["weight"].sum()
    out = []
    for t in thresholds:
        mult = df["corr"] > t
        out.append({
            "threshold": t,
            "tiles_mult": int(mult.sum()),
            "tiles_pct": 100 * mult.mean(),
            "dau_pct": 100 * df.loc[mult, "weight"].sum() / total if total else 0.0,
        })
    return pd.DataFrame(out)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("pkl", type=Path, help="mozaic_objects.*.pkl from a desktop build")
    p.add_argument("--metric", default="DAU")
    p.add_argument("--csv", type=Path, default=None, help="Write the per-tile frame here.")
    args = p.parse_args()

    df = tile_frame(args.pkl, args.metric)
    total = df["weight"].sum()
    print(f"tiles: {len(df)}   summed tile weight: {total:,.0f}")

    print("\nshare running MULTIPLICATIVE at each threshold (corr > threshold):")
    print(f"{'threshold':>10s} {'n tiles':>8s} {'tile %':>8s} {'DAU %':>8s}")
    for _, r in threshold_table(df, DEFAULT_THRESHOLDS).iterrows():
        mark = "  <- legacy hardcoded cutoff" if r["threshold"] == 0.0 else ""
        print(f"{r['threshold']:>10.3f} {int(r['tiles_mult']):>8d} "
              f"{r['tiles_pct']:>7.1f}% {r['dau_pct']:>7.1f}%{mark}")

    print("\nheaviest tiles (these drive the headline; a threshold just below a tile's "
          "corr flips it):")
    heavy = df.nlargest(15, "weight")[["country", "population", "corr", "weight"]]
    print(heavy.to_string(index=False,
                          formatters={"corr": lambda v: f"{v:+.4f}",
                                      "weight": lambda v: f"{v:,.0f}"}))

    if args.csv:
        df.sort_values("weight", ascending=False).to_csv(args.csv, index=False)
        print(f"\nwrote {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
