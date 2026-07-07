#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Generate the canonical forecast plot set from a mozaic-daily forecast checkpoint.

Mirrors the prior cycle's `csv/plots` set:
  - `global_<platform>.png`     — World (country=ALL) series
  - `<platform>_grid.png`       — per-country small-multiples grid

Each plot shows the metric on a 28-day moving average, with the **real actuals** (black, the
`training` rows) and the **forecast** (colored, the `forecast` rows) split at the forecast-start
divider. Reads the forecast checkpoint parquet written by `scripts/run_main.py`.

Usage:
    python scripts/plot_forecast_set.py                       # latest checkpoint, DAU
    python scripts/plot_forecast_set.py --metric new_profiles
    python scripts/plot_forecast_set.py --input <parquet> --start 2023-01-01 --out-dir <dir>
"""

import argparse
import glob
import os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import db_dtypes  # noqa: F401  (registers the BigQuery `dbdate` dtype for parquet reads)

repo_root = Path(__file__).parent.parent

# Platform -> (display label, ALL-population selector). Desktop encodes the OS aggregate in the
# `segment` JSON; mobile encodes the app aggregate in `app_name`.
PLATFORMS = {
    "legacy_desktop": ("Desktop (legacy)", ("segment", '{"os": "ALL"}')),
    "glean_mobile": ("Mobile (glean)", ("app_name", "ALL MOBILE")),
}
FORECAST_COLOR = "#1f77b4"
ACTUAL_COLOR = "black"


def _million_formatter(vmax: float) -> mticker.FuncFormatter:
    """Pick M-suffix precision from the axis range so adjacent ticks stay distinguishable."""
    decimals = 2 if vmax < 5e6 else (1 if vmax < 5e7 else 0)
    return mticker.FuncFormatter(lambda x, _: f"{x / 1e6:.{decimals}f}M")


def all_population_series(df: pd.DataFrame, data_source: str, country: str, value_col: str):
    """Return (28d-MA series, data_type series) for the ALL-population of one (source, country)."""
    col, val = PLATFORMS[data_source][1]
    d = df[(df["data_source"] == data_source) & (df["country"] == country) & (df[col] == val)]
    d = d[["target_date", "data_type", value_col]].dropna(subset=[value_col]).sort_values("target_date")
    if d.empty:
        return None, None
    daily = d.set_index("target_date")[value_col]
    ma = daily.rolling(28, min_periods=1).mean()
    return ma, d.set_index("target_date")["data_type"]


def _plot_one(ax, ma, data_type, forecast_start, display_start, display_end):
    """Draw actuals (black) + forecast (colored) 28d-MA on ax, split at forecast_start."""
    ma = ma[(ma.index >= display_start) & (ma.index <= display_end)]
    data_type = data_type.reindex(ma.index)
    actuals = ma[data_type == "training"]
    forecast = ma[ma.index >= forecast_start]
    ax.plot(actuals.index, actuals.values, color=ACTUAL_COLOR, lw=1.6, label="Actuals (28d MA)")
    ax.plot(forecast.index, forecast.values, color=FORECAST_COLOR, lw=1.4, label="Forecast (28d MA)")
    ax.axvline(forecast_start, color="#aaaaaa", ls=":", lw=1)
    vmax = ma.max() if len(ma) else 1.0
    ax.yaxis.set_major_formatter(_million_formatter(vmax))
    ax.grid(alpha=0.3)


def plot_global(df, data_source, value_col, forecast_start, display_start, display_end, out_path):
    label = PLATFORMS[data_source][0]
    ma, dt = all_population_series(df, data_source, "ALL", value_col)
    if ma is None:
        print(f"  skip global {data_source}: no World/ALL rows")
        return
    fig, ax = plt.subplots(figsize=(13, 5))
    _plot_one(ax, ma, dt, forecast_start, display_start, display_end)
    ax.set_title(f"{label} — World {value_col.upper()} (actuals vs forecast, 28d MA)")
    ax.set_ylabel(value_col)
    ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    print(f"  wrote {out_path}")


def plot_grid(df, data_source, value_col, forecast_start, display_start, display_end, out_path):
    label = PLATFORMS[data_source][0]
    countries = sorted(c for c in df[df["data_source"] == data_source]["country"].unique() if c != "ALL")
    ncol = 4
    nrow = -(-len(countries) // ncol)
    fig, axes = plt.subplots(nrow, ncol, figsize=(4.2 * ncol, 2.6 * nrow), squeeze=False)
    for i, country in enumerate(countries):
        ax = axes[i // ncol][i % ncol]
        ma, dt = all_population_series(df, data_source, country, value_col)
        if ma is None:
            ax.set_visible(False)
            continue
        _plot_one(ax, ma, dt, forecast_start, display_start, display_end)
        ax.set_title(country, fontsize=10)
        ax.tick_params(labelsize=7)
    for j in range(len(countries), nrow * ncol):
        axes[j // ncol][j % ncol].set_visible(False)
    fig.suptitle(f"{label} — per-country {value_col.upper()} (actuals vs forecast, 28d MA)", fontsize=12)
    fig.tight_layout(rect=[0, 0, 1, 0.98])
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    print(f"  wrote {out_path}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", default=None, help="Forecast checkpoint parquet (default: latest in repo root)")
    p.add_argument("--metric", default="dau", choices=["dau", "new_profiles"], help="Metric column to plot")
    p.add_argument("--start", default="2024-01-01", help="Display window start (YYYY-MM-DD)")
    p.add_argument("--out-dir", default=None, help="Output directory (default: data-official/2026-07/plots)")
    args = p.parse_args()

    if args.input:
        path = args.input
    else:
        candidates = sorted(glob.glob(str(repo_root / "mozaic_daily_forecast.*.parquet")))
        if not candidates:
            raise SystemExit("No forecast checkpoint found; pass --input")
        path = candidates[-1]
    print(f"Input: {path}")

    df = pd.read_parquet(path)
    df["target_date"] = pd.to_datetime(df["target_date"])
    if args.metric not in df.columns:
        raise SystemExit(f"Metric column '{args.metric}' not in checkpoint columns {list(df.columns)}")

    forecast_rows = df[df["data_type"] == "forecast"]
    forecast_start = forecast_rows["target_date"].min()
    display_start = pd.Timestamp(args.start)
    display_end = df["target_date"].max()

    out_dir = Path(args.out_dir) if args.out_dir else repo_root / "data-official" / "2026-07" / "plots"
    os.makedirs(out_dir, exist_ok=True)
    print(f"Metric={args.metric}  forecast_start={forecast_start.date()}  window={display_start.date()}->{display_end.date()}")

    for data_source in PLATFORMS:
        if data_source not in set(df["data_source"]):
            print(f"  skip {data_source}: not in checkpoint")
            continue
        plot_global(df, data_source, args.metric, forecast_start, display_start, display_end,
                    out_dir / f"global_{data_source}.{args.metric}.png")
        plot_grid(df, data_source, args.metric, forecast_start, display_start, display_end,
                  out_dir / f"{data_source}_grid.{args.metric}.png")


if __name__ == "__main__":
    main()
