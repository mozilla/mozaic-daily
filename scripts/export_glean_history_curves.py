#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Canonical curves, Dec-15 summary and plots for a Glean-history desktop build (DAU + MAU).

Reads one ``mozaic_daily_forecast.<seam>.gh-*.raw.parquet`` (through ``load_forecast``) and writes:

    <out>/csv/glean2027_canonical_curves.csv     date, data_type, dau_28ma_<os>, mau_<os>  (os = world, windows, mac, linux)
    <out>/csv/glean2027_dec15_summary.csv        2026-12-15 and 2027-12-15 per OS and metric, with the export's
                                                 translation band (correlated, % of series, held at its last value)
    <out>/plots/glean2027_dau_28ma.png           world + per-OS, full range from 2024 and a seam zoom
    <out>/plots/glean2027_mau_daily.png          same for MAU (raw daily rolling-28 MAU, no further smoothing)
    <out>/plots/glean2027_{mau,dau}_by_country.png  4x4 per-country panels, history + forecast

DAU is shown as the package's ``display_ma`` (variance-matched seam transition, byte-identical to a plain
trailing mean from seam+27 on). MAU is the raw daily value: it is already a 28-day quantity and the
headline statistic is "raw daily MAU on 2027-12-15" (Brendan, 2026-10-12). Every plot carries a PRE-DRAFT
watermark (purple) until the build is locked.

Usage:
    python scripts/export_glean_history_curves.py \\
        --parquet data-official/2027-glean-draft/desktop_glean_history_g01_2026-09-09/<slug>/mozaic_daily_forecast.2026-09-09.gh-D+M.raw.parquet \\
        --out-dir data-official/2027-glean-draft [--label "g01, seam 2026-09-09"]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
if str(repo_root / "src") not in sys.path:
    sys.path.insert(0, str(repo_root / "src"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from mozaic_daily.seam_ma import display_ma  # noqa: E402

OS_ORDER = ["world", "windows", "mac", "linux"]
WATERMARK_TEXT = "PRE-DRAFT"
WATERMARK_COLOR = "#6a1b9a"
SEGMENT_FOR = {"world": "ALL", "windows": "windows", "mac": "mac", "linux": "linux"}
HEADLINE_DATES = ["2026-12-15", "2027-12-15"]
EXPORT_DIR = repo_root / "data-official/2027-glean-draft/inputs/legacy_to_glean_export"
BAND_FILES = {"dau": ("glean_equivalent_dau_history_rollups.parquet", "band_correlated_pct_28d"),
              "mau": ("glean_equivalent_mau_history_rollups.parquet", "band_correlated_pct")}
BAND_CELL = {"world": "global_all", "windows": "win_total", "mac": "mac_total", "linux": "linux_total"}


def world_series(df: pd.DataFrame, metric_col: str, os_name: str) -> pd.DataFrame:
    seg = json.dumps({"os": SEGMENT_FOR[os_name]})
    rows = df[(df["country"] == "ALL") & (df["segment"] == seg)].sort_values("target_date")
    return rows.set_index("target_date")[[metric_col, "data_type"]]


def build_curves(df: pd.DataFrame, seam: pd.Timestamp) -> pd.DataFrame:
    out = None
    for os_name in OS_ORDER:
        dau = world_series(df, "dau", os_name)
        mau = world_series(df, "mau", os_name)
        ma = display_ma(pd.Series(dau.index), dau["dau"], seam)
        part = pd.DataFrame({f"dau_28ma_{os_name}": ma, f"mau_{os_name}": mau["mau"]})
        if out is None:
            out = pd.DataFrame({"data_type": dau["data_type"]}).join(part)
        else:
            out = out.join(part)
    out.index.name = "date"
    return out


def translation_band_pct(metric: str, os_name: str) -> float:
    """The export's correlated band at the roll-up's last delivered day, % of the series."""
    file, col = BAND_FILES[metric]
    ex = pd.read_parquet(EXPORT_DIR / file)
    cell = ex[ex["cell"] == BAND_CELL[os_name]].sort_values("submission_date")
    return float(cell[col].iloc[-1])


def build_summary(curves: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for metric in ("dau", "mau"):
        for os_name in OS_ORDER:
            col = f"dau_28ma_{os_name}" if metric == "dau" else f"mau_{os_name}"
            band = translation_band_pct(metric, os_name)
            for on in HEADLINE_DATES:
                value = float(curves.loc[pd.Timestamp(on), col])
                rows.append({"metric": metric, "statistic": "28d MA" if metric == "dau" else "daily",
                             "os": os_name, "date": on, "value": round(value),
                             "translation_band_pct": round(band, 3),
                             "translation_band_abs": round(value * band / 100)})
    return pd.DataFrame(rows)


def _fmt_axis(ax):
    lo, hi = ax.get_ylim()
    step = (hi - lo) / 5
    prec = max(0, int(-np.floor(np.log10(max(step, 1) / 1e6))) + 1) if step > 0 else 1
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _, p=prec: f"{v / 1e6:.{p}f}M"))


def plot_metric(curves: pd.DataFrame, metric: str, seam: pd.Timestamp, label: str, out_path: Path) -> None:
    fig, axes = plt.subplots(2, 4, figsize=(22, 9))
    title = "DAU, 28-day trailing MA (display_ma)" if metric == "dau" else "MAU, raw daily (rolling 28-day metric)"
    for j, os_name in enumerate(OS_ORDER):
        col = f"dau_28ma_{os_name}" if metric == "dau" else f"mau_{os_name}"
        s = curves[col]
        for i, (start, end) in enumerate([("2024-01-01", "2027-12-31"), (seam - pd.Timedelta(days=120), seam + pd.Timedelta(days=150))]):
            ax = axes[i, j]
            win = s[pd.Timestamp(start):pd.Timestamp(end)]
            act = win[curves.loc[win.index, "data_type"] == "training"]
            fc = win[curves.loc[win.index, "data_type"] == "forecast"]
            ax.plot(act.index, act.values, color="#444", lw=1.3, label="history (Glean-equivalent)")
            ax.plot(fc.index, fc.values, color="#d62728", lw=1.3, label="forecast")
            ax.axvline(seam, color="k", ls=":", lw=0.8)
            for on in HEADLINE_DATES:
                d = pd.Timestamp(on)
                if win.index.min() <= d <= win.index.max():
                    ax.axvline(d, color="#1f77b4", ls="--", lw=0.8)
            ax.set_title(f"{os_name} — {'full range' if i == 0 else 'seam zoom'}", fontsize=11)
            ax.grid(alpha=0.3)
            _fmt_axis(ax)
            if i == 0 and j == 0:
                ax.legend(fontsize=8)
    fig.suptitle(f"{title} — {label}", fontsize=13)
    fig.text(0.5, 0.5, WATERMARK_TEXT, fontsize=110, color=WATERMARK_COLOR, alpha=0.15, ha="center", va="center", rotation=25)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(out_path, dpi=100)
    plt.close(fig)


def plot_by_country(df: pd.DataFrame, seam: pd.Timestamp, label: str, out_dir: Path, stem: str) -> list[Path]:
    """4x4 per-country panels (all OS), MAU daily and DAU 28d MA: history black, forecast green, seam and Dec-15 lines."""
    import matplotlib.ticker as mt
    countries = [c for c in sorted(df["country"].unique()) if c != "ALL"]
    seg = json.dumps({"os": "ALL"})
    written = []
    for col, title in [("mau", "MAU daily"), ("dau", "DAU 28d MA")]:
        fig, axes = plt.subplots(4, 4, figsize=(22, 14))
        for ax, c in zip(axes.ravel(), countries):
            w = df[(df["country"] == c) & (df["segment"] == seg)].set_index("target_date").sort_index()
            s = w[col].rolling(28).mean() if col == "dau" else w[col]
            h = s[w["data_type"] == "training"][pd.Timestamp("2025-01-01"):]
            f = s[w["data_type"] == "forecast"]
            ax.plot(h.index, h.values, color="black", lw=1.2, label="history")
            ax.plot(f.index, f.values, color="green", lw=1.1, label="forecast")
            ax.axvline(seam, color="grey", ls="--", lw=0.8)
            ax.axvline(pd.Timestamp("2027-12-15"), color="red", ls=":", lw=0.8)
            ax.set_title(f"{c} — {title}", fontsize=10)
            ax.grid(alpha=0.3)
            _fmt_axis(ax)
            ax.text(0.5, 0.5, WATERMARK_TEXT, transform=ax.transAxes, fontsize=30, color=WATERMARK_COLOR, alpha=0.14,
                    ha="center", va="center", rotation=28, fontweight="bold")
        fig.suptitle(f"By country — {title} ({label})")
        fig.tight_layout(rect=(0, 0, 1, 0.97))
        out = out_dir / "plots" / f"{stem}_{col}_by_country.png"
        fig.savefig(out, dpi=90)
        plt.close(fig)
        written.append(out)
    return written


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--parquet", type=Path, required=True)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--label", default=None)
    p.add_argument("--stem", default="glean2027")
    return p.parse_args()


def main():
    args = parse_args()
    df, meta = load_forecast(str(args.parquet))
    df["target_date"] = pd.to_datetime(df["target_date"])
    seam = pd.Timestamp(meta["forecast_start_date"])
    label = args.label or f"{args.parquet.parent.name}, seam {seam.date()}"

    curves = build_curves(df, seam)
    summary = build_summary(curves)
    (args.out_dir / "csv").mkdir(parents=True, exist_ok=True)
    (args.out_dir / "plots").mkdir(parents=True, exist_ok=True)
    curves_path = args.out_dir / "csv" / f"{args.stem}_canonical_curves.csv"
    summary_path = args.out_dir / "csv" / f"{args.stem}_dec15_summary.csv"
    curves.to_csv(curves_path, float_format="%.3f")
    summary.to_csv(summary_path, index=False)
    for metric in ("dau", "mau"):
        out = args.out_dir / "plots" / f"{args.stem}_{'dau_28ma' if metric == 'dau' else 'mau_daily'}.png"
        plot_metric(curves, metric, seam, label, out)
        print(f"plot  {out}")
    for out in plot_by_country(df, seam, label, args.out_dir, args.stem):
        print(f"plot  {out}")
    print(f"csv   {curves_path}\ncsv   {summary_path}\n")
    print(summary.pivot_table(index=["metric", "os"], columns="date", values="value").reindex(
        [(m, o) for m in ("mau", "dau") for o in OS_ORDER]).to_string())


if __name__ == "__main__":
    main()
