#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Canonical curves, Dec-15 summary and plots for the 2027 mobile MAU build (world + per app + per country).

Mobile twin of ``export_glean_history_curves.py``: reads one ``mozaic_daily_forecast.<seam>.gm-M.raw.parquet`` (through
``load_forecast``; mobile rows carry ``app_name`` with ``ALL MOBILE`` for the world and an empty ``segment``) and writes

    <out>/csv/<stem>_canonical_curves.csv   date, data_type, mau_world, mau_fenix_android, mau_firefox_ios, mau_focus_android, mau_focus_ios
    <out>/csv/<stem>_dec15_summary.csv      2026-12-15 and 2027-12-15 per population (no translation band: the history is real Glean)
    <out>/plots/<stem>_mau_daily.png        world + per app, full range from 2024 and a seam zoom
    <out>/plots/<stem>_mau_by_country.png   4x4 per-country panels (all apps), history + forecast

MAU is the raw daily value of the rolling 28-day metric, never smoothed again. Every plot carries the purple PRE-DRAFT
watermark. The curves CSV uses the same column convention as the desktop one (``mau_<population>``) so
``plot_glean_history_canonical.py --populations ...`` renders it unchanged.

Usage:
    python scripts/export_mobile_mau_curves.py \\
        --parquet data-official/2027-glean-draft/mobile_mau_2026-10-10/<slug>/mozaic_daily_forecast.2026-10-10.gm-M.raw.parquet \\
        --out-dir data-official/2027-glean-draft/mobile_mau_2026-10-10 --stem mobile2027
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
if str(repo_root / "src") not in sys.path:
    sys.path.insert(0, str(repo_root / "src"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from export_glean_history_curves import HEADLINE_DATES, WATERMARK_COLOR, WATERMARK_TEXT, _fmt_axis  # noqa: E402

POPULATIONS = ["world", "fenix_android", "firefox_ios", "focus_android", "focus_ios"]
APP_NAME_FOR = {"world": "ALL MOBILE", "fenix_android": "fenix_android", "firefox_ios": "firefox_ios",
                "focus_android": "focus_android", "focus_ios": "focus_ios"}


def population_series(df: pd.DataFrame, population: str) -> pd.DataFrame:
    rows = df[(df["country"] == "ALL") & (df["app_name"] == APP_NAME_FOR[population])].sort_values("target_date")
    if rows.empty:
        raise ValueError(f"no country=ALL rows for app_name {APP_NAME_FOR[population]!r}; app_names present: {sorted(df['app_name'].unique())}")
    return rows.set_index("target_date")[["mau", "data_type"]]


def build_curves(df: pd.DataFrame) -> pd.DataFrame:
    out = None
    for population in POPULATIONS:
        s = population_series(df, population)
        part = pd.DataFrame({f"mau_{population}": s["mau"]})
        out = pd.DataFrame({"data_type": s["data_type"]}).join(part) if out is None else out.join(part)
    out.index.name = "date"
    return out


def build_summary(curves: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for population in POPULATIONS:
        for on in HEADLINE_DATES:
            rows.append({"metric": "mau", "statistic": "daily", "population": population, "date": on,
                         "value": round(float(curves.loc[pd.Timestamp(on), f"mau_{population}"]))})
    return pd.DataFrame(rows)


def plot_populations(curves: pd.DataFrame, seam: pd.Timestamp, label: str, out_path: Path) -> None:
    fig, axes = plt.subplots(2, len(POPULATIONS), figsize=(26, 9))
    for j, population in enumerate(POPULATIONS):
        s = curves[f"mau_{population}"]
        for i, (start, end) in enumerate([("2024-01-01", "2027-12-31"), (seam - pd.Timedelta(days=120), seam + pd.Timedelta(days=150))]):
            ax = axes[i, j]
            win = s[pd.Timestamp(start):pd.Timestamp(end)]
            act = win[curves.loc[win.index, "data_type"] == "training"]
            fc = win[curves.loc[win.index, "data_type"] == "forecast"]
            ax.plot(act.index, act.values, color="#444", lw=1.3, label="history (real Glean)")
            ax.plot(fc.index, fc.values, color="#d62728", lw=1.3, label="forecast")
            ax.axvline(seam, color="k", ls=":", lw=0.8)
            for on in HEADLINE_DATES:
                d = pd.Timestamp(on)
                if win.index.min() <= d <= win.index.max():
                    ax.axvline(d, color="#1f77b4", ls="--", lw=0.8)
            ax.set_title(f"{population} — {'full range' if i == 0 else 'seam zoom'}", fontsize=11)
            ax.grid(alpha=0.3)
            _fmt_axis(ax)
            if i == 0 and j == 0:
                ax.legend(fontsize=8)
    fig.suptitle(f"Mobile MAU, raw daily (rolling 28-day metric) — {label}", fontsize=13)
    fig.text(0.5, 0.5, WATERMARK_TEXT, fontsize=110, color=WATERMARK_COLOR, alpha=0.15, ha="center", va="center", rotation=25)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(out_path, dpi=100)
    plt.close(fig)


def plot_by_country(df: pd.DataFrame, seam: pd.Timestamp, label: str, out_path: Path) -> None:
    countries = [c for c in sorted(df["country"].unique()) if c != "ALL"]
    fig, axes = plt.subplots(4, 4, figsize=(22, 14))
    for ax, c in zip(axes.ravel(), countries):
        w = df[(df["country"] == c) & (df["app_name"] == "ALL MOBILE")].set_index("target_date").sort_index()
        h = w["mau"][w["data_type"] == "training"][pd.Timestamp("2025-01-01"):]
        f = w["mau"][w["data_type"] == "forecast"]
        ax.plot(h.index, h.values, color="black", lw=1.2, label="history")
        ax.plot(f.index, f.values, color="green", lw=1.1, label="forecast")
        ax.axvline(seam, color="grey", ls="--", lw=0.8)
        ax.axvline(pd.Timestamp("2027-12-15"), color="red", ls=":", lw=0.8)
        ax.set_title(f"{c} — MAU daily", fontsize=10)
        ax.grid(alpha=0.3)
        _fmt_axis(ax)
        ax.text(0.5, 0.5, WATERMARK_TEXT, transform=ax.transAxes, fontsize=30, color=WATERMARK_COLOR, alpha=0.14,
                ha="center", va="center", rotation=28, fontweight="bold")
    fig.suptitle(f"By country — mobile MAU daily ({label})")
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(out_path, dpi=90)
    plt.close(fig)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--parquet", type=Path, required=True)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--label", default=None)
    p.add_argument("--stem", default="mobile2027")
    args = p.parse_args()
    df, meta = load_forecast(str(args.parquet))
    df["target_date"] = pd.to_datetime(df["target_date"])
    seam = pd.Timestamp(meta["forecast_start_date"])
    label = args.label or f"{args.parquet.parent.name}, seam {seam.date()}"

    curves = build_curves(df)
    summary = build_summary(curves)
    (args.out_dir / "csv").mkdir(parents=True, exist_ok=True)
    (args.out_dir / "plots").mkdir(parents=True, exist_ok=True)
    curves_path = args.out_dir / "csv" / f"{args.stem}_canonical_curves.csv"
    summary_path = args.out_dir / "csv" / f"{args.stem}_dec15_summary.csv"
    curves.to_csv(curves_path, float_format="%.3f")
    summary.to_csv(summary_path, index=False)
    pop_plot = args.out_dir / "plots" / f"{args.stem}_mau_daily.png"
    country_plot = args.out_dir / "plots" / f"{args.stem}_mau_by_country.png"
    plot_populations(curves, seam, label, pop_plot)
    plot_by_country(df, seam, label, country_plot)
    print(f"plot  {pop_plot}\nplot  {country_plot}\ncsv   {curves_path}\ncsv   {summary_path}\n")
    print(summary.pivot_table(index="population", columns="date", values="value").reindex(POPULATIONS).to_string())


if __name__ == "__main__":
    main()
