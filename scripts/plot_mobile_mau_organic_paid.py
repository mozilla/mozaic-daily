#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""The 2 x 5 (full range + seam zoom, world + per app) mobile MAU figure, drawn for ORGANIC and for PAID separately.

Brendan, 2026-10-15: "do the section-3 5x2 plot for organic only and paid only separately". Organic history and forecasts come
from probe parquets (their training rows are the organic series the model saw, Iran as real actuals); paid is the measured
paid MAU from the split (Fenix only, measured from the split's first day; the other apps carry no paid, so their paid
panels are flat zero and the world paid panel equals Fenix).

Usage:
    python scripts/plot_mobile_mau_organic_paid.py --probe cps0.035_cpr0.725 --probe cps0.035_cpr0.87
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
for sub in ("src", "scripts"):
    if str(repo_root / sub) not in sys.path:
        sys.path.insert(0, str(repo_root / sub))
from mozaic_daily.adjustments import load_forecast  # noqa: E402
from export_glean_history_curves import HEADLINE_DATES, WATERMARK_COLOR, WATERMARK_TEXT, _fmt_axis  # noqa: E402
from export_mobile_mau_curves import APP_NAME_FOR, POPULATIONS  # noqa: E402

DRAFT = repo_root / "data-official/2027-glean-draft"
PROBES = DRAFT / "probes_mobile_mau"
SEAM = pd.Timestamp("2026-10-10")
COLORS = ["#1f77b4", "#2ca02c", "#9467bd", "#ff7f0e"]
APP_FLAG = {"fenix_android": "fenix_android", "firefox_ios": "firefox_ios", "focus_android": "focus_android", "focus_ios": "focus_ios"}


def probe_frames(slugs: list[str]) -> dict[str, pd.DataFrame]:
    out = {}
    for slug in slugs:
        path = sorted((PROBES / slug).glob("*/mozaic_daily_forecast.*.gm-M.raw.parquet"))[0]
        df, _ = load_forecast(str(path))
        df["target_date"] = pd.to_datetime(df["target_date"])
        out[slug] = df
    return out


def series(df: pd.DataFrame, population: str) -> pd.DataFrame:
    return df[(df["country"] == "ALL") & (df["app_name"] == APP_NAME_FOR[population])].set_index("target_date").sort_index()[["mau", "data_type"]]


def paid_series(split: pd.DataFrame, population: str) -> pd.Series:
    daily = split.groupby("submission_date")["paid_mau"].sum().astype(float)
    if population in ("world", "fenix_android"):
        return daily
    return daily * 0.0


def draw(ax, history: pd.Series, forecasts: dict[str, pd.Series], start, end, title, history_label):
    win = slice(pd.Timestamp(start), pd.Timestamp(end))
    if (history == 0).all():
        # apps with no paid acquisition: say so instead of drawing a zero line with a degenerate axis
        ax.text(0.5, 0.5, "no paid acquisition\n(organic by definition)", transform=ax.transAxes, ha="center", va="center", fontsize=11, color="#555")
        ax.set_ylim(0, 1); ax.set_yticks([]); ax.set_title(title, fontsize=11); ax.set_xlim(pd.Timestamp(start), pd.Timestamp(end))
        return
    ax.plot(history[win], color="#222", lw=1.4, label=history_label)
    for (label, fc), color in zip(forecasts.items(), COLORS):
        ax.plot(fc[win], color=color, lw=1.1, label=label)
    ax.axvline(SEAM, color="k", ls=":", lw=0.8)
    for on in HEADLINE_DATES:
        d = pd.Timestamp(on)
        if pd.Timestamp(start) <= d <= pd.Timestamp(end):
            ax.axvline(d, color="#1f77b4", ls="--", lw=0.8)
    ax.set_xlim(pd.Timestamp(start), pd.Timestamp(end))
    ax.set_title(title, fontsize=11)
    ax.grid(alpha=0.3)
    _fmt_axis(ax)


def figure(kind: str, history_for, forecasts_for, out: Path, suptitle: str, history_label: str):
    fig, axes = plt.subplots(2, len(POPULATIONS), figsize=(26, 9))
    for j, population in enumerate(POPULATIONS):
        h = history_for(population)
        fcs = forecasts_for(population)
        for i, (start, end) in enumerate([("2024-01-01", "2027-12-31"), (SEAM - pd.Timedelta(days=120), SEAM + pd.Timedelta(days=150))]):
            draw(axes[i, j], h, fcs, start, end, f"{population} — {'full range' if i == 0 else 'seam zoom'}", history_label)
            if i == 0 and j == 0:
                axes[i, j].legend(fontsize=8)
    fig.suptitle(suptitle, fontsize=13)
    fig.text(0.5, 0.5, WATERMARK_TEXT, fontsize=110, color=WATERMARK_COLOR, alpha=0.15, ha="center", va="center", rotation=25)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=100)
    plt.close(fig)
    print(f"plot  {out}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--probe", action="append", default=None, help="probe slug under probes_mobile_mau/ (repeatable); default the lock and cpr 0.87")
    p.add_argument("--out-dir", type=Path, default=PROBES / "plots")
    args = p.parse_args()
    slugs = args.probe or ["cps0.035_cpr0.725", "cps0.035_cpr0.87"]
    frames = probe_frames(slugs)
    first = frames[slugs[0]]
    split = pd.read_parquet(sorted(DRAFT.glob("organic_mau/fenix_paid_organic_mau.*.parquet"))[-1])
    split["submission_date"] = pd.to_datetime(split["submission_date"])

    def organic_history(pop):
        s = series(first, pop)
        return s.loc[s["data_type"] == "training", "mau"].astype(float)

    def organic_forecasts(pop):
        return {f"organic forecast {slug}": series(df, pop).loc[lambda s: s["data_type"] == "forecast", "mau"].astype(float) for slug, df in frames.items()}

    figure("organic", organic_history, organic_forecasts, args.out_dir / "mobile_mau_organic_2x5.png",
           "Mobile MAU, ORGANIC only (Fenix × measured organic share; other apps carry no paid) — history and probe forecasts",
           "organic history (real Glean)")
    figure("paid", lambda pop: paid_series(split, pop), lambda pop: {}, args.out_dir / "mobile_mau_paid_2x5.png",
           f"Mobile MAU, PAID only (measured from client-level attribution, {split['submission_date'].min().date()} on; Fenix only, no forecast — marketing forecasts paid)",
           "paid MAU (measured)")


if __name__ == "__main__":
    main()
