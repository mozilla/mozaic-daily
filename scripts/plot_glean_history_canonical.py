#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Canonical-format plots for the Glean-history 2027 draft, mimicking the monthly canonical notebook.

Same conventions as ``data-official/2026-10/october_canonical.ipynb`` ``[helpers]`` / ``[plot-headline]``:
14x6 figure, prior-year series in faint grey calendar-aligned, actuals black (lw 2), comparison forecast blue,
current forecast green, Dec 15 red dotted, seam grey dashed, two-decimal millions axis, month ticks rotated 45,
grid, legend, and the purple PRE-DRAFT stamp through ``save`` in exactly one place. No targets exist for 2027, so no
gold markers.

Differences forced by a two-year horizon: one figure per calendar year. The 2026 view has actuals through the
seam and the forecast after it, with 2025 actuals as the grey reference. The 2027 view is forecast only, with
the 2026 series (actuals to the seam, forecast after) as the grey reference and 2025 actuals fainter behind it.
An optional blue comparison line (``--comparison-curves``) is for internal diagnostics only; external outputs
never carry the raw model.

Outputs (``<out-dir>/canonical/``): ``{metric}_{os}_{year}.png`` for metric in {dau_28ma, mau}, os in {world,
windows, mac, linux}, year in {2026, 2027}; plus ``{metric}_by_os_2027.png`` small multiples.

Usage:
    python scripts/plot_glean_history_canonical.py \\
        --curves data-official/2027-glean-draft/csv/glean2027_canonical_curves.csv \\
        --comparison-curves data-official/2027-glean-draft/build2_plain_seam2026-10-10/csv/glean2027_canonical_curves.csv \\
        --seam 2026-10-10 --out-dir data-official/2027-glean-draft/plots
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

OS_ORDER = ["world", "windows", "mac", "linux"]
METRICS = {"dau_28ma": ("DAU", "28-Day MA"), "mau": ("MAU", "raw daily (rolling 28-day metric)")}
SHOW_DRAFT_WATERMARK = True
WATERMARK_TEXT = "PRE-DRAFT"      # Brendan, 2026-10-12: louder than the monthly DRAFT; purple so it cannot be mistaken for a cycle plot
WATERMARK_COLOR = "#6a1b9a"
MEASUREMENT_DAY = (12, 15)


def millions_formatter(x, pos):
    return f"{x / 1e6:.2f}M"   # two decimals so 0.5M-spaced ticks stay distinct


def add_draft_watermark(ax, text=None):
    text = text or WATERMARK_TEXT
    if not SHOW_DRAFT_WATERMARK:
        return
    ax.text(0.5, 0.5, text, transform=ax.transAxes, fontsize=96, color=WATERMARK_COLOR, alpha=0.16,
            ha="center", va="center", rotation=28, zorder=100, fontweight="bold", clip_on=True)


def style_time_axis(ax, *, title, ylabel, legend_loc="lower left"):
    ax.set_title(title, fontsize=14)
    ax.set_ylabel(ylabel)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(millions_formatter))
    ax.legend(loc=legend_loc, fontsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.grid(True, alpha=0.3)


def save(fig, path: Path):
    for ax in fig.axes:
        add_draft_watermark(ax)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=130)
    plt.close(fig)
    print(f"saved: {path}")


def shifted(series: pd.Series, from_year: int, to_year: int) -> pd.Series:
    s = series[series.index.year == from_year].dropna()
    s.index = s.index + pd.DateOffset(years=to_year - from_year)
    return s


def draw_year(ax, *, year, current, actual_mask, comparison, seam, ylabel):
    start, end = pd.Timestamp(f"{year}-01-01"), pd.Timestamp(f"{year}-12-31")
    measurement = pd.Timestamp(year, *MEASUREMENT_DAY)
    prior_year = year - 1
    # Grey references: the year before (calendar-aligned). For 2027 that is 2026 = actuals to the seam + forecast.
    ref = shifted(current, prior_year, year)
    if year == seam.year + 1:
        ref_actual = ref[ref.index <= seam + pd.DateOffset(years=1) - pd.Timedelta(days=1)]
        ref_forecast = ref[ref.index >= seam + pd.DateOffset(years=1)]
        ax.plot(ref_actual, color="grey", linewidth=0.9, alpha=0.55, zorder=1, label=f"{prior_year} actuals (calendar-aligned)")
        ax.plot(ref_forecast, color="grey", linewidth=0.9, alpha=0.55, linestyle=":", zorder=1, label=f"{prior_year} forecast (calendar-aligned)")
        ref2 = shifted(current, prior_year - 1, year)
        ax.plot(ref2, color="grey", linewidth=0.7, alpha=0.3, zorder=0, label=f"{prior_year - 1} actuals (calendar-aligned)")
    else:
        ax.plot(ref, color="grey", linewidth=0.9, alpha=0.55, zorder=1, label=f"{prior_year} actuals (calendar-aligned)")
    window = current[(current.index >= start) & (current.index <= end)]
    actual = window[actual_mask.reindex(window.index).fillna(False)]
    forecast = window[~actual_mask.reindex(window.index).fillna(False)]
    if not actual.empty:
        ax.plot(actual, color="black", linewidth=2, label="Actuals (Glean-equivalent history)")
    if comparison is not None:
        cmp_window = comparison[(comparison.index >= max(start, seam)) & (comparison.index <= end)]
        ax.plot(cmp_window, color="blue", linewidth=1, label="Raw model, no China correction (same seam)")
    ax.plot(forecast, color="green", linewidth=1, label="Current draft (build 4)")
    ax.axvline(measurement, color="red", linestyle=":", alpha=0.5, label="Dec 15")
    if start <= seam <= end:
        ax.axvline(seam, color="grey", linestyle="--", alpha=0.6, label=f"Seam {seam.date()}")
    ax.set_xlim(start - pd.Timedelta(days=3), end + pd.Timedelta(days=3))
    v = current.get(measurement)
    if v is not None and pd.notna(v):
        ax.annotate(f"Dec 15: {v:,.0f}", xy=(measurement, v), xytext=(-110, 18), textcoords="offset points",
                    fontsize=9, arrowprops=dict(arrowstyle="-", color="red", alpha=0.5))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--curves", type=Path, required=True)
    p.add_argument("--comparison-curves", type=Path, default=None,
                   help="INTERNAL ONLY: draw a second build in blue. External outputs never carry the raw model (Brendan, 2026-10-12)")
    p.add_argument("--seam", required=True)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--label", default="Glean-history draft")
    args = p.parse_args()
    seam = pd.Timestamp(args.seam)
    curves = pd.read_csv(args.curves, parse_dates=["date"]).set_index("date")
    comparison = pd.read_csv(args.comparison_curves, parse_dates=["date"]).set_index("date") if args.comparison_curves else None
    actual_mask = curves["data_type"] == "training"
    out = args.out_dir / "canonical"

    for metric_key, (metric_name, statistic) in METRICS.items():
        for os_name in OS_ORDER:
            col = f"{metric_key}_{os_name}"
            for year in (seam.year, seam.year + 1):
                fig, ax = plt.subplots(figsize=(14, 6))
                draw_year(ax, year=year, current=curves[col], actual_mask=actual_mask,
                          comparison=comparison[col] if comparison is not None else None, seam=seam, ylabel=metric_name)
                scope = "Desktop" if os_name == "world" else f"Desktop {os_name}"
                style_time_axis(ax, title=f"{year} {scope} {metric_name} — {statistic} ({args.label})", ylabel=metric_name,
                                legend_loc="lower left" if metric_key == "dau_28ma" else "upper right")
                save(fig, out / f"{metric_key}_{os_name}_{year}.png")
        # small multiples for the horizon year
        fig, axes = plt.subplots(2, 2, figsize=(16, 9))
        for ax, os_name in zip(axes.ravel(), OS_ORDER):
            col = f"{metric_key}_{os_name}"
            draw_year(ax, year=seam.year + 1, current=curves[col], actual_mask=actual_mask,
                      comparison=comparison[col] if comparison is not None else None, seam=seam, ylabel=metric_name)
            style_time_axis(ax, title=f"{seam.year + 1} {('Desktop' if os_name == 'world' else os_name)} {metric_name} — {statistic}",
                            ylabel=metric_name, legend_loc="lower left" if metric_key == "dau_28ma" else "upper right")
            ax.legend(fontsize=7)
        save(fig, out / f"{metric_key}_by_os_{seam.year + 1}.png")


if __name__ == "__main__":
    main()
