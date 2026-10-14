#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Plots for the two formal comparisons of the 2027 Glean-history forecast (PRE-DRAFT, canonical format).

(1) The same model on Legacy telemetry, against the locked forecast: both histories, both forecasts, and the
    Glean-equivalent / Legacy ratio through the horizon (the translation factor, if the model is translation-invariant).
(2) The same model on REAL Glean telemetry with no simulated history (17 months from 2025-05-22), against the
    locked forecast, with the 2025 Legacy-outage windows the run masked shaded, and the real / Glean-equivalent
    ratio over the overlap (the duplicate-id and Linux terms the ``glean_equivalent`` column excludes).

Style follows ``plot_glean_history_canonical.py`` (imported): actuals black, the locked forecast green, the
comparison forecast blue, Dec 15 red dotted, seam grey dashed, two-decimal millions axis, purple PRE-DRAFT stamp.
The raw model is never drawn; every line is a delivered build.

Outputs (``<out-dir>/``): ``legacy_vs_locked_{mau,dau_28ma}_by_os.png``, ``legacy_ratio_by_os.png``,
``glean_real_vs_locked_{mau,dau_28ma}_by_os.png``, ``glean_real_ratio_by_os.png``.

Usage:
    python scripts/plot_glean_history_comparisons.py --out-dir data-official/2027-glean-draft/plots/comparisons
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "scripts"))
sys.path.insert(0, str(repo_root / "src"))
from plot_glean_history_canonical import OS_ORDER, METRICS, MEASUREMENT_DAY, millions_formatter, save, style_time_axis  # noqa: E402
from mozaic_daily.glean_history_holidays import LEGACY_OUTAGE_WINDOWS_2025  # noqa: E402

DRAFT = repo_root / "data-official/2027-glean-draft"
LOCKED_CURVES = DRAFT / "csv/glean2027_canonical_curves.csv"
LEGACY_CURVES = DRAFT / "comparison_legacy_cpr087_2026-10-10/csv/comparison_legacy_canonical_curves.csv"
GLEAN_REAL_CURVES = DRAFT / "comparison_glean_real_cpr087_2026-10-10/csv/comparison_glean_real_canonical_curves.csv"   # first run (locked knobs, mask on); history + DAU
GLEAN_REAL_MAU_CURVES = DRAFT / "comparison_glean_real_mau_additive_2026-10-13/csv/comparison_glean_real_mau_canonical_curves.csv"   # adopted MAU build
SEAM = pd.Timestamp("2026-10-10")
HORIZON_END = pd.Timestamp("2027-12-31")
COMPARISON_COLOR = "blue"


def load_curves(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates=["date"]).set_index("date").sort_index()


def split(curves: pd.DataFrame, col: str):
    """(actuals, forecast) for one column."""
    actual = curves.loc[curves["data_type"] == "training", col].dropna()
    forecast = curves.loc[curves["data_type"] == "forecast", col].dropna()
    return actual, forecast


def shade_outage(ax):
    start = min(pd.Timestamp(a) for a, _ in LEGACY_OUTAGE_WINDOWS_2025.values())
    end = max(pd.Timestamp(b) for _, b in LEGACY_OUTAGE_WINDOWS_2025.values())
    ax.axvspan(start, end, color="#e0a800", alpha=0.15, zorder=0, label="2025 Legacy-outage window (masked in every run)")


def mark_dates(ax, start: pd.Timestamp, end: pd.Timestamp = HORIZON_END):
    for year in range(start.year, end.year + 1):
        day = pd.Timestamp(year, *MEASUREMENT_DAY)
        if start <= day <= end:
            ax.axvline(day, color="red", linestyle=":", alpha=0.5, label="Dec 15" if year == start.year else None)
    ax.axvline(SEAM, color="grey", linestyle="--", alpha=0.6, label=f"Seam {SEAM.date()}")


def draw_pair(ax, *, locked, other, col, start, locked_history_label, other_history_label, other_forecast_label, shade=False, reference=None):
    """Two builds on one axis from ``start``: both histories, the locked forecast (green), the other (blue), an optional dotted reference."""
    l_act, l_fc = split(locked, col)
    o_act, o_fc = split(other, col)
    if shade:
        shade_outage(ax)
    if reference is not None:
        _, r_fc = split(reference[0], col)
        ax.plot(r_fc, color="tab:red", linewidth=0.8, linestyle=":", alpha=0.8, label=reference[1])
    ax.plot(l_act[l_act.index >= start], color="dimgrey", linewidth=1.2, alpha=0.8, label=locked_history_label)
    ax.plot(o_act[o_act.index >= start], color="black", linewidth=2, label=other_history_label)
    ax.plot(l_fc, color="green", linewidth=1.4, label="Forecast (locked build, Glean-equivalent history)")
    ax.plot(o_fc, color=COMPARISON_COLOR, linewidth=1.2, label=other_forecast_label)
    mark_dates(ax, start)
    ax.set_xlim(start - pd.Timedelta(days=3), HORIZON_END + pd.Timedelta(days=3))
    for series, color in ((l_fc, "green"), (o_fc, COMPARISON_COLOR)):
        v = series.get(pd.Timestamp(2027, *MEASUREMENT_DAY))
        if v is not None and pd.notna(v):
            ax.annotate(f"{v:,.0f}", xy=(pd.Timestamp(2027, *MEASUREMENT_DAY), v), xytext=(-70, 10 if color == "green" else -16),
                        textcoords="offset points", fontsize=8, color=color)


def pair_figure(*, locked, other, metric_key, start, labels, shade, title, out, reference=None):
    metric_name, statistic = METRICS[metric_key]
    fig, axes = plt.subplots(2, 2, figsize=(16, 9))
    for ax, os_name in zip(axes.ravel(), OS_ORDER):
        draw_pair(ax, locked=locked, other=other, col=f"{metric_key}_{os_name}", start=start, shade=shade, reference=reference, **labels)
        scope = "Desktop" if os_name == "world" else f"Desktop {os_name}"
        style_time_axis(ax, title=f"{scope} {metric_name}: {title}", ylabel=f"{metric_name}, {statistic}",
                        legend_loc="lower left" if metric_key == "dau_28ma" else "upper left")
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        ax.legend(fontsize=7)
    save(fig, out)


def ratio_figure(*, numerator, denominator, start, end, title, label, hline_label, shade, out):
    """numerator / denominator per OS for MAU and DAU, history and (if present) forecast, with the last-30-day history mean."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 5.5))
    for ax, metric_key in zip(axes, METRICS):
        metric_name, statistic = METRICS[metric_key]
        if shade:
            shade_outage(ax)
        for os_name, color in zip(OS_ORDER, ("black", "tab:blue", "tab:orange", "tab:green")):
            col = f"{metric_key}_{os_name}"
            common = numerator.index.intersection(denominator.index)
            ratio = (numerator.loc[common, col] / denominator.loc[common, col]).dropna()
            ratio = ratio[(ratio.index >= start) & (ratio.index <= end)]
            hist = ratio[ratio.index < SEAM]
            fc = ratio[ratio.index >= SEAM]
            ax.plot(hist, color=color, linewidth=1.6, label=f"{os_name} (history)")
            if not fc.empty:
                ax.plot(fc, color=color, linewidth=1.2, linestyle="--", label=f"{os_name} (forecast ÷ forecast)")
            last = hist.iloc[-30:].mean()
            ax.annotate(f"{os_name}: {last:.3f}", xy=(hist.index[-1], last), xytext=(6, 0), textcoords="offset points", fontsize=8, color=color)
        ax.axhline(1.0, color="grey", linewidth=0.8)
        mark_dates(ax, start, end)
        ax.set_xlim(start - pd.Timedelta(days=3), end + pd.Timedelta(days=3))
        ax.set_title(f"{metric_name}: {title}", fontsize=13)
        ax.set_ylabel(label)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=7, loc="upper left", title=hline_label, title_fontsize=7)
    save(fig, out)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out-dir", type=Path, default=DRAFT / "plots/comparisons")
    args = p.parse_args()
    locked, legacy, real = load_curves(LOCKED_CURVES), load_curves(LEGACY_CURVES), load_curves(GLEAN_REAL_CURVES)
    real_mau = load_curves(GLEAN_REAL_MAU_CURVES)
    out = args.out_dir

    # (1) Legacy
    legacy_labels = dict(locked_history_label="Actuals, Glean-equivalent history (locked build's input)",
                         other_history_label="Actuals, Legacy telemetry",
                         other_forecast_label="Forecast (same model on Legacy telemetry)")
    for metric_key in METRICS:
        pair_figure(locked=locked, other=legacy, metric_key=metric_key, start=pd.Timestamp("2025-01-01"), labels=legacy_labels,
                    shade=True, title="Glean-equivalent vs Legacy", out=out / f"legacy_vs_locked_{metric_key}_by_os.png")
    ratio_figure(numerator=locked, denominator=legacy, start=pd.Timestamp("2025-01-01"), end=HORIZON_END,
                 title="Glean-equivalent ÷ Legacy", label="ratio", hline_label="last 30 days of history →",
                 shade=True, out=out / "legacy_ratio_by_os.png")

    # (2) real Glean, no simulated history
    real_start = real[real["data_type"] == "training"].index.min()
    real_labels = dict(locked_history_label="Glean-equivalent history (what the locked build trained on; NOT seen by this run)",
                       other_history_label="Actuals, real Glean telemetry (all this run saw)",
                       other_forecast_label="Forecast (real Glean only: additive regime, cps 0.005, 3 changepoints, range 0.8, mask off)")
    # MAU only (Brendan, 2026-10-13); the first run (locked knobs, outage mask on) stays as a dotted reference
    pair_figure(locked=locked, other=real_mau, metric_key="mau", start=real_start, labels=real_labels, shade=True,
                title="real Glean only vs locked", out=out / "glean_real_vs_locked_mau_by_os.png",
                reference=(real, "First run: locked knobs, outage mask on (COMPARISONS §2, for the record)"))
    ratio_figure(numerator=real, denominator=locked, start=real_start, end=SEAM - pd.Timedelta(days=1),
                 title="real Glean ÷ Glean-equivalent, overlap only", label="ratio", hline_label="last 30 days →",
                 shade=True, out=out / "glean_real_ratio_by_os.png")
    if GLEAN_REAL_MAU_B_CURVES.exists():
        round5_figure(locked, real_mau, load_curves(GLEAN_REAL_MAU_B_CURVES), real_start, out / "glean_real_round5_mau_by_os.png")


GLEAN_REAL_MAU_B_CURVES = DRAFT / "comparison_glean_real_mau_additive_b_2026-10-14/csv/comparison_glean_real_mau_b_canonical_curves.csv"
LINUX_BOT_SPEC = DRAFT / "linux_bot/linux_bot.glean_history.json"


def round5_figure(locked, adopted, with_b, start, out):
    """Round 5: the adopted real-Glean build with and without the Linux bot overlay `b`, per OS, against actuals and the locked build.

    The dotted orange line is the overlay build minus its own held add-back path (the `b` MAU curve after the seam, Linux only), i.e.
    what the model forecasts for the non-bot population; comparing it with the locked (Glean-equivalent) build per OS is the point.
    """
    import json
    spec = json.loads(LINUX_BOT_SPEC.read_text())
    curve = pd.read_parquet(LINUX_BOT_SPEC.parent / spec["metrics"]["MAU"]["data_file"])[spec["metrics"]["MAU"]["value_column"]]
    curve.index = pd.DatetimeIndex(curve.index)
    fig, axes = plt.subplots(2, 2, figsize=(16, 9))
    for ax, os_name in zip(axes.ravel(), OS_ORDER):
        col = f"mau_{os_name}"
        act, _ = split(adopted, col)
        _, l_fc = split(locked, col)
        _, a_fc = split(adopted, col)
        _, b_fc = split(with_b, col)
        shade_outage(ax)
        ax.plot(act[act.index >= start], color="black", linewidth=2, label="Actuals, real Glean")
        ax.plot(l_fc, color="green", linewidth=1.4, label="Locked build (Glean-equivalent history)")
        ax.plot(a_fc, color=COMPARISON_COLOR, linewidth=1.1, label="Adopted real-Glean build (no bot overlay)")
        ax.plot(b_fc, color="tab:orange", linewidth=1.3, label="With Linux bot overlay `b` (held flat add-back)")
        if os_name in ("world", "linux"):
            underlying = b_fc - curve.reindex(b_fc.index).fillna(0.0)
            ax.plot(underlying, color="tab:orange", linewidth=1.1, linestyle=":", label="… minus the add-back: the non-bot population")
        mark_dates(ax, start)
        ax.set_xlim(start - pd.Timedelta(days=3), HORIZON_END + pd.Timedelta(days=3))
        for series, color, dy in ((l_fc, "green", 10), (a_fc, COMPARISON_COLOR, -14), (b_fc, "tab:orange", 24)):
            v = series.get(pd.Timestamp(2027, *MEASUREMENT_DAY))
            if v is not None and pd.notna(v):
                ax.annotate(f"{v:,.0f}", xy=(pd.Timestamp(2027, *MEASUREMENT_DAY), v), xytext=(-70, dy), textcoords="offset points", fontsize=8, color=color)
        scope = "Desktop" if os_name == "world" else f"Desktop {os_name}"
        style_time_axis(ax, title=f"{scope} MAU: round 5, Linux bot overlay", ylabel="MAU, raw daily", legend_loc="upper left")
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3)); ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        ax.legend(fontsize=7)
    save(fig, out)


if __name__ == "__main__":
    main()
