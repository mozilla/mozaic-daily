"""Figures for the seasonality pane: Prophet's seasonality vs history vs what happened.

Drawn the same way as the published curves on the other tab — absolute DAU, 28-day trailing mean —
so the two panes read alike. Each curve is its own seasonal shape rescaled to 2026's
Feb 15 - Apr 15 level, so the three are directly comparable in DAU while carrying none of the
year-over-year size differences that would otherwise dominate.

Run: python research/forecast-vs-summer-actuals/seasonality_plots.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import seasonality as SZ  # noqa: E402
import series as S  # noqa: E402
from plots import (  # noqa: E402
    ACTUAL,
    FORECAST,
    HEADWIND,
    INK,
    INK_2,
    MUTED,
    PLOTS,
    SURFACE,
    TRACK_LABEL,
    TYPICAL,
    _save,
    _style,
)

MODEL, HISTORY, REALISED = FORECAST, TYPICAL, ACTUAL
Y_LABEL = "Desktop DAU · 28-day trailing mean"
ANCHOR = (pd.Timestamp("2026-02-15"), pd.Timestamp("2026-04-15"))
SUMMER_ZOOM = (pd.Timestamp("2026-07-01"), pd.Timestamp("2026-10-15"))
VINTAGE_LABEL = {"august": "August canonical", "july": "July canonical"}

SPEC = [
    ("history", HISTORY, "2022–25 average seasonality"),
    ("model", MODEL, "Prophet's seasonality"),
    ("realised", REALISED, "2026 realised"),
]
END_LABEL = {"history": "2022–25 average", "model": "Prophet", "realised": "actual"}


def _to_dates(series: pd.Series) -> pd.Series:
    """Map a 'MM-DD' index onto 2026 dates so matplotlib draws a real time axis."""
    return pd.Series(series.to_numpy(), index=pd.to_datetime("2026-" + series.index))


def _curves(vintage: str, track: str, reconciled: bool = True,
            anchor: str = "spring") -> dict[str, pd.Series]:
    builder = SZ.spring_normalised if anchor == "spring" else SZ.seam_normalised
    c = builder(vintage, track, reconciled)
    return {k: _to_dates(c[k]).dropna() for k in ("model", "history", "realised")}


def _millions(decimals: int = 1) -> FuncFormatter:
    return FuncFormatter(lambda v, _: f"{v / 1e6:.{decimals}f}M")


def _draw(ax, curves, window=None, end_labels=True, label_offsets=None):
    label_offsets = label_offsets or {}
    for key, color, legend in SPEC:
        data = curves[key]
        if window is not None:
            data = data.loc[window[0]:window[1]]
        if data.empty:
            continue
        ax.plot(data.index, data.to_numpy(), color=color, linewidth=2.0, label=legend, zorder=3)
        if end_labels:
            ax.annotate(
                f"  {END_LABEL[key]}",
                xy=(data.index[-1], data.iloc[-1]),
                xytext=(6, label_offsets.get(key, 0)), textcoords="offset points",
                va="center", ha="left", fontsize=9, color=color, fontweight="medium",
            )
    ax.yaxis.set_major_formatter(_millions(1))


def _shade_anchor(ax):
    ax.axvspan(*ANCHOR, color="#f0efec", zorder=0)
    ax.annotate("anchor\nFeb 15 – Apr 15", xy=(ANCHOR[0] + (ANCHOR[1] - ANCHOR[0]) / 2, 0.975),
                xycoords=("data", "axes fraction"), ha="center", va="top",
                fontsize=8.5, color=MUTED)


FOOTNOTE = (
    "Each curve is its own seasonal shape rescaled to 2026's Feb 15 – Apr 15 level, so none of the "
    "year-over-year decline in size is in play. Prophet's curve is its {year} cycle standing in "
    "for 2026 —\nvalid because its seasonality repeats to {drift:.2f}pp, but its HOLIDAYS do not "
    "(up to 3.95pp), so December is the least reliable part and Prophet's Easter sits at {year}'s "
    "date. Actuals stop at the last landed day.\nThe steep fall in late December and the climb "
    "through January are the same event — Christmas passing into and out of the 28-day trailing "
    "window. Both year boundaries are dominated by it; the summer is not."
)


def fig_seasonality(vintage: str) -> Path:
    """One vintage, both population tracks, the full seasonal year."""
    fig, axes = plt.subplots(2, 1, figsize=(11.5, 8.6), facecolor=SURFACE, sharex=True)
    for ax, track in zip(axes, S.TRACKS):
        _style(ax, Y_LABEL)
        _shade_anchor(ax)
        _draw(ax, _curves(vintage, track),
              label_offsets={"realised": 11, "history": -9, "model": 9})
        ax.set_xlim(pd.Timestamp("2026-01-01"), pd.Timestamp("2027-01-22"))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.set_title(TRACK_LABEL[track], color=INK, fontsize=10.5,
                     fontweight="semibold", loc="left")
    axes[0].legend(frameon=False, fontsize=9, labelcolor=INK_2, ncols=3,
                   loc="lower left", bbox_to_anchor=(0, 1.14))
    fig.suptitle(
        f"{VINTAGE_LABEL[vintage]} — the seasonal year, every curve rescaled to 2026's spring level",
        color=INK, fontsize=12.5, fontweight="semibold", x=0.005, ha="left", y=0.995,
    )
    fig.text(0.005, -0.012,
             FOOTNOTE.format(year=SZ.MODEL_CYCLE_YEAR, drift=SZ.PERIODICITY_DRIFT_PP),
             fontsize=8.5, color=MUTED, ha="left", linespacing=1.5)
    return _save(fig, f"seasonality_{vintage}.png")


SEAM_FOOTNOTE = (
    "Every curve is rescaled to 2026's actual level just after the seam, so all three start from "
    "where the forecast began and the fan-out is each one's seasonal trajectory.\n"
    "This view needs NO 2027 stand-in — it lies entirely inside the 2026 portion of the forecast "
    "window — and its anchor is a settled actual after Iran's recovery, so it carries none of the\n"
    "Iran contamination that affects the spring anchor's all-countries track. It smooths on a "
    "CENTRED 7-DAY window anchored at seam+3, which is forced: the model has no output before its\n"
    "seam, so a 28-day trailing mean is undefined for 27 days and filling that window with actuals "
    "makes the model curve inherit actuals' own trajectory. Noisier than 28 days as a result.\n"
    "What it gives up: it starts mid-summer, so it cannot see the spring-to-summer descent — which "
    "is where most of a vintage's seasonal error accumulates. The spring-anchored charts show that."
)


def fig_seasonality_seam(vintage: str) -> Path:
    """One vintage, both tracks, seam-anchored — judged on what it set out to predict."""
    fig, axes = plt.subplots(2, 1, figsize=(11.5, 8.6), facecolor=SURFACE, sharex=True)
    seam = S.VINTAGES[vintage]["seam"]
    for ax, track in zip(axes, S.TRACKS):
        _style(ax, "Desktop DAU · centred 7-day mean")
        _draw(ax, _curves(vintage, track, anchor="seam"),
              label_offsets={"realised": 11, "history": -9, "model": 9})
        ax.axvline(seam, color=MUTED, linewidth=1.0, linestyle=(0, (3, 3)), zorder=2)
        ax.annotate("seam", xy=(seam, 0.975), xycoords=("data", "axes fraction"),
                    xytext=(4, 0), textcoords="offset points", va="top",
                    fontsize=8.5, color=MUTED)
        ax.set_xlim(seam - pd.Timedelta(days=6), pd.Timestamp("2027-01-22"))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.set_title(TRACK_LABEL[track], color=INK, fontsize=10.5,
                     fontweight="semibold", loc="left")
    axes[0].legend(frameon=False, fontsize=9, labelcolor=INK_2, ncols=3,
                   loc="lower left", bbox_to_anchor=(0, 1.14))
    fig.suptitle(
        f"{VINTAGE_LABEL[vintage]} — seasonality from the seam forward, all curves starting together",
        color=INK, fontsize=12.5, fontweight="semibold", x=0.005, ha="left", y=0.995,
    )
    fig.text(0.005, -0.030, SEAM_FOOTNOTE, fontsize=8.5, color=MUTED, ha="left", linespacing=1.5)
    return _save(fig, f"seasonality_seam_{vintage}.png")


def fig_summer_zoom() -> Path:
    """Both vintages, ex-IR/CN, zoomed to where the summer question is decided."""
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6), facecolor=SURFACE, sharey=True)
    for ax, vintage in zip(axes, ("august", "july")):
        _style(ax)
        _draw(ax, _curves(vintage, "ex_ir_cn"), window=SUMMER_ZOOM, end_labels=False)
        ax.set_xlim(*SUMMER_ZOOM)
        seam = S.VINTAGES[vintage]["seam"]
        ax.axvline(seam, color=MUTED, linewidth=1.0, linestyle=(0, (3, 3)), zorder=2)
        ax.annotate("seam", xy=(seam, 0.02), xycoords=("data", "axes fraction"),
                    xytext=(4, 0), textcoords="offset points", fontsize=8.5, color=MUTED)
        ax.xaxis.set_major_locator(mdates.DayLocator(bymonthday=(1, 15)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.set_title(f"{VINTAGE_LABEL[vintage]}  (seam {seam.date()})", color=INK,
                     fontsize=10.5, fontweight="semibold", loc="left")
    axes[0].set_ylabel(Y_LABEL, color=INK_2, fontsize=9.5)
    axes[0].legend(frameon=False, fontsize=9, labelcolor=INK_2, ncols=3,
                   loc="lower left", bbox_to_anchor=(0, -0.30))
    sz = SZ.summary()
    aug = sz["vintages"]["august"]["tracks"]["ex_ir_cn"]
    jul = sz["vintages"]["july"]["tracks"]["ex_ir_cn"]
    fig.suptitle(
        f"The summer window, ex-Iran/ex-China — August's trough is "
        f"{abs(aug['model_vs_history']) / 1e3:,.0f}K shallower than history, July's is "
        f"{abs(jul['model_vs_history']) / 1e6:.2f}M deeper",
        color=INK, fontsize=12.5, fontweight="semibold", x=0.005, ha="left", y=1.04,
    )
    return _save(fig, "seasonality_summer_zoom.png")


def fig_reconciliation() -> Path:
    """Reconciled vs pre-reconciliation model seasonality, on its own axes."""
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.4), facecolor=SURFACE, sharey=True)
    for ax, vintage in zip(axes, ("august", "july")):
        _style(ax)
        rec = _curves(vintage, "all", reconciled=True)["model"]
        pre = _curves(vintage, "all", reconciled=False)["model"]
        ax.plot(rec.index, rec.to_numpy(), color=MODEL, linewidth=2.0,
                label="reconciled (what shipped)", zorder=3)
        ax.plot(pre.index, pre.to_numpy(), color=HEADWIND, linewidth=1.8,
                linestyle=(0, (6, 3)), label="pre-reconciliation (per-tile Prophet)", zorder=3)
        ax.yaxis.set_major_formatter(_millions(1))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        gap = (rec - pre).dropna()
        ax.set_title(
            f"{VINTAGE_LABEL[vintage]}   reconciliation moves it by up to "
            f"{gap.abs().max() / 1e6:.2f}M",
            color=INK, fontsize=10.5, fontweight="semibold", loc="left",
        )
    axes[0].set_ylabel(Y_LABEL, color=INK_2, fontsize=9.5)
    axes[0].legend(frameon=False, fontsize=9, labelcolor=INK_2, ncols=2,
                   loc="lower left", bbox_to_anchor=(0, -0.30))
    fig.suptitle(
        "Top-down reconciliation reshapes the seasonality the tiles produced — all countries",
        color=INK, fontsize=12.5, fontweight="semibold", x=0.005, ha="left", y=1.04,
    )
    return _save(fig, "seasonality_reconciliation.png")


def main() -> None:
    print("Building seasonality figures...")
    PLOTS.mkdir(parents=True, exist_ok=True)
    for vintage in ("august", "july"):
        fig_seasonality(vintage)
        fig_seasonality_seam(vintage)
    fig_summer_zoom()
    fig_reconciliation()
    print("Done.")


if __name__ == "__main__":
    main()
