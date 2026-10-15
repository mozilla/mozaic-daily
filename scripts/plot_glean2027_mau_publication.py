#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Doc-ready progress plots for the 2027 MAU pre-draft: desktop, mobile total, and desktop + mobile combined.

Reads the side-by-side handoff file (``handoff/glean2027_mau_desktop_and_mobile.csv``: desktop headline build beside the
mobile TOTAL = organic + paid) and draws Jan 2026 → Dec 2027 on one continuous axis, so the remainder of 2026 and all of
2027 sit in one view. Conventions follow ``plot_glean_history_canonical.py`` (imported): actuals black, forecast green,
Dec 15 red dotted, seam grey dashed, two-decimal millions axis, the purple PRE-DRAFT stamp applied in ``save``.

Seam gaps are shown, not hidden: actuals and forecast are separate lines with nothing bridging them, and each panel
states the seam step (mean of the first 7 forecast days vs the last 7 actual days, the LOCK.md definition). A zoomed
figure around the seam makes the step legible.

The combined figure sums platform MAU. MAU is the official proxy for monthly active profiles, and desktop and mobile
profiles are different things, so the sum is a valid profile count (Brendan's manager, 2026-10-15). The figure states
the definition so readers do not take it for a count of people.

Outputs (``<out-dir>/``): ``desktop_mau_2026_2027.png``, ``mobile_mau_2026_2027.png``, ``combined_mau_2026_2027.png``,
``seam_zoom_2026.png``, ``mau_overview_2026_2027.png`` (three stacked panels), ``numbers.csv``, and ``index.html`` (review page
with a freshness banner, regenerated on every run).

Usage:
    python scripts/plot_glean2027_mau_publication.py \\
        --curves data-official/2027-glean-draft/handoff/glean2027_mau_desktop_and_mobile.csv \\
        --out-dir data-official/2027-glean-draft/plots/publication
"""

from __future__ import annotations

import argparse
import html
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "scripts"))
from plot_glean_history_canonical import millions_formatter, save  # noqa: E402

VIEW_START = pd.Timestamp("2026-01-01")
VIEW_END = pd.Timestamp("2027-12-31")
MEASUREMENT_DATES = [pd.Timestamp("2026-12-15"), pd.Timestamp("2027-12-15")]
SEAM_WINDOW_DAYS = 7
ZOOM_START = pd.Timestamp("2026-09-01")
ZOOM_END = pd.Timestamp("2026-11-15")
DESKTOP_FILL = "#4a7fb5"
MOBILE_FILL = "#e08a3c"
VALUES_BOX_AT = {"desktop": (0.62, 0.03), "mobile": (0.99, 0.03), "combined": (0.99, 0.12)}   # clear of each curve's trough
PROFILE_NOTE = "MAU = monthly active profiles. Desktop and mobile profiles are distinct, so they add."


def load_platform_series(curves: pd.DataFrame) -> dict[str, tuple[pd.Series, pd.Series]]:
    """Return {platform: (values, is_actual)} for desktop, mobile and their sum, restricted to the dates both cover."""
    desktop_actual = curves["desktop_data_type"] == "training"
    mobile_actual = curves["mobile_data_type"] == "training"
    both_present = curves["desktop_data_type"].notna() & curves["mobile_data_type"].notna()   # desktop history starts 2025-05-22
    if not desktop_actual[both_present].equals(mobile_actual[both_present]):
        mismatched = curves.index[both_present & (desktop_actual != mobile_actual)]
        raise ValueError(f"desktop and mobile seams differ on {len(mismatched)} dates (first {mismatched.min().date()}); "
                         "the combined series needs one seam")
    combined = (curves["desktop_mau"] + curves["mobile_mau"]).where(curves["desktop_mau"].notna() & curves["mobile_mau"].notna())
    return {
        "desktop": (curves["desktop_mau"], desktop_actual),
        "mobile": (curves["mobile_mau"], mobile_actual),
        "combined": (combined, desktop_actual),
    }


def seam_step(values: pd.Series, is_actual: pd.Series) -> tuple[pd.Timestamp, float]:
    """Seam date and fractional step: first 7 forecast days' mean over the last 7 actual days' mean, minus one."""
    actual = values[is_actual].dropna()
    forecast = values[~is_actual].dropna()
    step = forecast.iloc[:SEAM_WINDOW_DAYS].mean() / actual.iloc[-SEAM_WINDOW_DAYS:].mean() - 1
    return forecast.index.min(), step


def draw_platform(ax, values: pd.Series, is_actual: pd.Series, *, start: pd.Timestamp, end: pd.Timestamp,
                  annotate_measurements: bool = True, values_box_at: tuple[float, float] = (0.99, 0.03)) -> None:
    window = (values.index >= start) & (values.index <= end)
    actual = values[window & is_actual]
    forecast = values[window & ~is_actual]
    seam, step = seam_step(values, is_actual)
    ax.plot(actual, color="black", linewidth=2, label="Actuals (real Glean)")
    ax.plot(forecast, color="green", linewidth=1.3, label="Forecast (pre-draft)")
    ax.axvline(seam, color="grey", linestyle="--", alpha=0.6, label=f"Seam {seam.date()}: step {step:+.1%} (7-day means)")
    if not annotate_measurements:
        return
    # Dec-15 values go in one corner box rather than leader-line labels, which collided with the curves.
    first, last = MEASUREMENT_DATES
    for measurement in MEASUREMENT_DATES:
        ax.axvline(measurement, color="red", linestyle=":", alpha=0.5)
        ax.plot(measurement, values[measurement], "o", color="red", markersize=6, zorder=5)
    box_text = (f"Dec 15 {first.year}: {values[first]:,.0f}\n"
                f"Dec 15 {last.year}: {values[last]:,.0f}  ({values[last] / values[first] - 1:+.1%})")
    x, y = values_box_at
    ax.text(x, y, box_text, transform=ax.transAxes, ha="right", va="bottom" if y < 0.5 else "top", fontsize=10,
            bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor="red", alpha=0.9))


def style_axis(ax, *, title: str, start: pd.Timestamp, end: pd.Timestamp, month_format: str = "%b %Y") -> None:
    ax.set_title(title, fontsize=13)
    ax.set_ylabel("MAU")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(millions_formatter))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter(month_format))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")
    ax.set_xlim(start - pd.Timedelta(days=3), end + pd.Timedelta(days=3))
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", fontsize=9)


PLATFORM_TITLES = {
    "desktop": "Desktop MAU (Firefox Desktop, all countries)",
    "mobile": "Mobile MAU (four apps, organic + marketing's paid plan)",
    "combined": "Desktop + Mobile MAU (active profiles, summed)",
}


def plot_single(series, platform: str, out_dir: Path) -> None:
    values, is_actual = series[platform]
    fig, ax = plt.subplots(figsize=(14, 6))
    draw_platform(ax, values, is_actual, start=VIEW_START, end=VIEW_END, values_box_at=VALUES_BOX_AT[platform])
    style_axis(ax, title=f"{PLATFORM_TITLES[platform]}, 2026–2027", start=VIEW_START, end=VIEW_END)
    save(fig, out_dir / f"{platform}_mau_2026_2027.png")


def plot_combined_stacked(series, out_dir: Path) -> None:
    """The combined total as a line, with desktop and mobile stacked beneath it so the composition is visible."""
    desktop, is_actual = series["desktop"]
    mobile, _ = series["mobile"]
    combined, _ = series["combined"]
    fig, ax = plt.subplots(figsize=(14, 6))
    for mask, alpha, suffix in [(is_actual, 0.55, "actuals"), (~is_actual, 0.25, "forecast")]:
        in_view = mask & (desktop.index >= VIEW_START) & (desktop.index <= VIEW_END)
        d, m = desktop[in_view], mobile[in_view]
        ax.fill_between(d.index, 0, d, color=DESKTOP_FILL, alpha=alpha, linewidth=0, label=f"Desktop ({suffix})")
        ax.fill_between(d.index, d, d + m, color=MOBILE_FILL, alpha=alpha, linewidth=0, label=f"Mobile ({suffix})")
    draw_platform(ax, combined, is_actual, start=VIEW_START, end=VIEW_END, values_box_at=(0.99, 0.55))
    style_axis(ax, title=f"{PLATFORM_TITLES['combined']}, 2026–2027", start=VIEW_START, end=VIEW_END)
    ax.set_ylim(0, combined[(combined.index >= VIEW_START)].max() * 1.12)
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    ax.text(0.99, 0.03, PROFILE_NOTE, transform=ax.transAxes, ha="right", va="bottom", fontsize=9, style="italic",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="none", alpha=0.85))
    save(fig, out_dir / "combined_mau_2026_2027.png")


def plot_overview(series, out_dir: Path) -> None:
    fig, axes = plt.subplots(3, 1, figsize=(14, 15), sharex=True)
    for ax, platform in zip(axes, ["desktop", "mobile", "combined"]):
        values, is_actual = series[platform]
        draw_platform(ax, values, is_actual, start=VIEW_START, end=VIEW_END, values_box_at=VALUES_BOX_AT[platform])
        style_axis(ax, title=PLATFORM_TITLES[platform], start=VIEW_START, end=VIEW_END)
    axes[-1].text(0.99, 0.02, PROFILE_NOTE, transform=axes[-1].transAxes, ha="right", va="bottom", fontsize=9, style="italic")
    save(fig, out_dir / "mau_overview_2026_2027.png")


def plot_seam_zoom(series, out_dir: Path) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
    for ax, platform in zip(axes, ["desktop", "mobile", "combined"]):
        values, is_actual = series[platform]
        draw_platform(ax, values, is_actual, start=ZOOM_START, end=ZOOM_END, annotate_measurements=False)
        style_axis(ax, title=f"{PLATFORM_TITLES[platform].split(' (')[0]}: seam close-up", start=ZOOM_START, end=ZOOM_END,
                   month_format="%b %d")
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO, interval=2))
        ax.legend(loc="lower left", fontsize=8)
    save(fig, out_dir / "seam_zoom_2026.png")


def summarize_numbers(series) -> pd.DataFrame:
    rows = []
    for platform, (values, is_actual) in series.items():
        seam, step = seam_step(values, is_actual)
        actual = values[is_actual].dropna()
        rows.append({
            "population": platform,
            "last_actual_date": actual.index.max().date(),
            "last_actual_mau": int(actual.iloc[-1]),
            "seam": seam.date(),
            "seam_step_7d": round(step, 5),
            "mau_2026_12_15": int(values[MEASUREMENT_DATES[0]]),
            "mau_2027_12_15": int(values[MEASUREMENT_DATES[1]]),
            "dec_to_dec": round(values[MEASUREMENT_DATES[1]] / values[MEASUREMENT_DATES[0]] - 1, 5),
        })
    return pd.DataFrame(rows)


FIGURES = [
    ("mau_overview_2026_2027.png", "Overview: desktop, mobile, and the sum, Jan 2026 to Dec 2027 on a shared axis."),
    ("desktop_mau_2026_2027.png", "Desktop MAU. Real Glean desktop telemetry, 17 months of history; the 2027 dip in January repeats the "
                                  "Dec-2025 duplicate-id step the model learned as seasonality (not corrected, by choice)."),
    ("mobile_mau_2026_2027.png", "Mobile MAU, all four apps: our organic forecast plus the marketing data-science team's 2027 paid plan."),
    ("combined_mau_2026_2027.png", "Desktop + Mobile, summed, with each platform stacked underneath. MAU counts active profiles, and desktop and mobile profiles are distinct, so they add."),
    ("seam_zoom_2026.png", "Seam close-up, Sep 1 to Nov 15 2026. The forecast is not bridged to the actuals; the gap is what the "
                           "model gives today."),
]


def write_review_page(out_dir: Path, numbers: pd.DataFrame, curves_path: Path) -> Path:
    """Self-contained review page; the banner is rebuilt on every run so stale browser tabs are recognisable."""
    generated = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, cwd=repo_root).stdout.strip()
    data_end = numbers["last_actual_date"].max()
    rows = "".join(
        f"<tr><td>{html.escape(str(r.population))}</td><td>{r.last_actual_mau:,}</td><td>{r.seam_step_7d:+.1%}</td>"
        f"<td>{r.mau_2026_12_15:,}</td><td>{r.mau_2027_12_15:,}</td><td>{r.dec_to_dec:+.1%}</td></tr>"
        for r in numbers.itertuples())
    figures = "".join(f'<figure><img src="{name}" alt="{name}"><figcaption><b>{name}</b> — {html.escape(caption)}</figcaption></figure>'
                      for name, caption in FIGURES)
    page = f"""<!doctype html><html><head><meta charset="utf-8"><title>2027 MAU pre-draft — progress plots</title>
<style>
body {{ font-family: -apple-system, Helvetica, Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 0 24px 48px; color: #222; }}
.banner {{ background: #6a1b9a; color: white; padding: 10px 16px; margin: 0 -24px 24px; font-size: 14px; }}
.banner b {{ color: #f3e5f5; }}
.callout {{ background: #f3e5f5; border-left: 5px solid #6a1b9a; padding: 10px 14px; margin: 16px 0; }}
table {{ border-collapse: collapse; margin: 12px 0; }} td, th {{ border: 1px solid #ccc; padding: 4px 10px; text-align: right; }}
td:first-child, th:first-child {{ text-align: left; }} th {{ background: #eee; }}
figure {{ margin: 28px 0; }} img {{ width: 100%; border: 1px solid #ddd; }} figcaption {{ font-size: 14px; color: #444; margin-top: 6px; }}
code {{ background: #f4f4f4; padding: 1px 4px; }}
</style></head><body>
<div class="banner"><b>Generated:</b> {generated} &nbsp;|&nbsp; <b>Source:</b> scripts/plot_glean2027_mau_publication.py @ {commit}
&nbsp;|&nbsp; <b>Data as of:</b> {data_end} (last actual; forecast from the next day)</div>
<h1>2027 MAU forecast — progress plots (PRE-DRAFT)</h1>
<div class="callout"><b>Progress, not a forecast.</b> Both halves are pre-draft: neither is backtested, the mobile India/Indonesia
correction is provisional, and the seam steps below are shown as they are, not smoothed. The combined line adds desktop and mobile
MAU: MAU is the official proxy for monthly active <i>profiles</i>, and desktop and mobile profiles are distinct, so they add.</div>
<p>Input: <code>{html.escape(str(curves_path))}</code> (desktop headline build beside the mobile total). PNGs sit beside this page, ready to paste into a doc.</p>
<table><tr><th>population</th><th>last actual MAU</th><th>seam step (7-day means)</th><th>Dec 15 2026</th><th>Dec 15 2027</th><th>Dec→Dec</th></tr>{rows}</table>
{figures}
</body></html>"""
    path = out_dir / "index.html"
    path.write_text(page)
    print(f"saved: {path}")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--curves", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    curves = pd.read_csv(args.curves, parse_dates=["date"]).set_index("date")
    series = load_platform_series(curves)
    for platform in ["desktop", "mobile"]:
        plot_single(series, platform, args.out_dir)
    plot_combined_stacked(series, args.out_dir)
    plot_overview(series, args.out_dir)
    plot_seam_zoom(series, args.out_dir)
    numbers = summarize_numbers(series)
    numbers.to_csv(args.out_dir / "numbers.csv", index=False)
    print(numbers.to_string(index=False))
    write_review_page(args.out_dir, numbers, args.curves)


if __name__ == "__main__":
    main()
