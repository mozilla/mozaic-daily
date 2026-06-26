"""Render june-canonical-style per-country DAU plots from the csv/per_country exports.

For each country CSV in csv/per_country/, plots the three 28dMA series for the chosen
platform — actuals, prior April forecast, current June forecast (forecast-only) — in
the same visual style as june_canonical_v2026-05-27.ipynb's plot-desktop cell, minus
the world-level stakeholder markers and the plus/no-Iran split (per-country files are
raw, single-series, no synthetic Iran, no headwind).

Writes one PNG per country to csv/per_country/plots/<platform>/ plus a combined grid
PNG (csv/per_country/plots/<platform>_grid.png) for at-a-glance comparison.

    source .venv/bin/activate && python3 data-official/2026-06/plot_per_country_curves.py
    source .venv/bin/activate && python3 data-official/2026-06/plot_per_country_curves.py --platform mobile
"""

import argparse
import glob
import math
import os
import re
import subprocess

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

PER_COUNTRY_DIR = "data-official/2026-06/csv/per_country"
PLOTS_DIR = f"{PER_COUNTRY_DIR}/plots"

DISPLAY_START = pd.Timestamp("2026-01-01")
DISPLAY_END = pd.Timestamp("2026-12-31")
FORECAST_START = pd.Timestamp("2026-05-26")
MEASUREMENT_DATE = pd.Timestamp("2026-12-15")

# Visual style mirrors the notebook's plot-desktop cell.
SERIES_STYLE = {
    "actuals": dict(label="Actuals", color="black", linewidth=2),
    "prior_april": dict(label="Prior (Apr 2026)", color="blue", linewidth=1, linestyle="--"),
    "current_june": dict(label="Current (Jun 2026)", color="orange", linewidth=1, linestyle="--"),
}


def country_from_path(path):
    """Extract the country code from a per-country CSV filename."""
    match = re.search(r"june_canonical_curves\.([A-Z]+)\.no-headwinds\.csv$", os.path.basename(path))
    return match.group(1) if match else os.path.basename(path)


def load_country_series(path, platform):
    """Return the three date-indexed 28dMA series for one country/platform."""
    df = pd.read_csv(path, parse_dates=["date"]).set_index("date")
    return {
        "actuals": df[f"{platform}_actuals"].dropna(),
        "prior_april": df[f"{platform}_prior_april"].dropna(),
        "current_june": df[f"{platform}_current_june"].dropna(),
    }


def make_dau_formatter(plotted_values):
    """Pick a unit (M/K) and decimal precision fine enough that adjacent ticks differ."""
    vmax = max(plotted_values)
    vmin = min(plotted_values)
    span = vmax - vmin
    if vmax >= 1e6:
        unit, divisor = "M", 1e6
    elif vmax >= 1e3:
        unit, divisor = "K", 1e3
    else:
        unit, divisor = "", 1.0
    # Aim for one significant figure finer than the ~5-tick step.
    step = (span / divisor) / 5 if span > 0 else (vmax / divisor)
    decimals = 1 if step <= 0 else min(3, max(0, -int(math.floor(math.log10(step))) + 1))
    return mticker.FuncFormatter(lambda v, _: f"{v / divisor:.{decimals}f}{unit}")


def style_axis(ax, series, title, *, legend_fontsize=9):
    """Plot the three series onto ax in canonical style and apply shared formatting."""
    for key, style in SERIES_STYLE.items():
        data = series[key]
        if not data.empty:
            ax.plot(data.index, data.values, **style)
    ax.axvline(MEASUREMENT_DATE, color="red", linestyle=":", alpha=0.5, label="Dec 15")

    plotted = pd.concat([series["actuals"], series["prior_april"], series["current_june"]])
    ax.yaxis.set_major_formatter(make_dau_formatter(plotted.values))
    ax.set_title(title, fontsize=14)
    ax.set_ylabel("DAU")
    ax.set_xlim(DISPLAY_START, DISPLAY_END)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="lower left", fontsize=legend_fontsize)


def render_per_country(platform):
    """Render one PNG per country plus a combined grid for the given platform."""
    paths = sorted(glob.glob(f"{PER_COUNTRY_DIR}/*.no-headwinds.csv"))
    if not paths:
        raise FileNotFoundError(f"No per-country CSVs found in {PER_COUNTRY_DIR}")

    out_dir = f"{PLOTS_DIR}/{platform}"
    os.makedirs(out_dir, exist_ok=True)

    countries = []
    for path in paths:
        country = country_from_path(path)
        countries.append((country, load_country_series(path, platform)))

        fig, ax = plt.subplots(figsize=(14, 6))
        style_axis(ax, countries[-1][1],
                   f"{country} — 2026 {platform.title()} DAU (28-Day MA, raw / no headwind)")
        fig.tight_layout()
        fig.savefig(f"{out_dir}/{country}.png", dpi=110)
        plt.close(fig)

    # Combined grid (5 rows x 3 cols covers 15 countries).
    n = len(countries)
    ncols = 3
    nrows = math.ceil(n / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 4 * nrows))
    axes = axes.flatten()
    for ax, (country, series) in zip(axes, countries):
        style_axis(ax, series, country, legend_fontsize=6)
    for ax in axes[n:]:
        ax.axis("off")
    fig.suptitle(f"2026 {platform.title()} DAU — 28-Day MA, raw per country (no headwind)",
                 fontsize=16, y=1.005)
    fig.tight_layout()
    grid_path = f"{PLOTS_DIR}/{platform}_grid.png"
    fig.savefig(grid_path, dpi=100, bbox_inches="tight")
    plt.close(fig)

    print(f"Wrote {n} per-country PNGs to {out_dir}/")
    print(f"Wrote combined grid to {grid_path}")
    return grid_path


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--platform", choices=["desktop", "mobile"], default="desktop")
    args = parser.parse_args()

    git_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    ).stdout.strip()
    os.chdir(git_root)

    render_per_country(args.platform)


if __name__ == "__main__":
    main()
