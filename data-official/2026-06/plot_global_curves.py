"""Render the global (ALL-level) desktop & mobile DAU plots from the canonical CSV.

These are the headline, stakeholder-facing charts: the 6 ALL-level series from
`csv/june_canonical_curves.csv` (actuals all / excl-IR, prior April ±Iran, current June
±Iran — headwind + marketing + Iran already baked in) plus the gold Dec-15 stakeholder
markers (low / baseline / stretch). Visual style mirrors the notebook's plot-desktop /
plot-mobile cells. Unlike `plot_per_country_curves.py` (raw, no-headwind, per country),
this is the world rollup that carries the adjustments.

    source .venv/bin/activate && python3 data-official/2026-06/plot_global_curves.py

Writes:
    csv/plots/global_desktop.png
    csv/plots/global_mobile.png
"""

import json
import os
import subprocess

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

CSV_PATH = "data-official/2026-06/csv/june_canonical_curves.csv"
PLOTS_DIR = "data-official/2026-06/csv/plots"
MOBILE_TARGETS_JSON = "data-official/2026-06/stakeholder_scenarios/data/stakeholder_targets.json"
MEASUREMENT_DATE = pd.Timestamp("2026-12-15")

# Desktop stakeholder Dec-15 benchmarks. These live only in the canonical notebook
# (no JSON), so they are mirrored here; keep in sync with june_canonical_v2026-05-27.ipynb.
DESKTOP_TARGETS = {"low": 49_039_852, "base": 49_513_157, "stretch": 49_772_388}

# (csv column suffix, label, color, linestyle) — order matches the notebook's legend.
SERIES = [
    ("actuals_all_countries", "Actuals (all countries)", "black", "-"),
    ("actuals_excl_ir", "Actuals (excluding country:IR)", "gray", "--"),
    ("prior_april_plus_iran", "Prior +Iran (Apr 2026)", "blue", "-"),
    ("prior_april_no_iran", "Prior No-Iran (Apr 2026)", "blue", "--"),
    ("current_june_plus_iran", "Current +Iran (Jun 2026)", "green", "-"),
    ("current_june_no_iran", "Current No-Iran (Jun 2026)", "orange", "--"),
]


def millions_formatter(x, _pos):
    return f"{x / 1e6:.0f}M"


def render_platform(df, platform, targets, legend_loc):
    """Render one platform's global plot to csv/plots/global_<platform>.png."""
    fig, ax = plt.subplots(figsize=(14, 6))
    for suffix, label, color, ls in SERIES:
        col = f"{platform}_{suffix}"
        s = df[col].dropna()
        lw = 2 if "actuals" in suffix else 1
        ax.plot(s.index, s.values, label=label, color=color, linewidth=lw, linestyle=ls)

    # Stakeholder markers — stretch=diamond, baseline=down-triangle, low=up-triangle.
    for value, marker, label in [
        (targets["stretch"], "D", "Stretch"),
        (targets["base"], "v", "Baseline"),
        (targets["low"], "^", "Low"),
    ]:
        ax.plot(MEASUREMENT_DATE, value, marker=marker, color="gold", markersize=12,
                markeredgecolor="black", markeredgewidth=0.8, linestyle="None",
                label=f"{label} ({value:,})")
    ax.axvline(MEASUREMENT_DATE, color="red", linestyle=":", alpha=0.5, label="Dec 15")

    ax.set_title(f"2026 {platform.title()} DAU — 28-Day Moving Average (world rollup)", fontsize=14)
    ax.set_ylabel("DAU")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(millions_formatter))
    ax.legend(loc=legend_loc, fontsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    path = f"{PLOTS_DIR}/global_{platform}.png"
    fig.savefig(path, dpi=110, bbox_inches="tight")
    plt.close(fig)
    return path


def main():
    git_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    ).stdout.strip()
    os.chdir(git_root)
    os.makedirs(PLOTS_DIR, exist_ok=True)

    df = pd.read_csv(CSV_PATH, parse_dates=["date"]).set_index("date")
    with open(MOBILE_TARGETS_JSON) as f:
        mobile_targets = json.load(f)

    paths = [
        render_platform(df, "desktop", DESKTOP_TARGETS, legend_loc="lower left"),
        render_platform(df, "mobile", mobile_targets, legend_loc="lower right"),
    ]
    for p in paths:
        print(f"Wrote {p}")


if __name__ == "__main__":
    main()
