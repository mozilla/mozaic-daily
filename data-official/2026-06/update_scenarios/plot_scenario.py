"""Plot the June 2026 canonical Desktop & Mobile DAU curves for the MozillaOnline scenario.

Fully portable: reads only ``augmented_curves.csv`` (produced by
``build_augmented_csv.py``) — no BigQuery, no model code. Styling mirrors the
KPI Looker-replica handoff chart (gray 2025 actuals, orange current actuals,
purple/blue June forecasts (+ pink MozillaOnline on desktop), gold Dec-15 stakeholder markers, red Dec-15 line,
legend below the axes).

Adds, vs the base canonical chart:
  * Desktop: a ``June Forecast + MozillaOnline`` line (June +Iran + a +500k
    daily-DAU step on 2026-06-02, rendered as a 28d ramp in MA space).
  * Mobile: the prior ``April Forecast`` line, clipped to start 2026-04-01.
  * Mobile (alternate PNG): the ``April Forecast (ex-Iran)`` line (blue dashed) instead,
    omitting the plain April +Iran line.

Run:
    source .venv/bin/activate
    python3 data-official/2026-06/update_scenarios/plot_scenario.py
"""

from __future__ import annotations

import os

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
AUGMENTED_CSV = os.path.join(HERE, "augmented_curves.csv")

# Palette (matched to the Looker replica).
ORANGE = "#E8761A"   # current actuals
PURPLE = "#7263AF"   # June forecast (+Iran) — both platforms; also April (dashed) on mobile
BLUE = "#4472C4"     # June forecast (ex-Iran)
PINK = "#C71585"     # June + MozillaOnline (desktop) — "pink" (purple-red)
GRAY_2025 = "#c0c0c0"

DEC15 = pd.Timestamp("2026-12-15")

# Dec-15 stakeholder KPI markers (label, value, marker shape).
DESKTOP_MARKERS = [
    ("Stretch", 49_772_388, "D"),
    ("Baseline", 49_513_157, "v"),
    ("Low", 49_039_852, "^"),
]
MOBILE_MARKERS = [
    ("Stretch", 17_742_615, "D"),
    ("Baseline", 17_522_795, "v"),
    ("Low", 17_019_424, "^"),
]


APRIL_START = pd.Timestamp("2026-04-01")


def forecast_line_specs(platform: str, mobile_april: str = "plus_iran"):
    """Forecast/actuals line specs for a platform.

    Each spec: (column, label, color, linestyle, linewidth, zorder, start_date,
    show_dec15_label). ``start_date`` clips the series to dates >= it; ``None`` = no clip.

    ``mobile_april`` selects which prior April line the mobile plot shows:
      * ``"plus_iran"`` — `April Forecast` (with Iran), purple dashed.
      * ``"no_iran"``   — `April Forecast (ex-Iran)`, blue dashed (matching June ex-Iran).
    """
    specs = [
        (f"{platform}_current_june_plus_iran", "June Forecast", PURPLE, "-", 1.8, 4, None, True),
        (f"{platform}_current_june_no_iran", "June Forecast (ex-Iran)", BLUE, "-", 1.8, 4, None, True),
    ]
    if platform == "desktop":
        specs.append(
            (f"{platform}_mozillaonline_plus_iran", "June Forecast + MozillaOnline",
             PINK, "-", 1.8, 4, None, True)
        )
    if platform == "mobile":
        if mobile_april == "plus_iran":
            specs.append(
                ("mobile_prior_april_plus_iran", "April Forecast",
                 PURPLE, "--", 1.6, 3, APRIL_START, True)
            )
        elif mobile_april == "no_iran":
            specs.append(
                ("mobile_prior_april_no_iran", "April Forecast (ex-Iran)",
                 BLUE, "--", 1.6, 3, APRIL_START, True)
            )
    return specs


def plot_platform(df: pd.DataFrame, platform: str, title: str, markers,
                  y_tick_step=None, mobile_april: str = "plus_iran"):
    fig, ax = plt.subplots(figsize=(14, 6))

    # 2025 actuals (already aligned onto the 2026 axis in the CSV).
    ax.plot(df.index, df[f"{platform}_actuals_2025"], color=GRAY_2025,
            linestyle="-", linewidth=1.8, label="2025 Actuals", zorder=1)

    # Current-year actuals, trimmed to end just before the forecast starts (no overlap).
    forecast_start = df[f"{platform}_current_june_plus_iran"].dropna().index.min()
    actuals = df[f"{platform}_actuals_all_countries"]
    actuals = actuals[actuals.index < forecast_start]
    ax.plot(actuals.index, actuals.values, color=ORANGE, linestyle="-",
            linewidth=3.0, label="Actuals", zorder=5)

    for col, label, color, ls, lw, z, start_date, show_dec15 in forecast_line_specs(platform, mobile_april):
        series = df[col]
        if start_date is not None:
            series = series[series.index >= start_date]
        if show_dec15:
            label = f"{label} {df.loc[DEC15, col] / 1e6:.1f}M"
        ax.plot(series.index, series.values, color=color, linestyle=ls,
                linewidth=lw, label=label, zorder=z)

    for name, val, mk in markers:
        ax.scatter([DEC15], [val], marker=mk, s=55, color="gold",
                   edgecolors="black", linewidths=0.8, zorder=6,
                   label=f"{name} {val / 1e6:.1f}M")

    ax.axvline(DEC15, color="red", linestyle=":", linewidth=1.2, zorder=2, label="Dec 15")

    ax.set_title(f"{title} DAU", fontsize=22, fontweight="bold")
    ax.set_ylabel("DAU", fontsize=16)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v / 1e6:.0f}M"))
    if y_tick_step is not None:
        ax.yaxis.set_major_locator(mticker.MultipleLocator(y_tick_step))

    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.tick_params(axis="both", labelsize=14)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    ax.grid(True, color="#e0e0e0", linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)

    # Legend below the plot, like the Looker replica.
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=5,
              fontsize=12, framealpha=1.0)

    plt.tight_layout()
    return fig, ax


def main() -> None:
    df = pd.read_csv(AUGMENTED_CSV, parse_dates=["date"]).set_index("date")

    fig, _ = plot_platform(df, "desktop", "Desktop", DESKTOP_MARKERS)
    desktop_png = os.path.join(HERE, "desktop_mozillaonline.png")
    fig.savefig(desktop_png, dpi=150, bbox_inches="tight")
    plt.close(fig)

    fig, _ = plot_platform(df, "mobile", "Mobile", MOBILE_MARKERS, y_tick_step=1e6)
    mobile_png = os.path.join(HERE, "mobile_with_april.png")
    fig.savefig(mobile_png, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # Alternate mobile: April ex-Iran (blue dashed) instead of the plain April +Iran line.
    fig, _ = plot_platform(df, "mobile", "Mobile", MOBILE_MARKERS, y_tick_step=1e6,
                           mobile_april="no_iran")
    mobile_exiran_png = os.path.join(HERE, "mobile_with_april_exiran.png")
    fig.savefig(mobile_exiran_png, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"Wrote {desktop_png}")
    print(f"Wrote {mobile_png}")
    print(f"Wrote {mobile_exiran_png}")


if __name__ == "__main__":
    main()
