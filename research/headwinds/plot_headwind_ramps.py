"""Plot the July vs. August display-layer adjustment ramps for desktop and mobile.

Renders each cycle's spec through the *production* applier
(``mozaic_daily.adjustments.render_adjustment``) rather than re-deriving the ramp
here, so the figure cannot drift from what the composite CSVs actually carry.

Desktop and mobile are separate panels on purpose: the desktop anchor is ~45x the
mobile one, and a shared axis would flatten mobile to a line at zero.

Run from the repo root:

    source .venv/bin/activate && python research/headwinds/plot_headwind_ramps.py
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter

from mozaic_daily.adjustments import render_adjustment

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = REPO_ROOT / "research" / "headwinds" / "plots" / "headwind_ramps_july_vs_august.png"

PLOT_START = pd.Timestamp("2026-03-01")
PLOT_END = pd.Timestamp("2026-12-31")
ANCHOR_DATE = pd.Timestamp("2026-12-15")

JULY_HEADWIND = REPO_ROOT / "data-official" / "2026-07" / "adjustments" / "headwind.json"
AUGUST_HEADWIND = REPO_ROOT / "data-official" / "2026-08" / "adjustments" / "headwind.json"
AUGUST_TAILWIND = REPO_ROOT / "data-official" / "2026-08" / "adjustments" / "tailwind.json"

# dataviz categorical slots 1-3 (light mode), validated all-pairs.
COLOR_JULY = "#2a78d6"
COLOR_AUGUST = "#eb6834"
COLOR_AUGUST_NET = "#1baf7a"

INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#8a8880"
SURFACE = "#fcfcfb"


def load_spec(path: Path) -> dict:
    """Read an adjustment spec JSON."""
    return json.loads(path.read_text())


def millions(value: float, _pos: int) -> str:
    """Y tick label in millions, two decimals — 1.32M and 1.35M must not collide."""
    return f"{value / 1e6:.2f}M"


def thousands(value: float, _pos: int) -> str:
    """Y tick label in thousands, one decimal."""
    return f"{value / 1e3:.0f}K"


def label_at_anchor(ax, series: pd.Series, text: str, color: str, y_offset: float) -> None:
    """Direct-label a ramp in the right margin; the text carries its Dec-15 anchor value.

    Anchored at the end of the plotted range rather than at Dec-15 so the label sits
    past the end of every line instead of on top of a crossing one.
    """
    ax.annotate(
        text,
        xy=(PLOT_END, series.loc[PLOT_END]),
        xytext=(10, y_offset),
        textcoords="offset points",
        color=color,
        fontsize=9,
        fontweight="bold",
        va="center",
    )


def build_figure() -> plt.Figure:
    """Render the two-panel desktop/mobile ramp comparison."""
    dates = pd.date_range(PLOT_START, PLOT_END, freq="D")

    july = render_adjustment(load_spec(JULY_HEADWIND), dates)
    august = render_adjustment(load_spec(AUGUST_HEADWIND), dates)
    august_tailwind = render_adjustment(load_spec(AUGUST_TAILWIND), dates)
    august_mobile_net = august["mobile"] + august_tailwind["mobile"]

    fig, (ax_desktop, ax_mobile) = plt.subplots(
        2, 1, figsize=(12, 9), sharex=True, facecolor=SURFACE
    )

    # Per series: (values, color, legend label, anchor annotation or None, label y-offset px).
    # Mobile July and August `h` share the same -27,162 anchor, so they get one merged label.
    panels = [
        (
            ax_desktop,
            "Desktop — Win10 headwind `h`",
            [
                (july["desktop"], COLOR_JULY, "July `h`", "July `h`  −1,345,000", 11),
                (august["desktop"], COLOR_AUGUST, "August `h`", "August `h`  −1,315,000", -11),
            ],
            millions,
            "lower left",
            (0.14, 0.10),
        ),
        (
            ax_mobile,
            "Mobile — headwind `h` and, from August, tailwind `t`",
            [
                (july["mobile"], COLOR_JULY, "July `h`", None, 0),
                (
                    august["mobile"],
                    COLOR_AUGUST,
                    "August `h`",
                    "July & August `h`  −27,162",
                    -13,
                ),
                (
                    august_mobile_net,
                    COLOR_AUGUST_NET,
                    "August net `h`+`t`",
                    "August net `h`+`t`  +271,838",
                    0,
                ),
            ],
            thousands,
            "upper left",
            (0.22, 0.10),
        ),
    ]

    for ax, title, series_specs, formatter, legend_loc, (pad_low, pad_high) in panels:
        ax.set_facecolor(SURFACE)
        ax.axhline(0, color=INK_MUTED, linewidth=1, zorder=1)
        ax.axvline(ANCHOR_DATE, color=INK_MUTED, linewidth=1, linestyle=":", zorder=1)

        for series, color, legend_label, annotation, offset in series_specs:
            ax.plot(
                series.index, series.values, color=color, linewidth=2, label=legend_label, zorder=3
            )
            if annotation:
                label_at_anchor(ax, series, annotation, color, offset)

        # Explicit limits: the anchor labels sit outside the data range and get clipped otherwise.
        low = min(s.min() for s, *_ in series_specs)
        high = max(s.max() for s, *_ in series_specs)
        span = high - low
        ax.set_ylim(low - pad_low * span, high + pad_high * span)

        ax.set_title(title, color=INK_PRIMARY, fontsize=12, fontweight="bold", loc="left", pad=10)
        ax.yaxis.set_major_formatter(FuncFormatter(formatter))
        ax.grid(axis="y", color=INK_MUTED, alpha=0.2, linewidth=0.8)
        ax.set_axisbelow(True)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        for spine in ("left", "bottom"):
            ax.spines[spine].set_color(INK_MUTED)
        ax.tick_params(colors=INK_SECONDARY, labelsize=9)
        ax.legend(loc=legend_loc, frameon=False, fontsize=9, labelcolor=INK_SECONDARY)
        ax.set_ylabel("DAU adjustment", color=INK_SECONDARY, fontsize=10)

    # Right margin holds the anchor labels; ticks stop at the last real month.
    ax_mobile.set_xlim(PLOT_START, PLOT_END + pd.Timedelta(days=62))
    ax_mobile.set_xticks(pd.date_range(PLOT_START, "2026-12-01", freq="MS"))
    ax_mobile.set_xticklabels(
        [d.strftime("%b %Y") for d in pd.date_range(PLOT_START, "2026-12-01", freq="MS")]
    )
    ax_desktop.annotate(
        "Dec 15 anchor",
        xy=(ANCHOR_DATE, 0),
        xytext=(-6, -14),
        textcoords="offset points",
        color=INK_MUTED,
        fontsize=8,
        ha="right",
    )

    fig.suptitle(
        "Display-layer DAU adjustments: July 2026 vs. August 2026 forecast cycles",
        color=INK_PRIMARY,
        fontsize=14,
        fontweight="bold",
        x=0.02,
        ha="left",
        y=0.98,
    )
    fig.text(
        0.02,
        0.935,
        "Linear ramps to a 2026-12-15 anchor. July ramps from 2026-04-01; August ramps from the "
        "2026-08-02 seam. Ramps are not clipped at the anchor.",
        color=INK_SECONDARY,
        fontsize=9,
        ha="left",
    )
    fig.tight_layout(rect=(0, 0, 1, 0.925))
    return fig


def main() -> None:
    """Render and save the figure."""
    fig = build_figure()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT_PATH, dpi=150, facecolor=SURFACE)
    print(f"Wrote {OUTPUT_PATH.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
