"""Render the figure set for the August-canonical summer-miss report.

Every figure is a static PNG written to `plots/`, produced only from `series.py` and
`analyze.py` — no numbers are typed in. Colors follow the validated three-slot categorical
palette (blue / orange / aqua); aqua sits below 3:1 on the light surface, so every chart carries
direct labels and the report page carries a table view, which is the documented relief.

Label convention: charts never print an internal adjustment code. `h` is how the repo names the
Win10 headwind, but a chart reader cannot resolve it, so every chart-facing string says
"Windows 10 headwind" (or "Win10 headwind" where space is tight).

Run: python research/forecast-vs-summer-actuals/plots.py
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

import series as S  # noqa: E402
from analyze import BASELINE_KINDS, WINDOW, build  # noqa: E402

PLOTS = HERE / "plots"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"

ACTUAL = "#2a78d6"    # slot 1 — series C
FORECAST = "#eb6834"  # slot 2 — series A, and the model-miss component
TYPICAL = "#1baf7a"   # slot 3 — series B, and the shallow-summer component
HEADWIND = "#4a3aa7"  # slot 7 — the Win10 headwind. Slot 4 (yellow) fails the normal-vision
                      # floor against orange (ΔE 13.7); violet clears every gate beside it.
DEEMPH = "#c3c2b7"

# A and A-without-the-headwind are the SAME entity in two adjustment states, so they share a
# hue and the
# dash carries the difference. Adding a fourth hue would say they are different series.
NO_HEADWIND_STYLE = dict(color=FORECAST, linestyle=(0, (6, 3)), linewidth=1.8)
# Same logic for the counterfactual under two anchors: one entity, two constructions, so
# one hue and a dash rather than a second green.
SPRING_B_STYLE = dict(color=TYPICAL, linestyle=(0, (6, 3)), linewidth=1.8)

TRACK_LABEL = {"all": "all countries", "ex_ir_cn": "ex-Iran, ex-China"}


def _style(ax, ylabel: str | None = None) -> None:
    ax.set_facecolor(SURFACE)
    ax.grid(True, color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
        ax.spines[side].set_linewidth(1.0)
    ax.tick_params(colors=MUTED, labelsize=9, length=0)
    if ylabel:
        ax.set_ylabel(ylabel, color=INK_2, fontsize=9.5)


def _millions(decimals: int = 1):
    """Tick formatter carrying one significant figure finer than the tick step."""
    return FuncFormatter(lambda v, _: f"{v / 1e6:.{decimals}f}M")


def _save(fig, name: str) -> Path:
    path = PLOTS / name
    PLOTS.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {path.relative_to(HERE.parent.parent)}")
    return path


ANCHOR_LABEL = {"seam": "seam-anchored", "spring": "spring-anchored"}
ANCHOR_BLURB = {
    "seam": (
        "B is rescaled to meet 2026 AT THE SEAM, so it starts there — this is the anchor the "
        "decomposition table uses, because it charges August only for the 22 days it actually\n"
        "forecast. It cannot show the spring-to-summer descent; the spring-anchored companion chart "
        "does that. Actuals stop at the last landed day; the December cliff is Christmas entering\n"
        "the 28-day trailing window."
    ),
    "spring": (
        "B is rescaled to the Feb 15 – Apr 15 window, so it spans the year and shows the whole "
        "spring-to-summer descent. This curve is byte-identical to the green line on the\n"
        "seasonality tab. NOTE the all-countries panel: B runs ~1M below actuals from May onward "
        "because 2026's anchor window sits inside Iran's 2026-03-01 → 05-25 outage, which\n"
        "depresses it. The ex-Iran/ex-China panel is the clean one. The seam-anchored companion "
        "chart is the basis for the decomposition table."
    ),
}


def fig_three_series(baseline: str = "seam") -> Path:
    """A, B and C across 2026, both population tracks — the chart that carries the story.

    One figure per anchor. The two anchors answer different questions — "how did August do over the
    days it forecast" versus "how did it do against a whole typical seasonal year" — and putting
    both counterfactuals on one axis made a five-line chart in which the important distinction was
    a dash pattern.

    Deliberately built to the same grammar as the seasonality pane: one figure, two stacked panels,
    a full calendar year on the x-axis, absolute DAU on the y-axis, direct end labels, and the
    construction caveats in a footnote. The two panes then read as one analysis rather than two.

    Only the *format* is shared. The counterfactual here is still seam-anchored, so B is drawn from
    the seam forward — it is rescaled to meet 2026 there, and anything earlier would be an artifact
    of that rescaling rather than a fact about the summer. Jan-Jul therefore shows actuals alone,
    which is honest: nothing else was being forecast yet.
    """
    fig, axes = plt.subplots(2, 1, figsize=(11.5, 8.6), facecolor=SURFACE, sharex=True)
    anchor_mmdd = S.baseline_window(baseline)[0]

    for ax, track in zip(axes, S.TRACKS):
        _style(ax, "Desktop DAU · 28-day trailing mean")
        A = S.published_forecast("august", track)
        A_nh = S.published_forecast_no_headwind("august", track)
        C = S.actuals_ma(track)
        B = S.typical_summer(track, baseline).loc[f"2026-{anchor_mmdd}":]

        ax.axvspan(S.EVAL_START, S.EVAL_END, color="#f0efec", zorder=0)
        ax.annotate("scored\n2026-08-02 → 08-23",
                    xy=(S.EVAL_START + (S.EVAL_END - S.EVAL_START) / 2, 0.975),
                    xycoords=("data", "axes fraction"), ha="center", va="top",
                    fontsize=8.5, color=MUTED)

        for data, color, legend, end_label, dy in (
            (C, ACTUAL, "C · actual", "actual", 11),
            (B, TYPICAL, f"B · typical summer ({ANCHOR_LABEL[baseline]})",
             "typical summer", 6),
            (A, FORECAST, "A · August canonical", "August canonical", 0),
        ):
            data = data.loc["2026-01-01":"2026-12-31"].dropna()
            ax.plot(data.index, data.to_numpy(), color=color, linewidth=2.0,
                    label=legend, zorder=3)
            ax.annotate(f"  {end_label}", xy=(data.index[-1], data.iloc[-1]),
                        xytext=(6, dy), textcoords="offset points", va="center", ha="left",
                        fontsize=9, color=color, fontweight="medium")

        nh = A_nh.loc["2026-01-01":"2026-12-31"].dropna()
        ax.plot(nh.index, nh.to_numpy(), label="A · Win10 headwind removed",
                zorder=4, **NO_HEADWIND_STYLE)
        ax.annotate("  same, Win10 headwind removed",
                    xy=(nh.index[-1], nh.iloc[-1]), xytext=(6, -13),
                    textcoords="offset points", va="center", ha="left", fontsize=8.5,
                    color=FORECAST, style="italic")

        ax.yaxis.set_major_formatter(_millions(1))
        ax.set_xlim(pd.Timestamp("2026-01-01"), pd.Timestamp("2027-01-22"))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.set_title(TRACK_LABEL[track], color=INK, fontsize=10.5,
                     fontweight="semibold", loc="left")

    axes[0].legend(frameon=False, fontsize=8.5, labelcolor=INK_2, ncols=3,
                   loc="lower left", bbox_to_anchor=(0, 1.14))
    fig.suptitle(
        f"August forecast comparison — published curve vs actuals vs a typical summer "
        f"({ANCHOR_LABEL[baseline]})",
        color=INK, fontsize=12.5, fontweight="semibold", x=0.005, ha="left", y=0.995,
    )
    fig.text(0.005, -0.022, ANCHOR_BLURB[baseline],
             fontsize=8.5, color=MUTED, ha="left", linespacing=1.5)
    return _save(fig, f"three_series_{baseline}.png")


def fig_gap_opens(results: dict) -> Path:
    """The daily miss C - A across the scored window, both tracks."""
    fig, ax = plt.subplots(figsize=(9.5, 4.4), facecolor=SURFACE)
    _style(ax, "Actual − forecast (DAU)")

    for track, color in (("all", ACTUAL), ("ex_ir_cn", FORECAST)):
        gap = (
            S.actuals_ma(track).reindex(WINDOW)
            - S.published_forecast("august", track).reindex(WINDOW)
        )
        ax.plot(gap.index, gap.to_numpy(), color=color, linewidth=2.0,
                label=TRACK_LABEL[track], zorder=3)
        ax.annotate(
            f"  {gap.iloc[-1]:+,.0f}",
            xy=(gap.index[-1], gap.iloc[-1]), xytext=(6, 0), textcoords="offset points",
            va="center", ha="left", fontsize=9.5, color=color, fontweight="medium",
        )

    ax.axhline(0, color=AXIS, linewidth=1.0, zorder=2)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1e3:+,.0f}K"))
    ax.set_xlim(WINDOW[0], WINDOW[-1] + pd.Timedelta(days=6))
    ax.set_title(
        "The gap opens steadily, not in a jump — it is drift, not a bad seam",
        color=INK, fontsize=12.5, fontweight="semibold", loc="left", pad=14,
    )
    ax.legend(frameon=False, loc="upper left", fontsize=9, labelcolor=INK_2)
    return _save(fig, "gap_opens.png")


def _level_label(base: str, row: dict) -> str:
    """Row label carrying the absolute levels the components are differences between.

    A stacked bar of deltas is unreadable as a magnitude without them: +205,245 means one thing
    against 43M and another against 4M. Two decimals in millions resolves 10,000 DAU, which is
    finer than any component plotted here.
    """
    return f"{base}\n{row['A_last'] / 1e6:.2f}M → {row['C_last'] / 1e6:.2f}M"


def _stacked_bars(ax, labels, rows, segments, *, inline_min_frac=0.13):
    """Horizontal stacked bars sharing one segment schema.

    Labels are SELECTIVE: a segment gets its value printed inside it only when it is at least
    `inline_min_frac` of the widest bar, i.e. wide enough to hold the text. Narrower segments are
    left unlabelled rather than crowded above the bar, where several of them collide into an
    unreadable row. Every bar still carries its total, and the report's table carries every
    component — so nothing is lost, it just is not on this chart.
    """
    widest = max(sum(abs(r[k]) for k, _, _ in segments) for r in rows)
    for i, row in enumerate(rows):
        left = 0.0
        for key, color, _ in segments:
            value = row[key]
            ax.barh(i, value, left=left, color=color, height=0.44, zorder=3,
                    edgecolor=SURFACE, linewidth=2)
            if abs(value) / widest >= inline_min_frac:
                ax.annotate(f"{value:+,.0f}", xy=(left + value / 2, i),
                            ha="center", va="center", zorder=4, fontsize=9.5,
                            fontweight="semibold", color="#ffffff")
            left += value
        ax.annotate(f"  total {left:+,.0f}", xy=(left, i), xytext=(8, 0),
                    textcoords="offset points", va="center", fontsize=9.5,
                    color=INK_2, fontweight="medium")

    ax.set_yticks(range(len(rows)), labels, color=INK_2, fontsize=10)
    ax.set_ylim(len(rows) - 0.55, -0.55)
    ax.set_xlim(0, widest * 1.26)
    ax.grid(axis="y", visible=False)
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for _, c, _ in segments]
    ax.legend(handles, [lab for _, _, lab in segments], frameon=False, fontsize=9,
              labelcolor=INK_2, ncols=len(segments) if len(segments) < 4 else 2,
              loc="upper left", bbox_to_anchor=(0, -0.18))


THREE_WAY_SEGMENTS = [
    ("shallow_summer", TYPICAL, "shallow summer · nobody's fault"),
    ("model_miss", FORECAST, "model ran below typical"),
    ("headwind", HEADWIND, "Windows 10 headwind · a judgement call"),
]


def fig_three_way(results: dict) -> Path:
    """The published shortfall split into shallow-summer, model, and headwind components."""
    rows = results["three_way"]
    fig, ax = plt.subplots(figsize=(10.5, 3.6), facecolor=SURFACE)
    _style(ax)
    _stacked_bars(ax, [_level_label(TRACK_LABEL[r["track"]], r) for r in rows], rows,
                  THREE_WAY_SEGMENTS)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1e3:,.0f}K"))
    ax.set_title(
        "Three owners, not two — a quarter of the miss is the Windows 10 headwind we chose",
        color=INK, fontsize=12.5, fontweight="semibold", loc="left", pad=12,
    )
    return _save(fig, "three_way_split.png")


def fig_split_ex_headwind(results: dict) -> Path:
    """The legitimate/illegitimate split with the exogenous Win10 headwind taken off the table."""
    rows = results["three_way"]
    fig, ax = plt.subplots(figsize=(10.5, 3.4), facecolor=SURFACE)
    _style(ax)
    # Levels here are forecast-with-the-headwind-removed -> actual, since that is the pair these
    # two components sit between.
    labels = [
        f"{TRACK_LABEL[r['track']]}\n"
        f"{r['A_no_headwind_last'] / 1e6:.2f}M → {r['C_last'] / 1e6:.2f}M"
        for r in rows
    ]
    _stacked_bars(
        ax, labels, rows,
        [("shallow_summer", TYPICAL, "legitimate · shallow summer"),
         ("model_miss", FORECAST, "illegitimate · model below typical")],
    )
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1e3:,.0f}K"))
    ex = next(r for r in rows if r["track"] == "ex_ir_cn")
    ax.set_title(
        f"With the Windows 10 headwind removed, the verdict flips on the year-comparable track: "
        f"{ex['legitimate_share_ex_headwind']:.0%} of the model's own miss is the shallow summer",
        color=INK, fontsize=11.5, fontweight="semibold", loc="left", pad=12,
    )
    return _save(fig, "split_ex_headwind.png")


def fig_split(results: dict) -> Path:
    """The two-way split of the miss, per track, at the window's last day."""
    rows = [r for r in results["decompositions"] if r["baseline"] == "seam"]
    _levels = {d["track"]: d for d in results["three_way"]}
    fig, ax = plt.subplots(figsize=(10, 3.2), facecolor=SURFACE)
    _style(ax)
    ax.grid(axis="y", visible=False)

    labels = [_level_label(TRACK_LABEL[r["track"]], _levels[r["track"]]) for r in rows]
    y = range(len(rows))
    for i, r in enumerate(rows):
        legit, illegit = r["legitimate_last"], r["illegitimate_last"]
        # 2px surface gap between adjacent segments, per the stacked-bar mark spec.
        ax.barh(i, legit, color=TYPICAL, height=0.44, zorder=3)
        ax.barh(i, illegit, left=legit, color=FORECAST, height=0.44, zorder=3,
                edgecolor=SURFACE, linewidth=2)
        ax.annotate(f"{legit:+,.0f}", xy=(legit / 2, i), ha="center", va="center",
                    fontsize=9.5, color="#ffffff", fontweight="semibold", zorder=4)
        ax.annotate(f"{illegit:+,.0f}", xy=(legit + illegit / 2, i), ha="center", va="center",
                    fontsize=9.5, color="#ffffff", fontweight="semibold", zorder=4)
        ax.annotate(f"  total {r['miss_last']:+,.0f}", xy=(r["miss_last"], i),
                    xytext=(8, 0), textcoords="offset points", va="center",
                    fontsize=9.5, color=INK_2, fontweight="medium")

    ax.set_yticks(list(y), labels, color=INK_2, fontsize=10)
    ax.set_xlim(0, max(r["miss_last"] for r in rows) * 1.28)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1e3:,.0f}K"))
    ax.set_title(
        "Most of the miss is the part we should have got right",
        color=INK, fontsize=12.5, fontweight="semibold", loc="left", pad=12,
    )
    handles = [
        plt.Rectangle((0, 0), 1, 1, color=TYPICAL),
        plt.Rectangle((0, 0), 1, 1, color=FORECAST),
    ]
    ax.set_ylim(len(rows) - 0.55, -0.45)
    ax.legend(handles, ["legitimate · summer beat a typical one (C−B)",
                        "illegitimate · forecast below even a typical summer (B−A)"],
              frameon=False, fontsize=9, labelcolor=INK_2, ncols=2,
              loc="upper left", bbox_to_anchor=(0, -0.16))
    return _save(fig, "miss_split.png")


def fig_trend_check() -> Path:
    """Each year's Aug-23 level as a share of its own Jun-15 — the intuition check.

    Two panels, one per population track, matching the house layout. Emphasis form within each:
    2026 in the accent hue, the norm years recessive. The reader's job is to see one bar standing
    apart, not to tell five categories apart.
    """
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.8), facecolor=SURFACE, sharey=True)
    for ax, track in zip(axes, S.TRACKS):
        _style(ax)
        ax.grid(axis="x", visible=False)
        table = S.trend_table(track)
        norm = table.loc[list(S.NORM_YEARS), "aug23_ratio"]

        for year, value in table["aug23_ratio"].items():
            color = ACTUAL if year == 2026 else DEEMPH
            ax.bar(str(year), value, color=color, width=0.62, zorder=3)
            ax.annotate(f"{value:.1%}", xy=(str(year), value), xytext=(0, 5),
                        textcoords="offset points", ha="center", fontsize=9.5,
                        color=INK if year == 2026 else INK_2,
                        fontweight="semibold" if year == 2026 else "normal")
            # The ratio is the comparable quantity, but on its own it hides that every one of these
            # years sits at a materially lower absolute level than the one before it.
            ax.annotate(
                f"{table.loc[year, 'aug23'] / 1e6:.2f}M\n"
                f"of {table.loc[year, 'baseline'] / 1e6:.2f}M",
                xy=(str(year), 0.0), xycoords=("data", "axes fraction"),
                xytext=(0, 9), textcoords="offset points",
                ha="center", va="bottom", fontsize=8.2, linespacing=1.35,
                color="#ffffff" if year == 2026 else INK_2, zorder=4,
            )

        ax.axhline(norm.mean(), color=INK_2, linewidth=1.4, linestyle=(0, (5, 3)), zorder=4)
        ax.annotate(f"2022–25 average  {norm.mean():.1%}", xy=(0.008, norm.mean()),
                    xycoords=("axes fraction", "data"), xytext=(0, 6),
                    textcoords="offset points", ha="left", fontsize=9, color=INK_2)
        ax.set_ylim(0.878, table["aug23_ratio"].max() + 0.020)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0%}"))
        excess = (table.loc[2026, "aug23_ratio"] - norm.mean()) * 100
        ax.set_title(f"{TRACK_LABEL[track]}   2026 is {excess:+.2f} points above the average",
                     color=INK, fontsize=10.5, fontweight="semibold", loc="left")

    axes[0].set_ylabel("Aug 23 as a share of that year's Jun 15", color=INK_2, fontsize=9.5)
    fig.suptitle(
        "2026 gave up less of its pre-summer level than any of the four norm years",
        color=INK, fontsize=12.5, fontweight="semibold", x=0.005, ha="left", y=1.03,
    )
    fig.text(0.005, -0.06,
             "Each year measured against its own Jun-15 baseline, so absolute size and long-run "
             "decline cancel. The absolute pair inside each bar shows the level that ratio is "
             "taken of —\nfalling ~10M across the five years, which is exactly what the ratio is "
             "designed to remove.",
             fontsize=8.5, color=MUTED, ha="left", linespacing=1.5)
    return _save(fig, "trend_check.png")


def fig_vintage_ladder(results: dict) -> Path:
    """Each published vintage's shortfall at 2026-08-23, split three ways."""
    rows = results["three_way_ladder"]
    fig, ax = plt.subplots(figsize=(10.5, 4.0), facecolor=SURFACE)
    _style(ax)
    labels = [
        _level_label(
            f"{r['vintage'].capitalize()}  (seam {S.VINTAGES[r['vintage']]['seam'].date()})", r
        )
        for r in rows
    ]
    _stacked_bars(ax, labels, rows, THREE_WAY_SEGMENTS, inline_min_frac=0.16)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1e6:.1f}M"))
    hw = [r["headwind_share_of_total"] for r in rows]
    ax.set_title(
        f"Each vintage's shortfall at 2026-08-23, all countries\n"
        f"the Windows 10 headwind takes a near-constant {min(hw):.0%}–{max(hw):.0%} of every one",
        color=INK, fontsize=11.5, fontweight="semibold", loc="left", pad=12,
    )
    return _save(fig, "vintage_ladder.png")


def fig_baseline_sensitivity(results: dict) -> Path:
    """How the legitimate/illegitimate split moves with the counterfactual's anchor."""
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.6), facecolor=SURFACE, sharey=True)
    for ax, track in zip(axes, S.TRACKS):
        _style(ax)
        ax.grid(axis="x", visible=False)
        rows = [r for r in results["decompositions"] if r["track"] == track]
        rows = sorted(rows, key=lambda r: BASELINE_KINDS.index(r["baseline"]))
        x = range(len(rows))
        ax.bar([i - 0.19 for i in x], [r["legitimate_last"] for r in rows], width=0.36,
               color=TYPICAL, zorder=3, label="legitimate (C−B)")
        ax.bar([i + 0.19 for i in x], [r["illegitimate_last"] for r in rows], width=0.36,
               color=FORECAST, zorder=3, label="illegitimate (B−A)")
        ax.axhline(0, color=AXIS, linewidth=1.0, zorder=2)
        ax.set_xticks(list(x), [r["baseline"] for r in rows], color=INK_2, fontsize=9.5)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1e6:+.1f}M"))
        ax.set_title(TRACK_LABEL[track], color=INK, fontsize=10.5,
                     fontweight="semibold", loc="left")
    axes[0].set_ylabel("DAU at 2026-08-23", color=INK_2, fontsize=9.5)
    axes[0].legend(frameon=False, fontsize=9, labelcolor=INK_2, ncols=2,
                   loc="upper left", bbox_to_anchor=(0, -0.13))
    fig.suptitle(
        "The split depends on where the counterfactual is anchored — the total miss does not",
        color=INK, fontsize=12.5, fontweight="semibold", x=0.005, ha="left", y=1.06,
    )
    return _save(fig, "baseline_sensitivity.png")


def main() -> None:
    print("Building figures...")
    results = build()
    for anchor in ("seam", "spring"):
        fig_three_series(anchor)
    fig_gap_opens(results)
    fig_split(results)
    fig_three_way(results)
    fig_split_ex_headwind(results)
    fig_trend_check()
    fig_vintage_ladder(results)
    fig_baseline_sensitivity(results)
    print("Done.")


if __name__ == "__main__":
    main()
