"""Write the desktop-only DAILY (unsmoothed) twin of a cycle's canonical curves CSV, with `h` on it.

The published `*_canonical_curves.csv` holds 28-day trailing moving averages (MAs). This writes
`*_canonical_curves.DESKTOP_ONLY.DAILY.csv` with the same layout (`date`, `desktop_actuals`,
`desktop_prior_<prev>`, `desktop_current_<cycle>`) but one raw daily DAU value per row, read from
the desktop forecast parquets. Weekly seasonality is intact; nothing is smoothed.

Why `h` needs care here. The Win10 headwind is a display-layer adjustment: the pipeline only ever
adds its `linear_ramp` to the 28-day MA (`apply_net_adjustment_to_series`), never to a daily row.
A trailing 28-day mean of a linear ramp is that same ramp delayed by 13.5 days (the window's mean
lag), so adding the published ramp to the daily series and re-smoothing lands 13.5 * slope ABOVE
the published curve on every date (131,500 DAU for August 2026). The mathematically consistent
daily headwind is therefore the ramp **advanced by 13.5 days**:

    daily_h(t) = ramp(t + 13.5 days)          for t >= seam,  0 before

which makes `rolling(28).mean()` of the daily file reproduce the published MA to the DAU from
seam + 27 onward (verified below, not assumed). The price is a visible step on the seam day: the
daily headwind starts at 13.5 * slope, not 0. That step is not introduced here -- the published MA
already carries the ramp's full daily increment from its first forecast day, when 27 of the 28
days in its window are actuals, which no daily series that is zero on actuals can match.

Inside the 27-day seam transition the published curve is `display_ma`'s variance-matched splice,
which is non-linear in an added ramp, so no daily series reproduces it there exactly. The
transition window and the window where the daily file cannot be smoothed back are the same 27 days.

**Clamped ramps (September 2026 on).** A spec with `clamp_at_anchor: true` holds the MA-space ramp
flat after Dec-15. The 13.5-day advance is exact only while the whole 28-day window sits on one
straight piece, so around the Dec-15 kink no smooth daily series reproduces the published curve.
Three rules for the daily headwind after the anchor are implemented (`--post-anchor-rule`):

- `exact`           -- the unique daily series whose rolling mean IS the published curve through
                       Dec-31. Through Dec-15 it is the unclamped advanced ramp; after Dec-15 it
                       repeats itself with period 28 (a flat MA over a ramp history forces that).
                       Exact everywhere from seam + 27; the daily headwind sawtooths after Dec-15.
- `flat_at_anchor`  -- unclamped advanced ramp through Dec-15, then held at the anchor value.
                       Exact through Dec-15; the file re-smooths too deep for ~4 weeks after it.
- `advanced_clamped`-- the clamped ramp advanced 13.5 days, read literally. Exact only through
                       Dec-1 and from Dec-29; ~3.4 slopes too shallow at Dec-15. Kept for reference.

The prior cycle's column gets the same treatment with its own frozen spec (read-only). Every
number this creates is printed in the "weirdness ledger" and recorded in the cycle's csv/ README.

Cycle-scoped: repoint the constants at each roll-forward.

Usage:
    python scripts/export_desktop_daily_csv.py            # write the file + plot + verify
    python scripts/export_desktop_daily_csv.py --dry-run  # print the ledger only
"""

import argparse
import glob
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from mozaic_daily.adjustments import load_forecast, render_adjustment  # noqa: E402

# --- Cycle-scoped configuration (repoint at each roll-forward) -------------------------------
CSV_DIR = "data-official/2026-09/csv"
PUBLISHED_CURVES = "september_canonical_curves.csv"
PLOT_PATH = "data-official/2026-09/plots/desktop_daily_export_vs_published_ma.png"

DESKTOP_FORECAST_PATH = (
    "data-official/2026-09/desktop_g01_2026-09-09/"
    "cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/"
    "mozaic_daily_forecast.2026-09-09.ld-D.adj-ijlo.parquet"
)
PREV_DESKTOP_FORECAST_PATH = (
    "data-official/2026-08/desktop_g01_2026-08-02/"
    "cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/"
    "mozaic_daily_forecast.2026-08-02.ld-D.adj-lo.parquet"
)
# The overlays baked into each parquet; `load_forecast` refuses anything else.
DESKTOP_REQUIRED_STATE = ["i", "j", "l", "o"]
PREV_DESKTOP_REQUIRED_STATE = ["l", "o"]

CURRENT_ADJUSTMENTS_DIR = "data-official/2026-09/adjustments"
PRIOR_ADJUSTMENTS_DIR = "data-official/2026-08/adjustments"
WIN10_SPEC_FILENAME = "headwind.json"

FORECAST_START = pd.Timestamp("2026-09-09")       # September desktop seam
PREV_FORECAST_START = pd.Timestamp("2026-08-02")  # August's seam
DISPLAY_START = pd.Timestamp("2026-01-01")
DISPLAY_END = pd.Timestamp("2026-12-31")
MEASUREMENT_DATE = pd.Timestamp("2026-12-15")

CURRENT_COLUMN = "desktop_current_september"
PRIOR_COLUMN = "desktop_prior_august"
ACTUALS_COLUMN = "desktop_actuals"
FILE_MARKER = "DESKTOP_ONLY.DAILY"

# How the daily headwind behaves after the anchor when the spec is clamped (see module docstring).
POST_ANCHOR_RULES = ("exact", "flat_at_anchor", "advanced_clamped")
DEFAULT_POST_ANCHOR_RULE = "exact"

DESKTOP_SEGMENT = '{"os": "ALL"}'
DESKTOP_APP_NAME = "desktop"
DESKTOP_DATA_SOURCE = "legacy_desktop"

MA_WINDOW = 28
# A trailing window of N days has mean lag (N - 1) / 2 days. The ramp is advanced by exactly this.
MA_MEAN_LAG_DAYS = (MA_WINDOW - 1) / 2

# The published file is rounded to whole DAU and so is this one, so the smoothed daily file can
# differ from the published MA by rounding only. One DAU is the bound; anything larger is a bug.
SMOOTH_TOLERANCE_DAU = 1.0
# Actuals get 1/28 more: the published actuals come from a later pull than the parquet's training
# rows, and one day landing a single DAU different shifts the 28d mean by 1/28 (seen 2026-09-17:
# 1.036 DAU on two August days). Anything past that is a real disagreement.
ACTUALS_SMOOTH_TOLERANCE_DAU = SMOOTH_TOLERANCE_DAU + 1.0 / 28 + 1e-6
# The daily ramp must invert the rolling mean to float precision where the rule promises exactness.
RAMP_INVERSION_TOLERANCE_DAU = 1e-6


# --- Load ---------------------------------------------------------------------------------------

def load_world_daily(path: str, required_state: list[str]) -> tuple[pd.Series, pd.DatetimeIndex]:
    """World (`ALL`) daily desktop DAU from a forecast parquet, plus its training dates."""
    df, _meta = load_forecast(path, require_state=required_state)
    rows = df[
        (df["data_source"] == DESKTOP_DATA_SOURCE)
        & (df["segment"] == DESKTOP_SEGMENT)
        & (df["app_name"] == DESKTOP_APP_NAME)
        & (df["country"] == "ALL")
    ].copy()
    rows["target_date"] = pd.to_datetime(rows["target_date"])
    if rows["target_date"].duplicated().any():
        raise ValueError(
            f"{path}: more than one world row per date after filtering to "
            f"{DESKTOP_DATA_SOURCE}/{DESKTOP_SEGMENT}/{DESKTOP_APP_NAME}/ALL."
        )
    daily = rows.set_index("target_date")["dau"].sort_index().astype(float)
    training_dates = pd.DatetimeIndex(
        sorted(rows.loc[rows["data_type"] == "training", "target_date"].unique())
    )
    return daily, training_dates


def load_win10_spec(adjustments_dir: str) -> dict:
    """The one spec in the directory that is the Win10 headwind. Other specs are named, not used."""
    spec_paths = sorted(glob.glob(f"{adjustments_dir}/*.json"))
    win10_path = os.path.join(adjustments_dir, WIN10_SPEC_FILENAME)
    if win10_path not in spec_paths:
        raise FileNotFoundError(f"{adjustments_dir} has no {WIN10_SPEC_FILENAME}.")
    for path in spec_paths:
        if path != win10_path:
            print(f"NOTE: {path} is NOT applied to the daily file; only {WIN10_SPEC_FILENAME} is.")
    with open(win10_path) as f:
        spec = json.load(f)
    if spec["type"] != "linear_ramp":
        raise ValueError(
            f"{win10_path} is type {spec['type']!r}; the 13.5-day advance is exact only for a "
            f"linear_ramp. A non-linear spec needs its own inversion."
        )
    return spec


# --- The daily headwind -------------------------------------------------------------------------

def is_clamped(spec: dict) -> bool:
    return bool(spec.get("clamp_at_anchor", False))


def ma_space_ramp(spec: dict, index: pd.DatetimeIndex, seam: pd.Timestamp) -> pd.Series:
    """The headwind exactly as the display layer applies it: rendered ramp, zero before the seam."""
    ramp = render_adjustment(spec, index)["desktop"]
    ramp[index < seam] = 0.0
    return ramp


def _advanced(spec: dict, index: pd.DatetimeIndex, seam: pd.Timestamp) -> pd.Series:
    """`ramp(t + 13.5 days)` of whatever `spec` renders, zero before the seam.

    On the integer date grid that is the mean of `ramp(t + 13)` and `ramp(t + 14)`, exact for a
    straight line. Rendered on an extended index so the last dates in `index` can look ahead.
    """
    lookahead = int(np.ceil(MA_MEAN_LAG_DAYS)) + 1
    extended = pd.DatetimeIndex(pd.date_range(index[0], index[-1] + pd.Timedelta(days=lookahead)))
    rendered = render_adjustment(spec, extended)["desktop"]
    lower, upper = int(np.floor(MA_MEAN_LAG_DAYS)), int(np.ceil(MA_MEAN_LAG_DAYS))
    advanced = 0.5 * (rendered.shift(-lower) + rendered.shift(-upper))
    daily = advanced.reindex(index)
    daily[index < seam] = 0.0
    return daily


def advanced_daily_ramp(
    spec: dict, index: pd.DatetimeIndex, seam: pd.Timestamp,
    post_anchor_rule: str = DEFAULT_POST_ANCHOR_RULE,
) -> pd.Series:
    """The daily headwind whose trailing 28-day mean is the MA-space ramp from seam + 27 onward.

    For an unclamped spec that is the advanced straight line and `post_anchor_rule` is ignored.
    For a clamped spec the rule decides what happens after the anchor (module docstring).
    """
    if post_anchor_rule not in POST_ANCHOR_RULES:
        raise ValueError(f"post_anchor_rule {post_anchor_rule!r} not in {POST_ANCHOR_RULES}")
    if not is_clamped(spec):
        return _advanced(spec, index, seam)
    if post_anchor_rule == "advanced_clamped":
        return _advanced(spec, index, seam)

    unclamped = {k: v for k, v in spec.items() if k != "clamp_at_anchor"}
    daily = _advanced(unclamped, index, seam)
    anchor = pd.Timestamp(spec["anchor_date"])
    after = index > anchor
    if post_anchor_rule == "flat_at_anchor":
        daily[after] = float(spec["desktop_dau"])
        return daily
    # "exact": rolling(28).mean() of the daily ramp must equal the MA-space ramp, which is flat
    # after the anchor. Flat MA means d(t) == d(t - 28) for every t past the anchor.
    values = daily.to_numpy().copy()
    for position in np.flatnonzero(after):
        values[position] = values[position - MA_WINDOW]
    return pd.Series(values, index=index)


def exact_through(spec: dict, post_anchor_rule: str) -> pd.Timestamp:
    """Last date on which the daily ramp is promised to re-smooth to the MA-space ramp exactly."""
    if not is_clamped(spec) or post_anchor_rule == "exact":
        return DISPLAY_END
    anchor = pd.Timestamp(spec["anchor_date"])
    if post_anchor_rule == "flat_at_anchor":
        return anchor
    # advanced_clamped: exact while the look-ahead window [t - 13.5, t + 13.5] is on one piece.
    return anchor - pd.Timedelta(days=int(np.ceil(MA_MEAN_LAG_DAYS)))


def check_ramp_inversion(
    daily_ramp: pd.Series, ma_ramp: pd.Series, seam: pd.Timestamp,
    through: pd.Timestamp = DISPLAY_END,
) -> None:
    """`rolling(28).mean()` of the daily ramp must equal the MA-space ramp from seam + 27 to `through`."""
    first_full = seam + pd.Timedelta(days=MA_WINDOW - 1)
    smoothed = daily_ramp.rolling(MA_WINDOW).mean()
    residual = (smoothed - ma_ramp)[first_full:through].abs().max()
    if residual > RAMP_INVERSION_TOLERANCE_DAU:
        raise AssertionError(
            f"advanced daily ramp does not invert the {MA_WINDOW}d mean: max residual "
            f"{residual:.3e} DAU between {first_full.date()} and {through.date()}. "
            f"The spec may not be a linear ramp."
        )


# --- Build --------------------------------------------------------------------------------------

def build_forecast_column(
    daily: pd.Series, spec: dict, seam: pd.Timestamp, index: pd.DatetimeIndex, post_anchor_rule: str,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """One cycle's daily column with its daily headwind, plus both ramps for the ledger.

    Training rows are the cycle's own actuals (as the published file does for the prior column);
    forecast rows are the model's daily forecast plus the daily headwind.
    """
    daily_ramp = advanced_daily_ramp(spec, index, seam, post_anchor_rule)
    ma_ramp = ma_space_ramp(spec, index, seam)
    check_ramp_inversion(daily_ramp, ma_ramp, seam, exact_through(spec, post_anchor_rule))
    column = daily.reindex(index) + daily_ramp
    return column, daily_ramp, ma_ramp


def build_curves(post_anchor_rule: str = DEFAULT_POST_ANCHOR_RULE) -> tuple[pd.DataFrame, dict]:
    """The daily frame plus everything the ledger and verification need."""
    index = pd.DatetimeIndex(pd.date_range(DISPLAY_START, DISPLAY_END))
    current_daily, current_training = load_world_daily(DESKTOP_FORECAST_PATH, DESKTOP_REQUIRED_STATE)
    prior_daily, prior_training = load_world_daily(PREV_DESKTOP_FORECAST_PATH, PREV_DESKTOP_REQUIRED_STATE)
    current_spec = load_win10_spec(CURRENT_ADJUSTMENTS_DIR)
    prior_spec = load_win10_spec(PRIOR_ADJUSTMENTS_DIR)

    actuals = current_daily.reindex(current_training).reindex(index)
    current, current_daily_ramp, current_ma_ramp = build_forecast_column(
        current_daily, current_spec, FORECAST_START, index, post_anchor_rule)
    prior, prior_daily_ramp, prior_ma_ramp = build_forecast_column(
        prior_daily, prior_spec, PREV_FORECAST_START, index, post_anchor_rule)
    # Mirror the published layout: the current column is blank before its seam.
    current[index < FORECAST_START] = np.nan

    curves = pd.DataFrame({
        "date": index,
        ACTUALS_COLUMN: actuals.round(0).values,
        PRIOR_COLUMN: prior.round(0).values,
        CURRENT_COLUMN: current.round(0).values,
    })
    context = {
        "post_anchor_rule": post_anchor_rule,
        "model_daily": {CURRENT_COLUMN: current_daily, PRIOR_COLUMN: prior_daily},
        "training_end": {CURRENT_COLUMN: current_training[-1], PRIOR_COLUMN: prior_training[-1]},
        "ramps": {
            CURRENT_COLUMN: (current_daily_ramp, current_ma_ramp, FORECAST_START, current_spec),
            PRIOR_COLUMN: (prior_daily_ramp, prior_ma_ramp, PREV_FORECAST_START, prior_spec),
        },
    }
    return curves, context


# --- Ledger -------------------------------------------------------------------------------------

def weirdness_ledger(curves: pd.DataFrame, context: dict) -> list[str]:
    """Every place the daily file behaves unlike the smooth one, as plain lines for stdout/README."""
    lines = []
    rule = context["post_anchor_rule"]
    for column, (daily_ramp, ma_ramp, seam, spec) in context["ramps"].items():
        anchor = pd.Timestamp(spec["anchor_date"])
        slope = spec["desktop_dau"] / (anchor - pd.Timestamp(spec["start_date"])).days
        model_daily = context["model_daily"][column]
        # Desktop drops ~40% at weekends, so a day-over-day step across the seam is meaningless on
        # daily data; compare the first forecast week with the last actual week instead.
        last_week = model_daily[seam - pd.Timedelta(days=7):seam - pd.Timedelta(days=1)].mean()
        first_week = (model_daily + daily_ramp.reindex(model_daily.index, fill_value=0.0))[
            seam:seam + pd.Timedelta(days=6)].mean()
        first_week_ramp = daily_ramp[seam:seam + pd.Timedelta(days=6)].mean()
        clamp_note = " then FLAT (clamp_at_anchor)" if is_clamped(spec) else " (unclamped, keeps ramping)"
        lines += [
            f"[{column}] spec ramps from {spec['start_date']} to {spec['desktop_dau']:+,} at "
            f"{spec['anchor_date']}{clamp_note}: slope {slope:+,.1f} DAU/day; 13.5-day advance = "
            f"{13.5 * slope:+,.0f} DAU.",
            f"[{column}] MA-space ramp at the seam {seam.date()}: {ma_ramp[seam]:+,.0f}; "
            f"daily headwind on the seam day: {daily_ramp[seam]:+,.0f}; at {MEASUREMENT_DATE.date()}: "
            f"daily {daily_ramp[MEASUREMENT_DATE]:+,.0f} vs MA-space {ma_ramp[MEASUREMENT_DATE]:+,.0f}.",
            f"[{column}] first forecast week ({seam.date()} on) averages {first_week - last_week:+,.0f} DAU/day "
            f"vs the last actual week, of which {first_week_ramp:+,.0f} is the daily headwind.",
        ]
        if is_clamped(spec):
            after = daily_ramp[anchor + pd.Timedelta(days=1):DISPLAY_END]
            resmoothed_gap = (daily_ramp.rolling(MA_WINDOW).mean() - ma_ramp)[
                seam + pd.Timedelta(days=MA_WINDOW - 1):DISPLAY_END]
            lines += [
                f"[{column}] post-anchor rule '{rule}': daily headwind after {anchor.date()} ranges "
                f"{after.min():+,.0f} to {after.max():+,.0f} (day after anchor {after.iloc[0]:+,.0f}); "
                f"file re-smooths exactly through {exact_through(spec, rule).date()}; max re-smoothed "
                f"headwind gap vs published anywhere from seam + 27: {resmoothed_gap.abs().max():,.0f} DAU "
                f"(at {MEASUREMENT_DATE.date()}: {resmoothed_gap[MEASUREMENT_DATE]:+,.0f}).",
            ]
    current_daily = context["model_daily"][CURRENT_COLUMN]
    prior_daily = context["model_daily"][PRIOR_COLUMN]
    overlap = current_daily.index.intersection(prior_daily.index)
    overlap = overlap[overlap <= context["training_end"][PRIOR_COLUMN]]
    actuals_drift = (current_daily - prior_daily)[overlap].abs().max()
    lines += [
        f"[{ACTUALS_COLUMN}] daily actuals end {context['training_end'][CURRENT_COLUMN].date()} (the "
        f"parquet's training rows); the published file's actuals run one day later, to the seam day.",
        f"[{PRIOR_COLUMN}] pre-seam rows are the prior cycle's own training rows (through "
        f"{context['training_end'][PRIOR_COLUMN].date()}); they differ from this cycle's pull by at most "
        f"{actuals_drift:,.0f} DAU on shared dates (late-landing telemetry).",
        f"[both forecast columns] inside the 27 days after each seam the published curve is "
        f"display_ma's non-linear splice, so rolling({MA_WINDOW}).mean() of this file does NOT "
        f"reproduce it there; from seam + 27 on it does to the DAU (asserted) wherever the rule promises.",
    ]
    return lines


# --- Verify -------------------------------------------------------------------------------------

def smoothed(frame: pd.DataFrame, column: str) -> pd.Series:
    """Trailing 28-day mean of a forecast column, with actuals filling the pre-seam window."""
    series = frame[column].where(frame[column].notna(), frame[ACTUALS_COLUMN])
    return series.rolling(MA_WINDOW).mean()


def verify(curves_path: Path, published: pd.DataFrame, context: dict) -> dict[str, float]:
    """Re-read the file and prove its rolling mean is the published curve past each transition.

    Returns the max transition-window discrepancy per column, for the ledger. Raises on any
    disagreement after the transition (within the window the rule promises exact), on actuals that
    do not re-smooth to the published actuals, or on a mobile/ALL column having leaked in.
    """
    frame = pd.read_csv(curves_path, parse_dates=["date"]).set_index("date")
    stray = [c for c in frame.columns if c.startswith(("mobile_", "all_"))]
    if stray:
        raise AssertionError(f"{curves_path} carries non-desktop columns {stray}.")

    actual_ma = frame[ACTUALS_COLUMN].rolling(MA_WINDOW).mean()
    actual_residual = (actual_ma - published[ACTUALS_COLUMN]).dropna().abs().max()
    if actual_residual > ACTUALS_SMOOTH_TOLERANCE_DAU:
        raise AssertionError(
            f"rolling {MA_WINDOW}d mean of {ACTUALS_COLUMN} differs from the published actuals by "
            f"{actual_residual:,.1f} DAU; the parquet's training rows are not the published actuals."
        )

    transition_max = {}
    for column, (_daily_ramp, _ma_ramp, seam, spec) in context["ramps"].items():
        first_full = seam + pd.Timedelta(days=MA_WINDOW - 1)
        through = exact_through(spec, context["post_anchor_rule"])
        diff = smoothed(frame, column) - published[column]
        settled = diff[first_full:through].abs().max()
        if settled > SMOOTH_TOLERANCE_DAU:
            raise AssertionError(
                f"rolling {MA_WINDOW}d mean of {column} differs from the published curve by up to "
                f"{settled:,.1f} DAU between {first_full.date()} and {through.date()}; the daily "
                f"ramp or the parquet is wrong."
            )
        transition_max[column] = diff[seam:first_full - pd.Timedelta(days=1)].abs().max()
    return transition_max


# --- Plot ---------------------------------------------------------------------------------------

def save_verification_plot(curves_path: Path, published: pd.DataFrame, context: dict, plot_path: Path) -> None:
    """Daily file over the published MA, plus the re-smoothed-minus-published residual per column."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    frame = pd.read_csv(curves_path, parse_dates=["date"]).set_index("date")
    fig, (top, bottom) = plt.subplots(
        2, 1, figsize=(14, 9), sharex=True, gridspec_kw={"height_ratios": [3, 1]})

    top.plot(frame.index, frame[ACTUALS_COLUMN], color="0.35", lw=0.8, label="daily actuals")
    top.plot(frame.index, frame[PRIOR_COLUMN], color="tab:orange", lw=0.8, alpha=0.7,
             label=f"daily {PRIOR_COLUMN}")
    top.plot(frame.index, frame[CURRENT_COLUMN], color="tab:blue", lw=0.8, label=f"daily {CURRENT_COLUMN}")
    top.plot(published.index, published[PRIOR_COLUMN], color="tab:orange", lw=2.2,
             label=f"published 28d MA {PRIOR_COLUMN}")
    top.plot(published.index, published[CURRENT_COLUMN], color="tab:blue", lw=2.2,
             label=f"published 28d MA {CURRENT_COLUMN}")
    top.plot(published.index, published[ACTUALS_COLUMN], color="black", lw=2.2, label="published 28d MA actuals")
    top.set_ylabel("desktop DAU")
    top.set_title(
        f"Desktop daily export vs published 28d MA -- post-anchor rule '{context['post_anchor_rule']}'")
    top.legend(loc="lower left", fontsize=8, ncol=2)
    top.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e6:.1f}M"))
    top.grid(alpha=0.3)

    for column, color in [(PRIOR_COLUMN, "tab:orange"), (CURRENT_COLUMN, "tab:blue")]:
        diff = smoothed(frame, column) - published[column]
        bottom.plot(diff.index, diff, color=color, lw=1.2, label=f"rolling(28) of daily {column} - published")
    for _column, (_d, _m, seam, _s) in context["ramps"].items():
        bottom.axvline(seam, color="0.6", ls=":", lw=1)
        bottom.axvline(seam + pd.Timedelta(days=MA_WINDOW - 1), color="0.6", ls="--", lw=1)
    bottom.axvline(MEASUREMENT_DATE, color="red", ls=":", lw=1)
    bottom.axhline(0, color="black", lw=0.8)
    bottom.set_ylabel("re-smoothed - published (DAU)")
    bottom.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e3:,.0f}K"))
    bottom.legend(loc="upper left", fontsize=8)
    bottom.grid(alpha=0.3)
    bottom.xaxis.set_major_formatter(mdates.DateFormatter("%b %d\n%Y"))

    fig.tight_layout()
    plot_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(plot_path, dpi=130)
    plt.close(fig)


# --- Main ---------------------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv-dir", default=CSV_DIR,
                        help=f"directory holding the published canonical CSV (default {CSV_DIR})")
    parser.add_argument("--post-anchor-rule", default=DEFAULT_POST_ANCHOR_RULE, choices=POST_ANCHOR_RULES,
                        help="daily headwind after the anchor for a clamped spec "
                             f"(default {DEFAULT_POST_ANCHOR_RULE}; see module docstring)")
    parser.add_argument("--plot-path", default=PLOT_PATH, help=f"verification plot (default {PLOT_PATH})")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the ledger and Dec-15 values without writing the file")
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    published = pd.read_csv(csv_dir / PUBLISHED_CURVES, parse_dates=["date"]).set_index("date")
    curves, context = build_curves(args.post_anchor_rule)

    print(f"Weirdness ledger (post-anchor rule '{args.post_anchor_rule}'):")
    for line in weirdness_ledger(curves, context):
        print("  " + line)
    frame = curves.set_index("date")
    print(f"\n{MEASUREMENT_DATE.date()}: daily {CURRENT_COLUMN} {frame.loc[MEASUREMENT_DATE, CURRENT_COLUMN]:,.0f} "
          f"(published 28d MA {published.loc[MEASUREMENT_DATE, CURRENT_COLUMN]:,.0f}); "
          f"daily {PRIOR_COLUMN} {frame.loc[MEASUREMENT_DATE, PRIOR_COLUMN]:,.0f} "
          f"(published 28d MA {published.loc[MEASUREMENT_DATE, PRIOR_COLUMN]:,.0f})")
    if args.dry_run:
        return

    curves_path = csv_dir / f"{Path(PUBLISHED_CURVES).stem}.{FILE_MARKER}.csv"
    curves.to_csv(curves_path, index=False)
    print(f"\nwrote {curves_path}  ({len(curves)} rows x {len(curves.columns)} cols)")

    transition_max = verify(curves_path, published, context)
    print("\nVerified:")
    print(f"  rolling {MA_WINDOW}d mean of {ACTUALS_COLUMN} reproduces the published actuals (<=1 DAU)")
    for column, (_d, _m, seam, spec) in context["ramps"].items():
        first_full = (seam + pd.Timedelta(days=MA_WINDOW - 1)).date()
        through = exact_through(spec, args.post_anchor_rule).date()
        print(f"  rolling {MA_WINDOW}d mean of {column} reproduces the published curve from {first_full} "
              f"through {through} (<=1 DAU); max discrepancy inside the transition "
              f"{transition_max[column]:,.0f} DAU")
    print("  no mobile or ALL columns present")

    plot_path = Path(args.plot_path)
    save_verification_plot(curves_path, published, context, plot_path)
    print(f"\nwrote {plot_path}")


if __name__ == "__main__":
    main()
