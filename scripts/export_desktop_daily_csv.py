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

The prior cycle's column gets the same treatment with its own frozen spec (read-only). Note July
2026's spec ramps from 2026-04-01, so its ramp is already non-zero at July's seam and its daily
seam step is larger; every such number is printed in the "weirdness ledger" and recorded in the
csv/ README.

Cycle-scoped: repoint the constants at each roll-forward.

Usage:
    python scripts/export_desktop_daily_csv.py            # write the file + verify
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
CSV_DIR = "data-official/2026-08/csv"
PUBLISHED_CURVES = "august_canonical_curves.csv"

DESKTOP_FORECAST_PATH = (
    "data-official/2026-08/desktop_g01_2026-08-02/"
    "cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/"
    "mozaic_daily_forecast.2026-08-02.ld-D.adj-lo.parquet"
)
PREV_DESKTOP_FORECAST_PATH = (
    "data-official/2026-07/desktop_locked/mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet"
)
CURRENT_ADJUSTMENTS_DIR = "data-official/2026-08/adjustments"
PRIOR_ADJUSTMENTS_DIR = "data-official/2026-07/adjustments"
WIN10_SPEC_FILENAME = "headwind.json"

FORECAST_START = pd.Timestamp("2026-08-02")       # August desktop seam
PREV_FORECAST_START = pd.Timestamp("2026-07-06")  # July's seam
DISPLAY_START = pd.Timestamp("2026-01-01")
DISPLAY_END = pd.Timestamp("2026-12-31")
MEASUREMENT_DATE = pd.Timestamp("2026-12-15")

CURRENT_COLUMN = "desktop_current_august"
PRIOR_COLUMN = "desktop_prior_july"
ACTUALS_COLUMN = "desktop_actuals"
FILE_MARKER = "DESKTOP_ONLY.DAILY"

DESKTOP_SEGMENT = '{"os": "ALL"}'
DESKTOP_APP_NAME = "desktop"
DESKTOP_DATA_SOURCE = "legacy_desktop"
DESKTOP_REQUIRED_STATE = ["l", "o"]

MA_WINDOW = 28
# A trailing window of N days has mean lag (N - 1) / 2 days. The ramp is advanced by exactly this.
MA_MEAN_LAG_DAYS = (MA_WINDOW - 1) / 2

# The published file is rounded to whole DAU and so is this one, so the smoothed daily file can
# differ from the published MA by rounding only. One DAU is the bound; anything larger is a bug.
SMOOTH_TOLERANCE_DAU = 1.0
# The advanced ramp must invert the rolling mean to float precision on a linear spec.
RAMP_INVERSION_TOLERANCE_DAU = 1e-6


# --- Load ---------------------------------------------------------------------------------------

def load_world_daily(path: str) -> tuple[pd.Series, pd.DatetimeIndex]:
    """World (`ALL`) daily desktop DAU from a forecast parquet, plus its training dates."""
    df, _meta = load_forecast(path, require_state=DESKTOP_REQUIRED_STATE)
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

def ma_space_ramp(spec: dict, index: pd.DatetimeIndex, seam: pd.Timestamp) -> pd.Series:
    """The headwind exactly as the display layer applies it: rendered ramp, zero before the seam."""
    ramp = render_adjustment(spec, index)["desktop"]
    ramp[index < seam] = 0.0
    return ramp


def advanced_daily_ramp(spec: dict, index: pd.DatetimeIndex, seam: pd.Timestamp) -> pd.Series:
    """The daily headwind whose trailing 28-day mean is the MA-space ramp from seam + 27 onward.

    `ramp(t + 13.5 days)` on the integer date grid is the mean of `ramp(t + 13)` and
    `ramp(t + 14)`, exact for a linear ramp. Rendered on an extended index so the last dates in
    `index` can look ahead. Zero before the seam: training rows are actuals and carry no headwind.
    """
    lookahead = int(np.ceil(MA_MEAN_LAG_DAYS)) + 1
    extended = pd.DatetimeIndex(pd.date_range(index[0], index[-1] + pd.Timedelta(days=lookahead)))
    rendered = render_adjustment(spec, extended)["desktop"]
    lower, upper = int(np.floor(MA_MEAN_LAG_DAYS)), int(np.ceil(MA_MEAN_LAG_DAYS))
    advanced = 0.5 * (rendered.shift(-lower) + rendered.shift(-upper))
    daily = advanced.reindex(index)
    daily[index < seam] = 0.0
    return daily


def check_ramp_inversion(daily_ramp: pd.Series, ma_ramp: pd.Series, seam: pd.Timestamp) -> None:
    """`rolling(28).mean()` of the daily ramp must equal the MA-space ramp from seam + 27 onward."""
    first_full = seam + pd.Timedelta(days=MA_WINDOW - 1)
    smoothed = daily_ramp.rolling(MA_WINDOW).mean()
    residual = (smoothed - ma_ramp)[first_full:].abs().max()
    if residual > RAMP_INVERSION_TOLERANCE_DAU:
        raise AssertionError(
            f"advanced daily ramp does not invert the {MA_WINDOW}d mean: max residual "
            f"{residual:.3e} DAU from {first_full.date()}. The spec may not be a linear ramp."
        )


# --- Build --------------------------------------------------------------------------------------

def build_forecast_column(
    daily: pd.Series, spec: dict, seam: pd.Timestamp, index: pd.DatetimeIndex
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """One cycle's daily column with its advanced headwind, plus both ramps for the ledger.

    Training rows are the cycle's own actuals (as the published file does for the prior column);
    forecast rows are the model's daily forecast plus the advanced daily ramp.
    """
    daily_ramp = advanced_daily_ramp(spec, index, seam)
    ma_ramp = ma_space_ramp(spec, index, seam)
    check_ramp_inversion(daily_ramp, ma_ramp, seam)
    column = daily.reindex(index) + daily_ramp
    return column, daily_ramp, ma_ramp


def build_curves() -> tuple[pd.DataFrame, dict]:
    """The daily frame plus everything the ledger and verification need."""
    index = pd.DatetimeIndex(pd.date_range(DISPLAY_START, DISPLAY_END))
    august_daily, august_training = load_world_daily(DESKTOP_FORECAST_PATH)
    july_daily, july_training = load_world_daily(PREV_DESKTOP_FORECAST_PATH)
    august_spec = load_win10_spec(CURRENT_ADJUSTMENTS_DIR)
    july_spec = load_win10_spec(PRIOR_ADJUSTMENTS_DIR)

    actuals = august_daily.reindex(august_training).reindex(index)
    current, current_daily_ramp, current_ma_ramp = build_forecast_column(
        august_daily, august_spec, FORECAST_START, index)
    prior, prior_daily_ramp, prior_ma_ramp = build_forecast_column(
        july_daily, july_spec, PREV_FORECAST_START, index)
    # Mirror the published layout: the current column is blank before its seam.
    current[index < FORECAST_START] = np.nan

    curves = pd.DataFrame({
        "date": index,
        ACTUALS_COLUMN: actuals.round(0).values,
        PRIOR_COLUMN: prior.round(0).values,
        CURRENT_COLUMN: current.round(0).values,
    })
    context = {
        "august_daily": august_daily, "july_daily": july_daily,
        "august_training_end": august_training[-1], "july_training_end": july_training[-1],
        "ramps": {
            CURRENT_COLUMN: (current_daily_ramp, current_ma_ramp, FORECAST_START, august_spec),
            PRIOR_COLUMN: (prior_daily_ramp, prior_ma_ramp, PREV_FORECAST_START, july_spec),
        },
    }
    return curves, context


# --- Ledger -------------------------------------------------------------------------------------

def weirdness_ledger(curves: pd.DataFrame, context: dict) -> list[str]:
    """Every place the daily file behaves unlike the smooth one, as plain lines for stdout/README."""
    lines = []
    for column, (daily_ramp, ma_ramp, seam, spec) in context["ramps"].items():
        slope = spec["desktop_dau"] / (pd.Timestamp(spec["anchor_date"]) - pd.Timestamp(spec["start_date"])).days
        model_daily = context["august_daily" if column == CURRENT_COLUMN else "july_daily"]
        # Desktop drops ~40% at weekends, so a day-over-day step across the seam is meaningless on
        # daily data; compare the first forecast week with the last actual week instead.
        last_week = model_daily[seam - pd.Timedelta(days=7):seam - pd.Timedelta(days=1)].mean()
        first_week = (model_daily + daily_ramp.reindex(model_daily.index, fill_value=0.0))[
            seam:seam + pd.Timedelta(days=6)].mean()
        first_week_ramp = daily_ramp[seam:seam + pd.Timedelta(days=6)].mean()
        lines += [
            f"[{column}] spec ramps from {spec['start_date']} to {spec['desktop_dau']:+,} at "
            f"{spec['anchor_date']}: slope {slope:+,.1f} DAU/day; 13.5-day advance = {13.5 * slope:+,.0f} DAU.",
            f"[{column}] MA-space ramp at the seam {seam.date()}: {ma_ramp[seam]:+,.0f}; "
            f"daily headwind on the seam day: {daily_ramp[seam]:+,.0f}; at {MEASUREMENT_DATE.date()}: "
            f"daily {daily_ramp[MEASUREMENT_DATE]:+,.0f} vs MA-space {ma_ramp[MEASUREMENT_DATE]:+,.0f}.",
            f"[{column}] first forecast week ({seam.date()} on) averages {first_week - last_week:+,.0f} DAU/day "
            f"vs the last actual week, of which {first_week_ramp:+,.0f} is the daily headwind.",
        ]
    overlap = context["august_daily"].index.intersection(context["july_daily"].index)
    overlap = overlap[overlap <= context["july_training_end"]]
    actuals_drift = (context["august_daily"] - context["july_daily"])[overlap].abs().max()
    lines += [
        f"[{ACTUALS_COLUMN}] daily actuals end {context['august_training_end'].date()} (the parquet's "
        f"training rows); the published file's actuals run one day later, to the seam day.",
        f"[{PRIOR_COLUMN}] pre-seam rows are July's own training rows (through "
        f"{context['july_training_end'].date()}); they differ from August's pull by at most "
        f"{actuals_drift:,.0f} DAU on shared dates (late-landing telemetry).",
        f"[both forecast columns] inside the 27 days after each seam the published curve is "
        f"display_ma's non-linear splice, so rolling({MA_WINDOW}).mean() of this file does NOT "
        f"reproduce it there; from seam + 27 on it does to the DAU (asserted).",
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
    disagreement after the transition, on actuals that do not re-smooth to the published actuals,
    or on a mobile/ALL column having leaked in.
    """
    frame = pd.read_csv(curves_path, parse_dates=["date"]).set_index("date")
    stray = [c for c in frame.columns if c.startswith(("mobile_", "all_"))]
    if stray:
        raise AssertionError(f"{curves_path} carries non-desktop columns {stray}.")

    actual_ma = frame[ACTUALS_COLUMN].rolling(MA_WINDOW).mean()
    actual_residual = (actual_ma - published[ACTUALS_COLUMN]).dropna().abs().max()
    if actual_residual > SMOOTH_TOLERANCE_DAU:
        raise AssertionError(
            f"rolling {MA_WINDOW}d mean of {ACTUALS_COLUMN} differs from the published actuals by "
            f"{actual_residual:,.1f} DAU; the parquet's training rows are not the published actuals."
        )

    transition_max = {}
    for column, (_daily_ramp, _ma_ramp, seam, _spec) in context["ramps"].items():
        first_full = seam + pd.Timedelta(days=MA_WINDOW - 1)
        diff = smoothed(frame, column) - published[column]
        settled = diff[first_full:DISPLAY_END].abs().max()
        if settled > SMOOTH_TOLERANCE_DAU:
            raise AssertionError(
                f"rolling {MA_WINDOW}d mean of {column} differs from the published curve by up to "
                f"{settled:,.1f} DAU from {first_full.date()}; the advanced ramp or the parquet is wrong."
            )
        transition_max[column] = diff[seam:first_full - pd.Timedelta(days=1)].abs().max()
    return transition_max


# --- Main ---------------------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv-dir", default=CSV_DIR,
                        help=f"directory holding the published canonical CSV (default {CSV_DIR})")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the ledger and Dec-15 values without writing the file")
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    published = pd.read_csv(csv_dir / PUBLISHED_CURVES, parse_dates=["date"]).set_index("date")
    curves, context = build_curves()

    print("Weirdness ledger:")
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
    for column, (_d, _m, seam, _s) in context["ramps"].items():
        first_full = (seam + pd.Timedelta(days=MA_WINDOW - 1)).date()
        print(f"  rolling {MA_WINDOW}d mean of {column} reproduces the published curve from {first_full} "
              f"(<=1 DAU); max discrepancy inside the transition {transition_max[column]:,.0f} DAU")
    print("  no mobile or ALL columns present")


if __name__ == "__main__":
    main()
