# -*- coding: utf-8 -*-
"""Tests for the variance-matched seam transition in
data-official/2026-06/export_canonical_curves.py.

The published forecast 28dMA curves oscillated for ~1 month after the forecast-start date
because the trailing 28-day MA window straddled the raw-actuals -> forecast seam, and the
forecast's weekly-seasonality amplitude differs from the recent actuals' amplitude for
high day-of-week-swing countries (see research/ma-seam-turbulence/). `display_ma` rebuilds
the first 27 forecast days to carry the recent actuals' weekly amplitude (via
`reconstruct_matched_daily`), so a trailing 28d window straddling the seam sees a
*continuous* weekly amplitude, cancels it, and the transition MA rides the true forecast
trend (curvature and all) — smoothly, and as an unbiased predictor of the realized 28dMA.
Every date from forecast_start + 27 onward (incl. Dec-15) stays byte-identical to the naive
blend (the clean forecast-only MA), so headline numbers do not move.

These tests construct synthetic series with a deliberate seam amplitude discontinuity so the
NAIVE blend genuinely wobbles — that is what makes the smoothness/improvement assertions
capable of catching a regression (if `display_ma` ever reverts to the blend, they fail).

🔒 SECURITY: synthetic data only.
"""

import importlib.util
import os
import subprocess

import numpy as np
import pandas as pd
import pytest

GIT_ROOT = subprocess.run(
    ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
).stdout.strip()
EXPORT_PY = os.path.join(GIT_ROOT, "data-official/2026-06/export_canonical_curves.py")

_spec = importlib.util.spec_from_file_location("export_canonical_curves", EXPORT_PY)
export = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export)

WINDOW = 28
FORECAST_START = pd.Timestamp("2026-06-01")
SEAM_CLEAR = FORECAST_START + pd.Timedelta(days=WINDOW - 1)  # first clean forecast-only MA point
LAST_TRANSITION = FORECAST_START + pd.Timedelta(days=WINDOW - 2)  # forecast day 27


def _rel_2nd_diff_ppm(series, start, end):
    """Mean |2nd difference| over [start, end] normalized by mean level, in ppm."""
    w = series[(series.index >= start) & (series.index <= end)].dropna()
    return 1e6 * w.diff().diff().abs().mean() / w.mean()


@pytest.fixture
def seam_series():
    """Daily series: flat trend + a weekday/weekend cycle whose AMPLITUDE drops at the seam,
    with the weekly MEAN preserved across the seam (both day-of-week profiles normalized to
    weekly mean 1).

    Actuals: weekend = 54% of weekday (large weekly amplitude, like AR).
    Forecast: weekend = 86% of weekday (damped amplitude).
    The amplitude discontinuity — not a level jump — is what makes the naive 28dMA wobble.
    (A real forecast seam preserves the weekly mean and changes only amplitude; an
    un-normalized level jump would inject a spurious transition ramp.)
    """
    dates = pd.date_range("2026-01-01", "2026-12-31", freq="D")
    level = 400_000.0
    is_forecast = dates >= FORECAST_START
    is_weekend = dates.dayofweek >= 5
    # (5*weekday + 2*weekend)/7 == 1 for each profile -> weekly mean preserved across the seam.
    actuals_dow = np.where(is_weekend, 0.625, 1.15)    # weekend/weekday = 0.54 (strong, like AR)
    forecast_dow = np.where(is_weekend, 0.893, 1.043)  # weekend/weekday = 0.86 (damped)
    dow_factor = np.where(is_forecast, forecast_dow, actuals_dow)
    values = pd.Series(level * dow_factor, index=dates)
    return dates.to_series(name="target_date").reset_index(drop=True), values.reset_index(drop=True)


@pytest.fixture
def curved_seam_series():
    """Series with a CURVED forecast trend × damped weekly, actuals = same trend × strong weekly.

    Both day-of-week profiles are normalized to weekly mean 1 (5 weekdays + 2 weekend days),
    so actuals and forecast share the same weekly mean and differ only in weekly *amplitude*
    — the realistic seam discontinuity, without a spurious level step.

    Returns (dates, daily_values, truth_daily) where truth_daily = forecast trend carried with
    the actuals' (strong) weekly amplitude — i.e. what reality looks like if the forecast trend
    is right but real data keeps its higher weekend swing. A straight bridge cannot reproduce
    the trend's curvature; the variance-matched reconstruction can.
    """
    dates = pd.date_range("2026-01-01", "2026-12-31", freq="D")
    idx = np.arange(len(dates))
    trend = 400_000 + 50_000 * np.sin(2 * np.pi * idx / 90.0)  # clearly curved over 27 days
    is_weekend = dates.dayofweek.values >= 5

    def dow(weekday_v, weekend_v):
        # (5*weekday + 2*weekend)/7 == 1 by construction below
        return np.where(is_weekend, weekend_v, weekday_v)

    actuals_dow = dow(1.2, 0.5)    # mean 1, strong swing (weekend/weekday = 0.42)
    forecast_dow = dow(1.06, 0.85)  # mean 1, damped swing (weekend/weekday = 0.80)
    is_forecast = dates >= FORECAST_START
    daily = np.where(is_forecast, trend * forecast_dow, trend * actuals_dow)
    truth_daily = trend * actuals_dow

    target_date = dates.to_series(name="target_date").reset_index(drop=True)
    return target_date, pd.Series(daily).reset_index(drop=True), pd.Series(truth_daily).reset_index(drop=True)


def test_naive_blend_actually_wobbles(seam_series):
    """Guard against a tautological test: the synthetic seam must wobble under the blend."""
    dates, values = seam_series
    blend = export.daily_to_28ma(dates, values)
    assert _rel_2nd_diff_ppm(blend, FORECAST_START, SEAM_CLEAR) > 1_000


def test_display_ma_smooths_the_seam(seam_series):
    """display_ma's variance-matched transition is far smoother than the naive blend's.

    A curved transition has nonzero 2nd difference (unlike the old linear bridge), so this
    asserts a *relative* improvement, not an absolute floor.
    """
    dates, values = seam_series
    blend = export.daily_to_28ma(dates, values)
    display = export.display_ma(dates, values, FORECAST_START)
    blend_ppm = _rel_2nd_diff_ppm(blend, FORECAST_START, SEAM_CLEAR)
    display_ppm = _rel_2nd_diff_ppm(display, FORECAST_START, SEAM_CLEAR)
    assert display_ppm < blend_ppm / 3


def test_far_horizon_byte_identical(seam_series):
    """Every date >= seam+27 must be unchanged vs the naive blend (Dec-15 preservation)."""
    dates, values = seam_series
    blend = export.daily_to_28ma(dates, values)
    display = export.display_ma(dates, values, FORECAST_START)
    post = display.index >= SEAM_CLEAR
    max_delta = (display[post] - blend.reindex(display.index)[post]).abs().max()
    assert max_delta < 1e-9
    # explicit Dec-15 check
    dec15 = pd.Timestamp("2026-12-15")
    assert abs(display.loc[dec15] - blend.loc[dec15]) < 1e-9


def test_seam_anchor_is_continuous_with_actuals(seam_series):
    """The day-1 transition point steps smoothly off the trailing actuals-only MA (no jump).

    The variance-matched transition is NOT byte-equal to the old blend at day 1, so this
    asserts continuity (a small step relative to level), not equality.
    """
    dates, values = seam_series
    s = pd.Series(values.values, index=pd.to_datetime(dates.values)).sort_index()
    actuals_only_ma = s[s.index < FORECAST_START].rolling(WINDOW).mean().dropna()
    display = export.display_ma(dates, values, FORECAST_START)
    last_actual_ma = actuals_only_ma.iloc[-1]
    day1 = display.loc[FORECAST_START]
    step = abs(day1 - last_actual_ma)
    assert step / day1 < 0.02  # continuity, not byte-equality


def test_actuals_region_unchanged(seam_series):
    """Dates before the seam are the plain 28dMA (the fix only touches the forecast region)."""
    dates, values = seam_series
    blend = export.daily_to_28ma(dates, values)
    display = export.display_ma(dates, values, FORECAST_START)
    pre = display.index < FORECAST_START
    max_delta = (display[pre] - blend.reindex(display.index)[pre]).abs().max()
    assert max_delta < 1e-9


def test_splice_is_smooth_across_day_27_to_28(seam_series):
    """No kink where the variance-matched transition hands off to the clean forecast-only MA
    at day 28: the step across the splice AND the local 2nd difference over days 26->27->28->29
    are both small relative to level. This encodes the explicit "smooth across 26,27 -> 28,29"
    requirement, and catches a broken handoff (mis-aligned splice indices, or a reconstruction
    whose weekly mean drifts off the forecast's level — e.g. dropping the mean-1 normalization,
    which would land the transition ~15% off the forecast-only MA at the splice).
    """
    dates, values = seam_series
    display = export.display_ma(dates, values, FORECAST_START)
    level = display[(display.index >= FORECAST_START) & (display.index <= SEAM_CLEAR)].mean()

    d26 = display.loc[FORECAST_START + pd.Timedelta(days=25)]  # forecast day 26
    d27 = display.loc[LAST_TRANSITION]                          # forecast day 27 (last transition)
    d28 = display.loc[SEAM_CLEAR]                               # forecast day 28 (first forecast-only)
    d29 = display.loc[SEAM_CLEAR + pd.Timedelta(days=1)]        # forecast day 29

    splice_step = abs(d28 - d27)
    assert splice_step / level < 0.01  # no visible jump at the handoff

    second_diff = abs((d29 - d28) - (d28 - d27)) + abs((d28 - d27) - (d27 - d26))
    assert second_diff / level < 0.02  # locally smooth across 26 -> 27 -> 28 -> 29


def test_continuous_splice_collapses_the_splice_corner(curved_seam_series):
    """The continuous-splice correction (default True) matches BOTH level and slope at the splice,
    so the CORNER (2nd-difference) across the day-(window-1) handoff collapses vs the uncorrected
    cliff. Smoothness is a 2nd-difference property: the correction matches slope, so the 1st-diff
    step is NOT zeroed (it equals the true local slope) — measuring the corner is the point. If
    the correction is removed, on==off and the strict-inequality fails.
    """
    dates, values, _ = curved_seam_series
    off = export.display_ma(dates, values, FORECAST_START, continuous_splice=False)
    on = export.display_ma(dates, values, FORECAST_START, continuous_splice=True)

    def splice_corner(ma):
        d = SEAM_CLEAR
        return abs((ma.loc[d + pd.Timedelta(days=1)] - ma.loc[d])
                   - (ma.loc[d] - ma.loc[d - pd.Timedelta(days=1)]))

    corner_off, corner_on = splice_corner(off), splice_corner(on)
    assert corner_off > 1000            # the uncorrected seam genuinely corners (guard against a no-op fixture)
    assert corner_on < corner_off / 5   # the C1 correction collapses the corner
    # day-0 continuity with actuals is preserved (correction is 0 at the seam).
    assert on.loc[FORECAST_START] == pytest.approx(off.loc[FORECAST_START])


def test_slope_match_trades_corner_for_deviation(curved_seam_series):
    """slope_match controls the corner<->deviation trade-off: matching more of the splice slope
    shrinks the handoff corner but grows the max deviation from the uncorrected curve (overshoot).
    Locks the parameter's dual effect and that it isn't ignored.
    """
    dates, values, _ = curved_seam_series
    cliff = export.display_ma(dates, values, FORECAST_START, continuous_splice=False)

    def splice_corner(ma):
        d = SEAM_CLEAR
        return abs((ma.loc[d + pd.Timedelta(days=1)] - ma.loc[d])
                   - (ma.loc[d] - ma.loc[d - pd.Timedelta(days=1)]))

    def corner_and_dev(sm):
        ma = export.display_ma(dates, values, FORECAST_START, slope_match=sm)
        return splice_corner(ma), (ma - cliff).abs().max()

    c0, d0 = corner_and_dev(0.0)
    c5, d5 = corner_and_dev(0.5)
    c1, d1 = corner_and_dev(1.0)
    assert c1 < c5 < c0        # more slope-matching -> smaller corner
    assert d1 > d5 > d0        # more slope-matching -> larger deviation (overshoot)


def test_curved_transition_beats_a_straight_line(curved_seam_series):
    """With a curved forecast trend, the variance-matched transition tracks the realized 28dMA
    better than a straight line between the transition endpoints — proving the refinement is
    real curvature capture, not cosmetic. Guards: the truth must be meaningfully curved so a
    straight line cannot trivially win.
    """
    dates, values, truth_daily = curved_seam_series
    new = export.display_ma(dates, values, FORECAST_START)
    truth_ma = export.daily_to_28ma(dates, truth_daily)

    mask = (new.index >= FORECAST_START) & (new.index <= LAST_TRANSITION)
    new_t = new[mask]
    truth_t = truth_ma.reindex(new.index)[mask]

    # Straight line between the realized transition's endpoints (what the linear bridge approximates).
    straight = np.linspace(truth_t.iloc[0], truth_t.iloc[-1], len(truth_t))

    mae_new = np.abs(new_t.values - truth_t.values).mean()
    mae_line = np.abs(straight - truth_t.values).mean()

    # The truth must genuinely curve, else a line could win for free.
    assert _rel_2nd_diff_ppm(truth_t, FORECAST_START, LAST_TRANSITION) > 100
    assert mae_new < mae_line / 3
