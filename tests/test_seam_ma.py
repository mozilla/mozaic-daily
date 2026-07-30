# -*- coding: utf-8 -*-
"""Tests for src/mozaic_daily/seam_ma.py — the variance-matched actuals->forecast seam MA.

Carried over from the retired tests/test_export_canonical_curves.py (which targeted the now-frozen
copy under data-official/2026-06/, see _archive/_index.md), with two deliberate changes and three
additions driven by the trend-estimator defect diagnosed in research/ma-seam-turbulence/.

CHANGED — `test_transition_converges_to_the_plain_ma` replaces the old
`test_seam_anchor_is_continuous_with_actuals`. The old test asserted that the display MA is
*continuous with the trailing actuals MA* at the seam, with a 2% band. That premise is wrong: a
trailing MA legitimately moves day to day, so "continuity" has no fixed correct value and the band
had to be loose enough to accommodate real movement — which is exactly why it accepted a +211K
reconstruction artifact (0.22% of level) for a full cycle. The honest requirement is that the
transition reproduce the *plain* trailing MA at the seam, which has one correct answer.

CHANGED — the fixtures now carry a full seven-level day-of-week profile and a parametrizable seam
date. The old fixtures had only two levels (weekday vs weekend) and hardcoded a Monday seam, so
they could not express a day-of-week-unbalanced window at all; the defect was invisible to them by
construction.

ADDED — weekday invariance, the identity invariant, and window completeness. See each docstring.

🔒 SECURITY: synthetic data only.
"""

import numpy as np
import pandas as pd
import pytest

from mozaic_daily import seam_ma

WINDOW = 28
FORECAST_START = pd.Timestamp("2026-06-01")            # a Monday, as in the retired suite
SEAM_CLEAR = FORECAST_START + pd.Timedelta(days=WINDOW - 1)
LAST_TRANSITION = FORECAST_START + pd.Timedelta(days=WINDOW - 2)

# Seven-level profiles, each normalized to mean 1 so the weekly mean is preserved across the seam
# and only the AMPLITUDE differs — the realistic seam discontinuity. Modelled on measured desktop
# behaviour (Tue ~1.14, Sun ~0.70); the forecast's swing is damped, as Prophet's is in practice.
_ACTUALS_DOW = np.array([1.11, 1.15, 1.14, 1.12, 1.03, 0.75, 0.70])
_FORECAST_DOW = np.array([1.06, 1.08, 1.07, 1.06, 1.02, 0.86, 0.85])
_ACTUALS_DOW = _ACTUALS_DOW / _ACTUALS_DOW.mean()
_FORECAST_DOW = _FORECAST_DOW / _FORECAST_DOW.mean()

# The flat fixture's true deseasonalized level. Because the fixture is exactly
# `trend x weekday factor` with a mean-1 profile, the CORRECT rebuilt value on any day is
# analytically known: _TRUE_TREND * _ACTUALS_DOW[weekday]. Several tests below score the
# estimator against that instead of against another curve, so they have one right answer.
_TRUE_TREND = 400_000.0


def _rel_2nd_diff_ppm(series, start, end):
    """Mean |2nd difference| over [start, end] normalized by mean level, in ppm."""
    w = series[(series.index >= start) & (series.index <= end)].dropna()
    return 1e6 * w.diff().diff().abs().mean() / w.mean()


def _build(seam, trend_fn=None, dates=None):
    """Synthetic daily series: trend x (strong weekly before the seam, damped after).

    Returns (dates, values) shaped like the callers' inputs (positional Series, not indexed).
    """
    dates = dates if dates is not None else pd.date_range("2026-01-01", "2026-12-31", freq="D")
    trend = np.full(len(dates), 400_000.0) if trend_fn is None else trend_fn(np.arange(len(dates)))
    weekday = dates.dayofweek.values
    factor = np.where(dates >= seam, _FORECAST_DOW[weekday], _ACTUALS_DOW[weekday])
    values = trend * factor
    return (dates.to_series(name="target_date").reset_index(drop=True),
            pd.Series(values).reset_index(drop=True))


@pytest.fixture
def seam_series():
    """Flat trend, strong weekly amplitude before the seam and damped after."""
    return _build(FORECAST_START)


@pytest.fixture
def curved_seam_series():
    """Curved trend so a straight bridge cannot trivially reproduce the transition.

    Third element is the 'truth': the forecast's trend carried with the ACTUALS' weekly
    amplitude — what reality looks like if the trend is right but real data keeps its swing.
    """
    dates = pd.date_range("2026-01-01", "2026-12-31", freq="D")

    # Amplitude doubled vs the retired suite's 50,000. The A1 trend estimator lands the
    # transition much closer to the forecast-only MA than the biased estimator did, so the
    # UNCORRECTED splice corner is now smaller; the fixture needs more curvature for
    # `test_continuous_splice_collapses_the_splice_corner`'s no-op guard to remain meaningful.
    def trend_fn(idx):
        return 400_000 + 100_000 * np.sin(2 * np.pi * idx / 90.0)

    d, v = _build(FORECAST_START, trend_fn=trend_fn, dates=dates)
    truth = trend_fn(np.arange(len(dates))) * _ACTUALS_DOW[dates.dayofweek.values]
    return d, v, pd.Series(truth).reset_index(drop=True)


# --------------------------------------------------------------- carried over from the old suite

def test_naive_blend_actually_wobbles(seam_series):
    """Guard against a tautological suite: the synthetic seam must wobble under the plain blend."""
    dates, values = seam_series
    blend = seam_ma.daily_to_28ma(dates, values)
    assert _rel_2nd_diff_ppm(blend, FORECAST_START, SEAM_CLEAR) > 1_000


def test_display_ma_smooths_the_seam(seam_series):
    """The variance-matched transition is far smoother than the naive blend's wobble."""
    dates, values = seam_series
    blend = seam_ma.daily_to_28ma(dates, values)
    display = seam_ma.display_ma(dates, values, FORECAST_START)
    blend_ppm = _rel_2nd_diff_ppm(blend, FORECAST_START, SEAM_CLEAR)
    display_ppm = _rel_2nd_diff_ppm(display, FORECAST_START, SEAM_CLEAR)
    assert display_ppm < blend_ppm / 3


def test_far_horizon_byte_identical(seam_series):
    """Every date >= seam+27 is unchanged vs the naive blend — the Dec-15 / KPI guarantee."""
    dates, values = seam_series
    blend = seam_ma.daily_to_28ma(dates, values)
    display = seam_ma.display_ma(dates, values, FORECAST_START)
    post = display.index >= SEAM_CLEAR
    assert (display[post] - blend.reindex(display.index)[post]).abs().max() < 1e-9
    dec15 = pd.Timestamp("2026-12-15")
    assert abs(display.loc[dec15] - blend.loc[dec15]) < 1e-9


def test_actuals_region_unchanged(seam_series):
    """Dates before the seam are the plain MA — the fix only touches the forecast region."""
    dates, values = seam_series
    blend = seam_ma.daily_to_28ma(dates, values)
    display = seam_ma.display_ma(dates, values, FORECAST_START)
    pre = display.index < FORECAST_START
    assert (display[pre] - blend.reindex(display.index)[pre]).abs().max() < 1e-9


def test_splice_is_smooth_across_day_27_to_28(seam_series):
    """No kink where the transition hands off to the clean forecast-only MA at day 28.

    Catches a mis-aligned splice index, and a reconstruction whose weekly mean drifts off the
    forecast's level (e.g. dropping the mean-1 normalization, which lands the transition ~15% off).
    """
    dates, values = seam_series
    display = seam_ma.display_ma(dates, values, FORECAST_START)
    level = display[(display.index >= FORECAST_START) & (display.index <= SEAM_CLEAR)].mean()

    d26 = display.loc[FORECAST_START + pd.Timedelta(days=25)]
    d27 = display.loc[LAST_TRANSITION]
    d28 = display.loc[SEAM_CLEAR]
    d29 = display.loc[SEAM_CLEAR + pd.Timedelta(days=1)]

    assert abs(d28 - d27) / level < 0.01
    second_diff = abs((d29 - d28) - (d28 - d27)) + abs((d28 - d27) - (d27 - d26))
    assert second_diff / level < 0.02


def test_continuous_splice_collapses_the_splice_corner(curved_seam_series):
    """The C1 correction collapses the 2nd-difference corner at the day-27 handoff."""
    dates, values, _ = curved_seam_series
    off = seam_ma.display_ma(dates, values, FORECAST_START, continuous_splice=False)
    on = seam_ma.display_ma(dates, values, FORECAST_START, continuous_splice=True)

    def splice_corner(ma):
        d = SEAM_CLEAR
        return abs((ma.loc[d + pd.Timedelta(days=1)] - ma.loc[d])
                   - (ma.loc[d] - ma.loc[d - pd.Timedelta(days=1)]))

    corner_off, corner_on = splice_corner(off), splice_corner(on)
    assert corner_off > 1000              # the uncorrected seam genuinely corners
    # The retired suite asserted a 5x collapse. That threshold was calibrated against the
    # biased trend estimator, whose large LANDING residual dominated the corner and which the
    # correction removes in full. A1 shrinks that residual ~14x, so what remains at the splice
    # is mostly a slope mismatch — and `slope_match=0.4` deliberately takes only 40% of it.
    # A ~1.6x reduction is therefore the honest expectation, not 5x. The monotone ordering in
    # `test_slope_match_trades_corner_for_deviation` is the stronger proof the knob works.
    assert corner_on < corner_off * 0.8   # the correction still measurably reduces the corner
    assert on.loc[FORECAST_START] == pytest.approx(off.loc[FORECAST_START])


def test_slope_match_trades_corner_for_deviation(curved_seam_series):
    """slope_match shrinks the handoff corner and grows the overshoot — both, and it isn't ignored."""
    dates, values, _ = curved_seam_series
    cliff = seam_ma.display_ma(dates, values, FORECAST_START, continuous_splice=False)

    def splice_corner(ma):
        d = SEAM_CLEAR
        return abs((ma.loc[d + pd.Timedelta(days=1)] - ma.loc[d])
                   - (ma.loc[d] - ma.loc[d - pd.Timedelta(days=1)]))

    def corner_and_dev(sm):
        ma = seam_ma.display_ma(dates, values, FORECAST_START, slope_match=sm)
        return splice_corner(ma), (ma - cliff).abs().max()

    c0, d0 = corner_and_dev(0.0)
    c5, d5 = corner_and_dev(0.5)
    c1, d1 = corner_and_dev(1.0)
    assert c1 < c5 < c0
    assert d1 > d5 > d0


def test_curved_transition_beats_a_straight_line(curved_seam_series):
    """With a curved trend the transition tracks the realized MA better than a straight line."""
    dates, values, truth_daily = curved_seam_series
    new = seam_ma.display_ma(dates, values, FORECAST_START)
    truth_ma = seam_ma.daily_to_28ma(dates, truth_daily)

    mask = (new.index >= FORECAST_START) & (new.index <= LAST_TRANSITION)
    new_t, truth_t = new[mask], truth_ma.reindex(new.index)[mask]
    straight = np.linspace(truth_t.iloc[0], truth_t.iloc[-1], len(truth_t))

    mae_new = np.abs(new_t.values - truth_t.values).mean()
    mae_line = np.abs(straight - truth_t.values).mean()
    assert _rel_2nd_diff_ppm(truth_t, FORECAST_START, LAST_TRANSITION) > 100
    assert mae_new < mae_line / 3


# ------------------------------------------------------------------------------------ new tests

def test_transition_ma_matches_the_analytically_correct_transition(seam_series):
    """REPLACES the old 2% "continuity with actuals" band.

    The retired test compared the day-1 transition point against the *previous* day's
    actuals-only MA and allowed a 2%-of-level step. That has no correct value — a trailing MA
    legitimately moves every day — so the band had to be loose enough to admit real movement,
    and it duly admitted a +211K reconstruction artifact (0.22% of level) for a full cycle.

    Here the correct answer is computable. The fixture is exactly `trend x weekday factor`, so
    the correct rebuilt day-1 value is `_TRUE_TREND * _ACTUALS_DOW[weekday]`, and the correct
    transition MA at the seam is the mean of the 27 trailing actuals plus that value. Note this
    is NOT the plain MA: the rebuilt day legitimately differs from the raw forecast day by the
    intended amplitude swap. Scoring against the plain MA would conflate that swap with error.
    """
    dates, values = seam_series
    display = seam_ma.display_ma(dates, values, FORECAST_START)
    s = pd.Series(values.values, index=pd.to_datetime(dates.values)).sort_index()

    trailing_actuals = s[(s.index >= FORECAST_START - pd.Timedelta(days=WINDOW - 1))
                         & (s.index < FORECAST_START)]
    correct_day1 = _TRUE_TREND * _ACTUALS_DOW[FORECAST_START.dayofweek]
    correct_ma = (trailing_actuals.sum() + correct_day1) / WINDOW

    error = abs(display.loc[FORECAST_START] - correct_ma) / correct_ma
    assert error < 0.001, f"transition MA is {error:.3%} off the analytically correct value"


def test_reconstruction_is_weekday_agnostic_across_every_seam_weekday():
    """THE TEST SHAPED LIKE THE BUG.

    The defect was a trend estimated from whichever weekdays happened to land in an incomplete
    window, so its magnitude AND SIGN tracked the seam's weekday — the published desktop seam
    step spanned ~390K across the seven possible seam weekdays purely from that.

    Scored against the analytically known correct value (see `_TRUE_TREND`), not against the raw
    forecast: `matched - raw` also contains the *intended* amplitude swap, which on this fixture
    reaches -17.7% on a Sunday seam by design and would swamp the estimator error.

    The biased estimator reaches ~+6.8% here for a Monday seam and swings by weekday; A1 stays
    inside ±0.9% with a ~1.7% spread. The bounds below separate those regimes while leaving room
    for the unavoidable trend-slope term at the seam's missing left half.
    """
    errors = {}
    for offset in range(7):
        seam = FORECAST_START + pd.Timedelta(days=offset)
        dates, values = _build(seam)
        s = pd.Series(values.values, index=pd.to_datetime(dates.values)).sort_index()
        pre, fc = s[s.index < seam], s[s.index >= seam]

        matched = seam_ma.reconstruct_matched_daily(pre, fc, seam)
        correct = _TRUE_TREND * _ACTUALS_DOW[seam.dayofweek]
        errors[seam.day_name()] = (matched.loc[seam] - correct) / correct

    worst = max(abs(e) for e in errors.values())
    spread = max(errors.values()) - min(errors.values())
    assert worst < 0.015, f"day-1 error up to {worst:.2%} of level: {errors}"
    assert spread < 0.025, f"error swings {spread:.2%} across seam weekdays: {errors}"


def test_transition_is_a_no_op_on_all_actuals_input():
    """THE IDENTITY INVARIANT — ground truth with no golden numbers.

    Feed a series that is actuals on BOTH sides of the nominal seam. The two sides then carry
    identical weekly amplitude, so there is nothing to variance-match and the transition has no
    work to do: it must reproduce the plain trailing MA. Any deviation is pure estimator error,
    measured against a known-correct answer rather than against another curve.

    This needs no reference build and cannot be satisfied by two errors cancelling, which is how
    the previous acceptance criteria were defeated.
    """
    dates = pd.date_range("2026-01-01", "2026-12-31", freq="D")
    # One amplitude throughout -> no seam discontinuity anywhere.
    values = pd.Series(400_000.0 * _ACTUALS_DOW[dates.dayofweek.values])
    target_date = dates.to_series(name="target_date").reset_index(drop=True)

    plain = seam_ma.daily_to_28ma(target_date, values)
    for offset in range(7):
        seam = FORECAST_START + pd.Timedelta(days=offset)
        display = seam_ma.display_ma(target_date, values, seam)
        window = display[(display.index >= seam)
                         & (display.index < seam + pd.Timedelta(days=WINDOW - 1))]
        deviation = (window - plain.reindex(window.index)).abs().max() / plain.loc[seam]
        assert deviation < 0.002, f"seam on {seam.day_name()}: {deviation:.3%} off the plain MA"


@pytest.mark.parametrize("start_offset", range(7))
def test_deseasonalized_trend_is_unbiased_at_the_series_edge(start_offset):
    """Window completeness, asserted where it actually matters: the EDGE.

    The enabling defect was that `min_periods` guards against too FEW points, not against a
    day-of-week UNBALANCED window. Crucially, an interior check cannot catch it — in the interior
    a centered 7-day mean of the raw series already spans a full week and is balanced, so the
    buggy estimator is *correct* there. The bug lives entirely in the first (and last) 3
    positions, where the window is incomplete and its weekday composition is whatever the start
    date happens to make it.

    So: the estimator's FIRST value must recover the true level, whatever weekday it starts on.
    The buggy estimator reaches ~6.8% off for a Monday start; A1 is exact on this fixture.
    """
    start = pd.Timestamp("2026-06-01") + pd.Timedelta(days=start_offset)
    dates = pd.date_range(start, periods=200, freq="D")
    fc = pd.Series(_TRUE_TREND * _FORECAST_DOW[dates.dayofweek.values], index=dates)

    trend = seam_ma._deseasonalized_trend(fc)
    error = abs(trend.iloc[0] - _TRUE_TREND) / _TRUE_TREND
    assert error < 0.005, f"edge trend on a {start.day_name()} start is {error:.2%} off"


def test_suite_rejects_the_known_bad_estimator(monkeypatch):
    """CANARY: prove the assertions above can actually fail.

    The defect that shipped was a centered mean of the RAW forecast with `min_periods=4`. This
    patches that exact estimator back in and asserts the suite's two load-bearing bounds now
    break. Without this, a future refactor could weaken those bounds until they no longer catch
    the regression they exist for, and every test would still be green.

    Deliberately embeds the old implementation rather than importing the frozen copy — the point
    is to pin the defect's shape, not to depend on a past cycle's file.
    """
    monkeypatch.setattr(seam_ma, "_deseasonalized_trend",
                        lambda fc: fc.rolling(seam_ma.TREND_WINDOW, center=True,
                                              min_periods=4).mean())

    # Weekday dependence returns.
    errors = []
    for offset in range(7):
        seam = FORECAST_START + pd.Timedelta(days=offset)
        dates, values = _build(seam)
        s = pd.Series(values.values, index=pd.to_datetime(dates.values)).sort_index()
        pre, fc = s[s.index < seam], s[s.index >= seam]
        matched = seam_ma.reconstruct_matched_daily(pre, fc, seam)
        correct = _TRUE_TREND * _ACTUALS_DOW[seam.dayofweek]
        errors.append((matched.loc[seam] - correct) / correct)
    assert max(errors) - min(errors) > 0.025, "weekday-invariance bound would not catch the bug"

    # The identity invariant breaks too.
    dates = pd.date_range("2026-01-01", "2026-12-31", freq="D")
    values = pd.Series(_TRUE_TREND * _ACTUALS_DOW[dates.dayofweek.values])
    target_date = dates.to_series(name="target_date").reset_index(drop=True)
    plain = seam_ma.daily_to_28ma(target_date, values)
    display = seam_ma.display_ma(target_date, values, FORECAST_START)
    window = display[(display.index >= FORECAST_START)
                     & (display.index < FORECAST_START + pd.Timedelta(days=WINDOW - 1))]
    deviation = (window - plain.reindex(window.index)).abs().max() / plain.loc[FORECAST_START]
    assert deviation > 0.002, "identity invariant would not catch the bug"


def test_short_forecast_degrades_without_raising():
    """A forecast too short to estimate its own profile falls back to a flat one, not an error."""
    dates = pd.date_range("2026-01-01", "2026-06-10", freq="D")
    seam = pd.Timestamp("2026-06-01")  # only 10 forecast days
    values = pd.Series(400_000.0 * _ACTUALS_DOW[dates.dayofweek.values])
    target_date = dates.to_series(name="target_date").reset_index(drop=True)

    result = seam_ma.display_ma(target_date, values, seam)
    assert result.notna().any()
    # Degrades to the plain blend, since the transition cannot complete without seam+27.
    plain = seam_ma.daily_to_28ma(target_date, values)
    assert (result.dropna() - plain.reindex(result.dropna().index)).abs().max() < 1e-9
