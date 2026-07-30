"""28-day moving averages for display, with a variance-matched actuals->forecast seam.

The published forecast curves are plotted as 28-day trailing moving averages. A trailing
window that straddles the actuals->forecast seam mixes raw actuals with forecast for its first
``window - 1`` points, and because the forecast's weekly (day-of-week) amplitude is *damped*
relative to recent actuals, that partial-window blend fails to cancel the weekly cycle: the MA
oscillates for ~a month and then settles. `display_ma` replaces those transition points with a
*variance-matched* transition — the forecast's first ``window - 1`` daily values are rebuilt to
carry the recent actuals' weekly amplitude, so both sides of the seam share one amplitude, the
window cancels it, and the transition rides the forecast's true trend.

Diagnosis and remediation history: ``research/ma-seam-turbulence/``.

**This module is the home for the seam-MA logic going forward.** Cycles through 2026-07 import
a frozen copy from ``data-official/2026-06/export_canonical_curves.py``; that file is
deliberately untouched so past cycles' delivered curves cannot move. New work imports from
here. See ``_archive/_index.md``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

DEFAULT_WINDOW = 28
TREND_WINDOW = 7        # days in the deseasonalizing mean — one full week, so it is DoW-balanced
DOW_WEEKS = 13          # profile lookback, aligned with mozaic's recency window

__all__ = ["daily_to_28ma", "display_ma", "reconstruct_matched_daily"]


def daily_to_28ma(dates, values, window: int = DEFAULT_WINDOW) -> pd.Series:
    """Plain trailing moving average of a daily series. Returns a date-indexed Series."""
    s = pd.Series(values.values, index=pd.to_datetime(dates.values))
    return s.sort_index().rolling(window).mean()


def _multiplicative_dow_profile(series: pd.Series, *, complete_windows_only: bool) -> pd.Series:
    """Day-of-week profile of ``series``, normalized to mean 1 over all seven weekdays.

    Estimated by detrending with a centered 7-day mean and averaging the residual ratios per
    weekday. ``complete_windows_only`` requires a full 7-day detrending window (used for the
    forecast, which runs for months, so dropping the 3 rows at each edge costs nothing); the
    actuals profile keeps ``min_periods=4`` to preserve the historical behaviour of the
    delivered curves.

    The mean-1 normalization is load-bearing: it is what makes the reconstruction level
    preserving over an aligned 28-day window (4 of each weekday).
    """
    min_periods = TREND_WINDOW if complete_windows_only else 4
    trend = series.rolling(TREND_WINDOW, center=True, min_periods=min_periods).mean()
    ratio = (series / trend).replace([np.inf, -np.inf], np.nan).dropna()
    profile = ratio.groupby(ratio.index.dayofweek).mean()
    # Normalize the weekdays that are present to mean 1, THEN neutral-fill any weekday missing
    # from a short or holiday-laden window. In this order the mean over all 7 stays exactly 1.
    profile = profile / profile.mean()
    return profile.reindex(range(7)).fillna(1.0)


def _deseasonalized_trend(fc: pd.Series) -> pd.Series:
    """Deseasonalized level of the forecast, free of day-of-week window-composition bias.

    Divide the forecast by its OWN day-of-week profile *before* smoothing, then take a centered
    7-day mean of the result.

    Why the division has to come first
    ----------------------------------
    Averaging the raw series over a window that is not day-of-week balanced does not estimate a
    deseasonalized level — it estimates the level of whichever weekdays happen to be in the
    window. At the seam a centered window has no days to its left, so it degenerates to a
    forward-looking sample; for a Monday-through-Thursday seam that sample is all weekdays and
    reads high by most of the weekday/weekend swing (measured on desktop: +10.3%, or +4.8M DAU).
    That inflated level was then multiplied by the actuals' weekday factor as though it still
    needed deseasonalizing, compounding the error to ~+5.9M on the first forecast day and
    +211K on the published 28-day MA.

    Once each day has been divided by its own weekday factor, every term in the window is
    already an estimate of the same deseasonalized level, so a short window is unbiased in
    day-of-week terms and ``min_periods=1`` is safe. The window stays centered — no widening,
    so no forward shift of the estimate beyond what the missing left half unavoidably costs.

    A previous implementation used ``fc.rolling(7, center=True, min_periods=4).mean()`` and
    documented the seam edge as safe because at least 4 forward points always exist. That is
    the defect: ``min_periods`` guards against too FEW points, not against a day-of-week
    UNBALANCED window. Do not reintroduce a raw-series mean here.
    """
    # A forecast shorter than a few weeks cannot support a profile estimate. Degrade to a flat
    # (neutral) profile rather than failing: the result is then a plain centered mean, which is
    # what the caller would have got anyway, and no weekday factor is invented from noise.
    if len(fc) < 3 * TREND_WINDOW:
        dow_fc = pd.Series(1.0, index=range(7))
    else:
        head = fc[fc.index < fc.index.min() + pd.Timedelta(weeks=DOW_WEEKS)]
        dow_fc = _multiplicative_dow_profile(head, complete_windows_only=True)

    deseasonalized = fc / dow_fc.reindex(fc.index.dayofweek).to_numpy()
    return deseasonalized.rolling(TREND_WINDOW, center=True, min_periods=1).mean()


def reconstruct_matched_daily(pre: pd.Series, fc: pd.Series, forecast_start: pd.Timestamp,
                             window: int = DEFAULT_WINDOW) -> pd.Series:
    """Rebuild the forecast's daily values to carry the recent actuals' weekly amplitude.

    The seam wobble is an *amplitude* discontinuity in the weekly cycle, not a level jump: the
    forecast's weekday/weekend swing is damped relative to recent actuals, so a trailing 28-day
    window straddling the seam cannot cancel the weekly cycle. This rebuilds the forecast as
    ``deseasonalized trend x actuals' weekday factor``, preserving the forecast's trend (level
    and curvature) while swapping its weekly amplitude for the actuals'. Spliced before the
    forecast, both sides of the seam then share one amplitude and the window cancels it.

    Args:
      pre: actuals daily series, date-indexed, dates < ``forecast_start``.
      fc:  forecast daily series, date-indexed, dates >= ``forecast_start``.
      forecast_start: the seam.
      window: trailing MA window the result will be consumed by (unused here; kept for
        signature compatibility with callers and tests).

    Returns the rebuilt forecast daily series over ``fc.index``.
    """
    trend_fc = _deseasonalized_trend(fc)
    recent = pre[pre.index >= forecast_start - pd.Timedelta(weeks=DOW_WEEKS)]
    dow_act = _multiplicative_dow_profile(recent, complete_windows_only=False)
    return trend_fc * dow_act.reindex(fc.index.dayofweek).to_numpy()


def display_ma(dates, values, forecast_start, window: int = DEFAULT_WINDOW,
               continuous_splice: bool = True, slope_match: float = 0.4) -> pd.Series:
    """Trailing MA for display, with a variance-matched transition across the seam.

    Returns a date-indexed Series where:
      - dates < ``forecast_start``: the plain trailing MA (actuals region; untouched).
      - dates >= ``forecast_start + (window - 1)``: the forecast-ONLY trailing MA. Byte-identical
        to the naive blend there, because the window is entirely forecast — so far-horizon
        values (e.g. Dec-15 and every headline number) are unaffected by anything in this
        module's reconstruction. The rebuilt daily values are never used in this region.
      - dates in between: the variance-matched transition (see `reconstruct_matched_daily`),
        made continuous with the forecast-only MA at the splice by the cubic correction below.

    ``continuous_splice`` (default True) ramps a cubic correction across the transition so it
    lands on the forecast-only MA in both level and (a fraction of) slope at the day-(window-1)
    handoff, removing the corner a level-only correction leaves. Set False to reproduce the
    pre-fix cliff; used only for before/after comparison.

    ``slope_match`` in [0, 1] is the fraction of the splice slope residual the correction
    matches. 1.0 drives the handoff 2nd-difference to ~0 but overshoots the level gap; 0.0
    matches level only and leaves a visible slope kink. 0.4 clears the corner target at the
    minimal deviation.
    """
    s = pd.Series(values.values, index=pd.to_datetime(dates.values)).sort_index()
    first_clean_date = forecast_start + pd.Timedelta(days=window - 1)

    pre = s[s.index < forecast_start]
    fc = s[s.index >= forecast_start]
    forecast_only_ma = fc.rolling(window).mean()

    # Plain blend everywhere (correct for the actuals region; overwritten in the forecast region).
    result = s.rolling(window).mean()

    # Existence guard: short series degrade to the plain blend without throwing.
    if forecast_start in s.index and first_clean_date in forecast_only_ma.index:
        matched = reconstruct_matched_daily(pre, fc, forecast_start, window)
        transition_ma = pd.concat([pre, matched]).sort_index().rolling(window).mean()

        transition_dates = pd.date_range(forecast_start, first_clean_date - pd.Timedelta(days=1),
                                         freq="D")
        if continuous_splice:
            # c(f) = a*f^3 + b*f^2 over the transition (f: 0 at the seam -> 1 at the splice) has
            # c(0) = c'(0) = 0, preserving continuity with the trailing actuals MA, and is solved
            # so the corrected transition meets forecast_only_ma in level AND slope at the
            # splice. When the slope neighbours are unavailable r_slope=0, degrading to a
            # smoothstep that still zeroes the level step.
            span = window - 1
            r_level = transition_ma.loc[first_clean_date] - forecast_only_ma.loc[first_clean_date]
            prev_day = first_clean_date - pd.Timedelta(days=1)
            next_day = first_clean_date + pd.Timedelta(days=1)
            if prev_day in transition_ma.index and next_day in forecast_only_ma.index:
                transition_slope = (transition_ma.loc[first_clean_date]
                                    - transition_ma.loc[prev_day])
                forecast_slope = (forecast_only_ma.loc[next_day]
                                  - forecast_only_ma.loc[first_clean_date])
                r_slope = slope_match * (transition_slope - forecast_slope)
            else:
                r_slope = 0.0
            f = (transition_dates - forecast_start).days.to_numpy(dtype=float) / span
            a = r_slope * span - 2 * r_level
            b = 3 * r_level - r_slope * span
            correction = a * f ** 3 + b * f ** 2
            transition_ma.loc[transition_dates] = (
                transition_ma.loc[transition_dates].to_numpy() - correction)

        # Splice (HARD CONSTRAINT): day (window-1) onward is the clean forecast-only MA, so the
        # far horizon is exactly the naive blend. Earlier days use the transition.
        result.loc[forecast_only_ma.index] = forecast_only_ma
        transition_mask = (result.index >= forecast_start) & (result.index < first_clean_date)
        result.loc[transition_mask] = transition_ma.reindex(result.index)[transition_mask]
    return result
