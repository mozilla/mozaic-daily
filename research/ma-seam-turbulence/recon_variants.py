"""Candidate replacements for ``reconstruct_matched_daily``'s trend estimator.

The shipped estimator is

    trend_fc = fc.rolling(7, center=True, min_periods=4).mean()

computed on the forecast only. At the seam the centered window has no left half, and
``min_periods=4`` accepts the first four forecast days. When the seam falls on a Monday
through Thursday those four days are all weekdays, so a "7-day deseasonalizing mean" is
computed from a weekday-only sample and reads high by the full weekday/weekend swing.

Each variant here has the same signature as ``reconstruct_matched_daily`` so it can be
swapped in via ``patched_reconstructor`` and scored by ``eval_recon_edge_fix.py``.

  ``current``   — the shipped behaviour, re-implemented here so the harness can score it
                  without depending on which version is checked out.
  ``forward7``  — the §5 hypothesis. Still forecast-only, so it cannot touch the day-27
                  splice the way June's rejected fix did, but every window is
                  day-of-week complete: where the centered window is short, use a
                  forward (or, at the right edge, backward) 7-day mean.
  ``concat``    — June's REJECTED fix, kept so the harness can reproduce the 0.698%
                  splice regression and thereby validate the metric definition.
                  Do not ship this.

The day-of-week profile estimator has the same ``min_periods=4`` defect at both edges of
its 13-week window (handoff §6); ``dow_profile`` exposes it as a separate knob.
"""

from __future__ import annotations

import contextlib

import numpy as np
import pandas as pd

TREND_WINDOW = 7
DOW_WEEKS = 13


def _centered_min4(series: pd.Series) -> pd.Series:
    """The shipped estimator: centered 7-day mean, accepting as few as 4 points."""
    return series.rolling(TREND_WINDOW, center=True, min_periods=4).mean()


def _dow_complete(series: pd.Series) -> pd.Series:
    """Centered 7-day mean, but every value averages a full 7 consecutive days.

    Where the centered window would be short (the first and last 3 positions), fall back
    to a one-sided 7-day window: forward at the left edge, backward at the right. The
    estimate's effective centre shifts by up to 3 days there, which costs (3 x trend
    slope) — an order of magnitude less than the weekday-only bias it removes — but the
    sample is day-of-week balanced by construction, for a seam on any weekday.
    """
    centered = series.rolling(TREND_WINDOW, center=True, min_periods=TREND_WINDOW).mean()
    backward = series.rolling(TREND_WINDOW, min_periods=TREND_WINDOW).mean()
    forward = backward.shift(-(TREND_WINDOW - 1))
    return centered.fillna(forward).fillna(backward)


def _dow_profile(pre: pd.Series, forecast_start: pd.Timestamp, complete_windows: bool) -> pd.Series:
    """Multiplicative day-of-week profile from the last ``DOW_WEEKS`` of actuals.

    ``complete_windows`` fixes the handoff-§6 defect: the shipped code slices ``recent``
    first and *then* takes a centered rolling mean, so the 3 rows at each end of the
    13-week window are detrended by a short (day-of-week-incomplete) window and their
    ratios still enter the profile. With ``complete_windows`` the detrending mean is
    computed on ``pre`` — which extends years further back — before slicing, so every
    ratio in the profile comes from a full 7-day window.
    """
    window_start = forecast_start - pd.Timedelta(weeks=DOW_WEEKS)
    if complete_windows:
        trend = _dow_complete(pre)
        recent, recent_trend = pre[pre.index >= window_start], trend[trend.index >= window_start]
    else:
        recent = pre[pre.index >= window_start]
        recent_trend = _centered_min4(recent)

    ratio = (recent / recent_trend).replace([np.inf, -np.inf], np.nan).dropna()
    dow = ratio.groupby(ratio.index.dayofweek).mean()
    dow = dow / dow.mean()
    return dow.reindex(range(7)).fillna(1.0)


def make_reconstructor(trend: str = "current", dow_complete_windows: bool = False):
    """Build a ``reconstruct_matched_daily``-compatible function from the two knobs."""

    def reconstruct(pre, fc, forecast_start, window=28):
        if trend == "current":
            trend_fc = _centered_min4(fc)
        elif trend == "forward7":
            trend_fc = _dow_complete(fc)
        elif trend == "concat":
            # June's rejected fix: centre on the concatenated actuals+forecast series, so
            # the seam's left half comes from actuals. Complete windows, but it drags the
            # actuals' level into the forecast trend and wrecked the day-27 splice.
            joined = pd.concat([pre, fc]).sort_index()
            trend_fc = _centered_min4(joined).reindex(fc.index)
        else:
            raise ValueError(f"unknown trend estimator {trend!r}; "
                             "expected 'current', 'forward7' or 'concat'")

        dow_act = _dow_profile(pre, forecast_start, dow_complete_windows)
        return trend_fc * dow_act.reindex(fc.index.dayofweek).to_numpy()

    reconstruct.__name__ = f"reconstruct_{trend}" + ("_dowfix" if dow_complete_windows else "")
    return reconstruct


@contextlib.contextmanager
def patched_reconstructor(export_module, reconstruct):
    """Temporarily swap ``export_module.reconstruct_matched_daily`` for ``reconstruct``."""
    original = export_module.reconstruct_matched_daily
    export_module.reconstruct_matched_daily = reconstruct
    try:
        yield
    finally:
        export_module.reconstruct_matched_daily = original
