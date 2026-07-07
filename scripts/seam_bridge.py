"""Seam smoothing for daily forecasts: detect the actuals->forecast "kink" and fix it.

The problem
-----------
The forecast's launch level sits *below* the recent actuals' level (recent actuals ran hot
— e.g. the Iran-recovery overshoot + marketing ramp — and Prophet reverts toward its fitted
trend). On a 28-day trailing MA this shows up as a local *kink*: the curve rises to a small
peak, then sags into a shallow valley as the high recent actuals roll out of the trailing
window, before the forecast trend resumes. It is also visible on the raw daily series as a
~150k/day step down right at the seam.

Two functions here, one concern each:

  kink_score()        measure the kink (diagnostic / pass-fail gate for any fix).
  bridge_seam_daily() fix it, on the DAILY series, via a decaying level offset.

Red flag vs yellow flag
-----------------------
The *corner* (a sharp curvature spike / notch, e.g. the variance-matched splice cliff) is the
RED flag — it looks broken and is the pass-fail gate. The *drawdown* (a gentle peak-to-valley
in the trailing MA) is only a YELLOW flag: a trailing MA can legitimately undulate (real
seasonal decline, e.g. desktop's summer slump, produces a large-but-honest drawdown). Judge a
fix primarily on whether it removes the corner; treat a residual drawdown as informational.

The bridge is applied to the daily values (not the MA) so it holds on both the raw-daily view
and any derived MA. The offset is full at the seam and ramps to zero by forecast day
``window - 1`` (the 28-day mark), so the daily series from day ``window`` onward is the
untouched model output, and the far-horizon MA (e.g. Dec-15) is unchanged by construction.

Platform-agnostic: pass a date-indexed daily DAU Series for desktop or mobile.
"""

import numpy as np
import pandas as pd


def kink_score(ma, forecast_start, pre_days=5, post_days=45):
    """Quantify the seam kink on a 28-day-MA curve.

    A healthy forecast MA rises (or is flat) through the seam. The kink is a local
    peak-then-trough. We measure two signatures over a window bracketing the seam:

      corner_ppm = max |second difference| over the window / mean level * 1e6   [RED flag]
        The sharpness of the worst corner (e.g. a hard splice notch). This is the headline
        pass-fail metric — a sharp corner is the artifact that looks broken.
      drawdown_dau = max_t ( running_max_{s<=t} ma_s - ma_t )   [DAU, YELLOW flag]
        The depth of the local dip below a prior peak. Informational only: a trailing MA can
        undulate legitimately (real seasonal decline gives a large but honest drawdown).

    Args:
      ma: date-indexed 28-day-MA Series.
      forecast_start: first forecast date (the seam).
      pre_days/post_days: window is [forecast_start - pre_days, forecast_start + post_days];
        defaults bracket the observed peak (~day 9) and trough (~day 28) and the recovery.

    Returns a dict: drawdown_dau, drawdown_ppm, corner_ppm, plus the peak/trough/corner dates
    and the window bounds (for interpretability).
    """
    lo = forecast_start - pd.Timedelta(days=pre_days)
    hi = forecast_start + pd.Timedelta(days=post_days)
    window = ma[(ma.index >= lo) & (ma.index <= hi)].dropna()
    if window.empty:
        raise ValueError(f"kink_score: no MA points in [{lo.date()}, {hi.date()}]")

    level = window.mean()
    drawdown = window.cummax() - window
    trough_date = drawdown.idxmax()
    peak_date = window.loc[:trough_date].idxmax()
    second_diff = window.diff().diff().abs()

    return {
        "drawdown_dau": float(drawdown.max()),
        "drawdown_ppm": float(1e6 * drawdown.max() / level),
        "corner_ppm": float(1e6 * second_diff.max() / level),
        "peak_date": peak_date.date().isoformat(),
        "trough_date": trough_date.date().isoformat(),
        "corner_date": second_diff.idxmax().date().isoformat(),
        "window": (lo.date().isoformat(), hi.date().isoformat()),
    }


def seam_level_gap(daily, forecast_start, gap_window=7):
    """Deseasonalized level mismatch at the seam, in DAU.

    gap = mean(last ``gap_window`` actual days) - mean(first ``gap_window`` forecast days).
    A 7-day window covers every weekday, so each side is deseasonalized. Positive means the
    forecast launches *below* recent actuals (the usual case here); this is the amount the
    bridge lifts the forecast's launch to.
    """
    last_actual = forecast_start - pd.Timedelta(days=1)
    pre = daily[daily.index <= last_actual].tail(gap_window)
    fc = daily[daily.index >= forecast_start].head(gap_window)
    if len(pre) < gap_window or len(fc) < gap_window:
        raise ValueError(
            f"seam_level_gap: need {gap_window} days on each side of {forecast_start.date()}; "
            f"got {len(pre)} actual / {len(fc)} forecast"
        )
    return float(pre.mean() - fc.mean())


def bridge_seam_daily(daily, forecast_start, window=28, gap=None, gap_window=7, easing="linear"):
    """Add a decaying level offset to the first ``window`` forecast days so the forecast
    launches at the recent-actuals level and glides into the untouched model by the day-
    ``window`` mark.

    The offset is additive on the DAILY series (so it shows up in both raw-daily views and any
    derived MA): full ``gap`` at the seam (forecast day 0), ramping to exactly 0 at forecast
    day ``window - 1`` (the 28-day mark), and 0 from forecast day ``window`` onward. Actuals
    are never modified; forecast day ``window`` onward is the untouched model output.

    Args:
      daily: date-indexed daily DAU Series (actuals then forecast).
      gap: level offset in DAU; if None, computed via seam_level_gap(gap_window).
      easing: "linear" (constant-slope ramp, per a plain ramp) or "smoothstep" (3f^2-2f^3,
        zero slope at both ends — the daily offset tapers in and out with no corner).

    Returns a new daily Series (input unmodified).
    """
    result = daily.copy()
    if gap is None:
        gap = seam_level_gap(daily, forecast_start, gap_window)

    forecast_dates = result.index[result.index >= forecast_start]
    day = (forecast_dates - forecast_start).days.to_numpy(dtype=float)  # 0-based forecast day
    fraction = np.clip(day / (window - 1), 0.0, 1.0)                    # 0 at seam -> 1 at day (window-1)
    if easing == "linear":
        remaining = 1.0 - fraction
    elif easing == "smoothstep":
        remaining = 1.0 - (3 * fraction**2 - 2 * fraction**3)
    else:
        raise ValueError(f"unknown easing {easing!r}; expected 'linear' or 'smoothstep'")

    result.loc[forecast_dates] = result.loc[forecast_dates].to_numpy() + gap * remaining
    return result
