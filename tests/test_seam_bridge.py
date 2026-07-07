# -*- coding: utf-8 -*-
"""Tests for scripts/seam_bridge.py — the seam kink diagnostic and the daily-level bridge.

The bridge fixes an actuals->forecast LEVEL mismatch (the model launches below recent actuals)
by adding a decaying offset to the first ``window`` forecast days, so the forecast launches at
the recent-actuals level and glides into the untouched model by the day-(window) mark. The
diagnostic (kink_score) grades a curve: corner = red flag (pass-fail), drawdown = yellow.

Synthetic data only. Each test constructs a case where a regression would flip the assertion.
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
_spec = importlib.util.spec_from_file_location(
    "seam_bridge", os.path.join(GIT_ROOT, "scripts/seam_bridge.py")
)
sb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sb)

WINDOW = 28
FS = pd.Timestamp("2026-06-29")


def _daily(actual_level, forecast_level, weekend_swing=0.0, start="2026-01-01", end="2026-12-31"):
    """Build a daily series: flat `actual_level` before FS, flat `forecast_level` from FS on,
    with an optional mean-preserving weekly swing (weekday up / weekend down)."""
    dates = pd.date_range(start, end, freq="D")
    base = np.where(dates < FS, actual_level, forecast_level).astype(float)
    if weekend_swing:
        is_weekend = dates.dayofweek >= 5
        # mean-preserving: (5*weekday + 2*weekend)/7 == 1
        factor = np.where(is_weekend, 1 - 2.5 * weekend_swing, 1 + weekend_swing)
        base = base * factor
    return pd.Series(base, index=dates)


# --------------------------------------------------------------------------- kink_score

def test_kink_score_zero_on_monotone_line():
    """A strictly rising smooth line has no dip and no corner -> both metrics are ~0."""
    idx = pd.date_range("2026-06-01", "2026-09-01", freq="D")
    ma = pd.Series(np.linspace(17.0e6, 17.3e6, len(idx)), index=idx)
    k = sb.kink_score(ma, FS)
    assert k["drawdown_dau"] == pytest.approx(0.0, abs=1.0)
    assert k["corner_ppm"] == pytest.approx(0.0, abs=1.0)


def test_kink_score_flags_corner_and_dip():
    """A curve that rises, then drops into a V-notch, then recovers must score high on BOTH
    the corner (sharp 2nd-difference at the notch) and the drawdown (the valley depth), and
    far above the smooth-line baseline (so the assertion is a real detection, not a constant)."""
    idx = pd.date_range("2026-06-01", "2026-09-01", freq="D")
    line = np.linspace(17.0e6, 17.3e6, len(idx))
    notch = np.zeros(len(idx))
    peak = idx.get_loc(FS + pd.Timedelta(days=9))
    trough = idx.get_loc(FS + pd.Timedelta(days=27))
    notch[peak:trough] = -np.linspace(0, 150_000, trough - peak)  # dip deeper than the line's rise
    kinked = pd.Series(line + notch, index=idx)
    smooth = pd.Series(line, index=idx)

    k = sb.kink_score(kinked, FS)
    base = sb.kink_score(smooth, FS)
    assert k["drawdown_dau"] > 50_000
    assert k["corner_ppm"] > 100 * max(base["corner_ppm"], 1.0)
    # trough localizes to the bottom of the constructed dip (~day 26-27), after the peak.
    assert k["peak_date"] < k["trough_date"]
    assert (FS + pd.Timedelta(days=24)).date().isoformat() <= k["trough_date"] <= (FS + pd.Timedelta(days=28)).date().isoformat()


# --------------------------------------------------------------------------- seam_level_gap

def test_seam_level_gap_matches_constructed_gap():
    """gap = mean(last 7 actuals) - mean(first 7 forecast); deseasonalized by the 7-day window
    even when a strong weekly swing is present."""
    daily = _daily(actual_level=17_100_000, forecast_level=16_950_000, weekend_swing=0.15)
    gap = sb.seam_level_gap(daily, FS)
    assert gap == pytest.approx(150_000, abs=1_000)


def test_seam_level_gap_negative_when_forecast_above():
    """Sign convention: forecast launching ABOVE actuals yields a negative gap."""
    daily = _daily(actual_level=16_900_000, forecast_level=17_050_000)
    assert sb.seam_level_gap(daily, FS) == pytest.approx(-150_000, abs=1_000)


def test_seam_level_gap_requires_enough_days():
    daily = _daily(17_000_000, 17_000_000, start="2026-06-27")  # only 2 actual days before FS
    with pytest.raises(ValueError):
        sb.seam_level_gap(daily, FS)


# --------------------------------------------------------------------------- bridge_seam_daily

def test_bridge_ramp_is_full_gap_at_seam_zero_at_day28():
    """With an explicit gap, the linear offset is exactly gap at day 0, ramps linearly, and is
    zero from forecast day (window-1) on. Uses a fixed gap so this exercises the ramp math
    independent of seam_level_gap."""
    daily = _daily(17_000_000, 17_000_000)  # flat: bridged - original == the pure offset
    gap = 84_000.0
    bridged = sb.bridge_seam_daily(daily, FS, gap=gap, easing="linear")
    offset = (bridged - daily)[daily.index >= FS]

    assert offset.loc[FS] == pytest.approx(gap)                                   # day 0: full gap
    assert offset.loc[FS + pd.Timedelta(days=WINDOW - 1)] == pytest.approx(0.0, abs=1e-6)  # day 27: 0
    # linear midpoint (day ~13.5): ~half the gap
    assert offset.loc[FS + pd.Timedelta(days=14)] == pytest.approx(gap * (1 - 14 / 27), abs=1.0)


def test_bridge_daily_untouched_from_day_window_on():
    """Forecast day `window` (07-27) onward is byte-identical to the model; actuals untouched."""
    daily = _daily(17_100_000, 16_950_000, weekend_swing=0.1)
    bridged = sb.bridge_seam_daily(daily, FS)
    day_window = FS + pd.Timedelta(days=WINDOW)  # 2026-07-27
    post = bridged.index >= day_window
    assert np.allclose(bridged[post].values, daily[post].values)
    pre = bridged.index < FS
    assert np.allclose(bridged[pre].values, daily[pre].values)


def test_bridge_shrinks_daily_seam_step():
    """The bridge's purpose: the first forecast week sits closer to the last actual week's level
    than the raw model does. Compare the deseasonalized weekly means across the seam."""
    daily = _daily(17_100_000, 16_950_000, weekend_swing=0.12)
    bridged = sb.bridge_seam_daily(daily, FS)
    last_actual_week = daily[daily.index < FS].tail(7).mean()
    raw_step = abs(daily[daily.index >= FS].head(7).mean() - last_actual_week)
    fixed_step = abs(bridged[bridged.index >= FS].head(7).mean() - last_actual_week)
    assert fixed_step < raw_step / 5  # launch level pulled ~onto the actuals level


def test_bridge_preserves_far_horizon_ma_but_moves_near():
    """Dec-15 (far past day-window) 28d-MA is unchanged; a near date inside the roll-off window
    genuinely moves — so the invariance test can actually fail if the bridge leaked forward."""
    daily = _daily(17_100_000, 16_950_000, weekend_swing=0.1)
    orig_ma = daily.rolling(WINDOW).mean()
    fixed_ma = sb.bridge_seam_daily(daily, FS).rolling(WINDOW).mean()

    dec15 = pd.Timestamp("2026-12-15")
    assert fixed_ma.loc[dec15] == pytest.approx(orig_ma.loc[dec15], abs=1e-6)
    near = FS + pd.Timedelta(days=WINDOW + 3)  # bridged days still in the trailing window here
    assert abs(fixed_ma.loc[near] - orig_ma.loc[near]) > 1_000


def test_bridge_rejects_unknown_easing():
    daily = _daily(17_000_000, 16_900_000)
    with pytest.raises(ValueError):
        sb.bridge_seam_daily(daily, FS, easing="cubic")
