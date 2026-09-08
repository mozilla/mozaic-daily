# -*- coding: utf-8 -*-
"""Tests for src/mozaic_daily/intervals.py — bands from a fitted mozaic's sample paths.

Synthetic paths only. Each test names the production bug it would catch.
"""

import numpy as np
import pandas as pd
import pytest

from mozaic_daily import intervals

SEAM = pd.Timestamp("2026-08-02")
N_PATHS = 400
N_HISTORY = 60
N_FORECAST = 90


class _FakeMozaic:
    """Just the attributes intervals.py reads off a mozaic.Mozaic."""

    def __init__(self, paths: pd.DataFrame, holiday: pd.DataFrame, actuals: np.ndarray):
        self.forecast_reconciled = paths
        self.forecasted_holiday_impacts = holiday
        self.forecast_dates = pd.date_range(SEAM, periods=len(paths))
        self.historical_dates = pd.date_range(SEAM - pd.Timedelta(days=len(actuals)), periods=len(actuals))
        self.raw_historical_data = actuals


@pytest.fixture
def paths():
    rng = np.random.default_rng(7)
    level = 100.0 + np.arange(N_FORECAST)  # rising trend
    noise = rng.normal(0, 10, size=(N_FORECAST, N_PATHS))
    return pd.DataFrame(level[:, None] + noise, index=pd.RangeIndex(N_FORECAST), columns=pd.RangeIndex(N_PATHS))


@pytest.fixture
def actuals():
    return np.full(N_HISTORY, 100.0)


def test_world_sample_paths_adds_holidays_clips_and_dates(paths):
    holiday = pd.DataFrame(0.0, index=paths.index, columns=paths.columns)
    holiday.iloc[3] = -1e6  # a "blackout" bigger than the level must clip at 0, as to_df does
    moz = _FakeMozaic(paths, holiday, np.zeros(N_HISTORY))
    out = intervals.world_sample_paths(moz)
    assert isinstance(out.index, pd.DatetimeIndex) and out.index[0] == SEAM
    assert (out.iloc[3] == 0).all()
    # untouched rows are the reconciled samples exactly (holiday impact 0)
    np.testing.assert_allclose(out.iloc[0].to_numpy(), paths.iloc[0].to_numpy())


def test_median_check_passes_on_true_median_and_fails_on_shifted(paths):
    dated = paths.set_axis(pd.date_range(SEAM, periods=N_FORECAST))
    truth = dated.quantile(0.5, axis=1)
    assert intervals.assert_median_matches_forecast(dated, truth) < 1e-9
    with pytest.raises(AssertionError, match="deviates"):
        intervals.assert_median_matches_forecast(dated, truth + 5.0)


def test_splice_requires_adjacent_seam(paths, actuals):
    dated = paths.set_axis(pd.date_range(SEAM, periods=N_FORECAST))
    hist = pd.Series(actuals, index=pd.date_range(SEAM - pd.Timedelta(days=N_HISTORY), periods=N_HISTORY))
    spliced = intervals.splice_actuals_onto_paths(hist, dated)
    assert len(spliced) == N_HISTORY + N_FORECAST
    assert (spliced.loc[hist.index].nunique(axis=1) == 1).all()  # history identical on every path
    with pytest.raises(ValueError, match="gap"):
        intervals.splice_actuals_onto_paths(hist.iloc[:-1], dated)  # one-day hole at the seam


def test_rolling_mean_is_quantile_of_ma_not_ma_of_quantile(paths, actuals):
    """Noise that averages out inside the window must narrow the 28d band relative to the daily band."""
    dated = paths.set_axis(pd.date_range(SEAM, periods=N_FORECAST))
    hist = pd.Series(actuals, index=pd.date_range(SEAM - pd.Timedelta(days=N_HISTORY), periods=N_HISTORY))
    ma = intervals.rolling_mean_paths(intervals.splice_actuals_onto_paths(hist, dated))
    daily_band = intervals.band_quantiles(dated, [0.8])
    ma_band = intervals.band_quantiles(ma, [0.8]).loc[dated.index[-1]]
    daily_width = daily_band["upper_80"].iloc[-1] - daily_band["lower_80"].iloc[-1]
    ma_width = ma_band["upper_80"] - ma_band["lower_80"]
    # iid noise sd 10 -> daily 80% width ≈ 25.6; the 28-day mean has sd 10/sqrt(28) -> width ≈ 4.8
    assert ma_width < daily_width / 3
    # and the MA of the median path equals the median of the MA paths only up to skew; here symmetric
    assert abs(ma_band["median"] - dated.quantile(0.5, axis=1).iloc[-28:].mean()) < 2.0


def test_band_quantiles_ordering_and_columns(paths):
    dated = paths.set_axis(pd.date_range(SEAM, periods=N_FORECAST))
    bands = intervals.band_quantiles(dated, [0.5, 0.8, 0.9])
    assert list(bands.columns) == ["median", "lower_50", "upper_50", "lower_80", "upper_80", "lower_90", "upper_90"]
    assert (bands["lower_90"] <= bands["lower_80"]).all() and (bands["lower_80"] <= bands["lower_50"]).all()
    assert (bands["lower_50"] <= bands["median"]).all() and (bands["median"] <= bands["upper_50"]).all()
    assert (bands["upper_50"] <= bands["upper_80"]).all() and (bands["upper_80"] <= bands["upper_90"]).all()
    # a Gaussian with sd 10 has a 90% interval of ±16.4; catches a swapped or mis-halved level
    halfwidth_90 = (bands["upper_90"] - bands["lower_90"]).mean() / 2
    assert 14.5 < halfwidth_90 < 18.5


def test_band_quantiles_rejects_bad_level(paths):
    with pytest.raises(ValueError):
        intervals.band_quantiles(paths, [1.2])


def test_point_summary_halfwidth_and_missing_date(paths):
    dated = paths.set_axis(pd.date_range(SEAM, periods=N_FORECAST))
    bands = intervals.band_quantiles(dated, [0.8])
    on = dated.index[10]
    summary = intervals.point_summary(bands, on, [0.8])
    assert summary["date"] == on.date().isoformat()
    assert summary["halfwidth_80"] == pytest.approx((summary["upper_80"] - summary["lower_80"]) / 2)
    with pytest.raises(KeyError):
        intervals.point_summary(bands, SEAM - pd.Timedelta(days=1), [0.8])


def test_trough_summary_finds_minimum_of_median(paths):
    dated = paths.set_axis(pd.date_range(SEAM, periods=N_FORECAST))
    dated.iloc[40] -= 500.0  # a dip on one day in every path
    bands = intervals.band_quantiles(dated, [0.8])
    trough = intervals.trough_summary(bands, dated.index[0], dated.index[-1])
    assert trough["trough_date"] == dated.index[40].date().isoformat()
    assert trough["median"] == pytest.approx(bands["median"].iloc[40])
