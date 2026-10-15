# -*- coding: utf-8 -*-
"""Tests for the organic + paid composition (``mozaic_daily.mobile_total_mau``) with hand-computed expectations."""

import numpy as np
import pandas as pd
import pytest

from mozaic_daily.mobile_total_mau import (
    cap_excess_forecast, check_plan_covers, combine_bands, compose_by_country, compose_populations,
)

DATES = pd.date_range("2026-01-01", periods=6, freq="D")
TRAINING_END = DATES[2]
POPS = ["world", "fenix_android", "firefox_ios"]


def _curves(fenix, ios):
    df = pd.DataFrame({"data_type": ["training"] * 3 + ["forecast"] * 3, "mau_fenix_android": fenix, "mau_firefox_ios": ios}, index=DATES)
    df["mau_world"] = df["mau_fenix_android"] + df["mau_firefox_ios"]
    df.index.name = "date"
    return df


def _plan(paid):
    return pd.DataFrame({"date": DATES[3:], "paid_mau": paid, "paid_mau_low_90": [p - 10 for p in paid], "paid_mau_high_90": [p + 20 for p in paid], "segment": "plan"})


def test_history_is_raw_and_forecast_adds_plan_plus_excess_to_fenix_and_world_only():
    organic = _curves([80, 80, 80, 82, 82, 82], [50, 50, 50, 51, 51, 51])
    raw = _curves([100, 100, 100, np.nan, np.nan, np.nan], [50, 50, 50, np.nan, np.nan, np.nan])
    excess = pd.Series([5.0, 5.0, 5.0], index=DATES[3:])
    out = compose_populations(organic, raw, _plan([20, 21, 22]), excess, POPS, TRAINING_END)
    assert out["paid_fenix_android"].tolist() == [20, 20, 20, 25, 26, 27]          # history = raw - organic; forecast = plan + excess
    assert out["total_fenix_android"].tolist() == [100, 100, 100, 107, 108, 109]
    assert out["total_world"].tolist() == [150, 150, 150, 158, 159, 160]
    assert (out["paid_firefox_ios"] == 0).all() and out["total_firefox_ios"].tolist() == [50, 50, 50, 51, 51, 51]
    assert out["paid_world_low_90"].isna().tolist() == [True, True, True, False, False, False]
    assert out.loc[DATES[3], "paid_world_low_90"] == 15 and out.loc[DATES[3], "paid_world_high_90"] == 45   # band shifted by the excess


def test_compose_refuses_organic_above_raw_in_history():
    organic = _curves([120, 80, 80, 82, 82, 82], [50] * 6)
    raw = _curves([100, 100, 100, np.nan, np.nan, np.nan], [50] * 6)
    with pytest.raises(ValueError, match="organic above raw"):
        compose_populations(organic, raw, _plan([20, 21, 22]), pd.Series(0.0, index=DATES[3:]), POPS, TRAINING_END)


def test_plan_must_cover_every_forecast_day():
    with pytest.raises(ValueError, match="misses 1 forecast day"):
        check_plan_covers(_plan([20, 21, 22]).iloc[:2], TRAINING_END, DATES[-1])


def test_cap_excess_forecast_holds_last_value_flat_or_drops():
    cap = pd.DataFrame({"submission_date": list(DATES[:3]) * 2, "country": ["IN"] * 3 + ["ID"] * 3, "removed_mau": [0, 10, 30, 0, 0, 4]})
    held = cap_excess_forecast(cap, TRAINING_END, DATES[-1], "hold_flat")
    assert list(held.index) == list(DATES[3:]) and held["IN"].tolist() == [30, 30, 30] and held["ID"].tolist() == [4, 4, 4]
    dropped = cap_excess_forecast(cap, TRAINING_END, DATES[-1], "drop")
    assert (dropped == 0).all().all()
    with pytest.raises(ValueError, match="ends"):
        cap_excess_forecast(cap, DATES[4], DATES[-1])


def test_combine_bands_adds_half_widths_in_quadrature():
    composed = pd.DataFrame({"organic_world": [100.0], "paid_world": [50.0], "total_world": [150.0],
                             "paid_world_low_90": [46.0], "paid_world_high_90": [53.0]}, index=[DATES[3]])
    out = combine_bands(composed, pd.Series([97.0], index=[DATES[3]]), pd.Series([104.0], index=[DATES[3]]))
    assert out["total_world_low_90"].item() == pytest.approx(150 - 5)      # sqrt(3^2 + 4^2)
    assert out["total_world_high_90"].item() == pytest.approx(150 + 5)     # sqrt(4^2 + 3^2)


def test_by_country_history_is_raw_and_forecast_splits_paid_by_share_plus_own_excess():
    organic_bc = pd.DataFrame({"IN": [80.0] * 6, "US": [40.0] * 6}, index=DATES)
    raw_bc = pd.DataFrame({"IN": [100.0] * 3 + [np.nan] * 3, "US": [41.0] * 3 + [np.nan] * 3}, index=DATES)
    plan_paid = pd.Series([20.0, 20.0, 20.0], index=DATES[3:])
    shares = pd.Series({"IN": 0.75, "US": 0.25})
    excess = pd.DataFrame({"IN": [5.0] * 3}, index=DATES[3:])
    out = compose_by_country(organic_bc, raw_bc, plan_paid, shares, excess, TRAINING_END)
    assert out["IN"].tolist() == [100, 100, 100, 100, 100, 100]      # 80 + 15 + 5
    assert out["US"].tolist() == [41, 41, 41, 45, 45, 45]            # 40 + 5
    with pytest.raises(ValueError, match="sum to"):
        compose_by_country(organic_bc, raw_bc, plan_paid, pd.Series({"IN": 0.5, "US": 0.25}), excess, TRAINING_END)
