"""Tests for scripts/score_near_horizon.py.

Two layers:
1. A synthetic-fixture unit test that exercises the real scoring math (28d-MA,
   ex-CN/IR subtraction, linear headwind ramp) against hand-computed expected
   values — CI-safe, no dependency on large parquets.
2. A guarded regression cross-check that the scorer reproduces the known
   Aug-22 KPI on the on-disk locked July parquet (skipped if absent).
"""

import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "score_near_horizon", REPO_ROOT / "scripts" / "score_near_horizon.py"
)
score_near_horizon = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(score_near_horizon)

OS_ALL = '{"os": "ALL"}'
HEADWIND_SPEC = {
    "type": "linear_ramp",
    "start_date": "2026-04-01",
    "anchor_date": "2026-12-15",
    "desktop_dau": -1_345_000,
    "mobile_dau": -27_162,
}


def _make_df(country_daily: dict[str, float], dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Build a forecast-shaped df with a constant daily value per country (os=ALL)."""
    rows = []
    for country, value in country_daily.items():
        for d in dates:
            rows.append(
                {"country": country, "segment": OS_ALL,
                 "target_date": d.strftime("%Y-%m-%d"), "dau": float(value)}
            )
    return pd.DataFrame(rows)


def test_headwind_ramp_endpoints_and_midpoint():
    ramp = score_near_horizon._headwind_ramp
    assert ramp(pd.Timestamp("2026-04-01"), HEADWIND_SPEC) == 0.0
    assert ramp(pd.Timestamp("2026-12-15"), HEADWIND_SPEC) == -1_345_000
    # Aug-22 is 143 days into a 258-day ramp -> known fraction.
    aug = ramp(pd.Timestamp("2026-08-22"), HEADWIND_SPEC)
    frac = (pd.Timestamp("2026-08-22") - pd.Timestamp("2026-04-01")).days / (
        pd.Timestamp("2026-12-15") - pd.Timestamp("2026-04-01")).days
    assert aug == pytest.approx(-1_345_000 * frac)


def test_score_dataframe_constant_series():
    # Constant daily values -> 28d-MA equals the constant; ex-CN/IR = ALL-CN-IR.
    dates = pd.date_range("2026-07-01", "2026-12-31", freq="D")
    df = _make_df({"ALL": 45_000_000, "CN": 3_000_000, "IR": 1_000_000}, dates)
    r = score_near_horizon.score_dataframe(df, target_date="2026-08-22",
                                           headwind_spec=HEADWIND_SPEC)
    hw_aug = score_near_horizon._headwind_ramp(pd.Timestamp("2026-08-22"), HEADWIND_SPEC)

    assert r["global_target_pre"] == pytest.approx(45_000_000)
    assert r["ex_cn_ir_target_pre"] == pytest.approx(41_000_000)  # 45 - 3 - 1
    assert r["global_target_post"] == pytest.approx(45_000_000 + hw_aug)
    assert r["ex_cn_ir_target_post"] == pytest.approx(41_000_000 + hw_aug)
    # Dec-15 headwind is the full anchor.
    assert r["global_dec15_post"] == pytest.approx(45_000_000 - 1_345_000)


def test_score_dataframe_ma_smooths_weekend_dip():
    # A weekly sawtooth (5 weekdays high, 2 weekend low) -> 28d-MA = the weekly mean,
    # not the trough day. Confirms we score the MA, not the daily value.
    dates = pd.date_range("2026-07-01", "2026-12-31", freq="D")
    vals = [50_000_000 if d.weekday() < 5 else 36_000_000 for d in dates]
    df = pd.DataFrame({
        "country": "ALL", "segment": OS_ALL,
        "target_date": [d.strftime("%Y-%m-%d") for d in dates], "dau": vals,
    })
    r = score_near_horizon.score_dataframe(df, target_date="2026-08-22", headwind_spec={})
    weekly_mean = (5 * 50_000_000 + 2 * 36_000_000) / 7
    assert r["global_target_pre"] == pytest.approx(weekly_mean, rel=1e-6)
    assert r["headwind_target"] == 0.0  # empty spec -> no headwind


LOCKED = (REPO_ROOT / "data-official/2026-07/desktop_locked"
          / "mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet")


@pytest.mark.skipif(not LOCKED.exists(), reason="locked July parquet not on disk")
def test_locked_parquet_regression():
    # Known values verified against csv/july_canonical_curves.csv and the user's chart.
    r = score_near_horizon.score_parquet(LOCKED)
    assert r["global_target_pre"] == pytest.approx(43_992_060, abs=1)
    assert r["global_target_post"] == pytest.approx(43_246_576, abs=1)
    assert r["ex_cn_ir_target_pre"] == pytest.approx(41_527_908, abs=1)
    assert r["ex_cn_ir_target_post"] == pytest.approx(40_782_423, abs=1)
    assert r["global_dec15_post"] == pytest.approx(48_585_483, abs=1)
