"""Tests for the July mobile sensitivity scorer (``scripts/mobile_sensitivity.py``).

Each test exercises the real scoring chain (extraction -> canonical seam MA ->
headwind application) against controlled fixtures, so a genuine regression would
break it:

- ``test_all_mobile_daily_*`` fails if the ALL-MOBILE row filter drifts (wrong
  country/app/segment key lets decoy rows leak in).
- ``test_apply_headwind_*`` fails if the headwind stops using the production
  daily-anchor convention (e.g. reverts to the 28d-average of the ramp) or forgets
  to gate on forecast_start.
- ``test_score_forecast_end_to_end`` fails if the far-horizon 28d-MA, the headwind,
  or the baseline wiring regress.
"""

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parent.parent
for p in (REPO / "scripts", REPO / "src", REPO / "data-official/2026-06"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import mobile_sensitivity as ms  # noqa: E402


def _all_mobile_rows(dates, values):
    return pd.DataFrame({
        "data_source": "glean_mobile",
        "country": "ALL",
        "app_name": "ALL MOBILE",
        "segment": "{}",
        "target_date": dates,
        "dau": values,
    })


def test_all_mobile_daily_filters_to_all_mobile_only():
    dates = pd.date_range("2026-12-01", periods=5, freq="D")
    good = _all_mobile_rows(dates, [1.0, 2.0, 3.0, 4.0, 5.0])
    decoys = pd.DataFrame({
        "data_source": ["glean_mobile", "glean_mobile", "legacy_desktop", "glean_mobile"],
        "country": ["US", "ALL", "ALL", "ALL"],
        "app_name": ["ALL MOBILE", "fenix_android", "ALL", "ALL MOBILE"],
        "segment": ["{}", "{}", "{}", '{"os": "x"}'],
        "target_date": [dates[0]] * 4,
        "dau": [99.0, 99.0, 99.0, 99.0],
    })
    df = pd.concat([good, decoys], ignore_index=True)
    s = ms.all_mobile_daily(df)
    assert list(s.values) == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert s.index.tolist() == list(dates)


def test_apply_headwind_uses_full_anchor_at_dec15_and_zero_before_forecast():
    idx = pd.date_range("2026-01-01", "2026-12-31", freq="D")
    base_level = 18_000_000.0
    ma = pd.Series(base_level, index=idx)
    out = ms.apply_headwind(ma)
    mobile_anchor = json.load(open(ms.HEADWIND_SPEC_PATH))["mobile_dau"]

    # At the anchor date (Dec-15) the ramp is at full magnitude — the production
    # convention adds the DAILY anchor value, not the ramp's 28d average.
    assert out.loc[ms.DEC15] == pytest.approx(base_level + mobile_anchor, abs=1.0)
    # Pre-forecast dates are untouched.
    assert out.loc[pd.Timestamp("2026-01-15")] == pytest.approx(base_level, abs=1e-6)


def test_score_forecast_end_to_end(tmp_path):
    pre = pd.date_range("2026-05-01", ms.FORECAST_START - pd.Timedelta(days=1), freq="D")
    fc = pd.date_range(ms.FORECAST_START, "2026-12-31", freq="D")
    dates = pre.append(fc)
    values = [17_000_000.0] * len(pre) + [18_000_000.0] * len(fc)
    df = _all_mobile_rows(dates, values)
    parquet = tmp_path / "f.gm-D.adj-m.parquet"
    df.to_parquet(parquet)

    r = ms.score_forecast(parquet)
    mobile_anchor = json.load(open(ms.HEADWIND_SPEC_PATH))["mobile_dau"]

    # Dec-15 window is entirely forecast (flat 18M) -> adj-m MA is exactly 18M.
    assert r["adjm_ma28"] == pytest.approx(18_000_000.0, abs=1.0)
    assert r["headwind"] == pytest.approx(mobile_anchor, abs=1.0)
    assert r["adjhm_ma28"] == pytest.approx(r["adjm_ma28"] + r["headwind"], abs=1e-6)
    assert r["net_vs_june"] == pytest.approx(r["adjhm_ma28"] - ms.JUNE_BASELINE_MA28, abs=1e-6)
    assert r["gap_to_target"] == pytest.approx(ms.TARGET_MA28 - r["adjhm_ma28"], abs=1e-6)
