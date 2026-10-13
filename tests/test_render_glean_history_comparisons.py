"""The two derived tables on the comparisons page (scripts/render_glean_history_comparisons.py)."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import render_glean_history_comparisons as rc  # noqa: E402


def _curves(start: str, seam: str, end: str, level: float, forecast_slope: float) -> pd.DataFrame:
    """One flat history then a linear forecast, same value in every OS column."""
    idx = pd.date_range(start, end, freq="D")
    seam_ts = pd.Timestamp(seam)
    is_forecast = idx >= seam_ts
    days_after = pd.Series((idx - seam_ts).days, index=idx).clip(lower=0).to_numpy()
    values = level + forecast_slope * days_after
    frame = pd.DataFrame({"data_type": ["forecast" if f else "training" for f in is_forecast]}, index=idx)
    for metric in rc.METRIC_COLS:
        for os_name in rc.OS_ORDER:
            frame[f"{metric}_{os_name}"] = values
    return frame


LOCKED = _curves("2020-01-01", "2026-10-10", "2027-12-31", level=100.0, forecast_slope=0.0)
REAL = _curves("2025-05-22", "2026-10-10", "2027-12-31", level=110.0, forecast_slope=1.0)


def test_real_vs_locked_reports_the_horizon_ratio_against_both_references():
    table = rc.real_vs_locked(LOCKED, REAL)
    world_mau = table[(table.metric == "MAU (raw daily)") & (table.os == "world")].set_index("horizon")
    assert len(table) == len(rc.METRIC_COLS) * len(rc.OS_ORDER) * len(rc.HORIZONS)
    headline = world_mau.loc["Dec 15 2027 (headline)"]
    days = (pd.Timestamp("2027-12-15") - pd.Timestamp("2026-10-10")).days
    assert headline.real_glean_only == pytest.approx(110.0 + days)
    assert headline.locked == 100.0
    assert headline.ratio_real_to_locked == pytest.approx((110.0 + days) / 100.0)
    # The own-last-actual column is relative to the real history's final day, not to the locked build
    assert headline["real_glean_last_actual_2026-10-09"] == 110.0
    assert headline.ratio_to_own_last_actual == pytest.approx((110.0 + days) / 110.0)


def test_in_real_units_uses_the_last_30_history_days_and_restates_the_locked_dec15():
    # Make the real/locked ratio drift: 1.20 for the first part of the overlap, 1.05 for the last 30 history days
    real = REAL.copy()
    history = real.index[real["data_type"] == "training"]
    for col in [c for c in real.columns if c != "data_type"]:
        real.loc[history, col] = 120.0
        real.loc[history[-30:], col] = 105.0
    table = rc.in_real_units(LOCKED, real).set_index(["metric", "os"])
    row = table.loc[("MAU (raw daily)", "world")]
    assert row.ratio_first30d_2025 == pytest.approx(1.20)
    assert row.ratio_last30d_2026 == pytest.approx(1.05)
    assert (row.ratio_min, row.ratio_max) == pytest.approx((1.05, 1.20))
    assert row["restated_2027-12-15"] == pytest.approx(105.0)   # locked 100 × 1.05
    assert row["locked_2027-12-15"] == 100.0
