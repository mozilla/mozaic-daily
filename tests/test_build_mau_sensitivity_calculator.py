"""The numbers behind the MAU what-if toy (scripts/build_mau_sensitivity_calculator.py)."""

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import build_mau_sensitivity_calculator as calc  # noqa: E402

LAST_ACTUAL = pd.Timestamp("2026-10-09")


def _daily(start: str, end: str, annual_growth: float, level_at_last: float = 100.0) -> pd.Series:
    """A series compounding at ``annual_growth`` per 364 days, equal to ``level_at_last`` on LAST_ACTUAL."""
    idx = pd.date_range(start, end, freq="D")
    days_from_last = (idx - LAST_ACTUAL).days
    return pd.Series(level_at_last * (1 + annual_growth) ** (days_from_last / 364), index=idx)


def _curves(desktop_growth: float, mobile_growth: float) -> pd.DataFrame:
    idx = pd.date_range("2023-01-01", "2027-12-31", freq="D")
    data_type = ["training" if d <= LAST_ACTUAL else "forecast" for d in idx]
    return pd.DataFrame({
        "desktop_data_type": data_type, "mobile_data_type": data_type,
        "desktop_mau": _daily("2023-01-01", "2027-12-31", desktop_growth, 160e6).values,
        "mobile_mau": _daily("2023-01-01", "2027-12-31", mobile_growth, 56e6).values,
    }, index=idx)


def test_implied_rate_recovers_a_known_compounding():
    # 100 -> 121 over two years is exactly 10% a year
    assert calc.implied_annual_rate(100.0, 121.0, 730) == pytest.approx(10.0)
    assert calc.implied_annual_rate(100.0, 81.0, 730) == pytest.approx(-10.0)


def test_implied_rate_rejects_nonpositive_inputs():
    with pytest.raises(ValueError, match="positive"):
        calc.implied_annual_rate(0.0, 10.0, 100)


def test_slider_rounding_snaps_to_half_points():
    assert calc.round_to_slider_step(-3.83) == -4.0
    assert calc.round_to_slider_step(14.67) == 14.5


def test_weekly_history_ends_on_the_last_actual_and_steps_seven_days():
    values = _daily("2025-12-01", "2026-12-31", 0.0)
    history = calc.weekly_history(values, start=pd.Timestamp("2026-01-01"), last_actual=LAST_ACTUAL)
    assert history.index.max() == LAST_ACTUAL
    assert set(history.index.to_series().diff().dropna().dt.days) == {7}
    assert history.index.min() >= pd.Timestamp("2026-01-01")


def test_periodic_yoy_reads_a_steady_growth_rate_back():
    values = _daily("2023-01-01", "2026-10-09", 0.05)
    readings = calc.periodic_yoy(values, last_actual=LAST_ACTUAL)
    assert len(readings) == 53
    assert readings.index.max() == LAST_ACTUAL
    assert (readings.index.to_series().diff().dropna().dt.days == 14).all()
    assert readings.to_numpy() == pytest.approx([5.0] * 53)


def test_periodic_yoy_refuses_a_series_too_short_for_two_years():
    values = _daily("2025-05-22", "2026-10-09", 0.05)   # the real-Glean desktop span
    with pytest.raises(ValueError, match="needs 2023"):
        calc.periodic_yoy(values, last_actual=LAST_ACTUAL)


def test_periodic_yoy_partial_keeps_only_supported_readings():
    values = _daily("2025-05-22", "2026-10-09", 0.05)   # the real-Glean desktop span: readings from 2026-05-22 only
    readings = calc.periodic_yoy(values, last_actual=LAST_ACTUAL, allow_partial=True)
    assert readings.index.min() >= pd.Timestamp("2026-05-21")
    assert len(readings) == 11
    assert readings.to_numpy() == pytest.approx([5.0] * 11)


def test_periodic_yoy_partial_still_refuses_almost_no_history():
    values = _daily("2025-09-01", "2026-10-09", 0.05)
    with pytest.raises(ValueError, match="readings possible"):
        calc.periodic_yoy(values, last_actual=LAST_ACTUAL, allow_partial=True)


def test_build_data_keeps_the_three_desktop_sources_apart_and_lands_on_the_forecast_dec15():
    curves = _curves(desktop_growth=-0.04, mobile_growth=0.12)
    legacy = _daily("2019-01-01", "2026-10-09", -0.02, 150e6)       # each source at its own rate, so a mix-up shows
    simulated_values = _daily("2019-01-01", "2027-12-31", -0.03, 152e6)
    simulated = pd.DataFrame({"data_type": ["training" if d <= LAST_ACTUAL else "forecast" for d in simulated_values.index],
                              "mau_world": simulated_values.values}, index=simulated_values.index)
    data = calc.build_calculator_data(curves, legacy_desktop=legacy, simulated_curves=simulated, generated="g", source="s")
    desktop_options = {o["id"]: o for o in data["yoy"]["desktop"]["options"]}
    assert list(desktop_options) == ["legacy", "simulated", "glean"]                  # legacy first = the default
    assert desktop_options["legacy"]["values"] == pytest.approx([-2.0] * 53, abs=0.01)
    assert desktop_options["simulated"]["values"] == pytest.approx([-3.0] * 53, abs=0.01)
    assert desktop_options["glean"]["values"] == pytest.approx([-4.0] * 53, abs=0.01)  # the curves file's own desktop rows
    assert [o["id"] for o in data["yoy"]["mobile"]["options"]] == ["glean"]
    # the dotted 2025->2026 line follows the bars' basis: simulated build for legacy/simulated, headline curves otherwise
    # (365 days of a 364-day compounding: a hair past the nominal rate)
    assert desktop_options["legacy"]["expected"] == pytest.approx(-3.0, abs=0.02)
    assert desktop_options["simulated"]["expected"] == pytest.approx(-3.0, abs=0.02)
    assert desktop_options["glean"]["expected"] == pytest.approx(-4.0, abs=0.02)
    assert data["yoy"]["mobile"]["options"][0]["expected"] == pytest.approx(12.0, abs=0.05)
    # no seasonality in the fixture, so the shape is flat and the default is the plain implied rate
    assert data["implied_exact"]["desktop"] == pytest.approx(-4.0, abs=0.05)
    assert data["implied_exact"]["mobile"] == pytest.approx(12.0, abs=0.05)


def test_render_page_substitutes_json_once_and_guards_the_template():
    page = calc.render_page({"a": 1}, "const DATA = __CALC_DATA__;")
    assert json.loads(page.removeprefix("const DATA = ").removesuffix(";")) == {"a": 1}
    with pytest.raises(ValueError, match="exactly once"):
        calc.render_page({}, "__CALC_DATA__ __CALC_DATA__")


def test_shipped_template_has_exactly_one_placeholder():
    assert calc.TEMPLATE.read_text().count(calc.PLACEHOLDER) == 1
