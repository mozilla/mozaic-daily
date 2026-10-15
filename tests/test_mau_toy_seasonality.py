"""Typical-year shape behind the MAU what-if toy (scripts/mau_toy_seasonality.py), and its JavaScript twin."""

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))
import mau_toy_seasonality as season  # noqa: E402

DAYS = pd.date_range("2019-01-01", "2026-10-09", freq="D")


def _seasonal_series(amplitude: float, annual_growth: float, peak_day: int = 75) -> pd.Series:
    """Growth × (1 + amplitude·cos), peaking on ``peak_day`` of the year."""
    doy = season.day_of_year_365(DAYS)
    shape = 1 + amplitude * np.cos(2 * np.pi * (doy - peak_day) / 365)
    years = (DAYS - DAYS[0]).days / 365
    return pd.Series(100e6 * (1 + annual_growth) ** years * shape, index=DAYS)


def test_day_of_year_folds_leap_day_onto_feb_28():
    days = pd.DatetimeIndex(["2024-02-28", "2024-02-29", "2024-03-01", "2025-03-01", "2025-12-31"])
    assert list(season.day_of_year_365(days)) == [59, 59, 60, 60, 365]


def test_profile_recovers_a_known_seasonal_wave_despite_growth():
    coefficients = season.seasonal_profile(_seasonal_series(amplitude=0.04, annual_growth=-0.05))
    index = season.seasonal_index(coefficients, np.arange(1, 366))
    assert coefficients[0] == 1.0
    assert index.max() - index.min() == pytest.approx(0.08, abs=0.01)    # ±4% wave
    assert 65 <= int(np.argmax(index)) + 1 <= 85                          # peak near day 75


def test_profile_ignores_a_one_off_crater():
    clean = season.seasonal_profile(_seasonal_series(0.04, 0.0))
    cratered = _seasonal_series(0.04, 0.0)
    cratered["2023-04-01":"2023-05-15"] *= 0.5                            # one bad spring out of seven years
    shocked = season.seasonal_profile(cratered)
    days = np.arange(1, 366)
    assert np.abs(season.seasonal_index(clean, days) - season.seasonal_index(shocked, days)).max() < 0.01


def test_profile_refuses_too_little_history():
    with pytest.raises(ValueError, match="two full years"):
        season.seasonal_profile(_seasonal_series(0.04, 0.0)["2024-06-01":])


def test_projection_rate_is_exactly_the_same_day_change():
    coefficients = season.seasonal_profile(_seasonal_series(0.04, 0.0))
    last = pd.Timestamp("2026-10-09")
    dec15_2026 = season.seasonal_projection(100.0, last, coefficients, 7.0, pd.Timestamp("2026-12-15"))
    dec15_2027 = season.seasonal_projection(100.0, last, coefficients, 7.0, pd.Timestamp("2027-12-15"))
    assert dec15_2027 / dec15_2026 == pytest.approx(1.07)
    assert season.seasonal_projection(100.0, last, coefficients, 7.0, last) == pytest.approx(100.0)


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_page_javascript_matches_the_python_projection():
    """The page re-implements the projection in JavaScript; a drift between the two would mislead every viewer."""
    template = (SCRIPTS / "mau_calculator_template.html").read_text()
    functions = [re.search(rf"function {name}\(.*?\n}}\n", template, re.S).group(0)
                 for name in ("dayOfYear", "seasonalIndex", "projectLevel")]
    coefficients = season.seasonal_profile(_seasonal_series(0.04, -0.03))
    data = {"seasonal": {"desktop": {"coefficients": coefficients}}, "last": {"desktop": 169e6}}
    dates = ["2026-10-09", "2027-02-28", "2027-08-20", "2027-12-15"]
    program = (f"const DATA = {json.dumps(data)}; const DAY_MS = 86400000; const lastDate = new Date('2026-10-09T00:00:00Z');\n"
               + "\n".join(functions)
               + f"\nconsole.log(JSON.stringify({json.dumps(dates)}.map(d => projectLevel('desktop', -4.5, new Date(d + 'T00:00:00Z')))));")
    js_values = json.loads(subprocess.run(["node", "-e", program], capture_output=True, text=True, check=True).stdout)
    py_values = [season.seasonal_projection(169e6, pd.Timestamp("2026-10-09"), coefficients, -4.5, pd.Timestamp(d)) for d in dates]
    assert js_values == pytest.approx(py_values, rel=1e-9)
