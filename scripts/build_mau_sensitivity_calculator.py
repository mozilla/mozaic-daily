#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build the stakeholder what-if toy for 2027 MAU: one self-contained HTML page with desktop and mobile rate sliders.

The page is deliberately a cartoon, so nobody reads it as a forecast. History is the real 2026 MAU (one point a week,
taken from ``handoff/glean2027_mau_desktop_and_mobile.csv``). After the last actual, each platform is
``last actual × S(day) / S(last actual's day) × (1 + yearly rate) ** years``, where ``S`` is a **simplified typical year**:
the median ratio of each calendar day to its centred 365-day mean over past years, fitted with four yearly harmonics
(Brendan 2026-10-15: the cartoon should keep the real curves' shape, at least yearly seasonality). Because ``S`` repeats
every year, the slider rate is exactly the change between the same day in consecutive years, the quantity the histogram
shows. No holidays, no changepoints, no model. Desktop's shape comes from Legacy (2020 on), mobile's from Glean (2021 on).
The sliders start at the rate that lands this cartoon on the pre-draft forecast's 2027-12-15 value; the pre-draft curve
itself is never drawn.

Above each slider a histogram shows the year-over-year change actually seen over the past two years, read every two
weeks back from the last actual (up to 53 readings), on the slider's own scale, so a chosen rate can be judged against
what has been normal. Desktop has three sources behind a radio button (Brendan 2026-10-15), each named on the page:
**Legacy** telemetry (``telemetry.active_users_aggregates``; the default), the **simulated** Glean-equivalent history
(build 5's training rows), and **actual Glean** (real Glean desktop starts 2025-05-22, so only the readings since
2026-05-22 exist). Mobile has one source, the real Glean mobile series in the curves file. A dotted line marks the
pre-draft's **expected** Dec 15 2025 → Dec 15 2026 change on the same basis as the bars (Brendan 2026-10-15): build 5
(Simulated Glean) for the Legacy and Simulated Glean options, the headline real-Glean build for actual Glean (whose
Dec 2025 level still carries the duplicate-id inflation), the mobile total for mobile.

Desktop and mobile are summed: MAU is the official proxy for monthly active profiles, and desktop and mobile profiles
are distinct (confirmed 2026-10-15).

The drawing and slider logic live in ``scripts/mau_calculator_template.html``; this script only computes the numbers
and substitutes them for ``__CALC_DATA__``. Output: ``<out-dir>/index.html`` (deployable as-is with ``quick deploy``).

Usage:
    python scripts/build_mau_sensitivity_calculator.py \\
        --curves data-official/2027-glean-draft/handoff/glean2027_mau_desktop_and_mobile.csv \\
        --legacy-desktop-mau data-official/2027-glean-draft/raw_legacy_osxcountry_2026-10-09/mozaic_parts.raw.glean_history.desktop.MAU.parquet \\
        --simulated-desktop-curves data-official/2027-glean-draft/csv/glean2027_canonical_curves.csv \\
        --out-dir data-official/2027-glean-draft/calculator
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path

import sys

import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "scripts"))
from mau_toy_seasonality import DAYS_PER_YEAR, day_of_year_365, seasonal_index, seasonal_profile  # noqa: E402
TEMPLATE = repo_root / "scripts" / "mau_calculator_template.html"
PLACEHOLDER = "__CALC_DATA__"
HISTORY_START = pd.Timestamp("2026-01-01")
TARGET_DATE = pd.Timestamp("2027-12-15")
EXPECTED_FROM, EXPECTED_TO = pd.Timestamp("2025-12-15"), pd.Timestamp("2026-12-15")
HISTORY_STEP_DAYS = 7
YOY_STEP_DAYS = 14
YOY_READINGS = 53            # two years of two-weekly readings, counting the last actual
YOY_LAG_DAYS = 364           # 52 weeks, so each reading compares the same weekday mix
YOY_BIN_WIDTH = {"desktop": 1.0, "mobile": 2.0}   # percentage points
SLIDER_LIMITS = {"desktop": (-15.0, 10.0), "mobile": (-10.0, 30.0)}   # percent per year; wide enough to cover any defensible case
SLIDER_STEP = 0.5


def implied_annual_rate(start_value: float, end_value: float, days: int) -> float:
    """Steady yearly growth rate (percent) that compounds start_value into end_value over ``days`` days."""
    if start_value <= 0 or end_value <= 0 or days <= 0:
        raise ValueError(f"implied_annual_rate needs positive inputs; got start={start_value}, end={end_value}, days={days}")
    return ((end_value / start_value) ** (DAYS_PER_YEAR / days) - 1) * 100


def round_to_slider_step(rate_percent: float) -> float:
    return round(rate_percent / SLIDER_STEP) * SLIDER_STEP


def weekly_history(values: pd.Series, *, start: pd.Timestamp, last_actual: pd.Timestamp) -> pd.Series:
    """Every 7th day counting back from the last actual (so the last actual is always the final point), from ``start``."""
    window = values[(values.index >= start) & (values.index <= last_actual)].dropna()
    days_back = (last_actual - window.index).days
    return window[days_back % HISTORY_STEP_DAYS == 0]


MIN_PARTIAL_READINGS = 4


def periodic_yoy(values: pd.Series, *, last_actual: pd.Timestamp, allow_partial: bool = False) -> pd.Series:
    """Year-over-year change (percent) every YOY_STEP_DAYS back from ``last_actual``, YOY_READINGS readings, oldest first.

    ``allow_partial`` keeps only the readings a short history can support (actual Glean desktop) instead of raising.
    """
    dates = pd.date_range(end=last_actual, periods=YOY_READINGS, freq=f"{YOY_STEP_DAYS}D")
    current = values.reindex(dates)
    year_ago = values.reindex(dates - pd.Timedelta(days=YOY_LAG_DAYS)).set_axis(dates)
    if allow_partial:
        supported = current.notna() & year_ago.notna()
        if supported.sum() < MIN_PARTIAL_READINGS:
            raise ValueError(f"periodic_yoy: only {supported.sum()} readings possible (need {MIN_PARTIAL_READINGS}); series covers "
                             f"{values.index.min().date()}..{values.index.max().date()}")
        current, year_ago = current[supported], year_ago[supported]
    elif current.isna().any() or year_ago.isna().any():
        missing = list(dates[current.isna() | year_ago.isna()].date[:3])
        raise ValueError(f"periodic_yoy: no value on or a year before {missing}...; series covers "
                         f"{values.index.min().date()}..{values.index.max().date()}, needs {(dates[0] - pd.Timedelta(days=YOY_LAG_DAYS)).date()} on")
    return (current / year_ago - 1) * 100


def load_legacy_desktop_mau(path: Path) -> pd.Series:
    """World desktop MAU per day from a Legacy raw pull (country x OS rows, column ``y``)."""
    raw = pd.read_parquet(path)
    return raw.groupby(pd.to_datetime(raw["x"]))["y"].sum()


def load_simulated_desktop_curves(path: Path) -> pd.DataFrame:
    """The Glean-equivalent ('simulated') build's canonical curves: ``data_type`` and world ``mau_world``, history + forecast."""
    return pd.read_csv(path, parse_dates=["date"]).set_index("date")[["data_type", "mau_world"]]


def expected_change(values: pd.Series) -> float:
    """Percent change from EXPECTED_FROM (an actual) to EXPECTED_TO (the pre-draft forecast) in one build's series."""
    return float(values[EXPECTED_TO] / values[EXPECTED_FROM] - 1) * 100


def build_calculator_data(curves: pd.DataFrame, *, legacy_desktop: pd.Series, simulated_curves: pd.DataFrame,
                          generated: str, source: str) -> dict:
    is_actual = curves["desktop_data_type"].eq("training") & curves["mobile_data_type"].eq("training")
    last_actual = curves.index[is_actual].max()
    days_to_target = (TARGET_DATE - last_actual).days
    data = {"generated": generated, "source": source, "horizon_end": str(curves.index.max().date()),
            "last": {"date": str(last_actual.date())}, "defaults": {}, "implied_exact": {}, "slider": {},
            "history": {}, "yoy": {}}
    # Desktop's long history (YoY readings and the typical-year shape) is Legacy; real Glean desktop starts 2025-05-22.
    mobile_actuals = curves.loc[curves["mobile_data_type"].eq("training"), "mobile_mau"]
    glean_desktop = curves.loc[curves["desktop_data_type"].eq("training"), "desktop_mau"]
    simulated_desktop = simulated_curves.loc[simulated_curves["data_type"].eq("training"), "mau_world"].dropna()
    simulated_expected = (expected_change(simulated_curves["mau_world"]), "Simulated Glean build")
    headline_expected = {platform: (expected_change(curves[f"{platform}_mau"]), "headline build") for platform in ["desktop", "mobile"]}
    long_history = {"desktop": (legacy_desktop, "Legacy desktop telemetry"), "mobile": (mobile_actuals, "Glean mobile telemetry")}
    # (id, radio label, series, partial history allowed, note, expected 2025->2026 change on the same basis); first = default
    yoy_options = {
        "desktop": [("legacy", "Legacy", legacy_desktop, False, "the older desktop telemetry system", simulated_expected),
                    ("simulated", "Simulated Glean", simulated_desktop, False, "Legacy translated into Glean terms (our build-5 input)",
                     simulated_expected),
                    ("glean", "Actual Glean", glean_desktop, True, "real Glean only starts May 2025, so only readings since May 2026 exist",
                     headline_expected["desktop"])],
        "mobile": [("glean", "Glean", mobile_actuals, False, "the only mobile telemetry", headline_expected["mobile"])],
    }
    data["seasonal"] = {}
    for platform in ["desktop", "mobile"]:
        values = curves[f"{platform}_mau"]
        last_value = float(values[last_actual])
        long_values, long_label = long_history[platform]
        coefficients = seasonal_profile(long_values)
        shape = seasonal_index(coefficients, day_of_year_365(pd.DatetimeIndex([TARGET_DATE, last_actual])))
        # the steady rate that lands the seasonal cartoon on the pre-draft forecast's 2027-12-15 value
        rate = implied_annual_rate(last_value * shape[0] / shape[1], float(values[TARGET_DATE]), days_to_target)
        data["seasonal"][platform] = {"coefficients": [round(c, 6) for c in coefficients], "source": long_label,
                                      "from": str(long_values.dropna().index.min().date())}
        history = weekly_history(values, start=HISTORY_START, last_actual=last_actual)
        data["last"][platform] = last_value
        data["implied_exact"][platform] = round(rate, 3)
        data["defaults"][platform] = round_to_slider_step(rate)
        data["slider"][platform] = {"min": SLIDER_LIMITS[platform][0], "max": SLIDER_LIMITS[platform][1]}
        data["history"][platform] = [int(v) for v in history.values]
        data["history"]["dates"] = [str(d.date()) for d in history.index]
        options = []
        for option_id, label, series, allow_partial, note, (expected, expected_build) in yoy_options[platform]:
            readings = periodic_yoy(series, last_actual=last_actual, allow_partial=allow_partial)
            options.append({"id": option_id, "label": label, "note": note, "values": [round(v, 2) for v in readings.values],
                            "first": str(readings.index.min().date()), "expected": round(expected, 2),
                            "expected_build": expected_build})
        data["yoy"][platform] = {"options": options, "bin_width": YOY_BIN_WIDTH[platform]}
    return data


def render_page(data: dict, template: str) -> str:
    if template.count(PLACEHOLDER) != 1:
        raise ValueError(f"template must contain {PLACEHOLDER} exactly once; found {template.count(PLACEHOLDER)}")
    return template.replace(PLACEHOLDER, json.dumps(data))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--curves", type=Path, required=True)
    parser.add_argument("--legacy-desktop-mau", type=Path, required=True,
                        help="Legacy raw desktop MAU pull (raw_legacy_osxcountry_*/mozaic_parts.raw.glean_history.desktop.MAU.parquet) for the desktop YoY histogram")
    parser.add_argument("--simulated-desktop-curves", type=Path, required=True,
                        help="Glean-equivalent build's canonical curves CSV (csv/glean2027_canonical_curves.csv): the 'simulated' desktop histogram")
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    curves = pd.read_csv(args.curves, parse_dates=["date"]).set_index("date")
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, cwd=repo_root).stdout.strip()
    data = build_calculator_data(curves, legacy_desktop=load_legacy_desktop_mau(args.legacy_desktop_mau),
                                 simulated_curves=load_simulated_desktop_curves(args.simulated_desktop_curves), generated=datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z"),
                                 source=f"scripts/build_mau_sensitivity_calculator.py @ {commit}")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out = args.out_dir / "index.html"
    out.write_text(render_page(data, TEMPLATE.read_text()))
    print(f"last actual {data['last']['date']}: desktop {data['last']['desktop']:,.0f}, mobile {data['last']['mobile']:,.0f}")
    print(f"implied pace to {TARGET_DATE.date()}: desktop {data['implied_exact']['desktop']:+.2f}%/yr, "
          f"mobile {data['implied_exact']['mobile']:+.2f}%/yr (sliders start at {data['defaults']})")
    for platform, yoy in data["yoy"].items():
        for option in yoy["options"]:
            values = pd.Series(option["values"])
            print(f"{platform} YoY [{option['label']}] {len(values)} readings from {option['first']}: min {values.min():+.1f}, "
                  f"median {values.median():+.1f}, max {values.max():+.1f}; expected 2025->2026 {option['expected']:+.2f}% ({option['expected_build']})")
    print(f"saved: {out}")


if __name__ == "__main__":
    main()
