#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build the stakeholder what-if toy for 2027 MAU: one self-contained HTML page with desktop and mobile rate sliders.

The page is deliberately a cartoon, so nobody reads it as a forecast. History is the real 2026 MAU (one point a week,
taken from ``handoff/glean2027_mau_desktop_and_mobile.csv``). After the last actual, each platform is just
``last actual × (1 + yearly rate) ** years``: no seasonality, no holidays, no model. The sliders start at the
**implied pace** of the pre-draft builds: the steady yearly rate that takes the last actual to the pre-draft forecast's
2027-12-15 value. The pre-draft forecast curve itself is never drawn.

Above each slider a histogram shows the year-over-year change actually seen over the past two years, read every two
weeks back from the last actual (53 readings), on the slider's own scale, so a chosen rate can be judged against what
has been normal. **Desktop's readings come from Legacy telemetry** (``telemetry.active_users_aggregates``, Brendan
2026-10-15: "you can use legacy for YoY estimates, just call it out"), because real Glean desktop starts 2025-05-22;
the page says so. Mobile's come from the real Glean mobile series in the curves file.

Desktop and mobile are summed: MAU is the official proxy for monthly active profiles, and desktop and mobile profiles
are distinct (confirmed 2026-10-15).

The drawing and slider logic live in ``scripts/mau_calculator_template.html``; this script only computes the numbers
and substitutes them for ``__CALC_DATA__``. Output: ``<out-dir>/index.html`` (deployable as-is with ``quick deploy``).

Usage:
    python scripts/build_mau_sensitivity_calculator.py \\
        --curves data-official/2027-glean-draft/handoff/glean2027_mau_desktop_and_mobile.csv \\
        --legacy-desktop-mau data-official/2027-glean-draft/raw_legacy_osxcountry_2026-10-09/mozaic_parts.raw.glean_history.desktop.MAU.parquet \\
        --out-dir data-official/2027-glean-draft/calculator
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
TEMPLATE = repo_root / "scripts" / "mau_calculator_template.html"
PLACEHOLDER = "__CALC_DATA__"
HISTORY_START = pd.Timestamp("2026-01-01")
TARGET_DATE = pd.Timestamp("2027-12-15")
DAYS_PER_YEAR = 365
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


def periodic_yoy(values: pd.Series, *, last_actual: pd.Timestamp) -> pd.Series:
    """Year-over-year change (percent) every YOY_STEP_DAYS back from ``last_actual``, YOY_READINGS readings, oldest first."""
    dates = pd.date_range(end=last_actual, periods=YOY_READINGS, freq=f"{YOY_STEP_DAYS}D")
    current = values.reindex(dates)
    year_ago = values.reindex(dates - pd.Timedelta(days=YOY_LAG_DAYS)).set_axis(dates)
    if current.isna().any() or year_ago.isna().any():
        missing = list(dates[current.isna() | year_ago.isna()].date[:3])
        raise ValueError(f"periodic_yoy: no value on or a year before {missing}...; series covers "
                         f"{values.index.min().date()}..{values.index.max().date()}, needs {(dates[0] - pd.Timedelta(days=YOY_LAG_DAYS)).date()} on")
    return (current / year_ago - 1) * 100


def load_legacy_desktop_mau(path: Path) -> pd.Series:
    """World desktop MAU per day from a Legacy raw pull (country x OS rows, column ``y``)."""
    raw = pd.read_parquet(path)
    return raw.groupby(pd.to_datetime(raw["x"]))["y"].sum()


def build_calculator_data(curves: pd.DataFrame, *, desktop_yoy_values: pd.Series, generated: str, source: str) -> dict:
    is_actual = curves["desktop_data_type"].eq("training") & curves["mobile_data_type"].eq("training")
    last_actual = curves.index[is_actual].max()
    days_to_target = (TARGET_DATE - last_actual).days
    data = {"generated": generated, "source": source, "horizon_end": str(curves.index.max().date()),
            "last": {"date": str(last_actual.date())}, "defaults": {}, "implied_exact": {}, "slider": {},
            "history": {}, "yoy": {}}
    yoy_sources = {"desktop": (desktop_yoy_values, "Legacy desktop telemetry"),
                   "mobile": (curves["mobile_mau"], "Glean mobile telemetry")}
    for platform in ["desktop", "mobile"]:
        values = curves[f"{platform}_mau"]
        last_value = float(values[last_actual])
        rate = implied_annual_rate(last_value, float(values[TARGET_DATE]), days_to_target)
        history = weekly_history(values, start=HISTORY_START, last_actual=last_actual)
        data["last"][platform] = last_value
        data["implied_exact"][platform] = round(rate, 3)
        data["defaults"][platform] = round_to_slider_step(rate)
        data["slider"][platform] = {"min": SLIDER_LIMITS[platform][0], "max": SLIDER_LIMITS[platform][1]}
        data["history"][platform] = [int(v) for v in history.values]
        data["history"]["dates"] = [str(d.date()) for d in history.index]
        yoy_values, yoy_label = yoy_sources[platform]
        readings = periodic_yoy(yoy_values, last_actual=last_actual)
        data["yoy"][platform] = {"values": [round(v, 2) for v in readings.values], "source": yoy_label,
                                 "first": str(readings.index.min().date()), "bin_width": YOY_BIN_WIDTH[platform]}
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
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()

    curves = pd.read_csv(args.curves, parse_dates=["date"]).set_index("date")
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, cwd=repo_root).stdout.strip()
    data = build_calculator_data(curves, desktop_yoy_values=load_legacy_desktop_mau(args.legacy_desktop_mau), generated=datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z"),
                                 source=f"scripts/build_mau_sensitivity_calculator.py @ {commit}")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out = args.out_dir / "index.html"
    out.write_text(render_page(data, TEMPLATE.read_text()))
    print(f"last actual {data['last']['date']}: desktop {data['last']['desktop']:,.0f}, mobile {data['last']['mobile']:,.0f}")
    print(f"implied pace to {TARGET_DATE.date()}: desktop {data['implied_exact']['desktop']:+.2f}%/yr, "
          f"mobile {data['implied_exact']['mobile']:+.2f}%/yr (sliders start at {data['defaults']})")
    for platform, yoy in data["yoy"].items():
        values = pd.Series(yoy["values"])
        print(f"{platform} YoY ({yoy['source']}, {len(values)} readings from {yoy['first']}): min {values.min():+.1f}, "
              f"median {values.median():+.1f}, max {values.max():+.1f}")
    print(f"saved: {out}")


if __name__ == "__main__":
    main()
