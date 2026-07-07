#!/usr/bin/env python3
"""Scoring library for the July mobile parameter-sensitivity probe.

Pure functions (no I/O beyond reading a forecast parquet) that turn a
with-marketing (`adj-m`) mobile forecast into the single headline number the grid
search targets: the **ALL-MOBILE, plus-Iran, Dec-15 28-day-MA** with the headwind
(`h`) applied, and its net delta from the June baseline.

Conventions — matched to the canonical July notebook (`july_canonical_v2026-06-29.ipynb`):
- **MA definition:** the variance-matched seam MA (`export_canonical_curves.display_ma`).
  At the Dec-15 far horizon the 28-day window is entirely forecast, so this is
  byte-identical to a plain `rolling(28).mean()` there — but we use `display_ma`
  so the full plotted curve matches the canonical.
- **Headwind application:** `adjustments.apply_net_adjustment_to_series`, which adds
  the *daily* ramp value on forecast dates. At the anchor (Dec-15) that is the full
  `mobile_dau` anchor (−27,162), NOT the ramp's 28-day average. See memory
  `feedback_headwind_ma28_alignment`.
- **Baseline:** the June delivered mobile plus-Iran curve is already a `display_ma`
  28d-MA; its Dec-15 value is the constant below.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# export_canonical_curves lives under the June cycle dir (June's canonical exporter,
# generic in forecast_start). Reused so July's MA matches June's exactly.
_JUNE_DIR = REPO_ROOT / "data-official/2026-06"
if str(_JUNE_DIR) not in sys.path:
    sys.path.insert(0, str(_JUNE_DIR))

from export_canonical_curves import display_ma  # noqa: E402
from mozaic_daily.adjustments import (  # noqa: E402
    render_adjustment,
    apply_net_adjustment_to_series,
)

# --- Constants (single source of truth for the target math) ----------------
DEC15 = pd.Timestamp("2026-12-15")
FORECAST_START = pd.Timestamp("2026-06-29")
# June delivered mobile plus-Iran ALL-MOBILE Dec-15 28d-MA (display_ma), read directly
# from data-official/2026-06/csv/june_canonical_curves.csv (column mobile_current_june_plus_iran).
JUNE_BASELINE_MA28 = 17_511_100.0
TARGET_UPLIFT = 400_000.0
TARGET_MA28 = JUNE_BASELINE_MA28 + TARGET_UPLIFT
HEADWIND_SPEC_PATH = REPO_ROOT / "data-official/2026-07/adjustments/headwind.json"


def all_mobile_daily(forecast_df: pd.DataFrame) -> pd.Series:
    """Extract the ALL-MOBILE glean_mobile daily DAU series (date-indexed, sorted)."""
    df = forecast_df.copy()
    df["target_date"] = pd.to_datetime(df["target_date"])
    mask = (
        (df["data_source"] == "glean_mobile")
        & (df["country"] == "ALL")
        & (df["app_name"] == "ALL MOBILE")
        & (df["segment"] == "{}")
    )
    return df.loc[mask].set_index("target_date")["dau"].sort_index()


def forecast_display_ma(daily: pd.Series, forecast_start: pd.Timestamp = FORECAST_START) -> pd.Series:
    """Variance-matched seam 28d-MA over a daily series (canonical `display_ma`)."""
    return display_ma(pd.Series(daily.index), daily.reset_index(drop=True), forecast_start)


def apply_headwind(ma_series: pd.Series, spec_path: Path = HEADWIND_SPEC_PATH,
                   platform: str = "mobile", forecast_start: pd.Timestamp = FORECAST_START) -> pd.Series:
    """Add the headwind ramp (daily anchor convention) onto a 28d-MA curve."""
    with open(spec_path) as f:
        spec = json.load(f)
    net = render_adjustment(spec, ma_series.index)
    return apply_net_adjustment_to_series(ma_series, net, platform, forecast_start)


def score_forecast(adjm_parquet: str | Path, at: pd.Timestamp = DEC15) -> dict:
    """Score one adj-m mobile forecast parquet at the target date.

    Returns adj-m and adj-hm 28d-MA at ``at``, net vs June baseline, and gap to target.
    """
    daily = all_mobile_daily(pd.read_parquet(adjm_parquet))
    ma = forecast_display_ma(daily)
    adjm = float(ma.loc[at])
    ma_hm = apply_headwind(ma)
    adjhm = float(ma_hm.loc[at])
    return {
        "adjm_ma28": adjm,
        "adjhm_ma28": adjhm,
        "headwind": adjhm - adjm,
        "net_vs_june": adjhm - JUNE_BASELINE_MA28,
        "gap_to_target": TARGET_MA28 - adjhm,
    }
