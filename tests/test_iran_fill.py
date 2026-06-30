# -*- coding: utf-8 -*-
"""Tests for the Iran counterfactual-fill producer (scripts/generate_iran_fill.py).

Two layers:
- Unit tests on the pure transforms (population<->segment mapping, DOW profile, re-seasonalization)
  using synthetic fixtures — no BigQuery, no mozaic.
- Conformance tests on the produced on-disk artifacts (schema, invariants, restored weekly
  amplitude). These skip if the artifacts haven't been generated yet.
"""

import json
import pathlib
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import generate_iran_fill as gif  # noqa: E402
from mozaic_daily.queries import Platform  # noqa: E402

FILL_DIR = ROOT / "data-official" / "2026-07" / "iran_fill"
SOURCES = ["glean_desktop", "legacy_desktop", "glean_mobile"]
DESKTOP_SEG = ["modern_windows", "winX"]


# ---------------------------------------------------------------------------
# Pure-transform unit tests
# ---------------------------------------------------------------------------

def test_population_to_segments_desktop():
    assert gif.population_to_segments("modern_windows", Platform.DESKTOP) == {"modern_windows": True, "winX": False}
    assert gif.population_to_segments("winX", Platform.DESKTOP) == {"modern_windows": False, "winX": True}
    # 'other' (and any non-segment label) -> all flags False
    assert gif.population_to_segments("other", Platform.DESKTOP) == {"modern_windows": False, "winX": False}


def test_population_to_segments_mobile_single_label():
    segs = gif.population_to_segments("focus_ios", Platform.MOBILE)
    assert segs["focus_ios"] is True
    assert sum(bool(v) for v in segs.values()) == 1  # exactly one flag set


def test_dow_profile_recovers_known_pattern():
    # Build a flat-trend series with a known multiplicative weekday pattern; the profile should
    # recover that pattern (geomean-normalized), independent of the constant level.
    dow_mult = {0: 1.10, 1: 1.20, 2: 1.10, 3: 0.80, 4: 0.45, 5: 1.25, 6: 1.25}
    idx = pd.date_range("2025-09-01", "2026-02-27", freq="D")
    s = pd.Series([500_000 * dow_mult[d.dayofweek] for d in idx], index=idx)
    prof, n = gif._dow_profile(s)
    expected = pd.Series(dow_mult)
    expected = expected / np.exp(np.log(expected).mean())  # geomean-normalize
    assert n > 100
    for dow in range(7):
        assert prof[dow] == pytest.approx(expected[dow], rel=0.02)


def _synthetic_real_desktop(dow_mult):
    """Real-style IR desktop frame (single modern_windows population) over the clean window."""
    idx = pd.date_range("2025-09-01", "2026-02-27", freq="D")
    return pd.DataFrame({
        "x": idx,
        "country": "IR",
        "modern_windows": True,
        "winX": False,
        "y": [1_000_000 * dow_mult[d.dayofweek] for d in idx],
    })


def _synthetic_smooth_fill(dow_mult, base=800_000.0):
    """Smooth (damped) fill over the gap window, single modern_windows population, metric=DAU."""
    idx = pd.date_range(gif.FORECAST_START, gif.FILL_END_DEFAULT, freq="D")
    return pd.DataFrame({
        "metric": "DAU",
        "x": idx,
        "country": "IR",
        "modern_windows": True,
        "winX": False,
        "y": [base * dow_mult[d.dayofweek] for d in idx],
    })


def test_reseasonalize_restores_amplitude_and_preserves_mean():
    real_dow = {0: 1.10, 1: 1.20, 2: 1.10, 3: 0.80, 4: 0.45, 5: 1.25, 6: 1.25}  # strong (real)
    damped_dow = {0: 1.05, 1: 1.05, 2: 1.06, 3: 0.92, 4: 0.85, 5: 1.04, 6: 1.03}  # Prophet-like
    real_df = _synthetic_real_desktop(real_dow)
    smooth = _synthetic_smooth_fill(damped_dow)

    corrected = gif.reseasonalize_source(smooth, {"DAU": real_df}, Platform.DESKTOP)

    # Mean preserved (shape changed, level unchanged).
    assert corrected["y"].mean() == pytest.approx(smooth["y"].mean(), rel=1e-6)

    # Amplitude restored toward the real profile, and clearly larger than the damped input.
    def peak_trough(df):
        s = df.groupby("x")["y"].sum()
        s.index = pd.to_datetime(s.index)
        p, _ = gif._dow_profile(s)
        return p.max() / p.min()

    real_pt = max(real_dow.values()) / min(real_dow.values())
    damped_pt = max(damped_dow.values()) / min(damped_dow.values())
    corrected_pt = peak_trough(corrected)
    assert corrected_pt > damped_pt + 0.5            # meaningfully amplified
    assert corrected_pt == pytest.approx(real_pt, rel=0.10)  # matches real swing


def test_reseasonalize_preserves_schema_and_dates():
    real_df = _synthetic_real_desktop({0: 1.1, 1: 1.2, 2: 1.1, 3: 0.8, 4: 0.45, 5: 1.25, 6: 1.25})
    smooth = _synthetic_smooth_fill({0: 1.05, 1: 1.05, 2: 1.06, 3: 0.92, 4: 0.85, 5: 1.04, 6: 1.03})
    corrected = gif.reseasonalize_source(smooth, {"DAU": real_df}, Platform.DESKTOP)
    assert list(corrected.columns) == list(smooth.columns)
    assert pd.to_datetime(corrected["x"]).min() == pd.Timestamp(gif.FORECAST_START)
    assert pd.to_datetime(corrected["x"]).max() == pd.Timestamp(gif.FILL_END_DEFAULT)
    assert (corrected["y"] >= 0).all()


# ---------------------------------------------------------------------------
# Artifact conformance (skip if not generated)
# ---------------------------------------------------------------------------

def _artifact(source):
    p = FILL_DIR / f"iran_fill.{source}.parquet"
    if not p.exists():
        pytest.skip(f"artifact not generated: {p}")
    return pd.read_parquet(p)


@pytest.mark.parametrize("source", SOURCES)
def test_artifact_invariants(source):
    d = _artifact(source)
    d["x"] = pd.to_datetime(d["x"])
    seg = [c for c in d.columns if c not in ("metric", "x", "country", "y")]
    assert (d["country"] == "IR").all()
    assert d["y"].notna().all() and (d["y"] >= 0).all()
    assert (d[seg].sum(axis=1) <= 1).all()  # single-label populations
    exp_end = {"Existing Engagement MAU": "2026-06-21"}
    for metric, g in d.groupby("metric"):
        assert g["x"].min() == pd.Timestamp("2026-02-28")
        assert g["x"].max() == pd.Timestamp(exp_end.get(metric, "2026-05-25"))


@pytest.mark.parametrize("source", ["glean_desktop", "legacy_desktop"])
def test_artifact_weekly_amplitude_restored(source):
    # Regression guard for the DOW re-seasonalization: desktop DAU must show a real-sized weekly
    # swing (peak/trough ~2.6). If reseasonalization regresses, Prophet's damped ~1.5 fails this.
    d = _artifact(source)
    dau = d[d["metric"] == "DAU"].groupby("x")["y"].sum()
    dau.index = pd.to_datetime(dau.index)
    prof, _ = gif._dow_profile(dau)
    assert prof.max() / prof.min() > 2.0


@pytest.mark.parametrize("source", SOURCES)
def test_artifact_sidecar_records_reseasonalization(source):
    meta_path = FILL_DIR / f"iran_fill.{source}.meta.json"
    if not meta_path.exists():
        pytest.skip(f"sidecar not generated: {meta_path}")
    meta = json.loads(meta_path.read_text())
    assert meta["weekly_reseasonalized"] is True
    assert meta["country"] == "IR"
