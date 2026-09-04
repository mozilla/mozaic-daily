"""Tests for scripts/mobile_scoring.py.

Two layers, mirroring tests/test_score_near_horizon.py:

1. Synthetic-fixture unit tests exercising the real math (mobile headwind ramp, row
   selection, 28d-MA extraction, seam step and slope kink, YoY) against hand-computed
   values. CI-safe, no large parquets.
2. A guarded regression cross-check that the scorer reproduces the canonical August
   mobile build's published Dec-15. Skipped when the parquet is absent (it is gitignored
   and GCS-bound).

The regressions worth catching here, and the test that catches each:
- reading ``desktop_dau`` instead of ``mobile_dau`` from the headwind spec, which would
  apply -1,220,000 to a 17M series  -> test_headwind_ramp_reads_mobile_key
- copying desktop's ``'{"os": "ALL"}'`` segment selector, which silently matches zero
  mobile rows                       -> test_mobile_daily_series_rejects_desktop_selector
- computing the seam step against the forecast MA on both sides, hiding the handoff
                                     -> test_seam_step_measures_actuals_against_forecast
- letting the ramp keep growing past the Dec-15 anchor
                                     -> test_headwind_ramp_clamps_after_anchor
"""

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "mobile_scoring", REPO_ROOT / "scripts" / "mobile_scoring.py"
)
mobile_scoring = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mobile_scoring)

# A SYNTHETIC fixture in the shape of August's real spec -- deliberately not wired to the live
# spec on disk, so editing a cycle's headwind never breaks these unit tests. The only property
# that matters here is that desktop_dau and mobile_dau differ by ~45x, which is what makes reading
# the wrong key catastrophic rather than merely wrong. The live spec on disk is exercised only
# indirectly, by the guarded `test_reproduces_published_canonical_dec15` below, which calls
# `score_forecast` and so reads `DEFAULT_HEADWIND` from disk.
HEADWIND_SPEC = {
    "type": "linear_ramp",
    "start_date": "2026-07-28",
    "anchor_date": "2026-12-15",
    "desktop_dau": -1_295_000,
    "mobile_dau": -27_162,
}

FORECAST_START = pd.Timestamp("2026-07-28")


def _make_mobile_df(values_by_date: "pd.Series") -> pd.DataFrame:
    """Build a mobile-shaped forecast frame from a date-indexed daily DAU series."""
    return pd.DataFrame({
        "country": "ALL",
        "segment": mobile_scoring.MOBILE_SEGMENT,
        "data_source": "glean_mobile",
        "app_name": mobile_scoring.MOBILE_APP,
        "target_date": values_by_date.index.strftime("%Y-%m-%d"),
        "dau": values_by_date.to_numpy(dtype=float),
        "data_type": ["training" if d < FORECAST_START else "forecast"
                      for d in values_by_date.index],
        "forecast_start_date": FORECAST_START.strftime("%Y-%m-%d"),
    })


def _flat_series(train_value: float, forecast_value: float) -> "pd.Series":
    """Daily series: constant through the seam, then a different constant."""
    dates = pd.date_range("2025-01-01", "2026-12-31", freq="D")
    values = [train_value if d < FORECAST_START else forecast_value for d in dates]
    return pd.Series(values, index=dates, dtype=float)


# --------------------------------------------------------------------------------------
# Headwind ramp
# --------------------------------------------------------------------------------------

def test_headwind_ramp_endpoints_and_midpoint():
    ramp = mobile_scoring.headwind_ramp
    assert ramp(pd.Timestamp("2026-07-28"), HEADWIND_SPEC) == 0.0
    assert ramp(pd.Timestamp("2026-12-15"), HEADWIND_SPEC) == -27_162
    # 2026-10-06 is 70 days into a 140-day ramp -> exactly half.
    assert ramp(pd.Timestamp("2026-10-06"), HEADWIND_SPEC) == pytest.approx(-13_581)


def test_headwind_ramp_reads_mobile_key():
    """Must use mobile_dau, never desktop_dau. The two differ by ~45x in the real spec."""
    value = mobile_scoring.headwind_ramp(pd.Timestamp("2026-12-15"), HEADWIND_SPEC)
    assert value == -27_162
    assert value != HEADWIND_SPEC["desktop_dau"]


def test_headwind_ramp_clamps_after_anchor():
    """Past the Dec-15 anchor the ramp holds; an unclamped elapsed/total keeps growing."""
    after = mobile_scoring.headwind_ramp(pd.Timestamp("2026-12-31"), HEADWIND_SPEC)
    assert after == -27_162


def test_headwind_ramp_is_zero_without_spec():
    assert mobile_scoring.headwind_ramp(pd.Timestamp("2026-12-15"), {}) == 0.0


# --------------------------------------------------------------------------------------
# Row selection
# --------------------------------------------------------------------------------------

def test_mobile_daily_series_rejects_desktop_selector():
    """A desktop-style segment must fail loudly, not return an empty frame."""
    df = _make_mobile_df(_flat_series(17_000_000, 17_000_000))
    df["segment"] = '{"os": "ALL"}'
    with pytest.raises(ValueError, match="No ALL-MOBILE world rows matched"):
        mobile_scoring.mobile_daily_series(df)


def test_mobile_daily_series_excludes_per_country_rows():
    """Only country='ALL' is the world headline; per-country rows must not be summed in."""
    base = _flat_series(17_000_000, 17_000_000)
    df = _make_mobile_df(base)
    extra = _make_mobile_df(base.copy())
    extra["country"] = "US"
    combined = pd.concat([df, extra], ignore_index=True)

    selected = mobile_scoring.mobile_daily_series(combined)
    assert len(selected) == len(base)
    assert selected["dau"].max() == pytest.approx(17_000_000)


# --------------------------------------------------------------------------------------
# Scoring math
# --------------------------------------------------------------------------------------

def test_score_dataframe_flat_series_hits_known_values():
    """Flat forecast at 18,000,000 -> 28d-MA is exactly that at Dec-15 (far past the seam)."""
    df = _make_mobile_df(_flat_series(16_000_000, 18_000_000))
    result = mobile_scoring.score_dataframe(df, HEADWIND_SPEC, forecast_start=FORECAST_START)

    assert result["dec15_pre"] == pytest.approx(18_000_000)
    assert result["headwind_dec15"] == -27_162
    assert result["dec15_post"] == pytest.approx(18_000_000 - 27_162)
    assert result["gap_to_target"] == pytest.approx(
        18_000_000 - 27_162 - mobile_scoring.TARGET_DEC15)


def test_in_band_respects_the_tolerance_boundary():
    """Just inside and just outside the +-50,000 band must classify differently."""
    target = mobile_scoring.TARGET_DEC15
    tol = mobile_scoring.TOLERANCE
    # dec15_post = flat - 27,162, so solve for the flat value that lands at the edge.
    inside = _make_mobile_df(_flat_series(16_000_000, target + 27_162 + tol - 1))
    outside = _make_mobile_df(_flat_series(16_000_000, target + 27_162 + tol + 1))

    assert mobile_scoring.score_dataframe(
        inside, HEADWIND_SPEC, forecast_start=FORECAST_START)["in_band"] is True
    assert mobile_scoring.score_dataframe(
        outside, HEADWIND_SPEC, forecast_start=FORECAST_START)["in_band"] is False


def test_seam_step_measures_actuals_against_forecast():
    """A known level jump at the seam must surface as seam_step of that size.

    Both sides are flat and 28 days of each side are available, so the actuals' trailing MA
    at the last training day is exactly the training level. Computing the step from the
    forecast MA on both sides would give ~0 and fail this.
    """
    df = _make_mobile_df(_flat_series(16_000_000, 16_500_000))
    result = mobile_scoring.score_dataframe(df, HEADWIND_SPEC, forecast_start=FORECAST_START)

    assert result["seam_actual_ma"] == pytest.approx(16_000_000)
    assert result["last_training_date"] == "2026-07-27"
    # The forecast-side MA at the seam is the variance-matched display_ma, which blends the
    # two levels; the step must be positive and strictly smaller than the raw 500,000 jump.
    assert 0 < result["seam_step"] < 500_000


def test_seam_slope_kink_is_zero_for_a_flat_handoff():
    """Identical flat levels either side -> no level step and no slope kink."""
    df = _make_mobile_df(_flat_series(17_000_000, 17_000_000))
    result = mobile_scoring.score_dataframe(df, HEADWIND_SPEC, forecast_start=FORECAST_START)

    assert result["seam_step"] == pytest.approx(0.0, abs=1.0)
    assert result["seam_slope_before"] == pytest.approx(0.0, abs=1.0)
    assert result["seam_slope_after"] == pytest.approx(0.0, abs=1.0)
    assert result["seam_slope_kink"] == pytest.approx(0.0, abs=1.0)


def test_yoy_compares_against_prior_year_actuals():
    """YoY must divide by the Dec-15-2025 actuals MA, not by the seam or the training mean."""
    df = _make_mobile_df(_flat_series(16_000_000, 17_600_000))
    result = mobile_scoring.score_dataframe(df, HEADWIND_SPEC, forecast_start=FORECAST_START)

    assert result["prior_dec15_ma"] == pytest.approx(16_000_000)
    assert result["yoy_dec15_pct"] == pytest.approx(10.0)


# --------------------------------------------------------------------------------------
# Guarded regression against the canonical build
# --------------------------------------------------------------------------------------

CANONICAL_PARQUET = (
    REPO_ROOT / "data-official/2026-08/mobile_organic_2026-07-28"
    / "cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1"
    / "mozaic_daily_forecast.2026-07-28.gm-D.adj-p.parquet"
)
#: Published in data-official/2026-08/_index.md and pinned in the canonical notebook.
CANONICAL_DEC15_POST = 17_601_155


@pytest.mark.skipif(not CANONICAL_PARQUET.exists(),
                    reason="canonical mobile parquet is gitignored / GCS-bound")
def test_reproduces_published_canonical_dec15():
    """The scorer must agree with the published headline to the DAU.

    This is the check that licenses comparing every scan probe against 17,601,155. If the
    scorer's MA convention or headwind handling drifts from the canonical notebook's, the
    whole search is measured against the wrong baseline.
    """
    scores = mobile_scoring.score_forecast(CANONICAL_PARQUET)
    assert scores["dec15_post"] == pytest.approx(CANONICAL_DEC15_POST, abs=1.0)
    assert scores["gap_to_target"] == pytest.approx(
        CANONICAL_DEC15_POST - mobile_scoring.TARGET_DEC15, abs=1.0)
    assert scores["in_band"] is False
