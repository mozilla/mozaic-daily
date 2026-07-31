"""Tests for scripts/mobile_app_breakdown.py.

Two layers:
1. Synthetic-fixture unit tests of the real transforms (flag -> app_name,
   share arithmetic, trailing-window mean, ALL MOBILE residual) against
   hand-computed literals — CI-safe, no large parquets needed.
2. A guarded regression check that the on-disk August mobile build still splits
   four ways with Fenix as the clear majority (skipped when the parquet is
   absent, e.g. on a fresh clone where it lives in GCS).
"""

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "mobile_app_breakdown", REPO_ROOT / "scripts" / "mobile_app_breakdown.py"
)
mobile_app_breakdown = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mobile_app_breakdown)

tidy_raw_parts = mobile_app_breakdown.tidy_raw_parts
tidy_forecast = mobile_app_breakdown.tidy_forecast
app_shares = mobile_app_breakdown.app_shares
all_mobile_residual = mobile_app_breakdown.all_mobile_residual
MOBILE_APPS = mobile_app_breakdown.MOBILE_APPS
ALL_MOBILE_LABEL = mobile_app_breakdown.ALL_MOBILE_LABEL


def _raw_row(date: str, country: str, app: str, dau: int) -> dict:
    row = {"x": date, "country": country, "y": dau}
    row.update({flag: (flag == app) for flag in MOBILE_APPS})
    return row


# --------------------------------------------------------------------------
# tidy_raw_parts
# --------------------------------------------------------------------------
def test_tidy_raw_parts_maps_each_flag_to_its_app():
    raw = pd.DataFrame(
        [
            _raw_row("2026-07-27", "US", "fenix_android", 100),
            _raw_row("2026-07-27", "US", "firefox_ios", 40),
            _raw_row("2026-07-27", "DE", "focus_android", 7),
            _raw_row("2026-07-27", "DE", "focus_ios", 3),
        ]
    )
    tidy = tidy_raw_parts(raw)
    assert sorted(tidy["app_name"]) == [
        "fenix_android", "firefox_ios", "focus_android", "focus_ios"
    ]
    fenix = tidy[tidy["app_name"] == "fenix_android"]
    assert fenix["dau"].item() == 100.0
    assert fenix["country"].item() == "US"


def test_tidy_raw_parts_rejects_row_with_two_apps_set():
    """A row matching two app patterns would be double-counted in every total."""
    raw = pd.DataFrame([_raw_row("2026-07-27", "US", "fenix_android", 100)])
    raw.loc[0, "firefox_ios"] = True
    with pytest.raises(ValueError, match="exactly one app flag"):
        tidy_raw_parts(raw)


def test_tidy_raw_parts_rejects_row_with_no_app_set():
    """A new upstream app_name matches no pattern and would silently vanish."""
    raw = pd.DataFrame([_raw_row("2026-07-27", "US", "fenix_android", 100)])
    raw.loc[0, "fenix_android"] = False
    with pytest.raises(ValueError, match="exactly one app flag"):
        tidy_raw_parts(raw)


def test_tidy_raw_parts_rejects_missing_flag_column():
    raw = pd.DataFrame([{"x": "2026-07-27", "country": "US", "y": 100,
                         "fenix_android": True}])
    with pytest.raises(ValueError, match="missing app flag columns"):
        tidy_raw_parts(raw)


# --------------------------------------------------------------------------
# app_shares
# --------------------------------------------------------------------------
def test_app_shares_sums_across_countries_at_one_date():
    """750/200/30/20 across two countries -> 75/20/3/2 percent."""
    raw = pd.DataFrame(
        [
            _raw_row("2026-07-27", "US", "fenix_android", 500),
            _raw_row("2026-07-27", "DE", "fenix_android", 250),
            _raw_row("2026-07-27", "US", "firefox_ios", 200),
            _raw_row("2026-07-27", "US", "focus_android", 30),
            _raw_row("2026-07-27", "US", "focus_ios", 20),
            # A different date that must not leak into the 07-27 split.
            _raw_row("2026-07-26", "US", "fenix_android", 999_999),
        ]
    )
    shares = app_shares(tidy_raw_parts(raw), [pd.Timestamp("2026-07-27")])

    assert list(shares.index) == [
        "fenix_android", "firefox_ios", "focus_android", "focus_ios"
    ]
    assert shares.loc["fenix_android", "dau"] == 750.0
    assert shares.loc["fenix_android", "share_pct"] == pytest.approx(75.0)
    assert shares.loc["firefox_ios", "share_pct"] == pytest.approx(20.0)
    assert shares.loc["focus_android", "share_pct"] == pytest.approx(3.0)
    assert shares.loc["focus_ios", "share_pct"] == pytest.approx(2.0)
    assert shares["share_pct"].sum() == pytest.approx(100.0)


def test_app_shares_window_is_a_daily_mean_not_a_sum():
    """Two days of 100 and 300 Fenix -> a 200 daily mean, not a 400 total."""
    raw = pd.DataFrame(
        [
            _raw_row("2026-07-26", "US", "fenix_android", 100),
            _raw_row("2026-07-27", "US", "fenix_android", 300),
            _raw_row("2026-07-26", "US", "firefox_ios", 100),
            _raw_row("2026-07-27", "US", "firefox_ios", 100),
        ]
    )
    dates = pd.date_range(end="2026-07-27", periods=2)
    shares = app_shares(tidy_raw_parts(raw), dates)

    assert shares.loc["fenix_android", "dau"] == pytest.approx(200.0)
    assert shares.loc["firefox_ios", "dau"] == pytest.approx(100.0)
    assert shares.loc["fenix_android", "share_pct"] == pytest.approx(200 / 300 * 100)


def test_app_shares_excludes_the_all_mobile_aggregate_row():
    """Leaving ALL MOBILE in would halve every share and sum to 100 anyway."""
    forecast = pd.DataFrame(
        [
            {"target_date": "2026-07-27", "country": "ALL", "app_name": ALL_MOBILE_LABEL,
             "dau": 1000.0, "data_type": "training"},
            {"target_date": "2026-07-27", "country": "ALL", "app_name": "fenix_android",
             "dau": 800.0, "data_type": "training"},
            {"target_date": "2026-07-27", "country": "ALL", "app_name": "firefox_ios",
             "dau": 200.0, "data_type": "training"},
        ]
    )
    shares = app_shares(tidy_forecast(forecast), [pd.Timestamp("2026-07-27")])

    assert ALL_MOBILE_LABEL not in shares.index
    assert shares["dau"].sum() == pytest.approx(1000.0)
    assert shares.loc["fenix_android", "share_pct"] == pytest.approx(80.0)


def test_app_shares_raises_on_a_date_with_no_rows():
    raw = pd.DataFrame([_raw_row("2026-07-27", "US", "fenix_android", 100)])
    with pytest.raises(ValueError, match="No app rows found"):
        app_shares(tidy_raw_parts(raw), [pd.Timestamp("2020-01-01")])


# --------------------------------------------------------------------------
# all_mobile_residual
# --------------------------------------------------------------------------
def _forecast_frame(all_mobile_dau: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"target_date": "2026-07-27", "country": "ALL", "app_name": ALL_MOBILE_LABEL,
             "dau": all_mobile_dau, "data_type": "training"},
            {"target_date": "2026-07-27", "country": "ALL", "app_name": "fenix_android",
             "dau": 800.0, "data_type": "training"},
            {"target_date": "2026-07-27", "country": "ALL", "app_name": "firefox_ios",
             "dau": 200.0, "data_type": "training"},
        ]
    )


def test_all_mobile_residual_is_zero_when_parts_sum_to_the_aggregate():
    tidy = tidy_forecast(_forecast_frame(1000.0))
    assert all_mobile_residual(tidy, pd.Timestamp("2026-07-27")) == pytest.approx(0.0)


def test_all_mobile_residual_surfaces_a_dropped_app():
    """Aggregate 1250 vs parts 1000 — the size of a missing third app."""
    tidy = tidy_forecast(_forecast_frame(1250.0))
    assert all_mobile_residual(tidy, pd.Timestamp("2026-07-27")) == pytest.approx(250.0)


def test_all_mobile_residual_is_none_without_an_aggregate_row():
    """The raw pull has no ALL MOBILE row; that is not a mismatch."""
    raw = pd.DataFrame([_raw_row("2026-07-27", "US", "fenix_android", 100)])
    assert all_mobile_residual(tidy_raw_parts(raw), pd.Timestamp("2026-07-27")) is None


# --------------------------------------------------------------------------
# Guarded regression against the on-disk August build
# --------------------------------------------------------------------------
@pytest.mark.skipif(
    not mobile_app_breakdown.DEFAULT_RAW.exists(),
    reason="August raw mobile pull not on disk (gitignored, lives in GCS)",
)
def test_august_build_splits_four_ways_with_fenix_majority():
    raw = pd.read_parquet(mobile_app_breakdown.DEFAULT_RAW)
    tidy = tidy_raw_parts(raw)
    shares = app_shares(tidy, [tidy["date"].max()])

    assert set(shares.index) == set(MOBILE_APPS), "mobile universe changed size"
    assert shares.index[0] == "fenix_android"
    assert 70.0 < shares.loc["fenix_android", "share_pct"] < 80.0
    assert 18.0 < shares.loc["firefox_ios", "share_pct"] < 25.0
    # Focus is small but must not be zero — a broken LIKE pattern would drop it.
    focus = shares.loc[["focus_android", "focus_ios"], "share_pct"].sum()
    assert 1.0 < focus < 6.0
