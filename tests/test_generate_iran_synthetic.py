# -*- coding: utf-8 -*-
"""Tests for the Iran synthetic data conversion pipeline.

Tests the ``convert_forecast_to_bq_format()`` function from
``scripts/generate_iran_synthetic.py`` which transforms mozaic forecast output
into BigQuery-compatible DataFrames with boolean segment columns.

No BigQuery calls — all data is mocked.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Make the script importable
repo_root = Path(__file__).parent.parent
scripts_path = repo_root / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))

from generate_iran_synthetic import convert_forecast_to_bq_format
from mozaic_daily.iran_utils import DESKTOP_SEGMENT_COLUMNS, MOBILE_SEGMENT_COLUMNS


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def desktop_forecast_df():
    """Minimal mozaic forecast output for desktop (to_granular_forecast_df format)."""
    return pd.DataFrame(
        [
            # Training row (should be filtered out)
            {"target_date": pd.Timestamp("2026-02-26"), "country": "IR", "population": "modern_windows", "source": "actual", "value": 50000.0},
            # Forecast rows (should be kept)
            {"target_date": pd.Timestamp("2026-02-27"), "country": "IR", "population": "modern_windows", "source": "forecast", "value": 48000.0},
            {"target_date": pd.Timestamp("2026-02-27"), "country": "IR", "population": "winX", "source": "forecast", "value": 12000.0},
            {"target_date": pd.Timestamp("2026-02-27"), "country": "IR", "population": "other", "source": "forecast", "value": 3000.0},
            # ALL rows (should be filtered out)
            {"target_date": pd.Timestamp("2026-02-27"), "country": "IR", "population": "ALL", "source": "forecast", "value": 63000.0},
            {"target_date": pd.Timestamp("2026-02-27"), "country": "ALL", "population": "ALL", "source": "forecast", "value": 999999.0},
        ]
    )


@pytest.fixture
def mobile_forecast_df():
    """Minimal mozaic forecast output for mobile."""
    return pd.DataFrame(
        [
            {"target_date": pd.Timestamp("2026-02-27"), "country": "IR", "population": "fenix_android", "source": "forecast", "value": 20000.0},
            {"target_date": pd.Timestamp("2026-02-27"), "country": "IR", "population": "firefox_ios", "source": "forecast", "value": 5000.0},
            {"target_date": pd.Timestamp("2026-02-27"), "country": "IR", "population": "focus_android", "source": "forecast", "value": 1500.0},
            {"target_date": pd.Timestamp("2026-02-27"), "country": "IR", "population": "focus_ios", "source": "forecast", "value": 500.0},
            {"target_date": pd.Timestamp("2026-02-27"), "country": "IR", "population": "other", "source": "forecast", "value": 200.0},
            # ALL rows
            {"target_date": pd.Timestamp("2026-02-27"), "country": "IR", "population": "ALL", "source": "forecast", "value": 27200.0},
        ]
    )


# ── Desktop conversion tests ────────────────────────────────────────────────

class TestDesktopConversion:
    def test_output_has_correct_columns(self, desktop_forecast_df):
        result = convert_forecast_to_bq_format(
            desktop_forecast_df, "desktop", "glean", "DAU", DESKTOP_SEGMENT_COLUMNS
        )
        expected_cols = {"platform", "telemetry_source", "metric", "x", "country", "y", "modern_windows", "winX"}
        assert set(result.columns) == expected_cols

    def test_filters_to_forecast_only(self, desktop_forecast_df):
        result = convert_forecast_to_bq_format(
            desktop_forecast_df, "desktop", "glean", "DAU", DESKTOP_SEGMENT_COLUMNS
        )
        # Should have 3 rows: modern_windows, winX, other (all forecast, all IR, non-ALL)
        assert len(result) == 3

    def test_no_source_or_population_columns(self, desktop_forecast_df):
        result = convert_forecast_to_bq_format(
            desktop_forecast_df, "desktop", "glean", "DAU", DESKTOP_SEGMENT_COLUMNS
        )
        assert "source" not in result.columns
        assert "population" not in result.columns

    def test_renames_columns_correctly(self, desktop_forecast_df):
        result = convert_forecast_to_bq_format(
            desktop_forecast_df, "desktop", "glean", "DAU", DESKTOP_SEGMENT_COLUMNS
        )
        assert "x" in result.columns
        assert "y" in result.columns
        assert "target_date" not in result.columns
        assert "value" not in result.columns

    def test_boolean_columns_are_correct(self, desktop_forecast_df):
        result = convert_forecast_to_bq_format(
            desktop_forecast_df, "desktop", "glean", "DAU", DESKTOP_SEGMENT_COLUMNS
        )
        modern_row = result[result["y"] == 48000.0].iloc[0]
        assert modern_row["modern_windows"] == True
        assert modern_row["winX"] == False

        winx_row = result[result["y"] == 12000.0].iloc[0]
        assert winx_row["modern_windows"] == False
        assert winx_row["winX"] == True

        other_row = result[result["y"] == 3000.0].iloc[0]
        assert other_row["modern_windows"] == False
        assert other_row["winX"] == False

    def test_excludes_all_country(self, desktop_forecast_df):
        result = convert_forecast_to_bq_format(
            desktop_forecast_df, "desktop", "glean", "DAU", DESKTOP_SEGMENT_COLUMNS
        )
        assert (result["country"] == "IR").all()


# ── Mobile conversion tests ──────────────────────────────────────────────────

class TestMobileConversion:
    def test_output_has_correct_columns(self, mobile_forecast_df):
        result = convert_forecast_to_bq_format(
            mobile_forecast_df, "mobile", "glean", "DAU", MOBILE_SEGMENT_COLUMNS
        )
        expected_cols = {
            "platform", "telemetry_source", "metric", "x", "country", "y",
            "fenix_android", "firefox_ios", "focus_android", "focus_ios",
        }
        assert set(result.columns) == expected_cols

    def test_filters_out_all_population(self, mobile_forecast_df):
        result = convert_forecast_to_bq_format(
            mobile_forecast_df, "mobile", "glean", "DAU", MOBILE_SEGMENT_COLUMNS
        )
        # 5 rows: fenix, firefox_ios, focus_android, focus_ios, other
        assert len(result) == 5

    def test_fenix_boolean_correct(self, mobile_forecast_df):
        result = convert_forecast_to_bq_format(
            mobile_forecast_df, "mobile", "glean", "DAU", MOBILE_SEGMENT_COLUMNS
        )
        fenix_row = result[result["y"] == 20000.0].iloc[0]
        assert fenix_row["fenix_android"] == True
        assert fenix_row["firefox_ios"] == False
        assert fenix_row["focus_android"] == False
        assert fenix_row["focus_ios"] == False


# ── Identifier columns ───────────────────────────────────────────────────────

class TestIdentifierColumns:
    def test_platform_set_correctly(self, desktop_forecast_df):
        result = convert_forecast_to_bq_format(
            desktop_forecast_df, "desktop", "legacy", "New Profiles", DESKTOP_SEGMENT_COLUMNS
        )
        assert (result["platform"] == "desktop").all()
        assert (result["telemetry_source"] == "legacy").all()
        assert (result["metric"] == "New Profiles").all()


# ── Edge cases ────────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_empty_input_returns_empty(self):
        empty_df = pd.DataFrame(columns=["target_date", "country", "population", "source", "value"])
        result = convert_forecast_to_bq_format(
            empty_df, "desktop", "glean", "DAU", DESKTOP_SEGMENT_COLUMNS
        )
        assert result.empty

    def test_no_forecast_rows_returns_empty(self):
        actual_only = pd.DataFrame(
            [{"target_date": pd.Timestamp("2026-02-26"), "country": "IR", "population": "modern_windows", "source": "actual", "value": 50000.0}]
        )
        result = convert_forecast_to_bq_format(
            actual_only, "desktop", "glean", "DAU", DESKTOP_SEGMENT_COLUMNS
        )
        assert result.empty
