# -*- coding: utf-8 -*-
"""Tests for splice_iran_synthetic_data() in mozaic_daily.data.

Tests the function that replaces real Iran data from the shutdown date onward
with synthetic forecast data loaded from a parquet file.
"""

import pandas as pd
import pytest

from mozaic_daily.data import splice_iran_synthetic_data


# ── Helpers ──────────────────────────────────────────────────────────────────

def make_desktop_df(countries, dates, value=100.0):
    """Create a mock desktop DataFrame matching BQ output schema."""
    rows = []
    for d in dates:
        for c in countries:
            rows.append({"x": pd.Timestamp(d), "country": c, "modern_windows": True, "winX": False, "y": value})
            rows.append({"x": pd.Timestamp(d), "country": c, "modern_windows": False, "winX": True, "y": value * 0.5})
    return pd.DataFrame(rows)


def make_mobile_df(countries, dates, value=100.0):
    """Create a mock mobile DataFrame matching BQ output schema."""
    rows = []
    for d in dates:
        for c in countries:
            rows.append({
                "x": pd.Timestamp(d), "country": c, "y": value,
                "fenix_android": True, "firefox_ios": False,
                "focus_android": False, "focus_ios": False,
            })
            rows.append({
                "x": pd.Timestamp(d), "country": c, "y": value * 0.3,
                "fenix_android": False, "firefox_ios": True,
                "focus_android": False, "focus_ios": False,
            })
    return pd.DataFrame(rows)


def make_synthetic_df(
    platform, telemetry_source, metric, dates, value=999.0, segment_type="desktop"
):
    """Create synthetic parquet rows matching Branch 1's output schema."""
    rows = []
    for d in dates:
        if segment_type == "desktop":
            rows.append({
                "platform": platform, "telemetry_source": telemetry_source,
                "metric": metric, "x": pd.Timestamp(d), "country": "IR", "y": value,
                "modern_windows": True, "winX": False,
                "fenix_android": False, "firefox_ios": False,
                "focus_android": False, "focus_ios": False,
            })
            rows.append({
                "platform": platform, "telemetry_source": telemetry_source,
                "metric": metric, "x": pd.Timestamp(d), "country": "IR", "y": value * 0.5,
                "modern_windows": False, "winX": True,
                "fenix_android": False, "firefox_ios": False,
                "focus_android": False, "focus_ios": False,
            })
        else:  # mobile
            rows.append({
                "platform": platform, "telemetry_source": telemetry_source,
                "metric": metric, "x": pd.Timestamp(d), "country": "IR", "y": value,
                "modern_windows": False, "winX": False,
                "fenix_android": True, "firefox_ios": False,
                "focus_android": False, "focus_ios": False,
            })
            rows.append({
                "platform": platform, "telemetry_source": telemetry_source,
                "metric": metric, "x": pd.Timestamp(d), "country": "IR", "y": value * 0.3,
                "modern_windows": False, "winX": False,
                "fenix_android": False, "firefox_ios": True,
                "focus_android": False, "focus_ios": False,
            })
    return pd.DataFrame(rows)


def write_synthetic_parquet(tmp_path, synthetic_dfs):
    """Write a combined synthetic parquet file and return its path."""
    combined = pd.concat(synthetic_dfs, ignore_index=True)
    path = tmp_path / "iran_synthetic.parquet"
    combined.to_parquet(path, index=False)
    return str(path)


# ── Test fixtures ────────────────────────────────────────────────────────────

SHUTDOWN_DATE = "2026-02-27"
DATES_BEFORE = ["2026-02-25", "2026-02-26"]
DATES_AFTER = ["2026-02-27", "2026-02-28", "2026-03-01"]
ALL_DATES = DATES_BEFORE + DATES_AFTER
TRAINING_END = "2026-03-15"


# ── Tests ────────────────────────────────────────────────────────────────────

class TestBasicSplice:
    def test_ir_data_before_shutdown_unchanged(self, tmp_path):
        """Iran data before the shutdown date should not be modified."""
        real_df = make_desktop_df(["IR", "US"], ALL_DATES)
        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER, value=999.0),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE, TRAINING_END)
        result_df = result["desktop"]["glean"]["DAU"]

        ir_before = result_df[
            (result_df["country"] == "IR") & (result_df["x"] < pd.Timestamp(SHUTDOWN_DATE))
        ]
        original_ir_before = real_df[
            (real_df["country"] == "IR") & (real_df["x"] < pd.Timestamp(SHUTDOWN_DATE))
        ]
        pd.testing.assert_frame_equal(
            ir_before.reset_index(drop=True),
            original_ir_before.reset_index(drop=True),
        )

    def test_ir_data_from_shutdown_replaced_with_synthetic(self, tmp_path):
        """Iran data from shutdown onward should be replaced with synthetic values."""
        real_df = make_desktop_df(["IR", "US"], ALL_DATES, value=100.0)
        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER, value=999.0),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE, TRAINING_END)
        result_df = result["desktop"]["glean"]["DAU"]

        ir_after = result_df[
            (result_df["country"] == "IR") & (result_df["x"] >= pd.Timestamp(SHUTDOWN_DATE))
        ]
        # Synthetic values are 999.0 and 499.5, not 100.0 and 50.0
        assert (ir_after["y"].isin([999.0, 499.5])).all()

    def test_non_ir_data_unchanged(self, tmp_path):
        """Non-Iran country data should be completely unchanged."""
        real_df = make_desktop_df(["IR", "US"], ALL_DATES, value=100.0)
        us_original = real_df[real_df["country"] == "US"].copy().reset_index(drop=True)

        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER, value=999.0),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE, TRAINING_END)
        result_df = result["desktop"]["glean"]["DAU"]
        us_result = result_df[result_df["country"] == "US"].reset_index(drop=True)

        pd.testing.assert_frame_equal(us_result, us_original)


class TestDateTruncation:
    def test_synthetic_data_truncated_to_training_end(self, tmp_path):
        """Synthetic data beyond training_end_date should not be included."""
        early_training_end = "2026-02-28"
        real_df = make_desktop_df(["IR"], ALL_DATES, value=100.0)
        # Synthetic has data through 2026-03-01 but training_end is 2026-02-28
        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER, value=999.0),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(
            datasets, synthetic_path, SHUTDOWN_DATE, early_training_end
        )
        result_df = result["desktop"]["glean"]["DAU"]

        # Should have synthetic data for 2/27 and 2/28 but NOT 3/1
        ir_synthetic = result_df[
            (result_df["country"] == "IR") & (result_df["y"].isin([999.0, 499.5]))
        ]
        synthetic_dates = ir_synthetic["x"].dt.strftime("%Y-%m-%d").unique()
        assert "2026-02-27" in synthetic_dates
        assert "2026-02-28" in synthetic_dates
        assert "2026-03-01" not in synthetic_dates


class TestMissingParquet:
    def test_raises_filenotfounderror_with_message(self):
        """Missing parquet file should raise FileNotFoundError with helpful message."""
        datasets = {"desktop": {"glean": {"DAU": pd.DataFrame()}}, "mobile": {"glean": {}}}

        with pytest.raises(FileNotFoundError, match="Run scripts/generate_iran_synthetic.py first"):
            splice_iran_synthetic_data(
                datasets, "/nonexistent/path.parquet", SHUTDOWN_DATE, TRAINING_END
            )


class TestColumnSchemaPreserved:
    def test_output_columns_match_input(self, tmp_path):
        """Output DataFrames should have the same columns as input."""
        real_df = make_desktop_df(["IR", "US"], ALL_DATES)
        original_cols = set(real_df.columns)
        original_dtypes = real_df.dtypes.to_dict()

        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE, TRAINING_END)
        result_df = result["desktop"]["glean"]["DAU"]

        assert set(result_df.columns) == original_cols


class TestMultiSourceFiltering:
    def test_desktop_gets_desktop_synthetic_only(self, tmp_path):
        """Desktop data should only receive desktop synthetic rows."""
        desktop_df = make_desktop_df(["IR"], ALL_DATES, value=100.0)
        mobile_df = make_mobile_df(["IR"], ALL_DATES, value=200.0)

        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER, value=999.0, segment_type="desktop"),
            make_synthetic_df("mobile", "glean", "DAU", DATES_AFTER, value=777.0, segment_type="mobile"),
        ])
        datasets = {
            "desktop": {"glean": {"DAU": desktop_df}},
            "mobile": {"glean": {"DAU": mobile_df}},
        }

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE, TRAINING_END)

        # Desktop should have 999.0 synthetic values, not 777.0
        desktop_result = result["desktop"]["glean"]["DAU"]
        ir_desktop_after = desktop_result[
            (desktop_result["country"] == "IR")
            & (desktop_result["x"] >= pd.Timestamp(SHUTDOWN_DATE))
        ]
        assert (ir_desktop_after["y"].isin([999.0, 499.5])).all()

        # Mobile should have 777.0 synthetic values, not 999.0
        mobile_result = result["mobile"]["glean"]["DAU"]
        ir_mobile_after = mobile_result[
            (mobile_result["country"] == "IR")
            & (mobile_result["x"] >= pd.Timestamp(SHUTDOWN_DATE))
        ]
        assert (ir_mobile_after["y"].isin([777.0, 233.1])).all()
