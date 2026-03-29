# -*- coding: utf-8 -*-
"""Tests for splice_iran_synthetic_data() in mozaic_daily.data.

Tests the function that replaces real Iran data from the shutdown date onward
with synthetic forecast data loaded from a parquet file.
"""

import datetime

import pandas as pd
import pytest

from mozaic_daily.data import splice_iran_synthetic_data


# ── Helpers ──────────────────────────────────────────────────────────────────

def make_desktop_df(countries, dates, value=100):
    """Create a mock desktop DataFrame matching BQ output schema.

    Uses datetime.date objects, nullable BooleanDtype, and Int64 to match
    what BigQuery actually returns.
    """
    rows = []
    for d in dates:
        date_obj = datetime.date.fromisoformat(d) if isinstance(d, str) else d
        for c in countries:
            rows.append({
                "x": date_obj, "country": c,
                "modern_windows": pd.array([True], dtype="boolean")[0],
                "winX": pd.array([False], dtype="boolean")[0],
                "y": pd.array([value], dtype="Int64")[0],
            })
            rows.append({
                "x": date_obj, "country": c,
                "modern_windows": pd.array([False], dtype="boolean")[0],
                "winX": pd.array([True], dtype="boolean")[0],
                "y": pd.array([value // 2], dtype="Int64")[0],
            })
    df = pd.DataFrame(rows)
    df["modern_windows"] = df["modern_windows"].astype("boolean")
    df["winX"] = df["winX"].astype("boolean")
    df["y"] = df["y"].astype("Int64")
    return df


def make_mobile_df(countries, dates, value=100):
    """Create a mock mobile DataFrame matching BQ output schema."""
    rows = []
    for d in dates:
        date_obj = datetime.date.fromisoformat(d) if isinstance(d, str) else d
        for c in countries:
            rows.append({
                "x": date_obj, "country": c, "y": pd.array([value], dtype="Int64")[0],
                "fenix_android": pd.array([True], dtype="boolean")[0],
                "firefox_ios": pd.array([False], dtype="boolean")[0],
                "focus_android": pd.array([False], dtype="boolean")[0],
                "focus_ios": pd.array([False], dtype="boolean")[0],
            })
            rows.append({
                "x": date_obj, "country": c, "y": pd.array([value // 3], dtype="Int64")[0],
                "fenix_android": pd.array([False], dtype="boolean")[0],
                "firefox_ios": pd.array([True], dtype="boolean")[0],
                "focus_android": pd.array([False], dtype="boolean")[0],
                "focus_ios": pd.array([False], dtype="boolean")[0],
            })
    df = pd.DataFrame(rows)
    for col in ["fenix_android", "firefox_ios", "focus_android", "focus_ios"]:
        df[col] = df[col].astype("boolean")
    df["y"] = df["y"].astype("Int64")
    return df


def make_synthetic_df(
    platform, telemetry_source, metric, dates, value=999.0, segment_type="desktop"
):
    """Create synthetic parquet rows matching the generation script's output.

    Uses Timestamp x, float64 y, and numpy bool — the raw parquet types
    before conversion.
    """
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


# ── Tests ────────────────────────────────────────────────────────────────────

class TestBasicSplice:
    def test_ir_data_before_shutdown_unchanged(self, tmp_path):
        """Iran data before the shutdown date should not be modified."""
        real_df = make_desktop_df(["IR", "US"], ALL_DATES)
        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER, value=999.0),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE)
        result_df = result["desktop"]["glean"]["DAU"]

        shutdown_obj = datetime.date.fromisoformat(SHUTDOWN_DATE)
        ir_before = result_df[
            (result_df["country"] == "IR") & (result_df["x"] < shutdown_obj)
        ]
        original_ir_before = real_df[
            (real_df["country"] == "IR") & (real_df["x"] < shutdown_obj)
        ]
        pd.testing.assert_frame_equal(
            ir_before.reset_index(drop=True),
            original_ir_before.reset_index(drop=True),
        )

    def test_ir_data_from_shutdown_replaced_with_synthetic(self, tmp_path):
        """Iran data from shutdown onward should be replaced with synthetic values."""
        real_df = make_desktop_df(["IR", "US"], ALL_DATES, value=100)
        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER, value=999.0),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE)
        result_df = result["desktop"]["glean"]["DAU"]

        shutdown_obj = datetime.date.fromisoformat(SHUTDOWN_DATE)
        ir_after = result_df[
            (result_df["country"] == "IR") & (result_df["x"] >= shutdown_obj)
        ]
        # Synthetic values are 999 and 500 (rounded from 999.0 and 499.5, cast to Int64)
        assert (ir_after["y"].isin([999, 500])).all()

    def test_non_ir_data_unchanged(self, tmp_path):
        """Non-Iran country data should be completely unchanged."""
        real_df = make_desktop_df(["IR", "US"], ALL_DATES, value=100)
        us_original = real_df[real_df["country"] == "US"].copy().reset_index(drop=True)

        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER, value=999.0),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE)
        result_df = result["desktop"]["glean"]["DAU"]
        us_result = result_df[result_df["country"] == "US"].reset_index(drop=True)

        pd.testing.assert_frame_equal(us_result, us_original)


class TestDateRange:
    def test_synthetic_extends_to_max_date_in_real_data(self, tmp_path):
        """Synthetic IR data should extend through the max date in real data,
        not just training_end_date."""
        # Real data goes through 2026-03-01 (beyond any training_end)
        real_df = make_desktop_df(["IR", "US"], ALL_DATES, value=100)

        # Synthetic covers all DATES_AFTER
        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER, value=999.0),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE)
        result_df = result["desktop"]["glean"]["DAU"]

        # IR should have synthetic data through 2026-03-01 (the max real date)
        ir_dates = sorted(
            result_df[result_df["country"] == "IR"]["x"].unique()
        )
        us_dates = sorted(
            result_df[result_df["country"] == "US"]["x"].unique()
        )
        assert ir_dates == us_dates, "IR and US should have identical date sets"

    def test_synthetic_not_extended_beyond_real_data(self, tmp_path):
        """Synthetic data should not introduce dates beyond what real data has."""
        # Real data only goes through 2026-02-28
        short_dates = ["2026-02-25", "2026-02-26", "2026-02-27", "2026-02-28"]
        real_df = make_desktop_df(["IR", "US"], short_dates, value=100)

        # Synthetic has data through 2026-03-01
        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER, value=999.0),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE)
        result_df = result["desktop"]["glean"]["DAU"]

        max_date = result_df["x"].max()
        assert max_date == datetime.date(2026, 2, 28)


class TestTypeConversion:
    def test_synthetic_x_converted_to_date(self, tmp_path):
        """Synthetic x column should be datetime.date, not Timestamp."""
        real_df = make_desktop_df(["IR"], ALL_DATES)
        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE)
        result_df = result["desktop"]["glean"]["DAU"]

        # All x values should be datetime.date
        for val in result_df["x"]:
            assert type(val) is datetime.date, f"Expected datetime.date, got {type(val)}"

    def test_synthetic_y_converted_to_int64(self, tmp_path):
        """Synthetic y should be converted from float64 to Int64."""
        real_df = make_desktop_df(["IR"], ALL_DATES)
        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE)
        result_df = result["desktop"]["glean"]["DAU"]

        assert result_df["y"].dtype == "Int64"

    def test_synthetic_bools_converted_to_nullable(self, tmp_path):
        """Synthetic boolean columns should be nullable BooleanDtype."""
        real_df = make_desktop_df(["IR"], ALL_DATES)
        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE)
        result_df = result["desktop"]["glean"]["DAU"]

        assert result_df["modern_windows"].dtype == "boolean"
        assert result_df["winX"].dtype == "boolean"


class TestMissingParquet:
    def test_raises_filenotfounderror_with_message(self):
        """Missing parquet file should raise FileNotFoundError with helpful message."""
        datasets = {"desktop": {"glean": {"DAU": pd.DataFrame()}}, "mobile": {"glean": {}}}

        with pytest.raises(FileNotFoundError, match="Run scripts/generate_iran_synthetic.py first"):
            splice_iran_synthetic_data(
                datasets, "/nonexistent/path.parquet", SHUTDOWN_DATE
            )


class TestColumnSchemaPreserved:
    def test_output_columns_match_input(self, tmp_path):
        """Output DataFrames should have the same columns as input."""
        real_df = make_desktop_df(["IR", "US"], ALL_DATES)
        original_cols = set(real_df.columns)

        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER),
        ])
        datasets = {"desktop": {"glean": {"DAU": real_df}}, "mobile": {"glean": {}}}

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE)
        result_df = result["desktop"]["glean"]["DAU"]

        assert set(result_df.columns) == original_cols


class TestMultiSourceFiltering:
    def test_desktop_gets_desktop_synthetic_only(self, tmp_path):
        """Desktop data should only receive desktop synthetic rows."""
        desktop_df = make_desktop_df(["IR"], ALL_DATES, value=100)
        mobile_df = make_mobile_df(["IR"], ALL_DATES, value=200)

        synthetic_path = write_synthetic_parquet(tmp_path, [
            make_synthetic_df("desktop", "glean", "DAU", DATES_AFTER, value=999.0, segment_type="desktop"),
            make_synthetic_df("mobile", "glean", "DAU", DATES_AFTER, value=777.0, segment_type="mobile"),
        ])
        datasets = {
            "desktop": {"glean": {"DAU": desktop_df}},
            "mobile": {"glean": {"DAU": mobile_df}},
        }

        result = splice_iran_synthetic_data(datasets, synthetic_path, SHUTDOWN_DATE)

        shutdown_obj = datetime.date.fromisoformat(SHUTDOWN_DATE)

        # Desktop should have 999 and 500 (rounded from 999.0 and 499.5)
        desktop_result = result["desktop"]["glean"]["DAU"]
        ir_desktop_after = desktop_result[
            (desktop_result["country"] == "IR")
            & (desktop_result["x"] >= shutdown_obj)
        ]
        assert (ir_desktop_after["y"].isin([999, 500])).all()

        # Mobile should have 777 and 233 (rounded from 777.0 and 233.1)
        mobile_result = result["mobile"]["glean"]["DAU"]
        ir_mobile_after = mobile_result[
            (mobile_result["country"] == "IR")
            & (mobile_result["x"] >= shutdown_obj)
        ]
        assert (ir_mobile_after["y"].isin([777, 233])).all()
