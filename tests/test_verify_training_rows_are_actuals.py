"""Tests for scripts/verify_training_rows_are_actuals.py.

The script's whole value is that it can FAIL — it licenses substituting parquet `training` rows for a
~$5 BigQuery actuals query, so a version that always passes would silently authorise using modified
training data as if it were actuals. These tests therefore focus on the failure branches: a perturbed
value, a missing date, and an empty probe (which must not pass vacuously).

No BigQuery access and no large parquets — `compare()` is pure and is exercised directly against
hand-built series.
"""

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "verify_training_rows_are_actuals",
    REPO_ROOT / "scripts" / "verify_training_rows_are_actuals.py",
)
verify = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(verify)


def _series(values, start="2025-03-01"):
    """Build a date-indexed float DAU series."""
    index = pd.date_range(start, periods=len(values), freq="D")
    return pd.Series([float(v) for v in values], index=index)


class TestCompare:
    """compare() returns None on an exact match and a non-empty summary on any discrepancy."""

    def test_identical_series_passes(self):
        actuals = _series([40_429_966, 37_024_418, 57_214_517])
        # Separately constructed object with the same values -- not the same reference, so this is
        # not a tautological self-comparison.
        training = _series([40_429_966, 37_024_418, 57_214_517])
        assert verify.compare("desktop", training, actuals) is None

    def test_single_day_off_by_one_dau_fails(self):
        """A 1-DAU perturbation must fail: the substitution claim is exact equality, not closeness."""
        actuals = _series([40_429_966, 37_024_418, 57_214_517])
        training = _series([40_429_966, 37_024_419, 57_214_517])
        failure = verify.compare("desktop", training, actuals)
        assert failure is not None
        assert "desktop" in failure

    def test_realistic_overlay_contamination_fails(self):
        """The actual risk being guarded against: an overlay subtracted from training rows."""
        actuals = _series([47_000_000, 47_100_000, 47_200_000])
        training = actuals - 180_000  # e.g. a Launch-at-Login-style subtraction
        failure = verify.compare("mobile", training, actuals)
        assert failure is not None
        assert "180,000" in failure

    def test_date_missing_from_training_fails(self):
        """Probed days absent from the training rows are a failure, not a silent skip."""
        actuals = _series([47_000_000, 47_100_000, 47_200_000])
        training = actuals.iloc[:2]  # third probed day has no training row
        failure = verify.compare("desktop", training, actuals)
        assert failure is not None
        assert "missing" in failure

    def test_empty_probe_fails_rather_than_passing_vacuously(self):
        """An empty probe compares nothing, so it must not report success."""
        training = _series([47_000_000, 47_100_000])
        actuals = pd.Series(dtype="float64", index=pd.DatetimeIndex([]))
        failure = verify.compare("desktop", training, actuals)
        assert failure is not None
        assert "0 rows" in failure

    def test_extra_training_dates_are_ignored(self):
        """Training spans 2020-2026; only the probed days are compared."""
        actuals = _series([47_000_000, 47_100_000])
        training = _series([47_000_000, 47_100_000, 99_999_999, 88_888_888])
        assert verify.compare("desktop", training, actuals) is None


class TestBuildActualsQuery:
    """The probe SQL must reproduce the notebooks' [bq-actuals] definition."""

    def test_window_clauses_are_or_combined_and_all_present(self):
        sql = verify.build_actuals_query(
            "proj.dataset.table", 'app_name = "Firefox Desktop"',
            [("2025-03-01", "2025-03-31"), ("2025-09-01", "2025-09-30")],
        )
        assert sql.count("submission_date BETWEEN") == 2
        assert " OR " in sql
        for boundary in ["2025-03-01", "2025-03-31", "2025-09-01", "2025-09-30"]:
            assert boundary in sql

    def test_app_filter_and_table_are_applied(self):
        """A dropped app filter would sum unrelated apps and inflate every comparison."""
        app_filter = 'app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS")'
        sql = verify.build_actuals_query(
            "moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates",
            app_filter, [("2025-03-01", "2025-03-31")],
        )
        assert app_filter in sql
        assert "`moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates`" in sql
        assert "SUM(dau)" in sql
        assert "GROUP BY submission_date" in sql

    def test_default_targets_match_notebook_query_definitions(self):
        """Locks the source tables/filters against accidental edits (Legacy desktop, Glean mobile)."""
        desktop = verify.DEFAULT_TARGETS["desktop"]
        assert desktop["table"] == "moz-fx-data-shared-prod.telemetry.active_users_aggregates"
        assert desktop["app_filter"] == 'app_name = "Firefox Desktop"'
        assert desktop["data_source"] == "legacy_desktop"

        mobile = verify.DEFAULT_TARGETS["mobile"]
        assert mobile["table"] == (
            "moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates"
        )
        assert mobile["app_name"] == "ALL MOBILE"
        assert mobile["data_source"] == "glean_mobile"


class TestParseWindow:
    def test_valid_window(self):
        assert verify.parse_window("2025-03-01:2025-03-31") == ("2025-03-01", "2025-03-31")

    def test_missing_colon_is_rejected(self):
        import argparse
        with pytest.raises(argparse.ArgumentTypeError):
            verify.parse_window("2025-03-01")


class TestLoadTrainingRows:
    """load_training_rows() must fail loudly rather than return an empty frame."""

    def test_unmatched_selector_raises_with_context(self, tmp_path):
        parquet = tmp_path / "forecast.parquet"
        pd.DataFrame({
            "country": ["ALL"], "segment": ['{"os": "ALL"}'],
            "data_source": ["legacy_desktop"], "app_name": ["desktop"],
            "data_type": ["forecast"],  # no training rows at all
            "target_date": [pd.Timestamp("2026-12-01")], "dau": [48_000_000.0],
        }).to_parquet(parquet)

        with pytest.raises(ValueError, match="No training rows matched"):
            verify.load_training_rows(parquet, "legacy_desktop", '{"os": "ALL"}', "desktop")

    def test_selects_only_training_rows_of_the_requested_series(self, tmp_path):
        parquet = tmp_path / "forecast.parquet"
        pd.DataFrame({
            "country": ["ALL", "ALL", "ALL", "US"],
            "segment": ['{"os": "ALL"}'] * 4,
            "data_source": ["legacy_desktop"] * 4,
            "app_name": ["desktop"] * 4,
            "data_type": ["training", "forecast", "training", "training"],
            "target_date": pd.to_datetime(
                ["2025-03-01", "2025-03-02", "2025-03-03", "2025-03-01"]
            ),
            "dau": [47_000_000.0, 99_999_999.0, 47_200_000.0, 1_000_000.0],
        }).to_parquet(parquet)

        result = verify.load_training_rows(
            parquet, "legacy_desktop", '{"os": "ALL"}', "desktop"
        )
        # The forecast row and the US-country row must both be excluded.
        assert list(result.index) == [pd.Timestamp("2025-03-01"), pd.Timestamp("2025-03-03")]
        assert result.tolist() == [47_000_000.0, 47_200_000.0]
