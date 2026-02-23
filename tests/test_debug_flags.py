# -*- coding: utf-8 -*-
"""Tests for debug/research flags added to the mozaic-daily pipeline.

Covers:
- Query filtering: --dau-only, --data-source
- Null reporting: print_null_report()
- Raw data saving: save_raw_datasets()
- Output collision detection: check_output_collisions()
- historical_only return type from main()

🔒 SECURITY: All BigQuery interactions are MOCKED. No real BigQuery calls.
All test data is synthetic.
"""

import io
import sys
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch

from mozaic_daily.data import get_queries
from mozaic_daily.main import save_raw_datasets
from mozaic_daily.reports import print_null_report
from tests.conftest import generate_desktop_raw_data, generate_mobile_raw_data


# =============================================================================
# QUERY FILTERING TESTS
# =============================================================================

def test_get_queries_dau_only():
    """With dau_only=True, only DAU specs are returned across all platforms/sources.

    Expects 3 total queries: desktop/glean/DAU, desktop/legacy/DAU, mobile/glean/DAU.
    """
    queries = get_queries("'US', 'DE'", dau_only=True)

    all_metrics = [
        metric
        for sources in queries.values()
        for metrics in sources.values()
        for metric in metrics.keys()
    ]

    assert all(metric == 'DAU' for metric in all_metrics), (
        f"Expected only DAU metrics with dau_only=True, got: {set(all_metrics)}"
    )
    assert len(all_metrics) == 3, (
        f"Expected 3 DAU queries (desktop/glean, desktop/legacy, mobile/glean), got {len(all_metrics)}"
    )


def test_get_queries_data_source_filter_glean_desktop():
    """With data_source='glean_desktop', only glean_desktop specs are returned.

    Expects 4 queries: desktop/glean/DAU, desktop/glean/New Profiles,
    desktop/glean/Existing Engagement DAU, desktop/glean/Existing Engagement MAU.
    """
    queries = get_queries("'US', 'DE'", data_source_filter='glean_desktop')

    # Only desktop/glean should have entries
    desktop_glean_metrics = list(queries['desktop']['glean'].keys())
    desktop_legacy_metrics = list(queries['desktop']['legacy'].keys())
    mobile_glean_metrics = list(queries['mobile']['glean'].keys())

    assert len(desktop_glean_metrics) == 4, (
        f"Expected 4 metrics in desktop/glean, got {len(desktop_glean_metrics)}"
    )
    assert len(desktop_legacy_metrics) == 0, (
        f"Expected 0 metrics in desktop/legacy with glean_desktop filter, got {len(desktop_legacy_metrics)}"
    )
    assert len(mobile_glean_metrics) == 0, (
        f"Expected 0 metrics in mobile/glean with glean_desktop filter, got {len(mobile_glean_metrics)}"
    )


def test_get_queries_data_source_filter_glean_mobile():
    """With data_source='glean_mobile', only mobile/glean specs are returned."""
    queries = get_queries("'US', 'DE'", data_source_filter='glean_mobile')

    desktop_glean_metrics = list(queries['desktop']['glean'].keys())
    desktop_legacy_metrics = list(queries['desktop']['legacy'].keys())
    mobile_glean_metrics = list(queries['mobile']['glean'].keys())

    assert len(desktop_glean_metrics) == 0
    assert len(desktop_legacy_metrics) == 0
    assert len(mobile_glean_metrics) == 4, (
        f"Expected 4 metrics in mobile/glean, got {len(mobile_glean_metrics)}"
    )


def test_get_queries_dau_only_plus_data_source_composable():
    """dau_only and data_source_filter stack as filters, producing exactly 1 query."""
    queries = get_queries("'US', 'DE'", dau_only=True, data_source_filter='glean_desktop')

    all_metrics = [
        metric
        for sources in queries.values()
        for metrics in sources.values()
        for metric in metrics.keys()
    ]

    assert all_metrics == ['DAU'], (
        f"Expected exactly ['DAU'] for dau_only + glean_desktop filter, got {all_metrics}"
    )


def test_get_queries_default_returns_all_twelve():
    """With no filters, all 12 query specs are returned (baseline regression test)."""
    queries = get_queries("'US', 'DE'")

    all_metrics = [
        metric
        for sources in queries.values()
        for metrics in sources.values()
        for metric in metrics.keys()
    ]

    assert len(all_metrics) == 12, (
        f"Expected 12 queries with no filters, got {len(all_metrics)}"
    )


# =============================================================================
# NULL REPORT TESTS
# =============================================================================

def _make_forecast_df(rows, metric_values=None):
    """Create a minimal forecast DataFrame for null report testing.

    Args:
        rows: list of dicts with keys: target_date, country, segment, data_type, data_source
        metric_values: dict mapping row index to {metric: value or None}

    Returns:
        DataFrame matching the schema expected by print_null_report()
    """
    metric_defaults = {
        'dau': 100.0,
        'new_profiles': 10.0,
        'existing_engagement_dau': 80.0,
        'existing_engagement_mau': 500.0,
    }

    records = []
    for i, row in enumerate(rows):
        record = dict(row)
        for metric, default_value in metric_defaults.items():
            if metric_values and i in metric_values and metric in metric_values[i]:
                record[metric] = metric_values[i][metric]
            else:
                record[metric] = default_value
        records.append(record)

    return pd.DataFrame(records)


def _base_rows():
    """Return a minimal set of rows for forecast DataFrame construction."""
    return [
        {'target_date': '2024-02-01', 'country': 'US', 'segment': '{}', 'data_type': 'forecast', 'data_source': 'glean_desktop'},
        {'target_date': '2024-02-01', 'country': 'DE', 'segment': '{}', 'data_type': 'forecast', 'data_source': 'glean_desktop'},
        {'target_date': '2024-02-02', 'country': 'US', 'segment': '{}', 'data_type': 'training', 'data_source': 'glean_mobile'},
    ]


def test_null_report_no_nulls(capsys):
    """With a clean DataFrame, null report prints 'No nulls found' for each section."""
    df = _make_forecast_df(_base_rows())
    print_null_report(df)

    captured = capsys.readouterr()
    assert 'NULL REPORT' in captured.out
    assert 'No nulls found' in captured.out


def test_null_report_with_nulls_shows_correct_counts(capsys):
    """With injected nulls, null report shows counts matching the injected data."""
    rows = _base_rows()
    # Inject nulls: row 0 has null dau, row 1 has null new_profiles
    metric_overrides = {
        0: {'dau': None},
        1: {'new_profiles': None},
    }
    df = _make_forecast_df(rows, metric_values=metric_overrides)
    print_null_report(df)

    captured = capsys.readouterr()
    output = captured.out

    # Summary should show 1 null for dau and 1 for new_profiles
    assert 'dau: 1 nulls' in output, f"Expected 'dau: 1 nulls' in output:\n{output}"
    assert 'new_profiles: 1 nulls' in output, f"Expected 'new_profiles: 1 nulls' in output:\n{output}"


def test_null_report_handles_missing_columns(capsys):
    """With a DataFrame that lacks all metric columns, null report exits gracefully."""
    df = pd.DataFrame({'target_date': ['2024-02-01'], 'country': ['US']})
    print_null_report(df)

    captured = capsys.readouterr()
    assert 'No metric columns found' in captured.out


# =============================================================================
# SAVE RAW DATASETS TESTS
# =============================================================================

def test_save_raw_datasets_creates_expected_files(tmp_path):
    """save_raw_datasets() creates one parquet file per platform/source/metric combination."""
    desktop_df = generate_desktop_raw_data(num_days=5)
    mobile_df = generate_mobile_raw_data(num_days=5)

    datasets = {
        'desktop': {
            'glean': {'DAU': desktop_df},
            'legacy': {'DAU': desktop_df},
        },
        'mobile': {
            'glean': {'DAU': mobile_df},
        },
    }

    save_raw_datasets(datasets, tmp_path)

    expected_files = [
        'raw_desktop_glean_DAU.parquet',
        'raw_desktop_legacy_DAU.parquet',
        'raw_mobile_glean_DAU.parquet',
    ]

    for filename in expected_files:
        filepath = tmp_path / filename
        assert filepath.exists(), (
            f"Expected file '{filename}' not found in {tmp_path}. "
            f"Files present: {list(tmp_path.iterdir())}"
        )


def test_save_raw_datasets_files_are_readable(tmp_path):
    """Files written by save_raw_datasets() can be read back as valid DataFrames."""
    desktop_df = generate_desktop_raw_data(num_days=5)
    datasets = {
        'desktop': {'glean': {'DAU': desktop_df}},
        'mobile': {'glean': {}},
    }

    save_raw_datasets(datasets, tmp_path)

    loaded = pd.read_parquet(tmp_path / 'raw_desktop_glean_DAU.parquet')
    assert len(loaded) == len(desktop_df), (
        f"Expected {len(desktop_df)} rows after round-trip, got {len(loaded)}"
    )


# =============================================================================
# OUTPUT COLLISION DETECTION TESTS
# =============================================================================

def _make_args(**kwargs):
    """Create a minimal argparse.Namespace with debug flag defaults."""
    import argparse
    defaults = {
        'repeat': 1,
        'historical_only': False,
        'save_raw_data': False,
        'save_intermediate': False,
        'forecast_only': False,
        'null_report': False,
        'dau_only': False,
        'data_source': None,
        'testing': False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_collision_check_exits_when_files_exist(tmp_path):
    """check_output_collisions() exits with code 1 when expected files already exist."""
    # Pre-create the expected output file
    (tmp_path / 'forecast_output.parquet').touch()

    args = _make_args()

    # Import here so we can call with the tmp_path
    from scripts.run_main import check_output_collisions

    with pytest.raises(SystemExit) as exc_info:
        check_output_collisions(tmp_path, args)

    assert exc_info.value.code == 1


def test_collision_check_passes_for_empty_directory(tmp_path):
    """check_output_collisions() does not exit when output directory is empty."""
    args = _make_args()

    from scripts.run_main import check_output_collisions

    # Should not raise
    check_output_collisions(tmp_path, args)


def test_collision_check_repeat_detects_run_files(tmp_path):
    """With --repeat 2, collision check detects existing forecast_run_1.parquet."""
    (tmp_path / 'forecast_run_1.parquet').touch()

    args = _make_args(repeat=2)

    from scripts.run_main import check_output_collisions

    with pytest.raises(SystemExit) as exc_info:
        check_output_collisions(tmp_path, args)

    assert exc_info.value.code == 1


def test_collision_check_passes_when_historical_only(tmp_path):
    """With --historical-only, no forecast_output.parquet is expected, so no collision."""
    # Pre-create forecast_output.parquet — should NOT trigger collision since historical_only
    # doesn't produce this file
    (tmp_path / 'forecast_output.parquet').touch()

    args = _make_args(historical_only=True)

    from scripts.run_main import check_output_collisions

    # Should not raise (historical_only doesn't produce forecast_output.parquet)
    check_output_collisions(tmp_path, args)


# =============================================================================
# HISTORICAL_ONLY RETURN TYPE TEST
# =============================================================================

def test_main_historical_only_returns_dict(mocker):
    """With historical_only=True, main() returns a dict of datasets, not a DataFrame."""
    import sys

    mock_client = MagicMock()
    mock_result = MagicMock()
    mock_result.to_dataframe.return_value = generate_desktop_raw_data(num_days=5)
    mock_client.query.return_value = mock_result

    mocker.patch('mozaic_daily.data.bigquery.Client', return_value=mock_client)

    # mozaic_daily.main is the function in __init__.py; patch the module's attribute directly
    mocker.patch.object(
        sys.modules['mozaic_daily.main'],
        'check_training_data_availability',
        return_value=None,
    )

    from mozaic_daily.main import main as pipeline_main

    result = pipeline_main(
        checkpoints=False,
        historical_only=True,
    )

    assert isinstance(result, dict), (
        f"Expected dict from historical_only=True, got {type(result)}"
    )
    assert 'desktop' in result, "Expected 'desktop' key in historical_only result"
    assert 'mobile' in result, "Expected 'mobile' key in historical_only result"
