# -*- coding: utf-8 -*-
"""Integration tests for enabling the Iran built-in gap fill in the forecast path.

Covers the mozaic-daily wiring: `data_source` is forwarded from the platform wrappers into
`mozaic.populate_tiles` so the package selects the matching built-in fill. (The query/market-list
side — IR no longer excluded, IR surfaced as its own market — is covered by
test_queries.py::test_build_query_includes_iran_natively; the fill values by test_iran_fill.py.)
"""

import pathlib
import sys
from unittest.mock import patch

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mozaic_daily.forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs  # noqa: E402


def _tiny_datasets():
    return {
        "DAU": pd.DataFrame({
            "x": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "country": ["US", "IR"],
            "modern_windows": [True, True],
            "winX": [False, False],
            "fenix_android": [False, False],
            "firefox_ios": [False, False],
            "focus_android": [False, False],
            "focus_ios": [False, False],
            "y": [100.0, 50.0],
        })
    }


def test_desktop_wrapper_forwards_data_source_to_populate_tiles():
    with patch("mozaic.populate_tiles") as pt, patch("mozaic.utils.curate_mozaics"):
        get_desktop_forecast_dfs(_tiny_datasets(), "2026-02-01", "2026-12-31", data_source="legacy_desktop")
        assert pt.call_count == 1
        assert pt.call_args.kwargs.get("data_source") == "legacy_desktop"


def test_mobile_wrapper_forwards_data_source_to_populate_tiles():
    with patch("mozaic.populate_tiles") as pt, patch("mozaic.utils.curate_mozaics"):
        get_mobile_forecast_dfs(_tiny_datasets(), "2026-02-01", "2026-12-31", data_source="glean_mobile")
        assert pt.call_count == 1
        assert pt.call_args.kwargs.get("data_source") == "glean_mobile"


def test_data_source_defaults_to_none_when_unset():
    # Backward-compat: callers that don't pass data_source (e.g. the fill producer) get None.
    with patch("mozaic.populate_tiles") as pt, patch("mozaic.utils.curate_mozaics"):
        get_desktop_forecast_dfs(_tiny_datasets(), "2026-02-01", "2026-12-31")
        assert pt.call_args.kwargs.get("data_source") is None
