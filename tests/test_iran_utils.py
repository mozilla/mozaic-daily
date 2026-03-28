# -*- coding: utf-8 -*-
"""Tests for mozaic_daily.iran_utils — population-to-segment reverse mapping."""

import pytest

from mozaic_daily.iran_utils import (
    DESKTOP_SEGMENT_COLUMNS,
    MOBILE_SEGMENT_COLUMNS,
    population_to_segment_bools,
)


# ── Desktop segment tests ───────────────────────────────────────────────────

class TestDesktopPopulations:
    def test_modern_windows(self):
        result = population_to_segment_bools("modern_windows", DESKTOP_SEGMENT_COLUMNS)
        assert result == {"modern_windows": True, "winX": False}

    def test_winX(self):
        result = population_to_segment_bools("winX", DESKTOP_SEGMENT_COLUMNS)
        assert result == {"modern_windows": False, "winX": True}

    def test_other(self):
        result = population_to_segment_bools("other", DESKTOP_SEGMENT_COLUMNS)
        assert result == {"modern_windows": False, "winX": False}


# ── Mobile segment tests ────────────────────────────────────────────────────

class TestMobilePopulations:
    def test_fenix_android(self):
        result = population_to_segment_bools("fenix_android", MOBILE_SEGMENT_COLUMNS)
        assert result == {
            "fenix_android": True,
            "firefox_ios": False,
            "focus_android": False,
            "focus_ios": False,
        }

    def test_firefox_ios(self):
        result = population_to_segment_bools("firefox_ios", MOBILE_SEGMENT_COLUMNS)
        assert result == {
            "fenix_android": False,
            "firefox_ios": True,
            "focus_android": False,
            "focus_ios": False,
        }

    def test_focus_android(self):
        result = population_to_segment_bools("focus_android", MOBILE_SEGMENT_COLUMNS)
        assert result == {
            "fenix_android": False,
            "firefox_ios": False,
            "focus_android": True,
            "focus_ios": False,
        }

    def test_focus_ios(self):
        result = population_to_segment_bools("focus_ios", MOBILE_SEGMENT_COLUMNS)
        assert result == {
            "fenix_android": False,
            "firefox_ios": False,
            "focus_android": False,
            "focus_ios": True,
        }

    def test_other(self):
        result = population_to_segment_bools("other", MOBILE_SEGMENT_COLUMNS)
        assert result == {
            "fenix_android": False,
            "firefox_ios": False,
            "focus_android": False,
            "focus_ios": False,
        }


# ── Error cases ──────────────────────────────────────────────────────────────

class TestInvalidPopulations:
    def test_unknown_value_raises(self):
        with pytest.raises(ValueError, match="Cannot reverse-map"):
            population_to_segment_bools("unknown_value", DESKTOP_SEGMENT_COLUMNS)

    def test_all_raises(self):
        """ALL is an aggregate population, not a valid segment."""
        with pytest.raises(ValueError, match="Cannot reverse-map"):
            population_to_segment_bools("ALL", DESKTOP_SEGMENT_COLUMNS)

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Cannot reverse-map"):
            population_to_segment_bools("", DESKTOP_SEGMENT_COLUMNS)
