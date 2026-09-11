"""Tests for `mozaic_daily.paid_curve_workbook`: one scenario column of a delivered workbook -> weekly frame.

Fixtures are small workbooks written with pandas so the reader is exercised end to end (sheet
lookup, footer rows, the fill sheet), not against a pre-parsed frame.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from mozaic_daily.paid_curve_workbook import ScenarioColumns, read_scenario_sheet

MONDAYS = pd.date_range("2026-01-05", periods=6, freq="7D")


def scenario_sheet(actual=(None, 100.0, 110.0, None, None, None), low=(None, None, None, 120.0, 125.0, 130.0),
                   footer=True) -> pd.DataFrame:
    frame = pd.DataFrame({"Weeks": [d.strftime("%Y-%m-%d") for d in MONDAYS],
                          "Actualized Total Paid DAU": actual, "High Forecast": [None] * 6, "Low Forecast": low})
    if footer:
        frame = pd.concat([frame, pd.DataFrame({"Weeks": [None, "Dec15 DAU"], "Actualized Total Paid DAU": [None, None],
                                                "High Forecast": [None, 999.0], "Low Forecast": [None, 127.5]})],
                          ignore_index=True)
    return frame


def write_book(path: Path, scenarios: pd.DataFrame, result: pd.DataFrame | None = None) -> Path:
    with pd.ExcelWriter(path, engine="openpyxl") as book:
        scenarios.to_excel(book, sheet_name="Scenarios", index=False)
        if result is not None:
            result.to_excel(book, sheet_name="result", index=False)
    return path


def result_sheet() -> pd.DataFrame:
    return pd.DataFrame({"date": [d.strftime("%Y-%m-%d") for d in MONDAYS],
                         "uac_actual": [90.0, 99.0, 109.0, None, None, None],
                         "uac_forecast": [None, None, None, 118.0, 123.0, 128.0]})


COLUMNS = ScenarioColumns(sheet="Scenarios", date="Weeks", actual="Actualized Total Paid DAU", forecast="Low Forecast")
FILLED = ScenarioColumns(**{**COLUMNS.__dict__, "fill_actual_from": "result!uac_actual"})


class TestReading:
    def test_actual_beats_forecast_and_basis_names_the_column(self, tmp_path):
        book = write_book(tmp_path / "b.xlsx", scenario_sheet(actual=(95.0, 100.0, 110.0, None, None, None)))
        weekly = read_scenario_sheet(book, COLUMNS).weekly
        assert weekly["paid_dau_used"].tolist() == [95.0, 100.0, 110.0, 120.0, 125.0, 130.0]
        assert weekly["basis"].tolist() == ["Actualized Total Paid DAU"] * 3 + ["Low Forecast"] * 3
        assert weekly["is_actual"].tolist() == [True] * 3 + [False] * 3

    def test_footer_rows_are_dropped_and_reported_not_silently(self, tmp_path):
        book = write_book(tmp_path / "b.xlsx", scenario_sheet(actual=(95.0, 100.0, 110.0, None, None, None)))
        read = read_scenario_sheet(book, COLUMNS)
        assert read.dropped_row_labels == ["Dec15 DAU"]
        assert len(read.weekly) == 6
        assert 127.5 not in read.weekly["paid_dau_used"].tolist()

    def test_blank_week_is_filled_from_the_named_sheet_and_labelled(self, tmp_path):
        book = write_book(tmp_path / "b.xlsx", scenario_sheet(), result_sheet())
        read = read_scenario_sheet(book, FILLED)
        assert read.weekly.loc[0, "paid_dau_used"] == 90.0
        assert read.weekly.loc[0, "basis"] == "result!uac_actual"
        assert bool(read.weekly.loc[0, "is_actual"]) is True
        assert read.filled_weeks == ["2026-01-05"]
        # the fill is used only where both delivered columns are blank
        assert read.weekly.loc[1, "paid_dau_used"] == 100.0

    def test_blank_week_without_a_fill_source_halts(self, tmp_path):
        book = write_book(tmp_path / "b.xlsx", scenario_sheet())
        with pytest.raises(ValueError, match="2026-01-05 has no value"):
            read_scenario_sheet(book, COLUMNS)

    def test_blank_week_the_fill_sheet_cannot_cover_halts(self, tmp_path):
        result = result_sheet()
        result.loc[0, "uac_actual"] = None
        book = write_book(tmp_path / "b.xlsx", scenario_sheet(), result)
        with pytest.raises(ValueError, match="none in result!uac_actual"):
            read_scenario_sheet(book, FILLED)

    def test_disagreeing_actual_and_forecast_on_one_week_halts(self, tmp_path):
        book = write_book(tmp_path / "b.xlsx", scenario_sheet(actual=(95.0, 100.0, 110.0, 111.0, None, None)))
        with pytest.raises(ValueError, match="disagree"):
            read_scenario_sheet(book, COLUMNS)

    def test_equal_actual_and_forecast_on_the_handoff_week_is_taken_as_actual(self, tmp_path):
        book = write_book(tmp_path / "b.xlsx", scenario_sheet(actual=(95.0, 100.0, 110.0, 120.0, None, None)))
        weekly = read_scenario_sheet(book, COLUMNS).weekly
        assert bool(weekly.loc[3, "is_actual"]) is True

    def test_actual_after_forecast_halts(self, tmp_path):
        book = write_book(tmp_path / "b.xlsx", scenario_sheet(actual=(95.0, 100.0, None, None, 126.0, None),
                                                              low=(None, None, 115.0, 120.0, None, 130.0)))
        with pytest.raises(ValueError, match="actual week follows a forecast week"):
            read_scenario_sheet(book, COLUMNS)

    def test_non_monday_or_skipped_week_halts(self, tmp_path):
        sheet = scenario_sheet(actual=(95.0, 100.0, 110.0, None, None, None), footer=False)
        sheet.loc[2, "Weeks"] = "2026-01-20"
        book = write_book(tmp_path / "b.xlsx", sheet)
        with pytest.raises(ValueError, match="ISO Mondays"):
            read_scenario_sheet(book, COLUMNS)
        sheet = scenario_sheet(actual=(95.0, 100.0, 110.0, None, None, None), footer=False).drop(index=2)
        book = write_book(tmp_path / "c.xlsx", sheet)
        with pytest.raises(ValueError, match="not consecutive"):
            read_scenario_sheet(book, COLUMNS)

    def test_missing_column_names_what_is_there(self, tmp_path):
        book = write_book(tmp_path / "b.xlsx", scenario_sheet())
        with pytest.raises(ValueError, match=r"lacks columns \['Med Forecast'\]"):
            read_scenario_sheet(book, ScenarioColumns(**{**COLUMNS.__dict__, "forecast": "Med Forecast"}))

    def test_malformed_fill_reference_halts(self, tmp_path):
        book = write_book(tmp_path / "b.xlsx", scenario_sheet(), result_sheet())
        with pytest.raises(ValueError, match="sheet!column"):
            read_scenario_sheet(book, ScenarioColumns(**{**COLUMNS.__dict__, "fill_actual_from": "uac_actual"}))
