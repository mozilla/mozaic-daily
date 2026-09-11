"""Where a paid-DAU curve came from: a query run, or a workbook the marketing team delivered.

`paid_curve_files` writes the same parquet / csv / meta / workbook / plot whichever way the weekly
rows arrived; the two classes here supply the parts that differ — the `source_data` block of the
meta, the composition sentence, the plot subtitle and the hand-off note's source lines.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path


def sha1_of(path: Path) -> str:
    return hashlib.sha1(path.read_bytes()).hexdigest()


def relative_to_repo(path: Path, repo: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo.resolve()))
    except ValueError:
        return str(path)


@dataclass(frozen=True)
class PullProvenance:
    """A query run through bq_query.py: what was run, where it came from, and when."""
    template_sql: Path
    resolved_sql: Path
    raw_json: Path
    raw_csv: Path
    feed_tables: list[str]
    template_params: dict[str, str]
    pull_date: str
    gb_processed: float
    # None for the point estimate; a slug such as `ci90lo` when the query returns another quantile.
    variant: str | None = None

    origin = "the marketing team's GMIO cross-channel feed query"
    composition = ("COALESCE(uac_meta_actual, uac_actual) for actual weeks; "
                   "COALESCE(uac_meta_forecast, uac_forecast) for forecast weeks")
    plot_subtitle = "GMIO feed, UAC+Meta where present else UAC"
    raw_sheet_name = "raw_query"

    def source_section(self, repo: Path) -> dict:
        return {"kind": "query",
                "template_sql": relative_to_repo(self.template_sql, repo), "template_sql_sha1": sha1_of(self.template_sql),
                "query_sql": relative_to_repo(self.resolved_sql, repo), "query_sql_sha1": sha1_of(self.resolved_sql),
                "query_json": relative_to_repo(self.raw_json, repo), "query_json_sha1": sha1_of(self.raw_json),
                "query_csv": relative_to_repo(self.raw_csv, repo), "query_csv_sha1": sha1_of(self.raw_csv),
                "feed_tables": self.feed_tables, "template_params": self.template_params,
                "pulled_on": self.pull_date, "gb_processed": self.gb_processed}

    def note_lines(self, repo: Path) -> list[str]:
        return [f"- feed tables: {', '.join(self.feed_tables)}"]


@dataclass(frozen=True)
class DeliveredFileProvenance:
    """A workbook handed over as a file, copied byte for byte into `source_data/`."""
    delivered_copy: Path
    original_path: str
    sheet: str
    date_column: str
    actual_column: str
    forecast_column: str
    fill_actual_from: str | None
    dropped_row_labels: list[str]
    filled_weeks: list[str]
    # The producer's own words for the chosen column (the sheet legend), so the meta says what "low" meant.
    column_legend: str
    pull_date: str
    variant: str | None = None

    origin = "a workbook delivered by the marketing team"
    raw_sheet_name = "delivered_sheet"

    @property
    def composition(self) -> str:
        fill = f"; the week(s) {self.filled_weeks} blank in both were filled from {self.fill_actual_from}" if self.filled_weeks else ""
        return (f"{self.actual_column!r} where present (actual weeks), else {self.forecast_column!r} (forecast weeks), "
                f"every value a delivered cell verbatim{fill}")

    @property
    def plot_subtitle(self) -> str:
        return f"delivered workbook, sheet {self.sheet!r}, column {self.forecast_column!r}"

    def source_section(self, repo: Path) -> dict:
        return {"kind": "delivered_file",
                "delivered_file": relative_to_repo(self.delivered_copy, repo), "delivered_file_sha1": sha1_of(self.delivered_copy),
                "original_path": self.original_path, "sheet": self.sheet,
                "columns": {"date": self.date_column, "actual": self.actual_column, "forecast": self.forecast_column},
                "fill_actual_from": self.fill_actual_from, "filled_weeks": self.filled_weeks,
                "dropped_row_labels": self.dropped_row_labels, "column_legend": self.column_legend,
                "received_on": self.pull_date}

    def note_lines(self, repo: Path) -> list[str]:
        return [f"- delivered file: `{relative_to_repo(self.delivered_copy, repo)}` (sheet `{self.sheet}`, column `{self.forecast_column}`)",
                f"- what the producer calls that column: {self.column_legend}"]
