"""Read a delivered paid-DAU *workbook* into the weekly frame the paid-curve pipeline consumes.

The marketing team normally publishes paid mobile DAU as a query (`paid_curve.check_contract` +
`compose_weekly`). On 2026-09-10 they delivered a workbook instead: one sheet with a date column,
one column of actualized total paid DAU, and several forecast *scenario* columns side by side. This
module turns one chosen scenario into the same `date / paid_dau_used / basis / is_actual` frame the
query path produces, so everything downstream (interpolate → daily → files) is shared unchanged.

Rules, in the same spirit as the query contract:
  * every value used is one of the delivered cells verbatim, and `basis` names its column;
  * a week must have exactly one of actual / forecast (an equal pair on the handoff week is allowed);
  * a week with neither may be filled from one other named sheet/column (`fill_actual_from`) — the
    delivered `result` sheet's UAC-only actual is the case seen — and is labelled `sheet!column`;
  * rows whose date cell does not parse (footer rows such as a "Dec15 DAU" summary) are dropped and
    **reported**, never silently.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from mozaic_daily.paid_curve import check_actuals_precede_forecasts, check_weekly_dates


@dataclass(frozen=True)
class ScenarioColumns:
    """Which delivered columns mean what."""
    sheet: str
    date: str
    actual: str
    forecast: str
    # "sheet!column" for weeks where both actual and forecast are blank; None means such a week halts.
    fill_actual_from: str | None = None

    def fill_source(self) -> tuple[str, str] | None:
        if self.fill_actual_from is None:
            return None
        if "!" not in self.fill_actual_from:
            raise ValueError(f"fill_actual_from must be 'sheet!column', got {self.fill_actual_from!r}")
        sheet, column = self.fill_actual_from.split("!", 1)
        return sheet, column


@dataclass(frozen=True)
class ScenarioRead:
    """The weekly frame plus what was dropped or filled, for the meta and the console."""
    weekly: pd.DataFrame
    dropped_row_labels: list[str] = field(default_factory=list)
    filled_weeks: list[str] = field(default_factory=list)


def read_scenario_sheet(workbook: Path, columns: ScenarioColumns) -> ScenarioRead:
    """One scenario column of a delivered workbook -> the weekly frame (`date, paid_dau_used, basis, is_actual`)."""
    sheet = pd.read_excel(workbook, sheet_name=columns.sheet)
    missing = [c for c in (columns.date, columns.actual, columns.forecast) if c not in sheet.columns]
    if missing:
        raise ValueError(f"sheet {columns.sheet!r} lacks columns {missing}; has {list(sheet.columns)}")

    rows, dropped = _split_dated_rows(sheet, columns)
    fill = _fill_lookup(workbook, columns)
    picked = [_pick_week(row, columns, fill) for _, row in rows.iterrows()]
    weekly = pd.DataFrame({
        "date": rows["date"].to_numpy(),
        "paid_dau_used": [value for value, _, _ in picked],
        "basis": [basis for _, basis, _ in picked],
        "is_actual": [is_actual for _, _, is_actual in picked],
    })
    if len(weekly) < 2:
        raise ValueError(f"need at least 2 weekly rows, got {len(weekly)}")
    weekly = check_weekly_dates(weekly)
    check_actuals_precede_forecasts(weekly, f"columns {columns.actual!r} / {columns.forecast!r} overlap out of order")
    filled = weekly.loc[weekly["basis"].str.contains("!"), "date"].dt.date.astype(str).tolist()
    return ScenarioRead(weekly=weekly, dropped_row_labels=dropped, filled_weeks=filled)


def _split_dated_rows(sheet: pd.DataFrame, columns: ScenarioColumns) -> tuple[pd.DataFrame, list[str]]:
    """Keep rows whose date cell parses; return the labels of non-blank rows that did not (footers)."""
    present = sheet[sheet[columns.date].notna()].copy()
    parsed = pd.to_datetime(present[columns.date], errors="coerce")
    dropped = [str(label) for label in present.loc[parsed.isna(), columns.date]]
    rows = present[parsed.notna()].copy()
    rows["date"] = parsed[parsed.notna()].dt.normalize()
    return rows, dropped


def _fill_lookup(workbook: Path, columns: ScenarioColumns) -> tuple[str, pd.Series] | None:
    source = columns.fill_source()
    if source is None:
        return None
    sheet_name, column = source
    other = pd.read_excel(workbook, sheet_name=sheet_name)
    if column not in other.columns:
        raise ValueError(f"fill sheet {sheet_name!r} lacks column {column!r}; has {list(other.columns)}")
    dates = pd.to_datetime(other.iloc[:, 0], errors="coerce").dt.normalize()
    values = pd.to_numeric(other[column], errors="coerce")
    lookup = pd.Series(values.to_numpy(), index=dates).dropna()
    return f"{sheet_name}!{column}", lookup


def _pick_week(row: pd.Series, columns: ScenarioColumns, fill: tuple[str, pd.Series] | None) -> tuple[float, str, bool]:
    actual, forecast = row[columns.actual], row[columns.forecast]
    has_actual, has_forecast = pd.notna(actual), pd.notna(forecast)
    if has_actual and has_forecast and float(actual) != float(forecast):
        raise ValueError(f"week {row['date'].date()} has both an actual ({actual}) and a forecast ({forecast}) that disagree")
    if has_actual:
        return float(actual), columns.actual, True
    if has_forecast:
        return float(forecast), columns.forecast, False
    if fill is not None and row["date"] in fill[1].index:
        return float(fill[1].loc[row["date"]]), fill[0], True
    raise ValueError(f"week {row['date'].date()} has no value in {columns.actual!r} or {columns.forecast!r}"
                     + (f" and none in {fill[0]}" if fill else "; pass fill_actual_from to fill it from another sheet"))
