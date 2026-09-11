"""Pure transforms for the paid-DAU curve `p` consumes: weekly GMIO widget rows -> daily paid-DAU level.

The marketing team publishes paid mobile DAU as a *query*, not a file. `scripts/pull_paid_dau_curve.py`
runs that query and hands the rows here; this module is the deterministic part and touches nothing
on disk. It generalizes the September 2026 one-off producer
(`data-official/2026-09/marketing/build_paid_dau_curve.py`, frozen) and reproduces its parquet exactly.

Contract (strict): the query must return exactly the widget's four presentation columns plus `date`,
one row per consecutive ISO Monday. Any other shape raises — the composition rule below is only
meaningful for that shape, and a new shape is a deliberate extension, not a guess.

Method:
  1. Compose one weekly value per row: UAC+Meta where present, else UAC; actuals over forecast.
  2. Each value sits on its Monday; linear interpolation to daily; forward-fill to Dec 31.
  3. Write the level as delivered (`paid_dau_level_daily`) plus its 28d MA. `p` stacks it verbatim.

Through the first September 2026 pulls the file also carried `marketing_lift_daily` = level minus
its value on 2026-03-30, and `p` added that anchor back from `organic.json`. The round-trip cancelled
exactly and was retired 2026-09-09; the frozen September producer still documents it.
"""
from __future__ import annotations

import re

import pandas as pd

TEMPLATE_DEFAULTS = {"metric": "Total Paid DAU", "country": "All"}
REQUIRED_COLUMNS = ("date", "uac_actual", "uac_forecast", "uac_meta_actual", "uac_meta_forecast")
# Column preference when composing one value per week. UAC+Meta is the cumulative line (Meta stacked
# on UAC), so it is the total wherever it exists; actual beats forecast on the one-week handoff overlap.
COMPOSITION_ORDER = (("uac_meta_actual", True), ("uac_actual", True),
                     ("uac_meta_forecast", False), ("uac_forecast", False))
MA_WINDOW = 28
KPI_MONTH_DAY = (12, 15)

_TEMPLATE_PATTERN = re.compile(r"\{\{\s*(\w+)\s*\}\}")
_TABLE_PATTERN = re.compile(r"`([\w-]+\.[\w-]+\.[\w-]+)`")
_VARIANT_PATTERN = re.compile(r"[a-z0-9]+")


def resolve_template_params(sql: str, params: dict[str, str]) -> str:
    """Substitute `{{name}}` widget parameters; raise if any template is left unresolved."""
    found = set(_TEMPLATE_PATTERN.findall(sql))
    unresolved = sorted(found - set(params))
    if unresolved:
        raise ValueError(f"unresolved template params {unresolved}; known: {sorted(params)}. "
                         "Add a value for each or the query is not the GMIO widget query.")
    resolved = sql
    for name, value in params.items():
        resolved = re.sub(r"\{\{\s*" + re.escape(name) + r"\s*\}\}", value, resolved)
    return resolved


def feed_tables(sql: str) -> list[str]:
    """Fully-qualified backticked tables referenced by the query, for provenance."""
    return sorted(set(_TABLE_PATTERN.findall(sql)))


def check_contract(raw: pd.DataFrame) -> pd.DataFrame:
    """Enforce the four-column weekly contract; return the frame with `date` parsed and sorted."""
    columns = list(raw.columns)
    if set(columns) != set(REQUIRED_COLUMNS):
        raise ValueError(f"expected exactly columns {list(REQUIRED_COLUMNS)}, got {columns}. "
                         "This is not the GMIO paid-DAU widget output; do not compose it.")
    if len(raw) < 2:
        raise ValueError(f"need at least 2 weekly rows, got {len(raw)}")
    frame = raw.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = check_weekly_dates(frame)
    for column in REQUIRED_COLUMNS[1:]:
        frame[column] = pd.to_numeric(frame[column])
    for prefix in ("uac", "uac_meta"):
        overlap = frame.dropna(subset=[f"{prefix}_actual", f"{prefix}_forecast"])
        if (overlap[f"{prefix}_actual"] != overlap[f"{prefix}_forecast"]).any():
            raise ValueError(f"{prefix} actual and forecast disagree on the handoff week")
    return frame


def check_weekly_dates(frame: pd.DataFrame) -> pd.DataFrame:
    """Rows must sit on consecutive, distinct ISO Mondays; return the frame sorted by `date`.

    Shared by the query contract and the delivered-workbook reader: the weekly-to-daily step below
    is only meaningful on this grid, whichever source the rows came from.
    """
    frame = frame.sort_values("date").reset_index(drop=True)
    if (frame["date"].dt.dayofweek != 0).any():
        bad = frame.loc[frame["date"].dt.dayofweek != 0, "date"].dt.date.tolist()[:3]
        raise ValueError(f"rows must sit on ISO Mondays; offenders start {bad}")
    if frame["date"].duplicated().any():
        raise ValueError("duplicate week rows")
    gaps = frame["date"].diff().dropna().dt.days
    if gaps.ne(7).any():
        raise ValueError(f"weeks are not consecutive; gaps of {sorted(set(gaps[gaps.ne(7)]))} days found")
    return frame


def check_actuals_precede_forecasts(weekly: pd.DataFrame, source: str) -> None:
    """An actual week after a forecast week means the source's actual/forecast labelling is broken."""
    actual_after_forecast = weekly["is_actual"] & (~weekly["is_actual"]).cummax()
    if actual_after_forecast.any():
        raise ValueError(f"an actual week follows a forecast week; {source}")


def compose_weekly(frame: pd.DataFrame) -> pd.DataFrame:
    """One value per week (`paid_dau_used`) with its provenance (`basis`) and `is_actual` flag."""
    def pick(row) -> tuple[float, str, bool]:
        for column, is_actual in COMPOSITION_ORDER:
            if pd.notna(row[column]):
                return float(row[column]), column, is_actual
        raise ValueError(f"week {row['date'].date()} has no value in any of the four columns")

    picked = frame.apply(pick, axis=1, result_type="expand")
    weekly = frame.copy()
    weekly["paid_dau_used"], weekly["basis"], weekly["is_actual"] = picked[0], picked[1], picked[2].astype(bool)
    check_actuals_precede_forecasts(weekly, "the feed's was_forecast flag is inconsistent")
    return weekly


def interpolate_weekly_to_daily(weekly: pd.DataFrame, daily_end: pd.Timestamp) -> pd.Series:
    """Monday values -> daily by linear interpolation, forward-filled to `daily_end`."""
    series = pd.Series(weekly["paid_dau_used"].to_numpy(), index=pd.DatetimeIndex(weekly["date"]), dtype="float64")
    if daily_end < series.index.max():
        raise ValueError(f"daily_end {daily_end.date()} is before the last weekly row {series.index.max().date()}")
    daily_index = pd.date_range(series.index.min(), daily_end, freq="D", name="target_date")
    return series.reindex(daily_index).interpolate(method="linear", limit_area="inside").ffill()


def build_daily_table(level: pd.Series) -> pd.DataFrame:
    """The parquet `p` loads: the paid-DAU level as delivered, plus its 28d MA for plots."""
    level = level.astype("float64")
    daily = pd.DataFrame({
        "paid_dau_level_daily": level,
        "paid_dau_level_ma": level.rolling(MA_WINDOW, min_periods=14).mean().astype("float64"),
    })
    daily.index.name = "target_date"
    return daily


def daily_type_labels(daily: pd.DataFrame, weekly: pd.DataFrame) -> pd.Series:
    """`actuals` through the last actual Monday, `forecast` after — the tailwind-template convention."""
    last_actual = weekly.loc[weekly["is_actual"], "date"].max()
    return pd.Series(["actuals" if d <= last_actual else "forecast" for d in daily.index],
                     index=daily.index, name="type")


def curve_stem(basis: str, forecast_start: str, pull_date: str, variant: str | None = None) -> str:
    """`marketing_lift_model.gmio_uac_meta_{basis}[_{variant}].{seam}.pull{pull_date}` — siblings per pull, never overwrite.

    The `marketing_lift_model` prefix is historical (the file no longer carries a lift); it is kept
    so the skill's file-name documentation and existing siblings stay consistent within a cycle.
    `variant` names a query that is not the point estimate (e.g. `ci90lo`, the lower end of the
    90% credible interval) so the file is distinguishable from the point-estimate pull by name.
    """
    return f"marketing_lift_model.gmio_uac_meta_{basis_with_variant(basis, variant)}.{forecast_start}.pull{pull_date}"


def basis_with_variant(basis: str, variant: str | None) -> str:
    """`{basis}` for the point estimate, `{basis}_{variant}` otherwise; the variant slug must be [a-z0-9]+."""
    if variant is None:
        return basis
    if not _VARIANT_PATTERN.fullmatch(variant):
        raise ValueError(f"variant {variant!r} must match [a-z0-9]+ (e.g. ci90lo); it lands in file names")
    return f"{basis}_{variant}"


def basis_slug(metric: str) -> str:
    """The widget's view for a metric label, used in file names."""
    slugs = {"Total Paid DAU": "total", "2026-acquired Paid DAU": "current_year"}
    if metric == "Attributed New Profiles":
        raise ValueError("Attributed New Profiles is a flow, not a paid-DAU level; `p` cannot consume it")
    return slugs.get(metric, "rolling_12mo")


def key_values(level: pd.Series, forecast_start: str) -> dict:
    """The numbers the wiring step and the meta need. All levels; there is no anchor to copy."""
    year = pd.Timestamp(forecast_start).year
    kpi = pd.Timestamp(year=year, month=KPI_MONTH_DAY[0], day=KPI_MONTH_DAY[1])
    year_end = level.index.max()
    seam = pd.Timestamp(forecast_start)
    return {
        "level_at_seam": float(level.loc[seam]) if seam in level.index else None,
        "level_dec15": float(level.loc[kpi]),
        "level_year_end": float(level.loc[year_end]),
    }
