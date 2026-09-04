"""Inspect an external headwind/tailwind file: read it, guess its columns, check the contract.

This is the read-only half of the ingest step. Nothing here writes to disk. The
contract it checks is ``templates/tailwind/TAILWIND_CSV_FORMAT.md``: a daily series
with a date column, an optional ``actuals``/``forecast`` flag, and one incremental
world-total DAU column. Every guess carries the evidence it rests on so the skill can
show it to the user and get it confirmed before anything is built.

Weekly files stop here on purpose. Interpreting them needs a plot and a conversation
(week start or week end? total or average?), not a confirmed guess.
"""
from __future__ import annotations

import warnings
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

ACTUALS_TOKENS = {"actual", "actuals", "measured", "observed", "history", "historical", "pre-onset"}
FORECAST_TOKENS = {"forecast", "forecasted", "projected", "projection", "model", "modelled", "modeled", "predicted"}
MA_NAME_HINTS = ("ma", "28", "rolling", "avg", "average", "smooth", "trailing")
DATE_NAME_HINTS = ("date", "day", "ds", "time")
WEEKLY_MEDIAN_GAP_DAYS = 7
SMOOTHNESS_MA_THRESHOLD = 0.03   # mean |Δ| / mean |value| below this looks like a moving average
LARGE_DAU_WARNING = {"desktop": 5_000_000, "mobile": 2_000_000}


@dataclass
class ColumnGuess:
    column: Optional[str]
    evidence: str
    confidence: str  # "high" | "medium" | "low" | "none"


@dataclass
class Finding:
    level: str  # "error" | "warning" | "info"
    code: str
    message: str


@dataclass
class Inspection:
    source_path: str
    sheet: Optional[str]
    n_rows: int
    columns: list[str]
    date_column: ColumnGuess
    value_column: ColumnGuess
    type_column: ColumnGuess
    ma_column: ColumnGuess
    cadence: str  # "daily" | "weekly" | "irregular" | "unknown"
    sign_guess: str  # "tailwind" | "headwind" | "mixed" | "unknown"
    first_date: Optional[str]
    last_date: Optional[str]
    actuals_through: Optional[str]
    findings: list[Finding] = field(default_factory=list)
    sample_rows: list[dict] = field(default_factory=list)

    @property
    def halts(self) -> bool:
        return any(f.level == "error" for f in self.findings)

    def to_dict(self) -> dict:
        return asdict(self)


# --- reading -------------------------------------------------------------------

def read_source_table(path: str | Path, sheet: str | None = None) -> tuple[pd.DataFrame, Optional[str]]:
    """Read CSV / parquet / Excel as delivered. Returns ``(frame, sheet_used)``."""
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        frame = pd.read_parquet(path)
        if isinstance(frame.index, pd.DatetimeIndex) or frame.index.name:
            frame = frame.reset_index()
        return frame, None
    if suffix in (".xlsx", ".xlsm", ".xls"):
        book = pd.ExcelFile(path)
        sheet_used = sheet if sheet is not None else book.sheet_names[0]
        return book.parse(sheet_used), sheet_used
    if suffix in (".csv", ".tsv", ".txt"):
        return pd.read_csv(path, sep=None, engine="python", thousands=","), None
    raise ValueError(f"unsupported input type {suffix!r} for {path}; expected .csv, .parquet or .xlsx")


# --- guessing ---------------------------------------------------------------------

def _parse_rate(series: pd.Series) -> float:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # probing non-date columns for dates warns about inferred formats
        parsed = pd.to_datetime(series, errors="coerce")
    return float(parsed.notna().mean()) if len(series) else 0.0


def guess_date_column(frame: pd.DataFrame) -> ColumnGuess:
    scored = []
    for column in frame.columns:
        if pd.api.types.is_numeric_dtype(frame[column]) and not pd.api.types.is_datetime64_any_dtype(frame[column]):
            continue
        rate = _parse_rate(frame[column])
        name_hint = any(h in str(column).lower() for h in DATE_NAME_HINTS)
        scored.append((rate + (0.05 if name_hint else 0.0), rate, column))
    if not scored:
        return ColumnGuess(None, "no column parses as dates", "none")
    _, rate, column = max(scored)
    if rate < 0.9:
        return ColumnGuess(None, f"best candidate {column!r} parses only {rate:.0%} of rows as dates", "none")
    confidence = "high" if rate > 0.99 else "medium"
    return ColumnGuess(str(column), f"{rate:.0%} of rows parse as dates", confidence)


def guess_type_column(frame: pd.DataFrame, exclude: set[str]) -> ColumnGuess:
    for column in frame.columns:
        if column in exclude or pd.api.types.is_numeric_dtype(frame[column]):
            continue
        values = {str(v).strip().lower() for v in frame[column].dropna().unique()}
        if not values or len(values) > 4:
            continue
        if values <= (ACTUALS_TOKENS | FORECAST_TOKENS):
            return ColumnGuess(str(column), f"values {sorted(values)} all read as actuals/forecast labels", "high")
    return ColumnGuess(None, "no actuals/forecast flag column found", "none")


def _smoothness(values: pd.Series) -> float:
    clean = values.dropna().astype(float)
    if len(clean) < 3 or clean.abs().mean() == 0:
        return float("nan")
    return float(clean.diff().abs().mean() / clean.abs().mean())


def guess_value_columns(frame: pd.DataFrame, exclude: set[str]) -> tuple[ColumnGuess, ColumnGuess]:
    """The incremental daily DAU column and, if present, a moving-average twin."""
    numeric = [c for c in frame.columns if c not in exclude and pd.api.types.is_numeric_dtype(frame[c])]
    coerced = {}
    for column in frame.columns:
        if column in exclude or column in numeric:
            continue
        as_number = pd.to_numeric(frame[column].astype(str).str.replace(",", "").str.strip(), errors="coerce")
        if as_number.notna().mean() > 0.95:
            coerced[column] = as_number
            numeric.append(column)
    if not numeric:
        return ColumnGuess(None, "no numeric column found", "none"), ColumnGuess(None, "", "none")
    if len(numeric) == 1:
        return ColumnGuess(str(numeric[0]), "the only numeric column", "high"), ColumnGuess(None, "no second numeric column", "none")

    def series(column):
        return coerced[column] if column in coerced else frame[column]

    named_ma = [c for c in numeric if any(h in str(c).lower() for h in MA_NAME_HINTS)]
    others = [c for c in numeric if c not in named_ma]
    if len(named_ma) == 1 and len(others) == 1:
        return (ColumnGuess(str(others[0]), f"numeric and not named like a moving average (unlike {named_ma[0]!r})", "high"),
                ColumnGuess(str(named_ma[0]), "name suggests a moving average; validated but not used", "high"))
    # Fall back to roughness: the daily series is the rougher of the two.
    by_roughness = sorted(numeric, key=lambda c: -(_smoothness(series(c)) or 0))
    return (ColumnGuess(str(by_roughness[0]), f"roughest of {len(numeric)} numeric columns {list(map(str, numeric))}", "medium"),
            ColumnGuess(str(by_roughness[1]), "smoother numeric twin; possibly a moving average", "low"))


# --- cadence and contract checks -------------------------------------------------------

def detect_cadence(dates: pd.DatetimeIndex) -> str:
    if len(dates) < 2:
        return "unknown"
    gaps = pd.Series(dates.sort_values()).diff().dropna().dt.days
    median = float(gaps.median())
    if median == 1:
        return "daily"
    if median == WEEKLY_MEDIAN_GAP_DAYS:
        return "weekly"
    return "irregular"


def normalize_type_labels(raw: pd.Series) -> pd.Series:
    lowered = raw.astype(str).str.strip().str.lower()
    return lowered.map(lambda v: "actuals" if v in ACTUALS_TOKENS else ("forecast" if v in FORECAST_TOKENS else v))


def coerce_numeric(raw: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(raw):
        return raw.astype(float)
    return pd.to_numeric(raw.astype(str).str.replace(",", "").str.strip(), errors="coerce")


def contract_findings(
    dates: pd.DatetimeIndex,
    values: pd.Series,
    types: Optional[pd.Series],
    *,
    forecast_start: pd.Timestamp,
    forecast_year_end: pd.Timestamp,
    horizon_end: pd.Timestamp,
    platform: Optional[str] = None,
) -> list[Finding]:
    """Every contract check, as findings. An ``error`` finding halts the ingest."""
    findings: list[Finding] = []
    order = np.argsort(dates.values)
    dates, values = dates[order], values.iloc[order].reset_index(drop=True)
    if types is not None:
        types = types.iloc[order].reset_index(drop=True)

    if dates.has_duplicates:
        findings.append(Finding("error", "duplicate_dates", f"{int(dates.duplicated().sum())} duplicate dates"))
    if values.isna().any():
        findings.append(Finding("error", "missing_values", f"{int(values.isna().sum())} rows have no numeric value"))

    cadence = detect_cadence(dates)
    if cadence == "weekly":
        findings.append(Finding("error", "weekly_rows", "rows are weekly, not daily. Stop here: interpreting weekly "
                                "rows (week start vs end, total vs average) needs a plot and a decision, not a guess"))
    elif cadence == "irregular":
        findings.append(Finding("error", "irregular_cadence", "dates are neither daily nor weekly; inspect the file by hand"))
    elif cadence == "daily":
        expected = pd.date_range(dates.min(), dates.max(), freq="D")
        missing = expected.difference(dates)
        if len(missing):
            findings.append(Finding("warning", "skipped_days", f"{len(missing)} days skipped inside the range "
                                    f"(first {missing[0].date()}); they will be linearly interpolated"))

    if dates.min() > forecast_start:
        findings.append(Finding("error", "starts_after_seam", f"file starts {dates.min().date()}, after the seam "
                                f"{forecast_start.date()}; the curve must cover the cycle from its first forecast day"))
    if dates.max() < forecast_year_end:
        findings.append(Finding("error", "ends_before_year_end", f"file ends {dates.max().date()}, before "
                                f"{forecast_year_end.date()}; the curve must reach the end of the forecast year"))
    elif dates.max() < horizon_end:
        findings.append(Finding("info", "hold_flat_tail", f"file ends {dates.max().date()}; {(horizon_end - dates.max()).days} days "
                                f"will be held flat at the final 28-day mean through {horizon_end.date()}"))
    if dates.max() > horizon_end:
        findings.append(Finding("info", "beyond_horizon", f"{int((dates > horizon_end).sum())} rows past {horizon_end.date()} are dropped"))

    if types is not None:
        unknown = sorted(set(types.dropna().unique()) - {"actuals", "forecast"})
        if unknown:
            findings.append(Finding("error", "unknown_type_labels", f"type labels {unknown} are neither actuals nor forecast"))
        is_forecast = (types == "forecast").to_numpy()
        if is_forecast.any() and (~is_forecast[is_forecast.argmax():]).any():
            findings.append(Finding("error", "actuals_after_forecast", "an actuals row follows a forecast row; the file "
                                    "must be all actuals then all forecast"))
    else:
        findings.append(Finding("warning", "no_type_column", "no actuals/forecast flag; the actuals-through date must be supplied"))

    nonzero = values[values != 0].dropna()
    if len(nonzero):
        share_positive = float((nonzero > 0).mean())
        if 0.05 < share_positive < 0.95:
            findings.append(Finding("warning", "mixed_sign", f"{share_positive:.0%} of non-zero values are positive; a headwind or "
                                    "tailwind normally has one sign"))
    smooth = _smoothness(values)
    if smooth == smooth and smooth < SMOOTHNESS_MA_THRESHOLD and len(values) > 60:
        findings.append(Finding("warning", "looks_like_moving_average", f"day-to-day change is {smooth:.1%} of level; this "
                                "may be a moving average rather than a daily series"))
    if len(nonzero) > 30 and (nonzero.diff().dropna() >= 0).all():
        findings.append(Finding("warning", "monotone_increasing", "the series never decreases; check it is not a cumulative sum"))
    if platform in LARGE_DAU_WARNING and values.abs().max() > LARGE_DAU_WARNING[platform]:
        findings.append(Finding("warning", "very_large_values", f"peak |value| {values.abs().max():,.0f} exceeds "
                                f"{LARGE_DAU_WARNING[platform]:,} DAU; check the units"))
    return findings


MA_TWIN_TOLERANCE = 0.01


def check_ma_twin(values: pd.Series, ma_values: pd.Series, ma_column: str) -> Finding:
    """Does the delivered moving-average column equal the trailing 28d mean of the daily column?

    The MA column is never used downstream, but a mismatch means one of the two columns is not
    what its name says, so it is worth a warning.
    """
    recomputed = values.rolling(28, min_periods=1).mean()
    settled = recomputed.index >= 27
    scale = float(recomputed[settled].abs().mean()) or 1.0
    max_rel = float((recomputed[settled] - ma_values[settled]).abs().max() / scale)
    if max_rel <= MA_TWIN_TOLERANCE:
        return Finding("info", "ma_twin_consistent", f"{ma_column!r} matches the trailing 28-day mean of the daily column "
                       f"(max deviation {max_rel:.2%} of level); it is not used")
    return Finding("warning", "ma_twin_mismatch", f"{ma_column!r} differs from the trailing 28-day mean of the daily column by up "
                   f"to {max_rel:.1%} of level; one of the two is not what its name says")


def sign_guess(values: pd.Series) -> str:
    nonzero = values[values != 0].dropna()
    if nonzero.empty:
        return "unknown"
    share_positive = float((nonzero > 0).mean())
    if share_positive >= 0.95:
        return "tailwind"
    if share_positive <= 0.05:
        return "headwind"
    return "mixed"


# --- entry point ----------------------------------------------------------------------

def inspect_file(
    path: str | Path,
    *,
    forecast_start: str,
    sheet: str | None = None,
    platform: str | None = None,
    overrides: dict | None = None,
) -> Inspection:
    """Read, guess, and check one delivered file. ``overrides`` pins column names the user confirmed."""
    frame, sheet_used = read_source_table(path, sheet)
    overrides = overrides or {}
    frame.columns = [str(c) for c in frame.columns]

    date_guess = ColumnGuess(overrides["date"], "confirmed by user", "high") if "date" in overrides else guess_date_column(frame)
    taken = {date_guess.column} if date_guess.column else set()
    type_guess = ColumnGuess(overrides["type"], "confirmed by user", "high") if "type" in overrides else guess_type_column(frame, taken)
    if type_guess.column:
        taken.add(type_guess.column)
    if "value" in overrides:
        value_guess = ColumnGuess(overrides["value"], "confirmed by user", "high")
        ma_guess = (ColumnGuess(overrides["ma"], "confirmed by user", "high") if "ma" in overrides
                    else ColumnGuess(None, "not named by user", "none"))
    else:
        value_guess, ma_guess = guess_value_columns(frame, taken)

    seam = pd.Timestamp(forecast_start)
    forecast_year_end = pd.Timestamp(year=seam.year, month=12, day=31)
    horizon_end = pd.Timestamp(year=seam.year + 1, month=12, day=31)

    findings: list[Finding] = []
    cadence, first, last, actuals_through, guess = "unknown", None, None, None, "unknown"
    if date_guess.column is None or value_guess.column is None:
        findings.append(Finding("error", "columns_unresolved", "could not identify both a date column and a value column"))
    else:
        dates = pd.DatetimeIndex(pd.to_datetime(frame[date_guess.column], errors="coerce")).normalize()
        values = coerce_numeric(frame[value_guess.column])
        types = normalize_type_labels(frame[type_guess.column]) if type_guess.column else None
        valid = dates.notna()
        if (~valid).any():
            findings.append(Finding("error", "unparseable_dates", f"{int((~valid).sum())} rows have unparseable dates"))
        dates, values = dates[valid], values[valid].reset_index(drop=True)
        types = types[valid].reset_index(drop=True) if types is not None else None
        cadence = detect_cadence(dates)
        first, last = str(dates.min().date()), str(dates.max().date())
        if types is not None and (types == "actuals").any():
            actuals_through = str(dates[(types == "actuals").to_numpy()].max().date())
        guess = sign_guess(values)
        if ma_guess.column:
            findings.append(check_ma_twin(values, coerce_numeric(frame[ma_guess.column])[valid].reset_index(drop=True), ma_guess.column))
        findings += contract_findings(dates, values, types, forecast_start=seam, forecast_year_end=forecast_year_end,
                                      horizon_end=horizon_end, platform=platform)

    sample = frame.head(3).astype(str).to_dict(orient="records") + frame.tail(2).astype(str).to_dict(orient="records")
    return Inspection(
        source_path=str(path), sheet=sheet_used, n_rows=len(frame), columns=list(frame.columns),
        date_column=date_guess, value_column=value_guess, type_column=type_guess, ma_column=ma_guess,
        cadence=cadence, sign_guess=guess, first_date=first, last_date=last, actuals_through=actuals_through,
        findings=findings, sample_rows=sample,
    )
