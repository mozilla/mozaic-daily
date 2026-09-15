"""Prediction intervals for a fitted mozaic world total, from its stored sample paths.

Every fitted ``mozaic.Mozaic`` keeps Prophet's 1,000 predictive sample paths per tile
(``uncertainty_samples`` in ``mozaic.models``), and reconciliation shifts each tile's
paths by a per-day constant, so the top-level object still holds 1,000 coherent paths
for the world total. This module turns those paths into bands.

Two rules the functions here enforce:

1. **Quantiles of the moving average, not moving average of the quantiles.** A 28-day
   trailing mean is taken on every sample path first (after splicing the actuals in
   front, so the window is well-defined from the seam onward), and the band is the
   per-day quantile across the smoothed paths. The alternative — smoothing the per-day
   10th/90th percentiles — overstates the width, because day-to-day noise that averages
   out inside the window is treated as if it were persistent.

2. **The median path reproduces the published point forecast.** ``world_sample_paths``
   rebuilds exactly the matrix ``Mozaic.to_df`` takes its median of, so the q=0.5 band
   equals the parquet's world ``forecast`` column. ``assert_median_matches_forecast``
   checks that against the parquet before any band is trusted.

These are Prophet *predictive* intervals: MAP trend + simulated future changepoints +
observation noise. They are not calibrated against realised forecast error.

Pure functions only — the pickle and parquet are read by ``scripts/compute_forecast_intervals.py``.
"""

from __future__ import annotations

from typing import Iterable, Sequence

import numpy as np
import pandas as pd

MA_WINDOW = 28
DEFAULT_LEVELS: tuple[float, ...] = (0.5, 0.8, 0.9)
MEDIAN_COLUMN = "median"


def world_sample_paths(moz) -> pd.DataFrame:
    """Sample paths of the world total, one column per path, indexed by forecast date.

    Mirrors ``Mozaic.to_df``: reconciled samples plus forecasted holiday impacts, clipped at 0.
    """
    paths = (moz.forecast_reconciled + moz.forecasted_holiday_impacts).clip(lower=0)
    paths = paths.copy()
    paths.index = pd.DatetimeIndex(moz.forecast_dates)
    paths.columns = [int(c) for c in paths.columns]
    return paths


def world_actuals(moz) -> pd.Series:
    """The raw actuals the world total was trained on, indexed by date."""
    return pd.Series(
        np.asarray(moz.raw_historical_data, dtype=float),
        index=pd.DatetimeIndex(moz.historical_dates),
        name="actuals",
    )


def assert_median_matches_forecast(
    paths: pd.DataFrame, parquet_world_forecast: pd.Series, tolerance: float = 1.0
) -> float:
    """Raise unless the per-day median of ``paths`` equals the parquet's world forecast.

    Returns the worst absolute deviation. ``parquet_world_forecast`` must be indexed by date.
    """
    median = paths.quantile(0.5, axis=1)
    common = median.index.intersection(parquet_world_forecast.index)
    if len(common) == 0:
        raise ValueError(
            "No overlapping dates between sample paths and parquet forecast: "
            f"paths {paths.index.min().date()}..{paths.index.max().date()}, "
            f"parquet {parquet_world_forecast.index.min().date()}..{parquet_world_forecast.index.max().date()}"
        )
    deviation = (median.loc[common] - parquet_world_forecast.loc[common]).abs()
    worst = float(deviation.max())
    if worst > tolerance:
        worst_date = deviation.idxmax().date()
        raise AssertionError(
            f"Median path deviates from parquet forecast by {worst:,.2f} DAU on {worst_date} "
            f"(tolerance {tolerance}). The sample matrix is not the one to_df() took its median of."
        )
    return worst


def splice_actuals_onto_paths(actuals: pd.Series, paths: pd.DataFrame) -> pd.DataFrame:
    """Prepend the actuals to every path so trailing windows are defined across the seam.

    The actuals must end the day before the paths begin; a gap or overlap is an error because
    either would silently corrupt every window that touches the seam.
    """
    seam_gap = (paths.index.min() - actuals.index.max()).days
    if seam_gap != 1:
        raise ValueError(
            f"Actuals must end the day before the paths start: actuals end {actuals.index.max().date()}, "
            f"paths start {paths.index.min().date()} (gap {seam_gap} days, expected 1)."
        )
    history = pd.DataFrame(
        np.repeat(actuals.to_numpy()[:, None], paths.shape[1], axis=1),
        index=actuals.index,
        columns=paths.columns,
    )
    return pd.concat([history, paths])


def rolling_mean_paths(spliced_paths: pd.DataFrame, window: int = MA_WINDOW) -> pd.DataFrame:
    """Plain trailing mean of every path. Rows before the first full window are dropped."""
    return spliced_paths.rolling(window).mean().dropna(how="all")


def _level_bounds(level: float) -> tuple[float, float]:
    if not 0 < level < 1:
        raise ValueError(f"Interval level must be in (0, 1), got {level}")
    half = (1 - level) / 2
    return half, 1 - half


def band_column_names(levels: Iterable[float]) -> list[str]:
    """Column order for a band frame: median, then lower/upper per level, e.g. ``lower_80``."""
    names = [MEDIAN_COLUMN]
    for level in levels:
        pct = _level_label(level)
        names += [f"lower_{pct}", f"upper_{pct}"]
    return names


def _level_label(level: float) -> str:
    return f"{round(level * 100):d}"


def band_quantiles(paths: pd.DataFrame, levels: Sequence[float] = DEFAULT_LEVELS) -> pd.DataFrame:
    """Per-day median and central intervals across paths.

    Columns: ``median``, then ``lower_{L}`` / ``upper_{L}`` for each level L in percent.
    """
    out = pd.DataFrame(index=paths.index)
    out[MEDIAN_COLUMN] = paths.quantile(0.5, axis=1)
    for level in levels:
        lower_q, upper_q = _level_bounds(level)
        pct = _level_label(level)
        out[f"lower_{pct}"] = paths.quantile(lower_q, axis=1)
        out[f"upper_{pct}"] = paths.quantile(upper_q, axis=1)
    return out[band_column_names(levels)]


def point_summary(bands: pd.DataFrame, on_date: pd.Timestamp, levels: Sequence[float] = DEFAULT_LEVELS) -> dict:
    """One row of a band frame as a flat dict, with half-widths per level added."""
    on_date = pd.Timestamp(on_date)
    if on_date not in bands.index:
        raise KeyError(f"{on_date.date()} not in band index {bands.index.min().date()}..{bands.index.max().date()}")
    row = bands.loc[on_date]
    summary = {"date": on_date.date().isoformat(), MEDIAN_COLUMN: float(row[MEDIAN_COLUMN])}
    for level in levels:
        pct = _level_label(level)
        lower, upper = float(row[f"lower_{pct}"]), float(row[f"upper_{pct}"])
        summary[f"lower_{pct}"] = lower
        summary[f"upper_{pct}"] = upper
        summary[f"halfwidth_{pct}"] = (upper - lower) / 2
    return summary


def trough_summary(bands: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> dict:
    """Date and value of the median's minimum inside [start, end], with the bands on that date."""
    window = bands.loc[pd.Timestamp(start):pd.Timestamp(end)]
    if window.empty:
        raise ValueError(f"No band rows between {start} and {end}")
    trough_date = window[MEDIAN_COLUMN].idxmin()
    return {"trough_date": trough_date.date().isoformat(), **{k: float(v) for k, v in window.loc[trough_date].items()}}
