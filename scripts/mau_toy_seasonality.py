#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Simplified typical-year shape for the MAU what-if toy (used by ``build_mau_sensitivity_calculator.py``).

The toy's projection is ``last actual × S(day) / S(last actual's day) × (1 + yearly rate) ** years``. ``S`` is the median,
by calendar day, of value / centred 365-day mean over past years, fitted with a few yearly harmonics so it keeps the
broad seasonal shape (summer trough, spring and autumn highs) and drops holiday-scale detail. The page's JavaScript
(``dayOfYear`` / ``seasonalIndex`` / ``projectLevel`` in ``mau_calculator_template.html``) implements the same formulas;
change both together.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

DAYS_PER_YEAR = 365
SEASONAL_HARMONICS = 4       # simplified: keeps summer trough / spring and autumn highs, drops holiday-scale detail
SEASONAL_WINDOW_DAYS = 365


def day_of_year_365(dates: pd.DatetimeIndex) -> np.ndarray:
    """Day of a 365-day year (1..365); Feb 29 maps onto Feb 28 so every year has the same calendar."""
    return np.array([pd.Timestamp(2001, d.month, min(d.day, 28) if d.month == 2 else d.day).dayofyear for d in dates])


def seasonal_index(coefficients: list[float], day_of_year: np.ndarray) -> np.ndarray:
    """Evaluate ``1 + sum_k a_k cos(k x) + b_k sin(k x)``; coefficients are ``[1, a_1, b_1, a_2, b_2, ...]``."""
    angle = 2 * np.pi * (np.asarray(day_of_year, dtype=float) - 1) / 365
    index = np.full(angle.shape, coefficients[0], dtype=float)
    for k in range(1, (len(coefficients) - 1) // 2 + 1):
        index += coefficients[2 * k - 1] * np.cos(k * angle) + coefficients[2 * k] * np.sin(k * angle)
    return index


def seasonal_profile(values: pd.Series, harmonics: int = SEASONAL_HARMONICS) -> list[float]:
    """Typical-year shape: median (by calendar day) of value / centred 365-day mean, fitted with ``harmonics`` waves.

    The median across years keeps one-off events (an outage, the Iran shutdown) out of the shape. Returned
    coefficients are scaled so the constant term is 1, i.e. the index averages 1 over a year.
    """
    ratio = (values / values.rolling(SEASONAL_WINDOW_DAYS, center=True).mean()).dropna()
    if ratio.index.max() - ratio.index.min() < pd.Timedelta(days=2 * SEASONAL_WINDOW_DAYS):
        raise ValueError(f"seasonal_profile needs at least two full years after centring; got {ratio.index.min().date()}.."
                         f"{ratio.index.max().date()} from a series covering {values.index.min().date()}..{values.index.max().date()}")
    typical = pd.Series(ratio.values).groupby(day_of_year_365(ratio.index)).median()
    angle = 2 * np.pi * (typical.index.to_numpy(dtype=float) - 1) / 365
    columns = [np.ones_like(angle)] + [wave(k * angle) for k in range(1, harmonics + 1) for wave in (np.cos, np.sin)]
    coefficients, *_ = np.linalg.lstsq(np.column_stack(columns), typical.to_numpy(), rcond=None)
    return [float(c / coefficients[0]) for c in coefficients]


def seasonal_projection(last_value: float, last_date: pd.Timestamp, coefficients: list[float], rate_percent: float,
                        date: pd.Timestamp) -> float:
    """The cartoon's value on ``date``: the page's JavaScript implements the same formula."""
    shape = seasonal_index(coefficients, day_of_year_365(pd.DatetimeIndex([date, last_date])))
    years = (date - last_date).days / DAYS_PER_YEAR
    return float(last_value * shape[0] / shape[1] * (1 + rate_percent / 100) ** years)
