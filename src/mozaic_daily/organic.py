# -*- coding: utf-8 -*-
"""Adjustment ``p`` — mobile paid/organic split (`paid_organic_split`).

**Consumer side.** Applies the per-cycle measured split that
:mod:`mozaic_daily.organic_source` produces and
``data-official/{YYYY-MM}/organic/`` pins. Replaces the ``m`` marketing-lift overlay for mobile.

## What it does

Around a mozaic run, exactly like the ``m``/``l``/``o`` bidirectional appliers::

    training_df, measured_paid = split_training_to_organic(training_df, ...)
    forecast_df = mozaic(training_df)                      # forecasts ORGANIC DAU
    forecast_df = add_paid_to_forecast(forecast_df, measured_paid=..., marketing_paid=..., ...)

The difference from ``m`` is what gets subtracted and what gets added:

===============  ====================================  =========================================
                 ``m`` (marketing_lift)                ``p`` (paid_organic_split)
===============  ====================================  =========================================
subtracts        modelled lift *since an anchor*       **measured** paid, from the gclid flag
adds (training)  the same modelled lift                the same **measured** paid
adds (forecast)  the same modelled lift                marketing's paid **level** (lift + anchor)
paid's effect    absorbed ~58% by Prophet              **exactly its own value**
===============  ====================================  =========================================

`m` had to subtract and add the *same* series, so shrinking the curve handed Prophet a higher
organic trend that offset the smaller add-back. Here the subtraction is a measurement and the
forecast add-back is a level, so the two are decoupled and paid is additive — the property that
lets ``h`` change without a model re-run.

## The two-piece add-back is load-bearing

Training rows get the **measured** paid back, so ``organic + measured_paid == y`` exactly and the
published ``training`` rows stay byte-identical to raw BigQuery actuals
(``scripts/verify_training_rows_are_actuals.py`` enforces this, and the canonical notebook's
28-day MA straddles the seam). Forecast rows get **marketing's** level, because that is the
forecast of paid.

The two disagree where they meet — that disagreement *is* the seam step, and it is a real signal
about how well our measurement and marketing's model partition the total.
:func:`paid_seam_step` reports it so it can be asserted rather than discovered.

## Share from the mirror, level from production

``organic_y(d, c) = y_production(d, c) * organic_share(d, c)``. The mirror loses ~2.9% to shredder
attrition over 26 months; taking the level from it would read as +3.3pp of fake growth. See
``data-official/2026-08/organic/_index.md``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .adjustments import compute_country_shares

__all__ = [
    "load_organic_spec",
    "load_split_frame",
    "build_share_lookup",
    "split_training_to_organic",
    "measured_paid_country_shares",
    "marketing_paid_level",
    "add_paid_to_forecast",
    "paid_seam_step",
]

_SPEC_TYPE = "paid_organic_split"

#: Allocation keys understood by :func:`measured_paid_country_shares`.
#:
#: ``trailing_paid_dau_share`` allocates marketing's national paid level across countries by each
#: country's share of **measured paid DAU** in the trailing window. ``trailing_dau_share`` (what
#: ``m`` used) allocates by share of *total* Fenix DAU instead. The former is strongly preferred:
#: paid intensity varies from 0.2% of Fenix DAU in RU to 27.6% in ID, so a total-DAU key puts paid
#: into markets that have essentially none and starves the campaign markets.
_ALLOCATION_KEYS = ("trailing_paid_dau_share", "trailing_dau_share")


def load_organic_spec(spec_path: str | Path) -> dict:
    """Load and validate a ``paid_organic_split`` spec.

    Mirrors ``adjustments._load_lift_spec``: fail at load with a message naming the missing key,
    rather than at apply time with a ``KeyError`` deep in a merge.
    """
    spec_path = Path(spec_path)
    spec = json.loads(spec_path.read_text())

    if spec.get("type") != _SPEC_TYPE:
        raise ValueError(
            f"{spec_path}: expected type {_SPEC_TYPE!r}, got {spec.get('type')!r}"
        )
    for key in ("data_file", "share_column", "scope", "share_backfill", "paid_forecast", "allocation"):
        if key not in spec:
            raise ValueError(f"{spec_path}: missing required key {key!r}")
    if "app_flag_column" not in spec["scope"]:
        raise ValueError(f"{spec_path}: missing required key scope.app_flag_column")
    for key in ("data_file", "value_column", "anchor_paid_dau"):
        if key not in spec["paid_forecast"]:
            raise ValueError(f"{spec_path}: missing required key paid_forecast.{key}")
    if spec["allocation"].get("key") not in _ALLOCATION_KEYS:
        raise ValueError(
            f"{spec_path}: allocation.key must be one of {list(_ALLOCATION_KEYS)}, "
            f"got {spec['allocation'].get('key')!r}"
        )
    if "window_days" not in spec["allocation"]:
        raise ValueError(f"{spec_path}: missing required key allocation.window_days")
    policy = spec["share_backfill"].get("policy")
    if policy != "hold_earliest":
        raise ValueError(
            f"{spec_path}: share_backfill.policy {policy!r} is not implemented; "
            f"only 'hold_earliest' is."
        )
    return spec


def load_split_frame(spec: dict, spec_dir: str | Path) -> pd.DataFrame:
    """Load the measured split parquet named by the spec.

    Returns ``submission_date`` (normalized datetime64), ``country``, ``organic_dau``,
    ``paid_dau``, ``total_dau`` and the spec's share column.
    """
    path = Path(spec_dir) / spec["data_file"]
    df = pd.read_parquet(path)
    share_column = spec["share_column"]
    missing = {"submission_date", "country", share_column} - set(df.columns)
    if missing:
        raise ValueError(
            f"{path}: missing column(s) {sorted(missing)}; available: {list(df.columns)}"
        )
    df = df.copy()
    df["submission_date"] = pd.to_datetime(df["submission_date"]).dt.normalize()
    if df.duplicated(["submission_date", "country"]).any():
        raise ValueError(f"{path}: duplicate (submission_date, country) rows")
    return df.sort_values(["submission_date", "country"]).reset_index(drop=True)


def build_share_lookup(
    split: pd.DataFrame,
    *,
    share_column: str,
    training_dates: pd.DatetimeIndex,
    countries: Iterable[str],
) -> pd.Series:
    """Per-(date, country) organic share covering the **whole** training range.

    The measured split starts at the mirror's own start date (2024-06-01) because
    ``mozdata.fenix.active_users`` retains only a rolling ~25 months, while mobile DAU trains from
    2020-12-31. This reindexes each country onto the full training grid and fills the uncovered
    head by **holding its earliest measured share flat backwards**.

    That is an assumption, and it is the largest one in ``p``. It is cheap because the ex-IR paid
    share was only 1.10% at the oldest measured month (rising monotonically to 11.58% by
    2026-07), so the error it can introduce is bounded at ~1.1pp of Fenix DAU, spread over 3.5
    years. The alternative — truncating all mobile training to 2024-06-01 — would cost Firefox
    iOS and Focus 3.5 years of history and sit only ~55 days above Prophet's 730-observation
    yearly-seasonality gate. Masking is not available at all: mozaic requires one common date
    grid across tiles, so NaN-ing Fenix pre-2024-06 would corrupt the published ALL MOBILE
    training rows.

    A trailing ``ffill`` is also applied, so a split that stops a day short of ``training_end``
    holds rather than producing NaN. Both directions are logged by the caller.
    """
    training_dates = pd.DatetimeIndex(training_dates).normalize().unique().sort_values()
    wide = split.pivot(index="submission_date", columns="country", values=share_column)

    known = [c for c in countries if c in wide.columns]
    unknown = sorted(set(countries) - set(wide.columns))
    if unknown:
        raise ValueError(
            f"no measured organic share for {unknown}; those tiles' Fenix DAU would be treated "
            f"as 100% organic. Rebuild the split with scripts/build_fenix_organic_split.py."
        )

    filled = wide[known].reindex(training_dates).ffill().bfill()
    if filled.isna().any().any():
        bad = filled.columns[filled.isna().any()].tolist()
        raise ValueError(f"organic share still NaN after backfill for {bad}")

    lookup = filled.stack()
    lookup.index.names = ["submission_date", "country"]
    return lookup.rename("organic_share")


def split_training_to_organic(
    dau_training: pd.DataFrame,
    *,
    share_lookup: pd.Series,
    flag_column: str = "fenix_android",
    exclude_countries: Iterable[str] = (),
    sentinel_attr: str = "paid_organic_split_applied",
) -> tuple[pd.DataFrame, pd.Series]:
    """Scale Fenix training rows down to their organic component.

    For each row with ``flag_column == True`` whose country is not excluded::

        organic_y = round(y * organic_share[date, country])
        measured_paid = y - organic_y

    Returns ``(organic_training_df, measured_paid)`` where ``measured_paid`` is a Series indexed
    by ``(submission_date, country)`` — exactly what must be added back to training rows so they
    return to the observed level.

    - Rows outside the flagged segment are untouched (non-Fenix mobile apps are 100% organic).
    - Excluded countries are untouched. IR is excluded by default in the spec: it is 98.8%
      organic, marketing's curve is ex-IR, and subtracting a paid component that is never added
      back would knowingly bias the total low.
    - Sets ``df.attrs[sentinel_attr]``; re-applying raises rather than double-subtracting.
    - Returns a copy; never mutates ``dau_training``.
    """
    if dau_training.attrs.get(sentinel_attr):
        raise RuntimeError(
            f"split_training_to_organic called twice with sentinel {sentinel_attr!r} on the same "
            f"DataFrame; this would double-subtract paid. Pass the original training data."
        )
    excluded = set(exclude_countries)
    result = dau_training.copy()
    result.attrs = dict(dau_training.attrs)

    segment_mask = (result[flag_column] == True) & (~result["country"].isin(excluded))  # noqa: E712
    if not segment_mask.any():
        result.attrs[sentinel_attr] = True
        return result, pd.Series(dtype="int64")

    dates = pd.to_datetime(result.loc[segment_mask, "x"]).dt.normalize()
    keys = pd.MultiIndex.from_arrays([dates, result.loc[segment_mask, "country"]])
    shares = pd.Series(share_lookup.reindex(keys).to_numpy(), index=result.index[segment_mask])
    if shares.isna().any():
        n = int(shares.isna().sum())
        sample = keys[shares.isna().to_numpy()][:3].tolist()
        raise ValueError(
            f"{n} Fenix training row(s) have no organic share, e.g. {sample}. The share lookup "
            f"must cover every (date, country) in the training frame."
        )

    y = result.loc[segment_mask, "y"].astype("float64")
    organic = np.round(y * shares).astype("int64")
    paid = (y.astype("int64") - organic).rename("measured_paid")

    result.loc[segment_mask, "y"] = pd.array(organic.to_numpy(), dtype="Int64")
    result["y"] = result["y"].astype("Int64")
    result.attrs[sentinel_attr] = True

    measured_paid = paid.groupby([dates.to_numpy(), result.loc[segment_mask, "country"].to_numpy()]).sum()
    measured_paid.index.names = ["submission_date", "country"]
    return result, measured_paid.rename("measured_paid")


def measured_paid_country_shares(
    split: pd.DataFrame,
    *,
    training_end_date: pd.Timestamp,
    window_days: int,
    exclude_countries: Iterable[str] = (),
    allocation_key: str = "trailing_paid_dau_share",
    dau_training: pd.DataFrame | None = None,
    flag_column: str = "fenix_android",
) -> pd.Series:
    """Country weights for allocating marketing's national paid level across tiles.

    ``trailing_paid_dau_share`` (default) weights each country by its share of **measured paid
    DAU** over the trailing window. This is the right key and it is a deliberate departure from
    ``m``, which used share of *total* Fenix DAU: paid intensity ranges from 0.2% of Fenix DAU in
    RU to 27.6% in ID, so a total-DAU key would push marketing's paid forecast into markets with
    essentially no paid acquisition and starve the campaign markets. It also keeps the seam
    coherent — the geographic mix of forecast paid matches the geographic mix of measured paid
    that immediately precedes it.

    ``trailing_dau_share`` reproduces ``m``'s behaviour and needs ``dau_training``; kept so the
    difference stays measurable.

    Excluded countries are dropped and the remainder renormalized to sum 1.0, so the per-country
    add-back still totals the full national level (the same sum-to-1 guarantee
    ``compute_country_shares`` provides).
    """
    excluded = set(exclude_countries)
    end = pd.Timestamp(training_end_date).normalize()
    start = end - pd.Timedelta(days=window_days - 1)

    if allocation_key == "trailing_dau_share":
        if dau_training is None:
            raise ValueError("allocation key 'trailing_dau_share' requires dau_training")
        totals = compute_country_shares(
            dau_training, training_end_date=end, window_days=window_days, flag_column=flag_column
        )
    elif allocation_key == "trailing_paid_dau_share":
        window = split[(split["submission_date"] >= start) & (split["submission_date"] <= end)]
        totals = window.groupby("country")["paid_dau"].sum().astype("float64")
    else:
        raise ValueError(f"unknown allocation key {allocation_key!r}")

    kept = totals.drop(labels=[c for c in excluded if c in totals.index])
    kept = kept[kept > 0]
    if kept.empty:
        raise ValueError(
            f"no paid DAU in the window {start.date()} → {end.date()} after excluding "
            f"{sorted(excluded)}; cannot allocate marketing's paid level."
        )
    shares = (kept / kept.sum()).sort_index()
    shares.name = "paid_country_share"
    return shares


def marketing_paid_level(
    spec: dict,
    spec_dir: str | Path,
    *,
    forecast_start: pd.Timestamp,
    forecast_end: pd.Timestamp,
) -> pd.Series:
    """Marketing's paid DAU as a **level**, covering the whole forecast horizon.

    ``paid(d) = marketing_lift_daily(d) + anchor_paid_dau``. The delivered artifact is a *lift*
    because that is what ``m`` consumed; stacking needs the level, so the anchor is added back.
    (Compare cycles on levels, never lifts — August's lift is 18% below July's but its level is
    3.4% *higher*, because it also raised the anchor.)

    Past the curve's last day the level is **held flat**. ``forecast_end_date`` is Dec 31 of the
    following year while the curve stops at 2026-12-31, and zero-filling — what ``m`` does —
    would drop the whole paid level (~1.56M) on 2027-01-01. Holding flat is an explicit
    extrapolation into a period that is out of scope for planning; the alternative is a cliff
    that is certainly wrong.
    """
    paid_spec = spec["paid_forecast"]
    path = Path(spec_dir) / paid_spec["data_file"]
    df = pd.read_parquet(path)
    column = paid_spec["value_column"]
    if column not in df.columns:
        raise ValueError(f"{path}: no column {column!r}; available: {list(df.columns)}")

    anchor = paid_spec.get("anchor_paid_dau")
    if anchor is None:
        raise ValueError(
            f"{path}: paid_forecast.anchor_paid_dau is required. The delivered curve is a lift, "
            f"not a level, so without the anchor every total would be shifted by a constant with "
            f"the shape left right — nothing downstream would catch it."
        )

    lift = df[column]
    lift.index = pd.DatetimeIndex(lift.index).normalize()
    lift = lift.sort_index()
    level = (lift + float(anchor)).rename("marketing_paid_dau")

    forecast_start = pd.Timestamp(forecast_start).normalize()
    forecast_end = pd.Timestamp(forecast_end).normalize()
    if level.index.min() > forecast_start:
        raise ValueError(
            f"{path}: paid curve starts {level.index.min().date()}, after forecast_start "
            f"{forecast_start.date()}; the forecast region would have no paid level."
        )

    horizon = pd.date_range(forecast_start, forecast_end, freq="D")
    policy = paid_spec.get("tail_policy", "hold_last")
    if policy != "hold_last":
        raise ValueError(f"paid_forecast.tail_policy {policy!r} is not implemented")
    return level.reindex(level.index.union(horizon)).ffill().reindex(horizon)


def add_paid_to_forecast(
    forecast_df: pd.DataFrame,
    *,
    measured_paid: pd.Series,
    marketing_paid: pd.Series,
    country_shares: pd.Series,
    forecast_start: pd.Timestamp,
    metric_column: str = "DAU",
    population_value: str = "fenix_android",
    all_population_value: str = "ALL",
    all_country_value: str = "ALL",
) -> pd.DataFrame:
    """Stack paid DAU back onto an organic-only forecast frame.

    Called *after* mozaic and *before* the platform format function, while the population column
    still carries segment names. Two regions, by design:

    +---------------------------+------------------------------------------------------------+
    | ``target_date``           | value added                                                |
    +===========================+============================================================+
    | ``< forecast_start``      | ``measured_paid[date, country]`` — restores the row to the |
    |                           | observed level exactly                                     |
    +---------------------------+------------------------------------------------------------+
    | ``>= forecast_start``     | ``marketing_paid[date] * country_shares[country]``         |
    +---------------------------+------------------------------------------------------------+

    ``country == "ALL"`` rows receive the whole-world value for that date (the summed measured
    paid, or the full marketing level). Populations other than ``population_value`` and
    ``all_population_value`` are untouched.

    Returns a copy; never mutates ``forecast_df``.
    """
    result = forecast_df.copy()
    if metric_column not in result.columns:
        return result

    forecast_start = pd.Timestamp(forecast_start).normalize()
    dates = pd.to_datetime(result["target_date"]).dt.normalize()
    is_world_country = result["country"] == all_country_value

    # --- historical half: the measured paid we removed, put back exactly.
    measured_world = measured_paid.groupby(level="submission_date").sum()
    per_country = pd.Series(
        measured_paid.reindex(pd.MultiIndex.from_arrays([dates, result["country"]])).to_numpy(),
        index=result.index,
    )
    historical = per_country.where(~is_world_country, dates.map(measured_world)).fillna(0.0)

    # --- forecast half: marketing's level, allocated by the paid country mix.
    level_at_row = dates.map(marketing_paid).astype("float64")
    share_at_row = result["country"].map(country_shares).fillna(0.0)
    effective_share = share_at_row.where(~is_world_country, 1.0)
    forward = (level_at_row * effective_share).fillna(0.0)

    add_value = historical.where(dates < forecast_start, forward)

    is_eligible = (result["population"] == population_value) | (
        result["population"] == all_population_value
    )
    apply_mask = is_eligible & (add_value != 0)
    result.loc[apply_mask, metric_column] = (
        result.loc[apply_mask, metric_column].astype("float64") + add_value.loc[apply_mask]
    )
    return result


def paid_seam_step(
    measured_paid: pd.Series,
    marketing_paid: pd.Series,
    *,
    training_end_date: pd.Timestamp,
    window_days: int = 28,
) -> dict:
    """Size the measured→marketing discontinuity at the seam.

    Training rows carry *our* measurement of paid; forecast rows carry *marketing's* model of it.
    Where they meet, the total steps by their disagreement. That step is not a bug to be hidden —
    it is the visible part of how well the two definitions partition total DAU — but it does need
    to be measured, reported, and asserted against a threshold so a future divergence is loud.

    Returns the trailing-window mean of each side at ``training_end_date``, the absolute and
    relative step, and the first forecast day's level.
    """
    end = pd.Timestamp(training_end_date).normalize()
    start = end - pd.Timedelta(days=window_days - 1)
    measured_world = measured_paid.groupby(level="submission_date").sum()
    tail = measured_world[(measured_world.index >= start) & (measured_world.index <= end)]
    if tail.empty:
        raise ValueError(f"no measured paid in {start.date()} → {end.date()}")

    forward = marketing_paid[marketing_paid.index > end].head(window_days)
    if forward.empty:
        raise ValueError(f"marketing paid level does not extend past {end.date()}")

    measured_mean = float(tail.mean())
    marketing_mean = float(forward.mean())
    return {
        "training_end": str(end.date()),
        "window_days": window_days,
        "measured_paid_mean": measured_mean,
        "marketing_paid_mean": marketing_mean,
        "step_abs": marketing_mean - measured_mean,
        "step_rel": (marketing_mean - measured_mean) / measured_mean,
        "measured_last_day": float(measured_world.loc[end]) if end in measured_world.index else float("nan"),
        "marketing_first_day": float(forward.iloc[0]),
    }
