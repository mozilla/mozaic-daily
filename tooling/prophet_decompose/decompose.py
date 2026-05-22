"""Aggregate per-tile Prophet components for a desktop Mozaic forecast pkl into a global per-day series.

The pkl produced by `save_mozaic_objects()` stores a dict[metric -> Mozaic]. Each Tile inside has
its fitted Prophet model on `_prophet_model` and Mozaic's holiday-detrending artifacts on
`raw_historical_data` / `holiday_detrended_historical_data`.

Mozaic strips holidays before Prophet sees them, so Prophet's `holidays` column is unused.
The "holidays" component plotted by the comparison notebook is reconstructed as
    historical: raw_historical_data - holiday_detrended_historical_data  (per tile)
    forecast:   forecasted_holiday_impacts.median(axis=1)                (per tile)

Linear-space contribution rules:
- Logistic-growth tiles: Prophet trains in linear DAU space. Components add directly.
- Linear-growth tiles: Prophet trains on log(y+1). We convert components to linear-space
  attributions via
        trend_lin   = exp(trend) - 1
        weekly_lin  = exp(trend) * (exp(weekly_terms) - 1)
        yearly_lin  = exp(trend) * (exp(yearly) - 1)
  These don't sum to yhat_lin exactly (multiplicative cross-term), but the residual is small
  for our country-level tiles.

Output: a long-form DataFrame with columns
    submission_date, component, value, label
where component ∈ {trend, weekly, yearly, holidays, yhat, actuals}.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

import cloudpickle
import numpy as np
import pandas as pd

RECENT_WEEKS = 13  # both April and June runs used the default


def _build_future_df(tile, prophet_model, recent_weeks: int = RECENT_WEEKS) -> pd.DataFrame:
    """Build a Prophet future-df spanning historical + forecast dates with cap/floor/conditions."""
    all_dates = pd.concat(
        [tile.historical_dates.reset_index(drop=True),
         pd.Series(pd.to_datetime(tile.forecast_dates))],
        ignore_index=True,
    )
    all_dates = pd.to_datetime(all_dates).drop_duplicates().sort_values().reset_index(drop=True)
    future = pd.DataFrame({"ds": all_dates})

    forecast_start = pd.to_datetime(tile.forecast_start_date)
    recent_cutoff = forecast_start - pd.Timedelta(weeks=recent_weeks)
    future["is_historical"] = future["ds"] < recent_cutoff
    future["is_recent"] = future["ds"] >= recent_cutoff

    if prophet_model.growth == "logistic":
        # cap/floor are constant across all rows of the stored prophet_forecast
        cap = float(tile._prophet_forecast["cap"].iloc[0])
        floor = float(tile._prophet_forecast["floor"].iloc[0])
        future["cap"] = cap
        future["floor"] = floor

    return future


def _linear_space_components(prophet_forecast: pd.DataFrame, growth: str) -> pd.DataFrame:
    """Return trend / weekly / yearly / yhat columns in linear DAU space."""
    pf = prophet_forecast
    weekly_terms = pf["weekly_historical"].fillna(0) + pf["weekly_recent"].fillna(0)
    yearly = pf["yearly"].fillna(0)
    trend = pf["trend"]

    if growth == "logistic":
        trend_lin = trend
        weekly_lin = weekly_terms
        yearly_lin = yearly
        yhat_lin = pf["yhat"]
    else:
        trend_lin = np.exp(trend) - 1.0
        base = np.exp(trend)
        weekly_lin = base * (np.exp(weekly_terms) - 1.0)
        yearly_lin = base * (np.exp(yearly) - 1.0)
        yhat_lin = np.exp(pf["yhat"]) - 1.0

    return pd.DataFrame({
        "ds": pf["ds"].values,
        "trend": trend_lin.values,
        "weekly": weekly_lin.values,
        "yearly": yearly_lin.values,
        "yhat": yhat_lin.values,
    })


def _tile_components(tile) -> pd.DataFrame:
    """Decompose a single tile into linear-space per-day components.

    Returns: DataFrame with columns ds, trend, weekly, yearly, holidays, yhat, actuals.
    """
    future = _build_future_df(tile, tile._prophet_model)
    pf = tile._prophet_model.predict(future)
    comps = _linear_space_components(pf, tile._prophet_model.growth)

    hist = pd.DataFrame({
        "ds": pd.to_datetime(tile.historical_dates.values),
        "raw": tile.raw_historical_data.values,
        "detrended": tile.holiday_detrended_historical_data.values,
    })
    hist["holiday_train"] = hist["raw"] - hist["detrended"]

    fcst_dates = pd.to_datetime(tile.forecast_dates)
    if hasattr(tile, "forecasted_holiday_impacts") and tile.forecasted_holiday_impacts is not None:
        fhi = tile.forecasted_holiday_impacts
        if isinstance(fhi, pd.DataFrame):
            holiday_fcst = fhi.median(axis=1).values
        else:
            holiday_fcst = np.asarray(fhi)
        fc = pd.DataFrame({"ds": fcst_dates, "holiday_fcst": holiday_fcst})
    else:
        fc = pd.DataFrame({"ds": fcst_dates, "holiday_fcst": 0.0})

    df = comps.merge(hist[["ds", "raw", "holiday_train"]], on="ds", how="left")
    df = df.merge(fc, on="ds", how="left")
    df["holidays"] = df["holiday_train"].fillna(df["holiday_fcst"]).fillna(0.0)
    df["actuals"] = df["raw"]

    return df[["ds", "trend", "weekly", "yearly", "holidays", "yhat", "actuals"]]


def decompose_metric(mozaic_obj, label: str, *, verbose: bool = True) -> pd.DataFrame:
    """Aggregate all tiles of a single-metric Mozaic into a global per-day decomposition.

    Returns DataFrame with columns: ds, trend, weekly, yearly, holidays, yhat, actuals, label.
    """
    tiles = mozaic_obj.tiles
    per_tile = []
    for i, t in enumerate(tiles, 1):
        if verbose:
            print(f"  [{i:>2}/{len(tiles)}] {t.name} ({t._prophet_model.growth})", flush=True)
        per_tile.append(_tile_components(t))

    stacked = pd.concat(per_tile, ignore_index=True)
    agg = stacked.groupby("ds", as_index=False)[
        ["trend", "weekly", "yearly", "holidays", "yhat", "actuals"]
    ].sum(min_count=1)
    agg["label"] = label
    return agg


def load_pkl(path: Path):
    with open(path, "rb") as f:
        return cloudpickle.load(f)


def main(argv: Iterable[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pkl", required=True, type=Path,
                        help="Path to mozaic_objects.<source>.<date>.pkl")
    parser.add_argument("--metric", default="DAU",
                        help="Top-level metric key inside the pkl dict (default: DAU)")
    parser.add_argument("--label", required=True,
                        help="Label to attach to the output rows (e.g., 'april' or 'june')")
    parser.add_argument("--output", required=True, type=Path,
                        help="Where to write the aggregated parquet")
    args = parser.parse_args(list(argv) if argv is not None else None)

    print(f"Loading {args.pkl} ...", flush=True)
    obj = load_pkl(args.pkl)
    if args.metric not in obj:
        raise SystemExit(f"Metric {args.metric!r} not in pkl. Available: {list(obj.keys())}")
    moz = obj[args.metric]
    print(f"Decomposing {len(moz.tiles)} tiles for {args.metric} ({args.label}) ...", flush=True)

    agg = decompose_metric(moz, label=args.label)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(args.output, index=False)
    print(f"Wrote {args.output} ({len(agg)} rows, ds range {agg['ds'].min()} -> {agg['ds'].max()})", flush=True)


if __name__ == "__main__":
    main()
