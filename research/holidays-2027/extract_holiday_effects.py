"""Extract the locked 2027 build's holiday machinery into small CSVs for the 2027 holiday review.

Reads the fitted mozaic pickle of LOCK.md's build 5 and writes, under ``data/``:

- ``tile_effects.csv``     forecast dates: per (metric, country, os) the proportional holiday
                           effect the model applies, the detrended forecast, and the impact (people).
- ``observed_effects.csv`` per (metric, country, holiday, date_diff): the fitted average effect,
                           re-derived with mozaic's own ``Mozaic._fit_holiday_effects`` on the summed
                           country tiles (exactly what the country-level mozaic computed).
- ``history.csv``          per (metric, country, date): observed and holiday-detrended actuals.
- ``calendar.csv``         per (country, date): the holiday label(s) the model uses, all years.
- ``yearly.csv``           per (metric, country, os, date): each tile's Prophet yearly-seasonality term
                           over the dates its Prophet frame covers (to see where moving holidays'
                           leftover dips were absorbed).

Read-only on the build; no model run.
"""

import argparse
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import cloudpickle
import numpy as np
import pandas as pd
from mozaic import Mozaic

DEFAULT_PICKLE = Path(
    "data-official/2027-glean-draft/desktop_glean_history_cpr087_o_2026-10-10/"
    "cps0.1649_thresh032_recent17_cpr0.87_ncp40_clip0.6_sps0.00825_regimemultiplicative/"
    "mozaic_objects.glean_history_desktop.2026-10-10.pkl"
)
DEFAULT_OUTPUT_DIR = Path("research/holidays-2027/data")
HISTORY_START = "2022-01-01"


def log(message: str) -> None:
    sys.stdout.write(f"[{time.strftime('%H:%M:%S')}] {message}\n")
    sys.stdout.flush()


def tiles_of(metric_mozaic) -> list:
    """The leaf tiles of a metric mozaic (its tiles are leaves in curate_mozaics)."""
    return [tile for tile in metric_mozaic.tiles if not isinstance(tile, Mozaic)]


def single(value):
    return value[0] if isinstance(value, list) else value


def tile_forecast_rows(tile, metric: str) -> pd.DataFrame:
    impacts = getattr(tile, "forecasted_holiday_impacts", None)
    return pd.DataFrame(
        {
            "date": pd.to_datetime(tile.forecast_dates.values),
            "metric": metric,
            "country": single(tile.country),
            "os": single(tile.population),
            "proportional_effect": tile.proportional_holiday_effects.reset_index(drop=True).values,
            "forecast_detrended": tile.forecast_reconciled.median(axis=1).values,
            "holiday_impact": impacts.median(axis=1).values if impacts is not None else np.nan,
        }
    )


def tile_yearly_rows(tile, metric: str) -> pd.DataFrame:
    frame = tile._prophet_forecast
    return pd.DataFrame(
        {
            "date": pd.to_datetime(frame["ds"].values),
            "metric": metric,
            "country": single(tile.country),
            "os": single(tile.population),
            "yearly": frame["yearly"].values,
        }
    )


def country_level_view(tiles: list) -> SimpleNamespace:
    """The attributes Mozaic._fit_holiday_effects reads, summed across a country's tiles as Mozaic does."""
    def summed(attribute):
        return pd.Series(
            np.sum([getattr(t, attribute).astype(float).fillna(0.0) for t in tiles], axis=0)
        ).replace({np.nan: 0.0})

    first = tiles[0]
    return SimpleNamespace(
        historical_dates=first.historical_dates.reset_index(drop=True),
        forecast_start_date=first.forecast_start_date,
        fit_historical_data=summed("fit_historical_data"),
        holiday_detrended_historical_data=summed("holiday_detrended_historical_data"),
        holiday_calendar=pd.concat(t.holiday_calendar for t in tiles).drop_duplicates().reset_index(drop=True),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pickle", type=Path, default=DEFAULT_PICKLE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    log(f"loading {args.pickle} ({args.pickle.stat().st_size / 1e9:.2f} GB)")
    with open(args.pickle, "rb") as handle:
        mozaics = cloudpickle.load(handle)
    log(f"loaded: {type(mozaics).__name__} keys={list(mozaics)}")

    forecast_frames, observed_frames, history_frames, calendar_frames, yearly_frames = [], [], [], [], []
    for metric, metric_mozaic in mozaics.items():
        tiles = tiles_of(metric_mozaic)
        log(f"{metric}: {len(tiles)} tiles")
        forecast_frames += [tile_forecast_rows(t, metric) for t in tiles]
        yearly_frames += [tile_yearly_rows(t, metric) for t in tiles]

        countries = sorted({single(t.country) for t in tiles})
        for index, country in enumerate(countries, 1):
            country_tiles = [t for t in tiles if single(t.country) == country]
            view = country_level_view(country_tiles)
            Mozaic._fit_holiday_effects(view)
            observed = view.observed_holiday_effects.assign(metric=metric, country=country)
            observed["all_effects"] = observed["all_effects"].map(lambda xs: ";".join(f"{x:.5f}" for x in xs))
            observed_frames.append(observed)

            history = pd.DataFrame(
                {
                    "date": pd.to_datetime(view.historical_dates.values),
                    "metric": metric,
                    "country": country,
                    "observed": view.fit_historical_data.values,
                    "expected": view.holiday_detrended_historical_data.values,
                }
            )
            history_frames.append(history[history["date"] >= HISTORY_START])
            if metric == list(mozaics)[0]:
                calendar_frames.append(view.holiday_calendar.assign(country=country))
            sys.stdout.write(f"\r  {metric} country {index}/{len(countries)} {country}   ")
            sys.stdout.flush()
        sys.stdout.write("\n")

    outputs = {
        "tile_effects.csv": pd.concat(forecast_frames, ignore_index=True),
        "observed_effects.csv": pd.concat(observed_frames, ignore_index=True),
        "history.csv": pd.concat(history_frames, ignore_index=True),
        "calendar.csv": pd.concat(calendar_frames, ignore_index=True),
        "yearly.csv": pd.concat(yearly_frames, ignore_index=True),
    }
    for name, frame in outputs.items():
        frame.to_csv(args.output_dir / name, index=False)
        log(f"wrote {args.output_dir / name}: rows={len(frame)}")


if __name__ == "__main__":
    main()
