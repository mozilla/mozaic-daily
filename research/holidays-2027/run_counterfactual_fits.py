"""Re-fit the locked build's holiday effects under calendar variants, with mozaic's own code.

For every (metric, country) of LOCK.md's build 5 this rebuilds the country-level view the model
used and calls ``Mozaic._fit_holiday_effects`` / ``Mozaic._predict_holiday_effects`` on it with:

- ``as_built``        the calendar exactly as built (reproduces the build; asserted)
- ``no_duplicates``   same-day duplicate labels removed (see ``holiday_calendar.tag_labels``)
- ``no_outage_label`` the 2025 Legacy-outage holiday run removed from the effect fit only
- ``both``            both changes

Detrended actuals are untouched, so a variant isolates how effects are *attributed*, not how the
dips were smoothed. Writes ``data/counterfactual_effects.csv`` (forecast dates only). No model run.
"""

import argparse
import sys
import time
import warnings
from pathlib import Path

import cloudpickle
import numpy as np
import pandas as pd
from mozaic import Mozaic

from extract_holiday_effects import DEFAULT_PICKLE, country_level_view, log, single, tiles_of
from holiday_calendar import OUTAGE_LABEL, explode_labels, tag_labels

OUTPUT = Path("research/holidays-2027/data/counterfactual_effects.csv")


def rebuild_calendar(calendar: pd.DataFrame, drop: pd.DataFrame) -> pd.DataFrame:
    """Calendar rows with the (date, name) labels in ``drop`` removed; empty rows disappear."""
    country = calendar["country"].iloc[0]
    labels = explode_labels(calendar)
    keep = labels.merge(drop[["date", "name"]], how="left", on=["date", "name"], indicator=True)
    keep = keep[keep["_merge"] == "left_only"]
    rebuilt = (
        keep.assign(holiday=country + " " + keep["name"])
        .groupby("date")["holiday"].agg("; ".join).reset_index()
        .rename(columns={"date": "submission_date"})
        .assign(country=country)
    )
    return rebuilt[["submission_date", "holiday", "country"]]


def predicted_effects(view, calendar: pd.DataFrame) -> pd.Series:
    view.holiday_calendar = calendar
    Mozaic._fit_holiday_effects(view)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        Mozaic._predict_holiday_effects(view)
    return view.proportional_holiday_effects.reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pickle", type=Path, default=DEFAULT_PICKLE)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()

    log(f"loading {args.pickle}")
    with open(args.pickle, "rb") as handle:
        mozaics = cloudpickle.load(handle)

    frames = []
    for metric, metric_mozaic in mozaics.items():
        tiles = tiles_of(metric_mozaic)
        countries = sorted({single(t.country) for t in tiles})
        for index, country in enumerate(countries, 1):
            country_tiles = [t for t in tiles if single(t.country) == country]
            view = country_level_view(country_tiles)
            view.forecast_end_date = country_tiles[0].forecast_end_date
            view.forecast_dates = country_tiles[0].forecast_dates
            view.holiday_effect_floor = metric_mozaic.holiday_effect_floor

            built = view.holiday_calendar.copy()
            tagged = tag_labels(explode_labels(built))
            duplicates = tagged[tagged["global_duplicate"]]
            outage = tagged[tagged["name"].str.contains(OUTAGE_LABEL)]
            variants = {
                "as_built": built,
                "no_duplicates": rebuild_calendar(built, duplicates),
                "no_outage_label": rebuild_calendar(built, outage),
                "both": rebuild_calendar(built, pd.concat([duplicates, outage])),
            }
            for variant, calendar in variants.items():
                effects = predicted_effects(view, calendar)
                if variant == "as_built":
                    built_effects = country_tiles[0].proportional_holiday_effects.reset_index(drop=True)
                    gap = float(np.abs(effects.values - built_effects.values).max())
                    assert gap < 1e-12, f"as_built does not reproduce build: {metric} {country} max gap {gap}"
                frames.append(pd.DataFrame({
                    "date": pd.to_datetime(view.forecast_dates.values),
                    "metric": metric, "country": country, "variant": variant,
                    "proportional_effect": effects.values,
                }))
            sys.stdout.write(f"\r  {metric} {index}/{len(countries)} {country}   ")
            sys.stdout.flush()
        sys.stdout.write("\n")

    result = pd.concat(frames, ignore_index=True)
    result.to_csv(args.output, index=False)
    log(f"wrote {args.output}: rows={len(result)}")


if __name__ == "__main__":
    main()
