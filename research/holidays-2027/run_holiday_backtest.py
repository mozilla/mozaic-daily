"""Holdout test of the holiday-effect calendar variants on dates the locked build has actuals for.

Fits holiday effects with ``Mozaic._fit_holiday_effects`` on history before ``--holdout-start``,
predicts the holdout with ``Mozaic._predict_holiday_effects``, and writes, per (metric, country,
variant, date), the predicted proportional effect beside the realized one,
``observed / expected - 1`` (the quantity the effect exists to reproduce: the forecast is the
detrended forecast times ``1 + effect``). Variants match ``run_counterfactual_fits.py``.

Writes ``data/backtest_effects.csv``. No model run.
"""

import argparse
from pathlib import Path

import cloudpickle
import pandas as pd

from extract_holiday_effects import DEFAULT_PICKLE, country_level_view, log, single, tiles_of
from holiday_calendar import OUTAGE_LABEL, explode_labels, tag_labels
from run_counterfactual_fits import predicted_effects, rebuild_calendar

OUTPUT = Path("research/holidays-2027/data/backtest_effects.csv")
DEFAULT_HOLDOUT_START = "2025-12-01"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pickle", type=Path, default=DEFAULT_PICKLE)
    parser.add_argument("--holdout-start", default=DEFAULT_HOLDOUT_START)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()

    log(f"loading {args.pickle}")
    with open(args.pickle, "rb") as handle:
        mozaics = cloudpickle.load(handle)

    holdout_start = pd.Timestamp(args.holdout_start)
    frames = []
    for metric, metric_mozaic in mozaics.items():
        tiles = tiles_of(metric_mozaic)
        for country in sorted({single(t.country) for t in tiles}):
            view = country_level_view([t for t in tiles if single(t.country) == country])
            dates = pd.to_datetime(view.historical_dates)
            holdout_dates = pd.DatetimeIndex(dates[dates >= holdout_start])
            view.forecast_start_date = holdout_start
            view.forecast_end_date = holdout_dates.max()
            view.forecast_dates = pd.Series(holdout_dates)
            view.holiday_effect_floor = metric_mozaic.holiday_effect_floor

            in_holdout = (dates >= holdout_start).values
            observed = view.fit_historical_data[in_holdout].values
            expected = view.holiday_detrended_historical_data[in_holdout].values

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
                frames.append(pd.DataFrame({
                    "date": holdout_dates, "metric": metric, "country": country, "variant": variant,
                    "predicted_effect": predicted_effects(view, calendar).values,
                    "realized_effect": observed / expected - 1,
                    "expected": expected,
                }))
        log(f"{metric}: done")

    result = pd.concat(frames, ignore_index=True)
    result.to_csv(args.output, index=False)
    log(f"wrote {args.output}: rows={len(result)}")


if __name__ == "__main__":
    main()
