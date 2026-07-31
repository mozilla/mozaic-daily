#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Reproduce the external prototype's global-tile organic forecast from mozaic-daily's own inputs.

The prototype (`~/work/product-data-science-core/scratch/brwells/mobile_organic_aug/`) fits **three
independent single-tile Mozaics** — Fenix organic ex-IR, other apps ex-IR, and Iran — and sums them,
then stacks marketing's paid level on top. Production fits **16 countries x 4 apps and reconciles**.

This script rebuilds the prototype's *recipe* on mozaic-daily's *pinned inputs*, so the difference
between its answer and the prototype's isolates the input/plumbing terms, and the difference between
its answer and production's isolates the tile-architecture term.

Two arms, per the reproduction contract:

  --arm buggy    (default) suppresses the holiday knobs, matching the prototype. Its
                 `fit_organic_forecast` never forwards holiday_threshold / the two radii to
                 populate_tiles, nor holiday_effect_floor to Mozaic, so its published numbers ran at
                 mozaic's default -0.032 -- NOT the -0.055 its own config claims. Its reported
                 "holiday_threshold effect = 0.00%" is therefore tautological.
  --arm plumbed  forwards them properly, the way production does. The delta between the arms is what
                 the canonical holiday_threshold actually costs on this series.

## What CANNOT be reproduced from these inputs, and why it does not matter for the total

The prototype moves Farsi-locale clients geolocating outside IR from the ex-IR series into the Iran
series. Production's mobile QuerySpec has no `locale` column, so that reassignment is unavailable
here. It is a **transfer between two of the three tiles**, and the prototype's own SPEC.md notes
`ex_iran + iran = global organic` by construction -- the term the ex-IR series subtracts is exactly
the term the Iran series adds. So it cancels in the reported total, and only perturbs the two tiles'
individual fits. Named, not hidden.

Iran is also treated differently by construction: the prototype masks three outage windows out of
training, production relies on mozaic's built-in 2026 counterfactual fill. The prototype measured
the two against each other at +1.98% on Iran, which is ~0.25pp of the combined total.

Usage:
    source .venv/bin/activate
    python research/mobile-organic/reproduce_prototype.py            # buggy arm
    python research/mobile-organic/reproduce_prototype.py --arm plumbed
    python research/mobile-organic/reproduce_prototype.py --arm both --json tmp/repro.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import db_dtypes  # noqa: F401  — registers the dbdate extension dtype for read_parquet
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

import mozaic  # noqa: E402
from mozaic import Mozaic, TileSet, make_mobile_model, populate_tiles  # noqa: E402
from mozaic.holiday_smart import MobileEvents  # noqa: E402
from mozaic.models import MobileModelConfig  # noqa: E402

from mozaic_daily.organic import (  # noqa: E402
    build_share_lookup,
    load_organic_spec,
    load_split_frame,
    marketing_paid_level,
    split_training_to_organic,
)
from mozaic_daily.seam_ma import display_ma  # noqa: E402

# --- The prototype's published 4-app / canonical-params row -------------------------------------
# Source: variants/all_apps/_index.md. 4 apps is the scope that matches production; the 6-app
# headline differs by only -224 (Klar is 37 DAU/day).
PROTOTYPE_ORGANIC_DEC15 = 16_215_080
PROTOTYPE_PAID_DEC15 = 1_559_477
PROTOTYPE_TOTAL_DEC15 = 17_769_950

FORECAST_START = pd.Timestamp("2026-07-28")
FORECAST_END = "2026-12-31"
REPORT_DATE = pd.Timestamp("2026-12-15")

#: mozaic-daily's canonical mobile parameters — identical to the prototype's MOBILE_MODEL_PARAMS.
CANONICAL = dict(
    prophet_recent_weeks=13,
    prophet_changepoint_range=0.75,
    prophet_n_changepoints=25,
    holiday_threshold=-0.055,
    holiday_max_radius=5,
    holiday_min_radius=3,
    holiday_effect_floor=-0.6,
    seasonality_regime="auto",
    prophet_changepoint_prior_scale=0.035,
    prophet_seasonality_prior_scale=0.1,
)

ORGANIC_DIR = REPO_ROOT / "data-official" / "2026-08" / "organic"
RAW_MOBILE = (
    REPO_ROOT / "data-official/2026-08/mobile_organic_2026-07-28"
    "/cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1"
    "/mozaic_parts.raw.glean.mobile.DAU.parquet"
)


APP_FLAGS = ["fenix_android", "firefox_ios", "focus_android", "focus_ios"]


def fit_tile(
    train_rows: pd.DataFrame, label: str, *, plumb_holidays: bool, config: MobileModelConfig,
    apply_builtin_fills: bool = False,
) -> pd.Series:
    """Fit one Mozaic over `train_rows` and return actuals spliced to forecast, summed to a total.

    Mirrors the prototype's `fit_organic_forecast`. `train_rows` is already in mozaic's dataset
    shape (`x`, `country`, `y`, optionally the boolean app flags).

    `plumb_holidays=False` reproduces the prototype's defect: the holiday knobs never reach
    `populate_tiles` / `Mozaic`, so the fit silently runs at mozaic's defaults.

    `apply_builtin_fills` is needed only for the Iran tile, and it requires the app-flag columns to
    be present — mozaic's `splice_fill` matches the fill's schema against the dataset's, and the
    shipped `iran_2026/glean_mobile` fill is per-app. That is why Iran is fitted as four
    populations within one country rather than as a single aggregate: it is the only way to keep
    production's Iran treatment, which is the whole point of this arm.
    """
    train = train_rows[train_rows["x"] < FORECAST_START].copy()
    train["y"] = train["y"].astype(float)
    datasets = {"DAU": train}
    actuals = train.groupby("x")["y"].sum()

    tile_kwargs = {}
    moz_kwargs = {}
    if plumb_holidays:
        tile_kwargs = dict(
            holiday_threshold=config.holiday_threshold,
            holiday_max_radius=config.holiday_max_radius,
            holiday_min_radius=config.holiday_min_radius,
        )
        moz_kwargs = dict(holiday_effect_floor=config.holiday_effect_floor)

    tileset = TileSet()
    populate_tiles(
        datasets, tileset, make_mobile_model(config),
        FORECAST_START.strftime("%Y-%m-%d"), FORECAST_END,
        additional_holidays=[MobileEvents],
        data_source="glean_mobile",
        # Ex-IR tiles have no country=='IR' rows for the built-in Iran fill to replace, so leaving
        # it on would only emit a no-op warning. The IR tile DOES get it -- that is production's
        # Iran treatment, and the point of divergence from the prototype's masking.
        apply_builtin_fills=apply_builtin_fills,
        **tile_kwargs,
    )
    moz = Mozaic(tileset.fetch(metric="DAU"), forecast_model=make_mobile_model(config),
                 is_country_level=True, **moz_kwargs)
    moz.assign_holiday_effects()
    moz.aggregate_holiday_impacts_upward(use_reconciled=True)

    out = moz.to_df().set_index("submission_date").sort_index()
    col = next((c for c in ("forecast", "forecast_raw", "forecast_detrended_raw") if c in out), None)
    if col is None:
        raise RuntimeError(f"{label}: Mozaic.to_df() produced no forecast column")
    forecast = out.loc[out.index >= FORECAST_START, col].dropna()
    print(f"  [fit] {label:22s} {len(actuals):>5} train days -> {len(forecast):>4} forecast days "
          f"(holidays {'plumbed' if plumb_holidays else 'SUPPRESSED'}"
          f"{', iran fill ON' if apply_builtin_fills else ''})")
    return pd.concat([actuals, forecast]).sort_index().rename(label)


def build_tile_datasets() -> dict[str, pd.DataFrame]:
    """The prototype's three tiles, from mozaic-daily's pinned inputs, in mozaic dataset shape.

    The two ex-IR tiles are aggregated to a single global population and labelled `country="ROW"`
    (mozaic falls back to US + ROWHolidays for a global aggregate, as the prototype does). The IR
    tile keeps its per-app rows so the built-in Iran fill can splice.
    """
    spec = load_organic_spec(ORGANIC_DIR / "organic.json")
    split = load_split_frame(spec, ORGANIC_DIR)
    raw = pd.read_parquet(RAW_MOBILE).assign(x=lambda d: pd.to_datetime(d["x"]))

    excluded = spec["scope"]["exclude_countries"]
    lookup = build_share_lookup(
        split, share_column=spec["share_column"],
        training_dates=pd.DatetimeIndex(sorted(raw["x"].unique())),
        countries=sorted(set(raw["country"].unique()) - set(excluded)),
    )
    organic_rows, _ = split_training_to_organic(
        raw, share_lookup=lookup, exclude_countries=excluded)

    def as_global(rows: pd.DataFrame) -> pd.DataFrame:
        agg = rows.groupby("x", as_index=False)["y"].sum()
        agg["country"] = "ROW"
        return agg[["x", "country", "y"]]

    ex_ir = organic_rows[organic_rows["country"] != "IR"]
    # IR is not split (98.8% organic, and marketing's curve is ex-IR), so this tile is total IR DAU.
    iran = organic_rows[organic_rows["country"] == "IR"][["x", "country", "y"] + APP_FLAGS].copy()
    return {
        "fenix_organic": as_global(ex_ir[ex_ir["fenix_android"]]),
        "other_apps": as_global(ex_ir[~ex_ir["fenix_android"]]),
        "iran": iran,
    }


def run_arm(plumb_holidays: bool) -> dict:
    config = MobileModelConfig(**CANONICAL)
    print(f"\n=== arm: {'plumbed' if plumb_holidays else 'buggy (prototype-equivalent)'} ===")
    tiles = build_tile_datasets()
    fits = {
        "fenix_organic": fit_tile(tiles["fenix_organic"], "fenix organic ex-IR",
                                  plumb_holidays=plumb_holidays, config=config),
        "other_apps": fit_tile(tiles["other_apps"], "other apps ex-IR",
                               plumb_holidays=plumb_holidays, config=config),
        "iran": fit_tile(tiles["iran"], "iran (production fill)",
                         plumb_holidays=plumb_holidays, config=config,
                         apply_builtin_fills=True),
    }
    # Inner-join on date before summing, exactly as the prototype's `combine()` does. The tiles can
    # differ by a day at the head (a country x app with no rows on the very first date), and an
    # outer join would silently introduce a NaN — or, worse, a step — into the combined series.
    frame = pd.concat(
        [s.rename(key) for key, s in fits.items()], axis=1, join="inner").sort_index()
    if frame.isna().any().any():
        raise ValueError("gaps in the aligned per-tile series after the inner join")

    def ma(daily: pd.Series) -> pd.Series:
        return display_ma(pd.Series(daily.index), daily, forecast_start=FORECAST_START)

    organic_ma = ma(frame.sum(axis=1))
    ex_iran_ma = ma(frame["fenix_organic"] + frame["other_apps"])

    spec = load_organic_spec(ORGANIC_DIR / "organic.json")
    paid = marketing_paid_level(spec, ORGANIC_DIR,
                                forecast_start=FORECAST_START, forecast_end=FORECAST_END)
    organic = float(organic_ma.loc[REPORT_DATE])
    paid_dec15 = float(paid.loc[REPORT_DATE])
    return {
        "arm": "plumbed" if plumb_holidays else "buggy",
        "organic_dec15": organic,
        "paid_dec15": paid_dec15,
        "total_dec15": organic + paid_dec15,
        "ex_iran_organic_dec15": float(ex_iran_ma.loc[REPORT_DATE]),
        "iran_dec15": float(ma(frame["iran"]).loc[REPORT_DATE]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--arm", choices=["buggy", "plumbed", "both"], default="buggy")
    parser.add_argument("--json", type=Path, default=None, help="Also write results as JSON.")
    args = parser.parse_args()

    arms = {"buggy": [False], "plumbed": [True], "both": [False, True]}[args.arm]
    results = [run_arm(p) for p in arms]

    print(f"\n{'=' * 78}")
    print(f"Prototype target (4 apps, canonical params): organic {PROTOTYPE_ORGANIC_DEC15:,} + "
          f"paid {PROTOTYPE_PAID_DEC15:,} = total {PROTOTYPE_TOTAL_DEC15:,}")
    print(f"{'arm':10}{'organic':>15}{'paid':>13}{'total':>15}{'vs target':>13}{'':>8}")
    for r in results:
        gap = r["total_dec15"] - PROTOTYPE_TOTAL_DEC15
        print(f"{r['arm']:10}{r['organic_dec15']:>15,.0f}{r['paid_dec15']:>13,.0f}"
              f"{r['total_dec15']:>15,.0f}{gap:>13,.0f}{gap / PROTOTYPE_TOTAL_DEC15:>8.2%}")
    if len(results) == 2:
        d = results[1]["total_dec15"] - results[0]["total_dec15"]
        print(f"\nholiday_threshold -0.032 -> -0.055 is worth {d:+,.0f} "
              f"({d / results[0]['total_dec15']:+.2%}) on this series.")
        print("The prototype reported 0.00% for this, which was tautological — it never plumbed it.")

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps({
            "prototype_target": {
                "organic_dec15": PROTOTYPE_ORGANIC_DEC15,
                "paid_dec15": PROTOTYPE_PAID_DEC15,
                "total_dec15": PROTOTYPE_TOTAL_DEC15,
            },
            "mozaic_version": mozaic.__file__,
            "arms": results,
        }, indent=2))
        print(f"\nwrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
