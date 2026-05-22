#!/usr/bin/env python3
"""Run a desktop forecast with per-tile Prophet changepoints pinned to April's locations.

Motivation
----------
At equal Prophet config, June's per-tile fits land slightly higher on recent history
than April's did, which (a) raises the starting point of the forecast horizon and
(b) forces a steeper descending trend to reach the same far-horizon values — driving
a ~75-115k MA28 gap at Dec-15 that pure (cps, cpr, threshold) tuning cannot close.

By pinning each June tile's Prophet ``changepoints=`` to April's exact dates for that
same (country, population) tile, we hold the trend-flexibility skeleton fixed and let
only the trend slopes between changepoints adapt to June's data. This is the
"identical changepoint locations, different slopes" experiment.

Scope
-----
- Desktop, legacy_desktop DAU only (same scope as ``run_param_scan.py``).
- Pinning applies to **per-tile** Prophet fits (45 tiles in April pkl). The Mozaic-level
  rollup Prophet fit is left at its default behavior — April only saves the top-level
  Mozaic and tile-level fits, not intermediate country/population Mozaics, so we cannot
  cleanly pin every level. The tile-level fits drive the per-country forecast that the
  output aggregates.

Mechanism
---------
1. Load April pkl (``mozaic_objects.legacy_desktop.2026-04-01.pkl``) and build
   ``CHANGEPOINTS_BY_TILE = {(country, population): list[Timestamp]}``.
2. Monkey-patch ``mozaic.tile.Tile._run_forecast`` to set ``_CURRENT_TILE_KEY``
   before calling ``self.forecast_model(...)`` and clear it afterwards.
3. Build a custom factory ``desktop_forecast_pinned(...)`` that mirrors
   ``mozaic.models.desktop_forecast_model`` but, when ``_CURRENT_TILE_KEY`` is set
   and has matching changepoints in the dict, passes ``changepoints=...`` to Prophet
   (and omits ``changepoint_range`` / ``n_changepoints``, which Prophet ignores when
   ``changepoints`` is explicit).
4. Use the same ``run_param_scan.py`` plumbing (``process_data_source`` patch + raw-cache
   symlinks + ``.raw.`` state marker + sidecar meta) so outputs are directly comparable
   to existing scan results.

Usage
-----
    source .venv/bin/activate
    python scripts/run_pinned_scan.py \\
        --forecast-start-date 2026-05-17 \\
        --raw-cache-dir data-official/2026-06/desktop_cps0.15983_thresh032_recent13_clip0.6_cap426 \\
        --april-pkl data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_objects.legacy_desktop.2026-04-01.pkl \\
        --holiday-threshold -0.05 \\
        --changepoint-prior-scale 0.13

Outputs land in ``param_scan_results_pinned/<slug>/`` (separate dir from the unpinned scan
so the two sets don't get mixed up).
"""
from __future__ import annotations

import argparse
import importlib
import json
import pickle
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import prophet

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import mozaic  # noqa: E402
import mozaic.models  # noqa: E402
import mozaic.tile  # noqa: E402
from mozaic.models import DesktopModelConfig  # noqa: E402
from mozaic_daily.adjustments import insert_state_marker, write_meta  # noqa: E402
from mozaic_daily.main import (  # noqa: E402
    combine_tables,
    get_format_function,
)
from mozaic_daily.forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs  # noqa: E402
from mozaic_daily.queries import ADDITIONAL_HOLIDAYS, DataSource, Metric, Platform  # noqa: E402

run_main_module = importlib.import_module("mozaic_daily.main")
_original_process_data_source = run_main_module.process_data_source


# ---------------------------------------------------------------------------
# Module-level "current tile key" set by the patched Tile._run_forecast.
# A plain module-level variable (not threading.local) so cloudpickle can serialize
# any closures that capture _get_current_tile_key (save_mozaic_objects uses cloudpickle
# on the fitted Mozaic dict; tile.forecast_model is a closure that calls into here).
# ---------------------------------------------------------------------------

_CURRENT_TILE_KEY: Optional[tuple] = None


def _set_current_tile_key(key: Optional[tuple]) -> None:
    global _CURRENT_TILE_KEY
    _CURRENT_TILE_KEY = key


def _get_current_tile_key() -> Optional[tuple]:
    return _CURRENT_TILE_KEY


_orig_tile_run = mozaic.tile.Tile._run_forecast


def _patched_tile_run(self):
    _set_current_tile_key((self.country, self.population))
    try:
        _orig_tile_run(self)
    finally:
        _set_current_tile_key(None)


def install_tile_patch() -> None:
    mozaic.tile.Tile._run_forecast = _patched_tile_run


def uninstall_tile_patch() -> None:
    mozaic.tile.Tile._run_forecast = _orig_tile_run


# ---------------------------------------------------------------------------
# Load April's per-tile changepoints
# ---------------------------------------------------------------------------

def load_april_tile_changepoints(april_pkl: Path) -> dict[tuple, list[pd.Timestamp]]:
    """Returns {(country, population): [list of changepoint Timestamps]} for DAU tiles."""
    with open(april_pkl, "rb") as f:
        apr = pickle.load(f)
    if "DAU" not in apr:
        raise ValueError(f"April pkl missing 'DAU' key. Keys: {list(apr.keys())}")
    mosaic_obj = apr["DAU"]
    out: dict[tuple, list[pd.Timestamp]] = {}
    for tile in mosaic_obj.tiles:
        key = (tile.country, tile.population)
        cps = list(pd.to_datetime(tile._prophet_model.changepoints))
        out[key] = cps
    return out


# ---------------------------------------------------------------------------
# Custom pinned forecast factory
# ---------------------------------------------------------------------------

def _build_pinned_factory(
    config: DesktopModelConfig,
    changepoints_by_tile: dict[tuple, list[pd.Timestamp]],
    unmatched_tile_log: list,
):
    """Return a (data, historical_dates, forecast_dates) -> (samples, model, forecast) function.

    Mirrors ``mozaic.models.desktop_forecast_model`` line-for-line on data handling, but
    looks up ``changepoints`` from ``changepoints_by_tile`` keyed by the tile context
    set by the Tile._run_forecast patch. Falls back to default (changepoint_range,
    n_changepoints) when no tile context is present (e.g. Mozaic-level rollup fits).
    """
    recent_weeks = config.prophet_recent_weeks
    cps_scale = config.prophet_changepoint_prior_scale
    cp_range = config.prophet_changepoint_range
    n_cp = config.prophet_n_changepoints

    def model(historical_data, historical_dates, forecast_dates):
        # ---- params dict (copied from desktop_forecast_model) ----
        params = {
            "daily_seasonality": False,
            "weekly_seasonality": False,
            "yearly_seasonality": True,
            "uncertainty_samples": 1000,
            "seasonality_prior_scale": 0.00825,
            "changepoint_prior_scale": cps_scale,
            "growth": "logistic",
        }

        tile_key = _get_current_tile_key()
        if tile_key is not None and tile_key in changepoints_by_tile:
            # Pinned: pass explicit changepoints; Prophet ignores n_changepoints / changepoint_range
            params["changepoints"] = changepoints_by_tile[tile_key]
        else:
            # Default (unpinned) behavior. Mozaic-level rollup fits land here.
            params["changepoint_range"] = cp_range
            params["n_changepoints"] = n_cp
            if tile_key is not None:
                unmatched_tile_log.append(tile_key)

        # ---- rest of desktop_forecast_model (verbatim, no edits) ----
        x = historical_data
        if (x.abs().corr(x.diff().abs()) or 0) > 0.0:
            params["seasonality_mode"] = "multiplicative"
            params["growth"] = "linear"
            # Remove logistic-only params if we flipped to linear after they were set
            params.pop("changepoint_range", None)  # harmless under linear too, but be tidy
            # (n_changepoints / changepoints stay; both are valid under linear growth)

        if (len(x.dropna()) > (365 * 2)) and (
            np.quantile(x.dropna(), 0.5) / (np.quantile(x.dropna(), 0.1) + 1e-8) < 5
        ):
            params["yearly_seasonality"] = True

        historical_mask = historical_dates < forecast_dates[0]
        observed = (
            pd.DataFrame(
                {
                    "ds": historical_dates[historical_mask],
                    "y": historical_data[historical_mask],
                }
            )
            .dropna()
            .reset_index(drop=True)
            .copy(deep=True)
        )
        future = pd.DataFrame({"ds": forecast_dates})

        if params["growth"] == "logistic":
            cap = observed["y"].tail(426).max() * 1.05
            if cap > 100e6:
                floor = observed["y"].tail(426).min() * 1
            else:
                floor = observed["y"].tail(426).min() * 0.92
            observed["cap"] = cap
            observed["floor"] = floor
            future["cap"] = cap
            future["floor"] = floor
        else:
            with np.errstate(invalid="ignore"):
                observed["y"] = np.log(observed["y"] + 1.0)

        np.random.seed(42)
        m = prophet.Prophet(**params)

        # Pull in the conditional weekly seasonality helper directly (private name)
        from mozaic.models import _add_conditional_weekly_seasonality
        observed, future = _add_conditional_weekly_seasonality(
            m, observed, future, forecast_dates[0], recent_weeks=recent_weeks
        )
        m.fit(observed)

        prophet_forecast = m.predict(future)
        predictive_samples = pd.DataFrame(m.predictive_samples(future)["yhat"])

        if params["growth"] == "linear":
            predictive_samples = np.exp(predictive_samples) - 1

        predictive_samples[predictive_samples < 0] = 0
        prophet_forecast = prophet_forecast.drop(
            columns=["is_historical", "is_recent"], errors="ignore"
        )
        return predictive_samples, m, prophet_forecast

    return model


# ---------------------------------------------------------------------------
# process_data_source patch (use pinned factory for desktop only)
# ---------------------------------------------------------------------------

def _make_process_data_source(pinned_factory):
    def patched(
        data_source: DataSource,
        datasets,
        forecast_start,
        forecast_end,
        training_end_date=None,
        marketing_spec_path=None,
    ):
        platform = data_source.platform
        source = data_source.telemetry_source
        source_data = datasets[platform.value][source.value]
        additional_holidays = ADDITIONAL_HOLIDAYS.get(data_source, [])

        if platform == Platform.DESKTOP:
            # Bypass make_desktop_model; provide our pinned factory directly.
            # forecast.py's get_desktop_forecast_dfs builds a TileSet etc. — we need
            # to mimic that path with our factory.
            # Easiest: call mozaic.populate_tiles + curate_mozaics ourselves with our factory.
            forecast_result = _run_desktop_with_factory(
                source_data, forecast_start, forecast_end, pinned_factory,
                additional_holidays=additional_holidays,
            )
        else:
            # Shouldn't happen with our filter, but be safe.
            forecast_result = get_mobile_forecast_dfs(
                source_data, forecast_start, forecast_end,
                additional_holidays=additional_holidays,
            )

        df_combined = combine_tables(forecast_result["dfs"])
        format_func = get_format_function(platform)
        format_func(df_combined, data_source=data_source.value)
        return df_combined, forecast_result["mozaics"]

    return patched


def _run_desktop_with_factory(
    datasets, forecast_start, forecast_end, forecast_model, additional_holidays
):
    """Stripped-down version of get_desktop_forecast_dfs that uses our pinned factory.

    We can't just pass our factory through make_desktop_model + get_desktop_forecast_dfs
    because get_desktop_forecast_dfs builds its own factory from config. So we copy the
    minimal path: TileSet, populate_tiles, curate_mozaics, to_granular_forecast_df.
    """
    from collections import defaultdict
    import warnings
    from mozaic import TileSet, Mozaic
    from mozaic_daily.forecast import _check_data_health
    from mozaic_daily.config import FORECAST_CONFIG

    quantile = FORECAST_CONFIG["quantile"]
    if additional_holidays is None:
        additional_holidays = []

    _check_data_health(datasets)

    tileset = TileSet()
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", category=RuntimeWarning,
            message=".*divide by zero.*|.*overflow.*|.*invalid value.*",
        )
        # Use the same kwargs run_param_scan uses (thresh override comes from outer logic).
        mozaic.populate_tiles(
            datasets, tileset, forecast_model, forecast_start, forecast_end,
            additional_holidays=additional_holidays,
            holiday_threshold=PINNED_RUN_HOLIDAY_THRESHOLD,
            holiday_max_radius=PINNED_RUN_HOLIDAY_MAX_RADIUS,
            holiday_min_radius=PINNED_RUN_HOLIDAY_MIN_RADIUS,
        )

    mozaics = {}
    country_mozaics = defaultdict(lambda: defaultdict(Mozaic))
    population_mozaics = defaultdict(lambda: defaultdict(Mozaic))
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", category=RuntimeWarning,
            message=".*divide by zero.*|.*overflow.*|.*invalid value.*",
        )
        mozaic.utils.curate_mozaics(
            datasets, tileset, forecast_model, mozaics,
            country_mozaics, population_mozaics,
            holiday_effect_floor=PINNED_RUN_HOLIDAY_EFFECT_FLOOR,
        )

    dfs = {metric: moz.to_granular_forecast_df(quantile=quantile)
           for metric, moz in mozaics.items()}
    return {"dfs": dfs, "mozaics": mozaics}


# Globals set at run-time so the patched process_data_source can see them
PINNED_RUN_HOLIDAY_THRESHOLD = -0.032
PINNED_RUN_HOLIDAY_MAX_RADIUS = 5
PINNED_RUN_HOLIDAY_MIN_RADIUS = 3
PINNED_RUN_HOLIDAY_EFFECT_FLOOR = -0.6


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--forecast-start-date", default="2026-05-17")
    p.add_argument("--raw-cache-dir", type=Path, required=True,
                   help="Dir containing mozaic_parts.raw.legacy.desktop.*.parquet")
    p.add_argument("--april-pkl", type=Path,
                   default=REPO_ROOT / "data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_objects.legacy_desktop.2026-04-01.pkl",
                   help="April per-tile-changepoints pkl")
    p.add_argument("--results-dir", type=Path,
                   default=REPO_ROOT / "param_scan_results_pinned")
    p.add_argument("--changepoint-prior-scale", type=float, default=None)
    p.add_argument("--recent-weeks", type=int, default=None)
    p.add_argument("--holiday-threshold", type=float, default=None)
    p.add_argument("--holiday-max-radius", type=int, default=None)
    p.add_argument("--holiday-min-radius", type=int, default=None)
    p.add_argument("--holiday-effect-floor", type=float, default=None)
    return p.parse_args()


def build_config(args) -> DesktopModelConfig:
    overrides = {}
    if args.changepoint_prior_scale is not None:
        overrides["prophet_changepoint_prior_scale"] = args.changepoint_prior_scale
    if args.recent_weeks is not None:
        overrides["prophet_recent_weeks"] = args.recent_weeks
    if args.holiday_threshold is not None:
        overrides["holiday_threshold"] = args.holiday_threshold
    if args.holiday_max_radius is not None:
        overrides["holiday_max_radius"] = args.holiday_max_radius
    if args.holiday_min_radius is not None:
        overrides["holiday_min_radius"] = args.holiday_min_radius
    if args.holiday_effect_floor is not None:
        overrides["holiday_effect_floor"] = args.holiday_effect_floor
    return DesktopModelConfig(**overrides)


def symlink_raw_cache(raw_cache_dir: Path, slug_dir: Path) -> None:
    raw_name = "mozaic_parts.raw.legacy.desktop.DAU.parquet"
    src = raw_cache_dir / raw_name
    if not src.exists():
        raise FileNotFoundError(f"Raw cache missing: {src}")
    dst = slug_dir / raw_name
    if dst.exists() or dst.is_symlink():
        if dst.is_symlink() and dst.resolve() == src.resolve():
            return
        dst.unlink()
    dst.symlink_to(src.resolve())


def main_cli() -> None:
    global PINNED_RUN_HOLIDAY_THRESHOLD, PINNED_RUN_HOLIDAY_MAX_RADIUS
    global PINNED_RUN_HOLIDAY_MIN_RADIUS, PINNED_RUN_HOLIDAY_EFFECT_FLOOR

    args = parse_args()
    config = build_config(args)
    slug = config.to_slug() + "_pinned"
    slug_dir = args.results_dir / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    PINNED_RUN_HOLIDAY_THRESHOLD = config.holiday_threshold
    PINNED_RUN_HOLIDAY_MAX_RADIUS = config.holiday_max_radius
    PINNED_RUN_HOLIDAY_MIN_RADIUS = config.holiday_min_radius
    PINNED_RUN_HOLIDAY_EFFECT_FLOOR = config.holiday_effect_floor

    print("=" * 60, f"\nPinned param scan run\n", "=" * 60)
    print(f"Forecast start: {args.forecast_start_date}")
    print(f"Slug:           {slug}")
    print(f"Output dir:     {slug_dir}")
    print(f"Raw cache:      {args.raw_cache_dir}")
    print(f"April pkl:      {args.april_pkl}")
    print(f"Config:         {json.dumps(config.to_dict(), indent=2)}")

    # Load April changepoints
    print("\nLoading April per-tile changepoints ...")
    cps_by_tile = load_april_tile_changepoints(args.april_pkl)
    print(f"  Got changepoints for {len(cps_by_tile)} tiles "
          f"(sample: {next(iter(cps_by_tile.keys()))} -> {len(next(iter(cps_by_tile.values())))} dates)")

    # Save parameters.json
    (slug_dir / "parameters.json").write_text(json.dumps({
        "forecast_start_date": args.forecast_start_date,
        "slug": slug,
        "pinned_to_april_pkl": str(args.april_pkl),
        "n_pinned_tiles": len(cps_by_tile),
        "config": config.to_dict(),
    }, indent=2))

    symlink_raw_cache(args.raw_cache_dir, slug_dir)

    # Install patches
    install_tile_patch()
    unmatched_log: list = []
    pinned_factory = _build_pinned_factory(config, cps_by_tile, unmatched_log)
    run_main_module.process_data_source = _make_process_data_source(pinned_factory)

    try:
        run_main_module.main(
            checkpoints=True,
            data_source_filter={DataSource.LEGACY_DESKTOP},
            metric_filter={Metric.DAU},
            forecast_start_date=args.forecast_start_date,
            output_dir=str(slug_dir),
        )
    finally:
        run_main_module.process_data_source = _original_process_data_source
        uninstall_tile_patch()

    if unmatched_log:
        unique_unmatched = sorted(set(unmatched_log))
        print(f"\n[warn] {len(unique_unmatched)} unique tile keys had no April match "
              f"(fell back to changepoint_range/n_changepoints): {unique_unmatched}")
    else:
        print("\nAll tile fits used pinned April changepoints.")

    # Rename to .raw. + sidecar meta
    unmarked = slug_dir / f"mozaic_daily_forecast.{args.forecast_start_date}.ld-D.parquet"
    if not unmarked.exists():
        raise FileNotFoundError(f"Expected output missing: {unmarked}")
    target = insert_state_marker(unmarked, [])
    unmarked.rename(target)
    write_meta(
        target,
        forecast_start_date=args.forecast_start_date,
        data_source="legacy_desktop",
        produced_by="scripts/run_pinned_scan.py",
        model_config=config.to_dict(),
        adjustments_applied=[],
        extra={
            "pinned_to_april_pkl": str(args.april_pkl),
            "n_pinned_tiles": len(cps_by_tile),
            "n_unmatched_tile_keys": len(set(unmatched_log)),
        },
    )
    print(f"\nDone. Output: {target}")


if __name__ == "__main__":
    main_cli()
