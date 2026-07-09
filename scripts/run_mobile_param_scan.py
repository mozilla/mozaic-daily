#!/usr/bin/env python3
"""Run one mobile (glean_mobile DAU) forecast with a configurable MobileModelConfig.

Mobile analog of ``scripts/run_param_scan.py``. Two differences from the desktop
scan matter:

1. **Marketing-lift is applied in-pipeline.** The July mobile headline number is
   measured *with* the marketing tailwind (`m`), so this runner threads the
   marketing spec through a patched ``process_data_source`` that mirrors the real
   mobile+marketing branch of ``mozaic_daily.main.process_data_source`` — subtract
   lift from Fenix training rows before mozaic, add it back to the per-tile
   forecast after — but swaps the forecast call for ``get_mobile_forecast_dfs``
   with our ``MobileModelConfig`` injected. Output carries the ``.adj-m.`` marker.
2. **The all-level Iran fill is kept.** ``data_source="glean_mobile"`` is forwarded
   so the built-in Iran-2026 counterfactual fill is auto-applied, matching the
   canonical July build.

The pre-flight BigQuery data-availability check is patched to a no-op: when a raw
cache is symlinked in, the training data was already fetched when that cache was
built, so the check is redundant (and would otherwise require live gcloud creds
for every scan cell).

Usage
-----
    source .venv/bin/activate
    python scripts/run_mobile_param_scan.py \\
        --forecast-start-date 2026-06-29 \\
        --raw-cache-dir tmp/mobile_holidayskip_2026-06-29 \\
        --results-dir research/param-scans/mobile-july/results

Override one or more knobs (any unspecified field falls back to the package
default = the June mobile params):

    python scripts/run_mobile_param_scan.py \\
        --forecast-start-date 2026-06-29 \\
        --raw-cache-dir tmp/mobile_holidayskip_2026-06-29 \\
        --changepoint-prior-scale 0.025

Outputs
-------
``<results-dir>/<slug>/``
    parameters.json
    mozaic_parts.raw.glean.mobile.DAU.parquet            (symlinked from raw-cache-dir)
    mozaic_daily_forecast.<date>.gm-D.adj-m.parquet      (forecast, marketing applied)
    mozaic_daily_forecast.<date>.gm-D.adj-m.parquet.meta.json
    mozaic_objects.glean_mobile.<date>.pkl               (fitted mozaics, for decomposition)
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from mozaic.models import MobileModelConfig  # noqa: E402

run_main_module = importlib.import_module("mozaic_daily.main")  # noqa: E402
from mozaic_daily.main import (  # noqa: E402
    combine_tables,
    get_format_function,
    _apply_marketing_lift_pre_mozaic,
    _find_marketing_spec_for_forecast,
)
from mozaic_daily.adjustments import (  # noqa: E402
    add_marketing_lift_to_forecast,
    insert_state_marker,
    write_meta,
)
from mozaic_daily.forecast import get_mobile_forecast_dfs  # noqa: E402
from mozaic_daily.queries import ADDITIONAL_HOLIDAYS, DataSource, Metric  # noqa: E402


# ---------------------------------------------------------------------------
# Config injection (mirrors main.process_data_source mobile + marketing branch)
# ---------------------------------------------------------------------------

def _make_process_data_source_with_config(mobile_config: MobileModelConfig):
    """Wrap ``process_data_source`` so the mobile forecast uses our config.

    This is a faithful copy of the mobile path of
    ``mozaic_daily.main.process_data_source`` (marketing subtract-before /
    add-back around the mozaic call) with a single change: the forecast call is
    ``get_mobile_forecast_dfs(..., config=mobile_config)`` instead of the
    config-less ``forecast_func``. Kept in sync with main.py by construction.
    """

    def patched(
        data_source: DataSource,
        datasets,
        forecast_start,
        forecast_end,
        training_end_date=None,
        marketing_spec_path=None,
        lol_spec_path=None,
        mozillaonline_spec_path=None,
    ):
        # lol_spec_path / mozillaonline_spec_path are the desktop `l`/`o` overlays,
        # threaded through by main.generate_forecasts. This scan is glean_mobile-only,
        # so — exactly like the real process_data_source — they are no-ops here and are
        # accepted purely for signature parity. Kept in sync with main.py by construction.
        import pandas as pd

        platform = data_source.platform
        source = data_source.telemetry_source
        source_data = datasets[platform.value][source.value]

        # Normalize the date column (defensive, matches main.py).
        source_data = {
            metric: (df.assign(x=pd.to_datetime(df["x"]))
                     if isinstance(df, pd.DataFrame) and "x" in df.columns else df)
            for metric, df in source_data.items()
        }

        marketing_context = None
        if marketing_spec_path is not None and data_source == DataSource.GLEAN_MOBILE \
                and Metric.DAU.value in source_data:
            if training_end_date is None:
                raise ValueError("training_end_date is required when marketing_spec_path is set")
            source_data, marketing_context = _apply_marketing_lift_pre_mozaic(
                source_data, marketing_spec_path, training_end_date
            )

        additional_holidays = ADDITIONAL_HOLIDAYS.get(data_source, [])
        forecast_result = get_mobile_forecast_dfs(
            source_data,
            forecast_start,
            forecast_end,
            additional_holidays=additional_holidays,
            config=mobile_config,
            data_source=data_source.value,
        )

        df_combined = combine_tables(forecast_result.dfs)

        if marketing_context is not None and Metric.DAU.value in df_combined.columns:
            df_combined = add_marketing_lift_to_forecast(
                df_combined,
                daily_lift_series=marketing_context["daily_lift_series"],
                country_shares=marketing_context["country_shares"],
                forecast_start=pd.Timestamp(forecast_start),
                metric_column=Metric.DAU.value,
                app_population_value=marketing_context["spec"]["allocation"]["app_flag_column"],
            )
            n_total = len(df_combined)
            n_forecast = (df_combined["source"] == "forecast").sum()
            print(f"Marketing-lift: added back across {n_total} rows "
                  f"({n_forecast} forecast + {n_total - n_forecast} training/actual)")

        format_func = get_format_function(platform)
        format_func(df_combined, data_source=data_source.value)
        return df_combined, forecast_result.mozaics

    return patched


# ---------------------------------------------------------------------------
# Raw cache wiring
# ---------------------------------------------------------------------------

RAW_CHECKPOINT_FILES = [
    "mozaic_parts.raw.glean.mobile.DAU.parquet",
]


def symlink_raw_cache(raw_cache_dir: Path, slug_dir: Path) -> list[Path]:
    """Symlink raw BQ parquets from raw_cache_dir into slug_dir (skip BigQuery)."""
    created = []
    for filename in RAW_CHECKPOINT_FILES:
        source = raw_cache_dir / filename
        if not source.exists():
            raise FileNotFoundError(
                f"Raw checkpoint missing in --raw-cache-dir: {source}\n"
                f"Point --raw-cache-dir at a directory that already contains "
                f"{filename} (e.g. tmp/mobile_holidayskip_2026-06-29)."
            )
        dest = slug_dir / filename
        if dest.exists() or dest.is_symlink():
            if dest.is_symlink() and dest.resolve() == source.resolve():
                created.append(dest)
                continue
            dest.unlink()
        dest.symlink_to(source.resolve())
        created.append(dest)
    return created


# ---------------------------------------------------------------------------
# State marker + sidecar meta
# ---------------------------------------------------------------------------

def stamp_adjm_marker_and_meta(
    slug_dir: Path,
    forecast_start_date: str,
    config: MobileModelConfig,
    marketing_spec_path: Path,
) -> Path:
    """Rename the unmarked mobile forecast parquet to ``.adj-m.`` and write its sidecar."""
    unmarked = slug_dir / f"mozaic_daily_forecast.{forecast_start_date}.gm-D.parquet"
    if not unmarked.exists():
        raise FileNotFoundError(f"Expected forecast parquet not found after main(): {unmarked}")
    target = insert_state_marker(unmarked, ["m"])
    unmarked.rename(target)
    write_meta(
        target,
        forecast_start_date=forecast_start_date,
        data_source="glean_mobile",
        produced_by="scripts/run_mobile_param_scan.py",
        model_config=config.to_dict(),
        adjustments_applied=[{
            "code": "m",
            "name": "marketing_lift",
            "scope": "glean_mobile DAU only",
            "spec_file": str(marketing_spec_path.relative_to(REPO_ROOT))
            if marketing_spec_path.is_absolute() else str(marketing_spec_path),
        }],
    )
    return target


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_config(args: argparse.Namespace) -> MobileModelConfig:
    """Build a MobileModelConfig from CLI args, omitting any unspecified knob."""
    overrides = {}
    if args.changepoint_prior_scale is not None:
        overrides["prophet_changepoint_prior_scale"] = args.changepoint_prior_scale
    if args.changepoint_range is not None:
        overrides["prophet_changepoint_range"] = args.changepoint_range
    if args.n_changepoints is not None:
        overrides["prophet_n_changepoints"] = args.n_changepoints
    if args.recent_weeks is not None:
        overrides["prophet_recent_weeks"] = args.recent_weeks
    if args.holiday_threshold is not None:
        overrides["holiday_threshold"] = args.holiday_threshold
    if args.holiday_effect_floor is not None:
        overrides["holiday_effect_floor"] = args.holiday_effect_floor
    return MobileModelConfig(**overrides)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--forecast-start-date", default="2026-06-29",
                        help="Forecast start date (YYYY-MM-DD). Default 2026-06-29 (July cycle).")
    parser.add_argument("--raw-cache-dir", type=Path, default=None,
                        help="Directory with existing mozaic_parts.raw.glean.mobile.DAU.parquet "
                             "(symlinked in to skip BigQuery).")
    parser.add_argument("--results-dir", type=Path,
                        default=REPO_ROOT / "research/param-scans/mobile-july/results",
                        help="Root directory for all scan runs.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print resolved config + target dir, then exit.")

    g = parser.add_argument_group("Prophet knobs")
    g.add_argument("--changepoint-prior-scale", type=float, default=None,
                   help="MobileModelConfig.prophet_changepoint_prior_scale (default 0.02)")
    g.add_argument("--changepoint-range", type=float, default=None,
                   help="MobileModelConfig.prophet_changepoint_range (default 0.82)")
    g.add_argument("--n-changepoints", type=int, default=None,
                   help="MobileModelConfig.prophet_n_changepoints (default 25)")
    g.add_argument("--recent-weeks", type=int, default=None,
                   help="MobileModelConfig.prophet_recent_weeks (default 13)")

    g = parser.add_argument_group("Holiday knobs")
    g.add_argument("--holiday-threshold", type=float, default=None,
                   help="MobileModelConfig.holiday_threshold (default -0.032)")
    g.add_argument("--holiday-effect-floor", type=float, default=None,
                   help="MobileModelConfig.holiday_effect_floor (default -0.6)")

    return parser.parse_args()


def main_cli() -> None:
    args = parse_args()

    config = build_config(args)
    slug = config.to_slug()
    slug_dir = args.results_dir / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    marketing_spec_path = _find_marketing_spec_for_forecast(args.forecast_start_date)
    if marketing_spec_path is None:
        raise SystemExit(
            f"No marketing.json spec found for forecast_start={args.forecast_start_date}. "
            f"The mobile scan measures the with-marketing (adj-m) headline, so a spec is required."
        )

    print("=" * 60)
    print("Mobile param scan run")
    print("=" * 60)
    print(f"Forecast start date : {args.forecast_start_date}")
    print(f"Slug                : {slug}")
    print(f"Output dir          : {slug_dir}")
    print(f"Raw cache dir       : {args.raw_cache_dir}")
    print(f"Marketing spec      : {marketing_spec_path}")
    print(f"Config              : {json.dumps(config.to_dict(), indent=2)}")

    params_path = slug_dir / "parameters.json"
    params_path.write_text(json.dumps({
        "forecast_start_date": args.forecast_start_date,
        "slug": slug,
        "config": config.to_dict(),
        "marketing_spec": str(marketing_spec_path),
    }, indent=2))
    print(f"Wrote {params_path}")

    if args.raw_cache_dir is not None:
        created = symlink_raw_cache(args.raw_cache_dir, slug_dir)
        print(f"Symlinked raw cache files: {[p.name for p in created]}")
    else:
        print("No --raw-cache-dir provided; BigQuery will be queried fresh.")

    if args.dry_run:
        print("\n[dry-run] Skipping actual forecast.")
        return

    # Inject config via process_data_source patch; skip the redundant BQ pre-flight
    # (raw cache is authoritative — training data already landed when it was built).
    original_pds = run_main_module.process_data_source
    original_preflight = run_main_module.check_training_data_availability
    run_main_module.process_data_source = _make_process_data_source_with_config(config)
    run_main_module.check_training_data_availability = lambda *a, **k: None
    try:
        run_main_module.main(
            checkpoints=True,
            data_source_filter={DataSource.GLEAN_MOBILE},
            metric_filter={Metric.DAU},
            forecast_start_date=args.forecast_start_date,
            output_dir=str(slug_dir),
            marketing_lift=True,
        )
    finally:
        run_main_module.process_data_source = original_pds
        run_main_module.check_training_data_availability = original_preflight

    adjm_path = stamp_adjm_marker_and_meta(
        slug_dir, args.forecast_start_date, config, marketing_spec_path
    )
    print(f"Renamed forecast to: {adjm_path}")
    print(f"Wrote sidecar meta:  {adjm_path}.meta.json")
    print(f"\nDone. Results in: {slug_dir}")


if __name__ == "__main__":
    main_cli()
