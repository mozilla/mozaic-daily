#!/usr/bin/env python3
"""Run one mobile (glean_mobile DAU) forecast with a configurable MobileModelConfig.

Mobile analog of ``scripts/run_param_scan.py``. Two things differ from the desktop scan:

1. **The paid adjustment is applied in-pipeline.** The mobile headline is measured *with* the
   paid treatment, so whichever adjustment the cycle's specs gate on is applied by ``main()``
   itself and the output carries the matching state marker — ``.adj-p.`` for the paid/organic
   split (2026-08 onward) or ``.adj-m.`` for the retired marketing-lift overlay.
2. **The all-level Iran fill is kept.** ``data_source="glean_mobile"`` is forwarded so the
   built-in Iran-2026 counterfactual fill is auto-applied.

The config reaches Prophet via ``main(model_configs=...)``. **This runner used to monkeypatch
``process_data_source`` with a hand-copied mobile branch**, which meant every change to the real
mobile path had to be mirrored here or the scan would silently forecast with stale code. That
duplication is gone: ``main()`` now threads ``model_configs`` through, so there is one code path.

The pre-flight BigQuery data-availability check is still patched to a no-op: when a raw cache is
symlinked in, the training data was already fetched when that cache was built, so the check is
redundant (and would otherwise require live gcloud creds for every scan cell).

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
    mozaic_daily_forecast.<date>.gm-D.adj-p.parquet      (forecast, paid/organic split applied)
    mozaic_daily_forecast.<date>.gm-D.adj-p.parquet.meta.json
    mozaic_objects.glean_mobile.<date>.pkl               (fitted mozaics, for decomposition)
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from mozaic.models import MobileModelConfig  # noqa: E402

run_main_module = importlib.import_module("mozaic_daily.main")  # noqa: E402
from mozaic_daily.main import (  # noqa: E402
    _find_marketing_spec_for_forecast,
    _find_organic_spec_for_forecast,
)
from mozaic_daily.adjustments import insert_state_marker, write_meta  # noqa: E402
from mozaic_daily.queries import DataSource, Metric  # noqa: E402


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

#: Which adjustment code a given mobile spec kind produces.
_SPEC_CODES = {"organic": ("p", "paid_organic_split"), "marketing": ("m", "marketing_lift")}


def stamp_marker_and_meta(
    slug_dir: Path,
    forecast_start_date: str,
    config: MobileModelConfig,
    spec_kind: str,
    spec_path: Path,
) -> Path:
    """Rename the unmarked mobile forecast parquet to its ``.adj-<code>.`` state and write the sidecar.

    The code is derived from which spec actually gated the run, so the filename can never claim
    an adjustment the build did not apply.
    """
    code, name = _SPEC_CODES[spec_kind]
    unmarked = slug_dir / f"mozaic_daily_forecast.{forecast_start_date}.gm-D.parquet"
    if not unmarked.exists():
        raise FileNotFoundError(f"Expected forecast parquet not found after main(): {unmarked}")
    target = insert_state_marker(unmarked, [code])
    unmarked.rename(target)
    write_meta(
        target,
        forecast_start_date=forecast_start_date,
        data_source="glean_mobile",
        produced_by="scripts/run_mobile_param_scan.py",
        model_config=config.to_dict(),
        adjustments_applied=[{
            "code": code,
            "name": name,
            "scope": "glean_mobile DAU only",
            "spec_file": str(spec_path.relative_to(REPO_ROOT))
            if spec_path.is_absolute() else str(spec_path),
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

    # The mobile headline is always measured *with* the cycle's paid treatment, so exactly one
    # of the two mobile specs must gate this run. `p` supersedes `m`; both at once is the
    # double-count that main.process_data_source refuses.
    organic_spec_path = _find_organic_spec_for_forecast(args.forecast_start_date)
    marketing_spec_path = _find_marketing_spec_for_forecast(args.forecast_start_date)
    if organic_spec_path is not None and marketing_spec_path is not None:
        raise SystemExit(
            f"Both an organic.json ({organic_spec_path}) and a marketing.json "
            f"({marketing_spec_path}) claim forecast_start={args.forecast_start_date}. "
            f"They are mutually exclusive — clear the marketing spec's date gate."
        )
    if organic_spec_path is not None:
        spec_kind, spec_path = "organic", organic_spec_path
    elif marketing_spec_path is not None:
        spec_kind, spec_path = "marketing", marketing_spec_path
    else:
        raise SystemExit(
            f"No organic.json or marketing.json spec found for "
            f"forecast_start={args.forecast_start_date}. The mobile scan measures the "
            f"with-paid headline, so one is required."
        )

    print("=" * 60)
    print("Mobile param scan run")
    print("=" * 60)
    print(f"Forecast start date : {args.forecast_start_date}")
    print(f"Slug                : {slug}")
    print(f"Output dir          : {slug_dir}")
    print(f"Raw cache dir       : {args.raw_cache_dir}")
    print(f"Paid adjustment     : {_SPEC_CODES[spec_kind][0]} ({_SPEC_CODES[spec_kind][1]})")
    print(f"Spec                : {spec_path}")
    print(f"Config              : {json.dumps(config.to_dict(), indent=2)}")

    params_path = slug_dir / "parameters.json"
    params_path.write_text(json.dumps({
        "forecast_start_date": args.forecast_start_date,
        "slug": slug,
        "config": config.to_dict(),
        "adjustment_code": _SPEC_CODES[spec_kind][0],
        f"{spec_kind}_spec": str(spec_path),
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

    # The config goes through main()'s supported model_configs channel — no monkeypatch.
    # The BQ pre-flight is still skipped: the raw cache is authoritative, since the training
    # data already landed when it was built.
    original_preflight = run_main_module.check_training_data_availability
    run_main_module.check_training_data_availability = lambda *a, **k: None
    try:
        run_main_module.main(
            checkpoints=True,
            data_source_filter={DataSource.GLEAN_MOBILE},
            metric_filter={Metric.DAU},
            forecast_start_date=args.forecast_start_date,
            output_dir=str(slug_dir),
            model_configs={DataSource.GLEAN_MOBILE: config},
        )
    finally:
        run_main_module.check_training_data_availability = original_preflight

    marked_path = stamp_marker_and_meta(
        slug_dir, args.forecast_start_date, config, spec_kind, spec_path
    )
    print(f"Renamed forecast to: {marked_path}")
    print(f"Wrote sidecar meta:  {marked_path}.meta.json")
    print(f"\nDone. Results in: {slug_dir}")


if __name__ == "__main__":
    main_cli()
