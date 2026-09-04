#!/usr/bin/env python3
"""Run one desktop forecast with a configurable DesktopModelConfig for parameter scanning.

Designed for human-mediated gradient-descent exploration of mozaic-forecasting
Prophet parameters. Each invocation produces a single forecast for one config;
the output goes into a slug-named subdirectory derived from ``config.to_slug()``
so that re-running the same config writes back to the same dir, and different
configs are clearly distinguishable on disk.

The script is desktop-only (the parameter search is for desktop world DAU).
Mobile is intentionally excluded — see scope discussion in CLAUDE.md.

Usage
-----
Single config (defaults reproduce the June 2026 baseline):

    source .venv/bin/activate
    python scripts/run_param_scan.py \\
        --forecast-start-date 2026-05-13 \\
        --raw-cache-dir data-official/2026-06/desktop_cps0.15983_thresh50_recent13_clip0.6 \\
        --results-dir research/param-scans/results

Override one or more knobs:

    python scripts/run_param_scan.py \\
        --forecast-start-date 2026-05-13 \\
        --raw-cache-dir data-official/2026-06/desktop_cps0.15983_thresh50_recent13_clip0.6 \\
        --changepoint-prior-scale 0.10 \\
        --recent-weeks 8

Behavior
--------
1. Builds a ``DesktopModelConfig`` from CLI args (any unspecified field falls back
   to the package default).
2. Creates ``<results-dir>/<config.to_slug()>/`` (idempotent).
3. If ``--raw-cache-dir`` is set, symlinks every ``mozaic_parts.raw.legacy.desktop.*.parquet``
   from that dir into the slug dir so BigQuery is not re-queried.
4. Runs the desktop legacy DAU forecast (the run that produces the headline
   world DAU number) via ``main(model_configs={LEGACY_DESKTOP: config})``. The
   ``l`` / ``o`` overlays are applied by ``main()`` itself.
5. Writes the forecast parquet + the fitted mozaic pickle into the slug dir,
   plus a ``parameters.json`` capturing the exact config used.

Outputs
-------
``<results-dir>/<slug>/``
    parameters.json
    mozaic_parts.raw.legacy.desktop.DAU.parquet               (symlinked from raw-cache-dir)
    mozaic_daily_forecast.<date>.ld-D.adj-lo.parquet          (the forecast itself)
    mozaic_daily_forecast.<date>.ld-D.adj-lo.parquet.meta.json (sidecar provenance)
    mozaic_objects.legacy_desktop.<date>.pkl

The desktop launch-on-login (`l`) and MozillaOnline (`o`) overlays are applied
when their specs match the forecast start date (bidirectional: subtract from
modern_windows training pre-mozaic, add back post-mozaic), so scanned configs are
directly comparable to the canonical adj-lo desktop. The output parquet carries
the corresponding ``.adj-{codes}.`` state marker (``.raw.`` if no overlay matched)
required by ``mozaic_daily.adjustments.load_forecast``. The Win10 headwind (`h`)
is NOT applied here — it is a display-layer adjustment added in the canonical
notebook, so it stays out of the per-config scan.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import importlib  # noqa: E402

from mozaic.models import DesktopModelConfig  # noqa: E402
run_main_module = importlib.import_module("mozaic_daily.main")  # noqa: E402
from mozaic_daily.adjustments import (  # noqa: E402
    build_adjustments_applied_list,
    insert_state_marker,
    write_meta,
)
from mozaic_daily.overlays import (  # noqa: E402
    registered_overlay_codes,
    resolve_overlays,
)
from mozaic_daily.queries import DataSource, Metric  # noqa: E402


# ---------------------------------------------------------------------------
# Raw cache wiring
# ---------------------------------------------------------------------------

# Names of the raw BQ checkpoint files for the desktop legacy DAU run we scan.
RAW_CHECKPOINT_FILES = [
    "mozaic_parts.raw.legacy.desktop.DAU.parquet",
]


def symlink_raw_cache(raw_cache_dir: Path, slug_dir: Path) -> list[Path]:
    """Symlink raw BQ parquets from raw_cache_dir into slug_dir.

    Returns the list of destination paths created (or already present).
    Raises FileNotFoundError if a required raw parquet is missing in the source.
    """
    created = []
    for filename in RAW_CHECKPOINT_FILES:
        source = raw_cache_dir / filename
        if not source.exists():
            raise FileNotFoundError(
                f"Raw checkpoint missing in --raw-cache-dir: {source}\n"
                f"Run the baseline forecast first, or point --raw-cache-dir at a "
                f"directory that already contains mozaic_parts.raw.*.parquet files."
            )
        dest = slug_dir / filename
        if dest.exists() or dest.is_symlink():
            # If it points somewhere else, replace it; otherwise leave it alone.
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

def stamp_marker_and_meta(
    slug_dir: Path,
    forecast_start_date: str,
    config: DesktopModelConfig,
    applied_codes: list[str],
    code_to_spec_file: dict[str, Path],
) -> Path:
    """Rename the unmarked forecast parquet to its state marker and write the sidecar.

    ``main()`` writes ``mozaic_daily_forecast.<date>.ld-D.parquet`` (no state
    marker, no meta) — illegal under the naming convention. This renames it to
    ``.adj-{codes}.`` (or ``.raw.`` if no overlay applied) and writes a sidecar
    ``.meta.json`` so the result is loadable via
    ``mozaic_daily.adjustments.load_forecast``. ``applied_codes`` are the desktop
    overlays that were applied this run (``l``/``o`` when their specs matched the
    forecast start date).

    Returns the path to the renamed parquet.
    """
    unmarked = slug_dir / f"mozaic_daily_forecast.{forecast_start_date}.ld-D.parquet"
    if not unmarked.exists():
        raise FileNotFoundError(
            f"Expected forecast parquet not found after main(): {unmarked}"
        )
    target = insert_state_marker(unmarked, applied_codes)
    unmarked.rename(target)
    adjustments_applied = (
        build_adjustments_applied_list(applied_codes, code_to_spec_file)
        if applied_codes else []
    )
    write_meta(
        target,
        forecast_start_date=forecast_start_date,
        data_source="legacy_desktop",
        produced_by="scripts/run_param_scan.py",
        model_config=config.to_dict(),
        adjustments_applied=adjustments_applied,
    )
    return target


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_config(args: argparse.Namespace) -> DesktopModelConfig:
    """Build a DesktopModelConfig from CLI args, omitting any unspecified knob."""
    overrides = {}
    if args.changepoint_prior_scale is not None:
        overrides["prophet_changepoint_prior_scale"] = args.changepoint_prior_scale
    if args.recent_weeks is not None:
        overrides["prophet_recent_weeks"] = args.recent_weeks
    if args.changepoint_range is not None:
        overrides["prophet_changepoint_range"] = args.changepoint_range
    if args.n_changepoints is not None:
        overrides["prophet_n_changepoints"] = args.n_changepoints
    if args.seasonality_prior_scale is not None:
        overrides["prophet_seasonality_prior_scale"] = args.seasonality_prior_scale
    if args.seasonality_regime is not None:
        overrides["seasonality_regime"] = args.seasonality_regime
    if args.seasonality_corr_threshold is not None:
        overrides["seasonality_corr_threshold"] = args.seasonality_corr_threshold
    if args.holiday_threshold is not None:
        overrides["holiday_threshold"] = args.holiday_threshold
    if args.holiday_max_radius is not None:
        overrides["holiday_max_radius"] = args.holiday_max_radius
    if args.holiday_min_radius is not None:
        overrides["holiday_min_radius"] = args.holiday_min_radius
    if args.holiday_effect_floor is not None:
        overrides["holiday_effect_floor"] = args.holiday_effect_floor
    return DesktopModelConfig(**overrides)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--forecast-start-date",
        default="2026-05-13",
        help="Forecast start date (YYYY-MM-DD). Default 2026-05-13 (June 2026 baseline).",
    )
    parser.add_argument(
        "--raw-cache-dir",
        type=Path,
        default=None,
        help=(
            "Directory containing existing mozaic_parts.raw.*.parquet files. "
            "When set, those files are symlinked into the slug dir to skip BigQuery. "
            "Recommended: pass the data-official baseline run dir."
        ),
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=REPO_ROOT / "research/param-scans/results",
        help="Root directory for all scan runs (default: research/param-scans/results/).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resolved config and target directory, then exit without running.",
    )

    g = parser.add_argument_group("Prophet knobs")
    g.add_argument("--changepoint-prior-scale", type=float, default=None,
                   help="DesktopModelConfig.prophet_changepoint_prior_scale (default 0.15983)")
    g.add_argument("--recent-weeks", type=int, default=None,
                   help="DesktopModelConfig.prophet_recent_weeks (default 13)")
    g.add_argument("--changepoint-range", type=float, default=None,
                   help="DesktopModelConfig.prophet_changepoint_range (default 0.7)")
    g.add_argument("--n-changepoints", type=int, default=None,
                   help="DesktopModelConfig.prophet_n_changepoints (default 25)")
    g.add_argument("--seasonality-prior-scale", type=float, default=None,
                   help="DesktopModelConfig.prophet_seasonality_prior_scale (default 0.00825)")
    g.add_argument("--seasonality-regime", type=str, default=None,
                   choices=["auto", "additive", "multiplicative"],
                   help="DesktopModelConfig.seasonality_regime (default auto)")
    g.add_argument("--seasonality-corr-threshold", type=float, default=None,
                   help="DesktopModelConfig.seasonality_corr_threshold (default 0.0, the "
                        "legacy hardcoded cutoff). Per-tile cutoff on corr(|y|,|dy|): a "
                        "tile runs multiplicative+linear above it. Lower => more tiles "
                        "multiplicative, so this dials continuously between the additive "
                        "and multiplicative regimes. Only meaningful with regime=auto.")

    g = parser.add_argument_group("Holiday knobs")
    g.add_argument("--holiday-threshold", type=float, default=None,
                   help="DesktopModelConfig.holiday_threshold (default -0.032)")
    g.add_argument("--holiday-max-radius", type=int, default=None,
                   help="DesktopModelConfig.holiday_max_radius (default 5)")
    g.add_argument("--holiday-min-radius", type=int, default=None,
                   help="DesktopModelConfig.holiday_min_radius (default 3)")
    g.add_argument("--holiday-effect-floor", type=float, default=None,
                   help="DesktopModelConfig.holiday_effect_floor (default -0.6)")

    g = parser.add_argument_group("Overlay toggles (for isolating one overlay's effect)")
    g.add_argument("--disable-adjustment", action="append", default=[], metavar="CODE",
                   help="Disable one adjustment code even if its spec matches (repeatable). "
                        "Works for every registered overlay; the output is stamped without "
                        "that code so the filename describes what is baked into the parquet.")
    g.add_argument("--no-launch-on-login", action="store_true",
                   help="Alias for --disable-adjustment l.")
    g.add_argument("--no-mozillaonline", action="store_true",
                   help="Alias for --disable-adjustment o.")

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main_cli() -> None:
    args = parse_args()

    config = build_config(args)
    slug = config.to_slug()

    slug_dir = args.results_dir / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"Param scan run")
    print("=" * 60)
    print(f"Forecast start date : {args.forecast_start_date}")
    print(f"Slug                : {slug}")
    print(f"Output dir          : {slug_dir}")
    print(f"Raw cache dir       : {args.raw_cache_dir}")
    print(f"Config              : {json.dumps(config.to_dict(), indent=2)}")

    # Write parameters.json early so the metadata is present even if the run
    # fails partway through (e.g. Prophet optimization errors).
    params_path = slug_dir / "parameters.json"
    params_payload = {
        "forecast_start_date": args.forecast_start_date,
        "slug": slug,
        "config": config.to_dict(),
    }
    params_path.write_text(json.dumps(params_payload, indent=2))
    print(f"Wrote {params_path}")

    if args.raw_cache_dir is not None:
        created = symlink_raw_cache(args.raw_cache_dir, slug_dir)
        print(f"Symlinked raw cache files: {[p.name for p in created]}")
    else:
        print("No --raw-cache-dir provided; BigQuery will be queried fresh.")

    if args.dry_run:
        print("\n[dry-run] Skipping actual forecast.")
        return

    # Determine which desktop overlays apply for this forecast start. main() applies every
    # registry-resolved overlay whose spec gates on this date; we resolve the same set here
    # to stamp the output marker + meta correctly. A disabled code is left off the marker so
    # the filename always describes what is actually baked into the parquet.
    disabled = set(args.disable_adjustment)
    if args.no_launch_on_login:
        disabled.add("l")
    if args.no_mozillaonline:
        disabled.add("o")
    desktop_overlays = [
        o for o in resolve_overlays(args.forecast_start_date, disabled_codes=disabled)
        if o.data_source == DataSource.LEGACY_DESKTOP
    ]
    applied_codes: list[str] = [o.code for o in desktop_overlays]
    code_to_spec_file: dict[str, Path] = {o.code: o.spec_path for o in desktop_overlays}
    print(f"Desktop overlays applied this scan: {applied_codes or 'none (raw)'}")
    suppressed = [
        o.code for o in resolve_overlays(args.forecast_start_date)
        if o.code in disabled and o.data_source == DataSource.LEGACY_DESKTOP
    ]
    for code in suppressed:
        print(f"  NOTE: `{code}` spec matched but was suppressed by --disable-adjustment.")
    unknown = disabled - set(registered_overlay_codes()) - {"m", "p"}
    if unknown:
        raise SystemExit(f"--disable-adjustment names unregistered code(s): {sorted(unknown)}")

    # The config goes through main()'s supported model_configs channel; the overlays are
    # applied by main() itself. This used to monkeypatch process_data_source with a hand-copied
    # desktop branch, which had to be kept in sync with main.py by hand.
    run_main_module.main(
        checkpoints=True,
        data_source_filter={DataSource.LEGACY_DESKTOP},
        metric_filter={Metric.DAU},
        forecast_start_date=args.forecast_start_date,
        output_dir=str(slug_dir),
        disabled_adjustments=disabled,
        model_configs={DataSource.LEGACY_DESKTOP: config},
    )

    out_path = stamp_marker_and_meta(
        slug_dir, args.forecast_start_date, config, applied_codes, code_to_spec_file)
    print(f"Renamed forecast to: {out_path}")
    print(f"Wrote sidecar meta:  {out_path}.meta.json")

    print(f"\nDone. Results in: {slug_dir}")


if __name__ == "__main__":
    main_cli()
