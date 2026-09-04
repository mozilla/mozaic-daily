#!/usr/bin/env python3
"""Build (and cache) the desktop adjustment ladder for one cycle.

The ladder is the chart in the canonical notebook that starts from the raw model curve and
adds each adjustment in order of its Dec-15 impact, largest first, ending at the published
curve. Per-tile overlays are baked into the parquet, so every rung that adds one is a real
desktop forecast; display-layer adjustments (``h``, ...) are exact and need no run.

Runs are cached under ``<ladder-dir>/<codes>.<key>/`` where ``key`` hashes the seam, the
model config and the fingerprints of *only the overlays in that run*. Editing India's curve
therefore re-runs only the rungs that contain ``i``; the raw rung and the rungs before ``i``
stay cached. The notebook never runs the model -- it reads ``ladder_manifest.json`` from this
script, so the chart refreshes only when you run this.

First build for N overlays is 1 (raw) + N (singles, to measure impact) + up to N-2 (the
cumulative subsets the ordering needs) desktop forecasts, several minutes each.

Run (from the repo root):

    source .venv/bin/activate && python scripts/build_adjustment_ladder.py \\
        --cycle 2026-09 --forecast-start-date 2026-09-02 \\
        --raw-cache-dir data-official/2026-09/desktop_rawpull_2026-09-02 \\
        --config-from data-official/2026-09/<canonical desktop build>/mozaic_daily_forecast....meta.json

``--dry-run`` prints which runs are cached and which would be made, without forecasting.

**Every model run needs a human's explicit go-ahead.** The script lists the runs it is about to
make and waits for ``y`` before forecasting; ``--yes`` skips the prompt and must only be passed
when the user has approved *this* rebuild. A broad "refresh everything" instruction is not
approval for the ladder -- ask.
"""
from __future__ import annotations

import argparse
import glob
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "scripts"))

from mozaic.models import DesktopModelConfig  # noqa: E402
import run_param_scan  # noqa: E402  (reuses its raw-cache symlink + marker/meta stamping)
import importlib  # noqa: E402
main_module = importlib.import_module("mozaic_daily.main")  # noqa: E402  (the package exports a `main` FUNCTION)
from mozaic_daily.adjustments import load_forecast, render_adjustment  # noqa: E402
from mozaic_daily.ladder import (  # noqa: E402
    cumulative_subsets, fingerprint_overlay, ladder_rows, order_by_impact, rung_dir_name,
    rung_key, runs_required,
)
from mozaic_daily.overlays import resolve_overlays  # noqa: E402
from mozaic_daily.queries import DataSource, Metric  # noqa: E402

MA_WINDOW_DAYS = 28
MANIFEST_NAME = "ladder_manifest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cycle", required=True, help="cycle directory, e.g. 2026-09")
    parser.add_argument("--forecast-start-date", required=True, help="the seam, e.g. 2026-09-02")
    parser.add_argument("--raw-cache-dir", type=Path, required=True,
                        help="dir holding mozaic_parts.raw.legacy.desktop.DAU.parquet (no BigQuery re-query)")
    parser.add_argument("--config-from", type=Path, required=True,
                        help="a build's .meta.json (model_config) or parameters.json (config) to reproduce")
    parser.add_argument("--ladder-dir", type=Path, default=None,
                        help="default data-official/<cycle>/adjustment_ladder")
    parser.add_argument("--measurement-date", default="2026-12-15")
    parser.add_argument("--dry-run", action="store_true", help="print the plan; forecast nothing")
    parser.add_argument("--yes", action="store_true",
                        help="skip the confirmation prompt; only with the user's explicit approval of this rebuild")
    return parser.parse_args()


def load_model_config(path: Path) -> DesktopModelConfig:
    payload = json.loads(path.read_text())
    config_dict = payload.get("model_config") or payload.get("config")
    if config_dict is None:
        raise SystemExit(f"{path} has neither 'model_config' (sidecar) nor 'config' (parameters.json)")
    return DesktopModelConfig(**config_dict)


def desktop_display_effects(cycle_dir: Path, date_index: pd.DatetimeIndex, measurement_date: pd.Timestamp) -> dict[str, float]:
    """Dec-15 desktop effect of every display-layer spec in the cycle's adjustments/ dir, non-zero only."""
    effects = {}
    for spec_path in sorted(glob.glob(str(cycle_dir / "adjustments" / "*.json"))):
        spec = json.loads(Path(spec_path).read_text())
        code = spec.get("adjustment_code") or Path(spec_path).stem[0]
        value = float(render_adjustment(spec, date_index, spec_dir=Path(spec_path).parent)["desktop"].get(measurement_date, 0.0))
        if value != 0.0:
            effects[code] = value
    return effects


def world_plain_ma_dec15(parquet: Path, measurement_date: pd.Timestamp, require_state: list[str]) -> float:
    """Plain trailing 28d-MA of ALL-country desktop DAU at the measurement date.

    Plain rolling(28) equals display_ma from seam+27 on, so Dec-15 is identical either way.
    """
    df, _ = load_forecast(parquet, require_state=require_state)
    world = df[(df["country"] == "ALL") & (df["segment"] == '{"os": "ALL"}')
               & (df["data_source"] == "legacy_desktop") & (df["app_name"] == "desktop")].copy()
    world["target_date"] = pd.to_datetime(world["target_date"])
    daily = world.sort_values("target_date").set_index("target_date")["dau"].astype(float)
    return float(daily.rolling(MA_WINDOW_DAYS).mean().loc[measurement_date])


def confirm_runs(pending: list[str], approved: bool) -> None:
    """Stop unless a human approves the listed model runs (or --yes carried that approval)."""
    if not pending:
        return
    print(f"\nAbout to run {len(pending)} desktop forecast(s), several minutes each: {pending}")
    if approved:
        print("  approved via --yes")
        return
    if not sys.stdin.isatty():
        raise SystemExit("ladder reruns need human confirmation; re-run interactively or pass --yes after the user approves.")
    if input("  Proceed? [y/N] ").strip().lower() != "y":
        raise SystemExit("aborted: no runs made.")


def find_rung_parquet(rung_dir: Path, forecast_start: str) -> Path | None:
    matches = sorted(rung_dir.glob(f"mozaic_daily_forecast.{forecast_start}.ld-D.*.parquet"))
    return matches[0] if matches else None


def run_rung(subset: frozenset[str], rung_dir: Path, *, forecast_start: str, config: DesktopModelConfig,
             all_overlay_codes: set[str], raw_cache_dir: Path, code_to_spec: dict[str, Path]) -> Path:
    """One desktop forecast with exactly ``subset`` of the overlays enabled; returns the marked parquet."""
    rung_dir.mkdir(parents=True, exist_ok=True)
    run_param_scan.symlink_raw_cache(raw_cache_dir, rung_dir)
    (rung_dir / "parameters.json").write_text(json.dumps({
        "forecast_start_date": forecast_start, "overlays_enabled": sorted(subset), "config": config.to_dict(),
    }, indent=2))
    disabled = all_overlay_codes - subset
    main_module.main(
        checkpoints=True,
        data_source_filter={DataSource.LEGACY_DESKTOP},
        metric_filter={Metric.DAU},
        forecast_start_date=forecast_start,
        output_dir=str(rung_dir),
        disabled_adjustments=disabled,
        model_configs={DataSource.LEGACY_DESKTOP: config},
    )
    applied = sorted(subset)
    return run_param_scan.stamp_marker_and_meta(
        rung_dir, forecast_start, config, applied, {code: code_to_spec[code] for code in applied})


def main() -> None:
    args = parse_args()
    cycle_dir = REPO / "data-official" / args.cycle
    ladder_dir = args.ladder_dir or (cycle_dir / "adjustment_ladder")
    measurement_date = pd.Timestamp(args.measurement_date)
    forecast_start = args.forecast_start_date
    config = load_model_config(args.config_from)

    overlays = [o for o in resolve_overlays(forecast_start) if o.data_source == DataSource.LEGACY_DESKTOP]
    overlay_codes = {o.code for o in overlays}
    code_to_spec = {o.code: o.spec_path for o in overlays}
    fingerprints = {o.code: fingerprint_overlay(o.spec_path) for o in overlays}
    date_index = pd.date_range("2026-01-01", "2027-12-31", freq="D")
    display_effects = desktop_display_effects(cycle_dir, date_index, measurement_date)

    def rung_dir_for(subset: frozenset[str]) -> Path:
        key = rung_key(forecast_start=forecast_start, model_config=config.to_dict(),
                       enabled_codes=subset, fingerprints=fingerprints)
        return ladder_dir / rung_dir_name(subset, key)

    print(f"Ladder for {args.cycle} at seam {forecast_start}")
    print(f"  desktop overlays gating this seam : {sorted(overlay_codes) or 'none'}")
    print(f"  display-layer desktop effects     : {display_effects or 'none'}")
    print(f"  config                            : {config.to_slug()}")
    print(f"  ladder dir                        : {ladder_dir}")

    # Phase 1: raw + every single overlay, to measure each overlay's own Dec-15 impact.
    phase1 = [frozenset()] + [frozenset({code}) for code in sorted(overlay_codes)]
    # Phase 2 (after ordering): the cumulative subsets the ladder needs. Computed after phase 1.
    dec15_by_subset: dict[frozenset[str], float] = {}

    def ensure(subset: frozenset[str], timings: list[float]) -> None:
        rung_dir = rung_dir_for(subset)
        parquet = find_rung_parquet(rung_dir, forecast_start)
        label = "+".join(sorted(subset)) or "raw"
        if parquet is None:
            if args.dry_run:
                print(f"  [would run]  {label:<12s} -> {rung_dir.name}")
                return
            eta = f"ETA {statistics.median(timings) * remaining[0] / 60:.1f}m" if timings else "ETA unknown"
            sys.stdout.write(f"  [running]    {label:<12s} -> {rung_dir.name}  ({remaining[0]} left, {eta})\n")
            sys.stdout.flush()
            t0 = time.time()
            parquet = run_rung(subset, rung_dir, forecast_start=forecast_start, config=config,
                               all_overlay_codes=overlay_codes, raw_cache_dir=args.raw_cache_dir,
                               code_to_spec=code_to_spec)
            timings.append(time.time() - t0)
            sys.stdout.write(f"               took {timings[-1] / 60:.1f}m\n")
            sys.stdout.flush()
        else:
            print(f"  [cached]     {label:<12s} -> {rung_dir.name}")
        remaining[0] -= 1
        dec15_by_subset[subset] = world_plain_ma_dec15(parquet, measurement_date, sorted(subset))

    def pending_labels(subsets):
        return ["+".join(sorted(s)) or "raw" for s in subsets if find_rung_parquet(rung_dir_for(s), forecast_start) is None]

    timings: list[float] = []
    remaining = [len(phase1)]
    print(f"\nPhase 1: raw + {len(overlay_codes)} single-overlay runs")
    if not args.dry_run:
        confirm_runs(pending_labels(phase1), args.yes)
    for subset in phase1:
        ensure(subset, timings)
    if args.dry_run and any(s not in dec15_by_subset for s in phase1):
        print("\n[dry-run] phase 1 incomplete, so the impact order (and phase 2) cannot be planned yet.")
        return

    raw = dec15_by_subset[frozenset()]
    single_effects = {code: dec15_by_subset[frozenset({code})] - raw for code in sorted(overlay_codes)}
    order = order_by_impact({**single_effects, **display_effects})
    subsets = cumulative_subsets(order, overlay_codes)
    phase2 = [s for s in runs_required(subsets) if s not in dec15_by_subset]
    print(f"\nImpact order (|Dec-15 effect| desc): {order}")
    for code in order:
        source = "overlay run" if code in overlay_codes else "display layer"
        print(f"  {code}: {(single_effects | display_effects)[code]:>+12,.0f}  ({source})")
    print(f"\nPhase 2: {len(phase2)} cumulative run(s) still needed")
    remaining = [len(phase2)]
    if not args.dry_run:
        confirm_runs(pending_labels(phase2), args.yes)
    for subset in phase2:
        ensure(subset, timings)
    if args.dry_run:
        return

    rows = ladder_rows(order, overlay_codes, dec15_by_subset, display_effects)
    manifest = {
        "cycle": args.cycle,
        "forecast_start": forecast_start,
        "measurement_date": str(measurement_date.date()),
        "model_config": config.to_dict(),
        "config_from": str(args.config_from),
        "overlay_codes": sorted(overlay_codes),
        "overlay_fingerprints": fingerprints,
        "display_effects_dec15": display_effects,
        "single_overlay_effects_dec15": single_effects,
        "order": order,
        "runs": {("+".join(sorted(s)) or "raw"): {
            "overlay_subset": sorted(s),
            "parquet": str(find_rung_parquet(rung_dir_for(s), forecast_start).relative_to(REPO)),
            "dec15_plain_ma": dec15_by_subset[s],
        } for s in runs_required(subsets)},
        "rungs": rows,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    ladder_dir.mkdir(parents=True, exist_ok=True)
    (ladder_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2))
    print(f"\nwrote {ladder_dir / MANIFEST_NAME}")
    print(f"{'rung':>4} {'added':<8} {'Dec-15 28d-MA':>16} {'step':>12}")
    for row in rows:
        step = "" if row["step"] is None else f"{row['step']:+,.0f}"
        print(f"{row['rung']:>4} {row['added'] or 'raw':<8} {row['dec15']:>16,.0f} {step:>12}")


if __name__ == "__main__":
    main()
