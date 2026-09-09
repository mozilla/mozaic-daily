#!/usr/bin/env python3
"""Run (and cache) every subset of a cycle's droppable desktop overlays; write the combinatorics manifest.

Planning question: not every adjustment can ship this cycle, so which subset should? With N
droppable per-tile overlays there are 2^N subsets, each a real desktop forecast. Runs share
the ladder's cache (``data-official/<cycle>/adjustment_ladder/<codes>.<key>/``, same content
keys), so the raw run, the singles and the cumulative rungs the ladder already made are reused
and only the missing subsets are forecast.

Pinned adjustments are not varied. Display-layer codes (``h``) are always applied, exactly, at
assembly time; any per-tile overlay you want held fixed goes in ``--pin`` and appears in every
run. The manifest lists each subset's run parquet and plain Dec-15 28d-MA; the report renderer
(``scripts/render_adjustment_combinatorics.py``) adds the display layer and the deltas.

Run (from the repo root):

    source .venv/bin/activate && python scripts/build_adjustment_combinatorics.py \\
        --cycle 2026-09 --forecast-start-date 2026-09-02 \\
        --raw-cache-dir data-official/2026-09/desktop_rawpull_2026-09-02 \\
        --config-from data-official/2026-09/<canonical desktop build>/mozaic_daily_forecast....meta.json

``--dry-run`` prints which subsets are cached and which would be run, without forecasting.

**Every model run needs a human's explicit go-ahead.** The script lists the runs it is about to
make and waits for ``y``; ``--yes`` skips the prompt and must only be passed when the user has
approved *this* set of runs.
"""
from __future__ import annotations

import argparse
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

from build_adjustment_ladder import (  # noqa: E402  (shares the cache layout and the run machinery)
    confirm_runs, desktop_display_effects, find_rung_parquet, load_model_config, run_rung,
    world_plain_ma_dec15,
)
from mozaic_daily.combinatorics import all_subsets, subset_label  # noqa: E402
from mozaic_daily.ladder import fingerprint_overlay, rung_dir_name, rung_key  # noqa: E402
from mozaic_daily.overlays import resolve_overlays  # noqa: E402
from mozaic_daily.queries import DataSource  # noqa: E402

MANIFEST_NAME = "combinatorics_manifest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cycle", required=True, help="cycle directory, e.g. 2026-09")
    parser.add_argument("--forecast-start-date", required=True, help="the seam, e.g. 2026-09-02")
    parser.add_argument("--raw-cache-dir", type=Path, required=True,
                        help="dir holding mozaic_parts.raw.legacy.desktop.DAU.parquet (no BigQuery re-query)")
    parser.add_argument("--config-from", type=Path, required=True,
                        help="a build's .meta.json (model_config) or parameters.json (config) to reproduce")
    parser.add_argument("--pin", action="append", default=[],
                        help="per-tile overlay code to hold ON in every run (repeatable); display-layer codes are always on")
    parser.add_argument("--ladder-dir", type=Path, default=None,
                        help="run cache; default data-official/<cycle>/adjustment_ladder (shared with the ladder)")
    parser.add_argument("--out-dir", type=Path, default=None,
                        help="manifest destination; default data-official/<cycle>/adjustment_combinatorics")
    parser.add_argument("--measurement-date", default="2026-12-15")
    parser.add_argument("--dry-run", action="store_true", help="print the plan; forecast nothing")
    parser.add_argument("--yes", action="store_true",
                        help="skip the confirmation prompt; only with the user's explicit approval of these runs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cycle_dir = REPO / "data-official" / args.cycle
    ladder_dir = args.ladder_dir or (cycle_dir / "adjustment_ladder")
    out_dir = args.out_dir or (cycle_dir / "adjustment_combinatorics")
    measurement_date = pd.Timestamp(args.measurement_date)
    forecast_start = args.forecast_start_date
    config = load_model_config(args.config_from)

    overlays = [o for o in resolve_overlays(forecast_start) if o.data_source == DataSource.LEGACY_DESKTOP]
    overlay_codes = {o.code for o in overlays}
    unknown_pins = set(args.pin) - overlay_codes
    if unknown_pins:
        raise SystemExit(f"--pin {sorted(unknown_pins)} not among the desktop overlays gating {forecast_start}: {sorted(overlay_codes)}")
    pinned = frozenset(args.pin)
    droppable = sorted(overlay_codes - pinned)
    code_to_spec = {o.code: o.spec_path for o in overlays}
    fingerprints = {o.code: fingerprint_overlay(o.spec_path) for o in overlays}
    date_index = pd.date_range("2026-01-01", "2027-12-31", freq="D")
    display_effects = desktop_display_effects(cycle_dir, date_index, measurement_date)

    def run_dir_for(enabled: frozenset[str]) -> Path:
        key = rung_key(forecast_start=forecast_start, model_config=config.to_dict(),
                       enabled_codes=enabled, fingerprints=fingerprints)
        return ladder_dir / rung_dir_name(enabled, key)

    subsets = all_subsets(droppable)
    print(f"Adjustment combinatorics for {args.cycle} at seam {forecast_start}")
    print(f"  droppable desktop overlays        : {droppable}  -> {len(subsets)} subsets")
    print(f"  pinned overlays (in every run)    : {sorted(pinned) or 'none'}")
    print(f"  display layer, always on (Dec-15) : {display_effects or 'none'}")
    print(f"  config                            : {config.to_slug()}")
    print(f"  run cache                         : {ladder_dir}")

    pending = [subset_label(s) for s in subsets if find_rung_parquet(run_dir_for(s | pinned), forecast_start) is None]
    cached = len(subsets) - len(pending)
    print(f"\n{cached} cached, {len(pending)} to run: {pending or 'nothing'}")
    if args.dry_run:
        for s in subsets:
            state = "would run" if subset_label(s) in pending else "cached"
            print(f"  [{state:<9s}] {subset_label(s):<12s} -> {run_dir_for(s | pinned).name}")
        return
    confirm_runs(pending, args.yes)

    timings: list[float] = []
    remaining = len(pending)
    dec15_by_subset: dict[frozenset[str], float] = {}
    parquet_by_subset: dict[frozenset[str], Path] = {}
    for subset in subsets:
        enabled = subset | pinned
        run_dir = run_dir_for(enabled)
        parquet = find_rung_parquet(run_dir, forecast_start)
        label = subset_label(subset)
        if parquet is None:
            eta = f"ETA {statistics.median(timings) * remaining / 60:.1f}m" if timings else "ETA unknown"
            sys.stdout.write(f"  [running]    {label:<12s} -> {run_dir.name}  ({remaining} left, {eta})\n")
            sys.stdout.flush()
            t0 = time.time()
            parquet = run_rung(enabled, run_dir, forecast_start=forecast_start, config=config,
                               all_overlay_codes=overlay_codes, raw_cache_dir=args.raw_cache_dir,
                               code_to_spec=code_to_spec)
            timings.append(time.time() - t0)
            remaining -= 1
            sys.stdout.write(f"               took {timings[-1] / 60:.1f}m\n")
            sys.stdout.flush()
        else:
            print(f"  [cached]     {label:<12s} -> {run_dir.name}")
        parquet_by_subset[subset] = parquet
        dec15_by_subset[subset] = world_plain_ma_dec15(parquet, measurement_date, sorted(enabled))

    manifest = {
        "cycle": args.cycle,
        "forecast_start": forecast_start,
        "measurement_date": str(measurement_date.date()),
        "model_config": config.to_dict(),
        "config_from": str(args.config_from),
        "droppable_codes": droppable,
        "pinned_overlay_codes": sorted(pinned),
        "overlay_fingerprints": fingerprints,
        "display_effects_dec15": display_effects,
        "runs": {subset_label(s): {
            "overlay_subset": sorted(s),
            "overlays_enabled": sorted(s | pinned),
            "parquet": str(parquet_by_subset[s].relative_to(REPO)),
            "dec15_plain_ma": dec15_by_subset[s],
        } for s in subsets},
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2))
    print(f"\nwrote {out_dir / MANIFEST_NAME}")
    display_total = sum(display_effects.values())
    print(f"{'subset':<12s} {'Dec-15 plain MA':>16s} {'+ display layer':>16s}")
    for s in subsets:
        v = dec15_by_subset[s]
        print(f"{subset_label(s):<12s} {v:>16,.0f} {v + display_total:>16,.0f}")


if __name__ == "__main__":
    main()
