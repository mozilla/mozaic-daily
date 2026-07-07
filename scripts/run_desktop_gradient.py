#!/usr/bin/env python3
"""Drive desktop July-2026 forecast parameter-search rounds (gradient + combinations).

Each probe is one ``scripts/run_param_scan.py`` invocation (one config per process), so the
desktop launch-on-login (``l``) and MozillaOnline (``o``) overlays are applied exactly as in the
canonical adj-lo desktop forecast. The Win10 headwind (``h``) is a display-layer offset and is NOT
applied here — it is added when the results are read (desktop_gradient_round<N>.ipynb).

Rounds
------
- **round 1** (``--round 1``): one-at-a-time ±δ gradient over all 8 knobs, moderate δ. Historical.
- **round 2** (``--round 2``, default): re-anchored to current data (forecast_start 2026-07-06).
  Focuses on the top-3 levers — ``holiday_threshold``, ``changepoint_prior_scale``,
  ``changepoint_range`` — with larger δ, plus combinations of their KPI-raising directions.
  (``holiday_min_radius`` / ``holiday_max_radius`` deliberately excluded per Brendan.)

Each probe gets its own ``--results-dir`` (``.../desktop_gradient_round<N>/<label>``) so no probe
can overwrite another regardless of slug collisions.

Usage
-----
    source .venv/bin/activate
    # round 2 against the re-anchored raw cache produced by the fresh center run:
    python scripts/run_desktop_gradient.py --round 2 \\
        --forecast-start-date 2026-07-06 \\
        --raw-cache-dir research/param-scans/results/desktop_gradient_round2/center/<center-slug>
    python scripts/run_desktop_gradient.py --round 2 --dry-run --raw-cache-dir <dir>

Each fresh probe runs the ~30s BigQuery pre-flight availability check in main(), so live
``gcloud auth application-default`` creds are required. The heavy DAU query is served from the
symlinked raw cache.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RUN_PARAM_SCAN = REPO_ROOT / "scripts" / "run_param_scan.py"
RESULTS_ROOT = REPO_ROOT / "research/param-scans/results"

# DesktopModelConfig field -> run_param_scan.py CLI flag.
FLAG_MAP = {
    "prophet_changepoint_prior_scale": "--changepoint-prior-scale",
    "prophet_recent_weeks": "--recent-weeks",
    "prophet_changepoint_range": "--changepoint-range",
    "prophet_n_changepoints": "--n-changepoints",
    "holiday_threshold": "--holiday-threshold",
    "holiday_max_radius": "--holiday-max-radius",
    "holiday_min_radius": "--holiday-min-radius",
    "holiday_effect_floor": "--holiday-effect-floor",
}

# --- Round 1: one-at-a-time ±δ over all knobs (moderate δ). Kept for the record. ---
ROUND1_PARAMS = [
    ("prophet_changepoint_prior_scale", "cps",    0.12983, 0.18983),
    ("prophet_recent_weeks",            "recent", 11,      15),
    ("prophet_changepoint_range",       "cpr",    0.65,    0.75),
    ("prophet_n_changepoints",          "ncp",    20,      30),
    ("holiday_threshold",               "thresh", -0.037,  -0.027),
    ("holiday_max_radius",              "hmaxr",  4,       6),
    ("holiday_min_radius",              "hminr",  2,       4),
    ("holiday_effect_floor",            "clip",   -0.7,    -0.5),
]

# --- Round 2: top-3 levers, larger δ + combinations of KPI-raising directions. ---
# KPI-raising directions from round 1: holiday_threshold UP (less negative), cps DOWN, cpr DOWN.
CPS = "prophet_changepoint_prior_scale"
CPR = "prophet_changepoint_range"
THR = "holiday_threshold"

ROUND2_PROBES = [
    ("center", {}),
    # Single-knob scan (re-measured at the new baseline; favorable-leaning + one unfavorable each).
    ("thresh__m042", {THR: -0.042}),   # unfavorable side (gradient sign check)
    ("thresh__m022", {THR: -0.022}),
    ("thresh__m017", {THR: -0.017}),
    ("cps__0.20983", {CPS: 0.20983}),  # unfavorable side
    ("cps__0.10983", {CPS: 0.10983}),
    ("cps__0.08983", {CPS: 0.08983}),
    ("cpr__0.80", {CPR: 0.80}),        # unfavorable side
    ("cpr__0.60", {CPR: 0.60}),
    ("cpr__0.55", {CPR: 0.55}),
    # Combinations of KPI-raising directions (build up; measure actual vs naive sum).
    ("combo__thr022_cps10983", {THR: -0.022, CPS: 0.10983}),
    ("combo__thr022_cpr60", {THR: -0.022, CPR: 0.60}),
    ("combo__cps10983_cpr60", {CPS: 0.10983, CPR: 0.60}),
    ("combo__thr022_cps10983_cpr60", {THR: -0.022, CPS: 0.10983, CPR: 0.60}),
    ("combo__thr017_cps08983_cpr55", {THR: -0.017, CPS: 0.08983, CPR: 0.55}),
]


def build_probes(round_num: int) -> list[tuple[str, dict]]:
    if round_num == 1:
        probes = [("center", {})]
        for field, short, minus, plus in ROUND1_PARAMS:
            probes.append((f"{short}__minus", {field: minus}))
            probes.append((f"{short}__plus", {field: plus}))
        return probes
    if round_num == 2:
        return ROUND2_PROBES
    raise ValueError(f"unknown round: {round_num}")


def probe_done(results_dir: Path) -> bool:
    return any(results_dir.glob("*/mozaic_daily_forecast.*.ld-D.adj-*.parquet")) or \
        any(results_dir.glob("*/mozaic_daily_forecast.*.ld-D.raw.parquet"))


def run_probe(round_dir: Path, forecast_start: str, raw_cache_dir: Path | None,
              label: str, overrides: dict, force: bool) -> tuple[str, str, float]:
    results_dir = round_dir / label
    if not force and probe_done(results_dir):
        return (label, "skipped (exists)", 0.0)
    results_dir.mkdir(parents=True, exist_ok=True)

    cmd = [sys.executable, str(RUN_PARAM_SCAN),
           "--forecast-start-date", forecast_start,
           "--results-dir", str(results_dir)]
    if raw_cache_dir is not None:
        cmd += ["--raw-cache-dir", str(raw_cache_dir)]
    for field, value in overrides.items():
        cmd += [FLAG_MAP[field], str(value)]

    log_path = results_dir / "run.log"
    start = time.monotonic()
    with open(log_path, "w") as log:
        log.write("$ " + " ".join(cmd) + "\n\n")
        log.flush()
        proc = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT)
    elapsed = time.monotonic() - start
    status = "ok" if proc.returncode == 0 else f"FAILED (rc={proc.returncode}, see {log_path})"
    return (label, status, elapsed)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--round", type=int, default=2, choices=[1, 2])
    parser.add_argument("--forecast-start-date", default="2026-07-06")
    parser.add_argument("--raw-cache-dir", type=Path, default=None,
                        help="Dir with mozaic_parts.raw.legacy.desktop.DAU.parquet (skip BQ query).")
    parser.add_argument("--parallel", type=int, default=4)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    round_dir = RESULTS_ROOT / f"desktop_gradient_round{args.round}"
    probes = build_probes(args.round)

    print("=" * 70)
    print(f"Desktop gradient round {args.round} — {len(probes)} probes")
    print(f"Forecast start : {args.forecast_start_date}")
    print(f"Raw cache      : {args.raw_cache_dir or '(none: BQ queried per probe)'}")
    print(f"Round dir      : {round_dir}")
    print(f"Parallel       : {args.parallel}")
    print("=" * 70)
    for label, ov in probes:
        desc = ", ".join(f"{k.split('_')[-1]}={v}" for k, v in ov.items()) or "(center: defaults)"
        mark = "" if args.force or not probe_done(round_dir / label) else "  [done, skip]"
        print(f"  {label:32s} {desc}{mark}")

    if args.dry_run:
        print("\n[dry-run] Nothing executed.")
        return 0

    print(f"\nLaunching {len(probes)} probes (up to {args.parallel} at a time)...\n")
    started = time.monotonic()
    results = []
    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = {pool.submit(run_probe, round_dir, args.forecast_start_date,
                               args.raw_cache_dir, label, ov, args.force): label
                   for label, ov in probes}
        total = len(futures)
        for i, fut in enumerate(as_completed(futures), 1):
            label, status, elapsed = fut.result()
            wall = (time.monotonic() - started) / 60
            print(f"[{i}/{total}] {label:32s} {status:28s} {elapsed:6.1f}s (wall {wall:4.1f}m)")
            results.append((label, status, elapsed))

    failures = [r for r in results if r[1].startswith("FAILED")]
    print("\n" + "=" * 70)
    print(f"Done. {len(results)} probes, {len(failures)} failed, "
          f"wall {(time.monotonic() - started)/60:.1f}m")
    for label, status, _ in failures:
        print(f"  {label}: {status}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
