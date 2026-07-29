#!/usr/bin/env python3
"""Grid driver for the August 2026 desktop summer-trough search.

Objective
---------
Raise the **Aug-25 trough minimum** (28d-MA, post-headwind) while holding
**Dec-15 within +-50,000** of the canonical 48,672,970 -- i.e. inside
[48,622,970, 48,722,970]. The Win10 headwind stays fixed at -1,245,000: the
+-50k allowance *is* the headwind's own adjustability, so it is not a lever here.

Why this grid
-------------
The two known endpoints are the canonical build (``regime=auto``, trough
43,833,674 / Dec-15 48,672,970) and ``regime=multiplicative`` (45,140,569 /
48,925,520). The second is 5x outside the Dec-15 band, so the search is for the
interior and for any knob that moves Dec-15 and Aug-25 *asymmetrically*.

The primary axis is ``seasonality_corr_threshold`` (newly exposed). Desktop's
regime switch is per tile, so this cutoff dials the fraction of tiles running
multiplicative -- the genuine interior between the endpoints, which the 3-point
``seasonality_regime`` enum cannot express. Grid points are placed against the
measured per-tile corr distribution rather than spread evenly on [-1, 1]:

    threshold   tiles mult   DAU-weighted mult
      0.00        37.5%           7.6%   <- canonical
     -0.105       39.6%          ~11%
     -0.13        ~45%           ~16%
     -0.15        54.2%          48.9%   <- ROW/modern_windows (27% of weight) flips
     -0.26        81.2%          78.0%
     -0.35        93.8%          98.0%
     -1.00        100%          100%     <- should reproduce regime=multiplicative

Secondary axes cross the promising thresholds with the knobs most likely to move
Dec-15 harder than Aug-25: ``holiday_threshold`` (Dec-15's 28d window spans
Nov-18..Dec-15 and contains Thanksgiving, while the Aug window is nearly
holiday-free) and ``changepoint_range``. Plus single probes on ``sps``,
``recent_weeks`` and the ``additive`` endpoint to bound the chart.

Usage
-----
    source .venv/bin/activate
    python scripts/run_summer_trough_grid.py --dry-run      # print the plan
    python scripts/run_summer_trough_grid.py --workers 3    # run it

Each probe writes ``<results-dir>/<slug>/`` via ``run_param_scan.py`` and reuses
the canonical build's cached BigQuery pull, so nothing re-queries BQ. Probes are
idempotent by slug: re-running skips any probe whose forecast parquet exists
unless ``--force`` is passed.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCAN_SCRIPT = REPO_ROOT / "scripts/run_param_scan.py"

FORECAST_START = "2026-07-28"
CANONICAL_DIR = (REPO_ROOT / "data-official/2026-08/desktop_baseline_2026-07-28"
                 / "cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825")
DEFAULT_RESULTS_DIR = REPO_ROOT / "research/param-scans/summer-trough-v2/grid"

# The locked center. Every probe is this plus its own overrides.
CENTER = {
    "changepoint-prior-scale": 0.08983,
    "changepoint-range": 0.65,
    "n-changepoints": 25,
    "recent-weeks": 13,
    "seasonality-prior-scale": 0.00825,
    "holiday-threshold": -0.032,
    "holiday-max-radius": 5,
    "holiday-min-radius": 3,
    "holiday-effect-floor": -0.6,
}

# Thresholds chosen against the measured corr distribution (see module docstring).
CORR_FACTORIAL = [0.0, -0.13, -0.15, -0.26]
CORR_FILL = [-0.105, -0.14, -0.20, -0.35]
# Less-negative holiday_threshold detects more holidays, so it should bite hardest
# in the Dec-15 window. Crossed with cpr, the live trend knob under multiplicative.
HOLIDAY_THRESHOLDS = [-0.032, -0.024]
CHANGEPOINT_RANGES = [0.65, 0.55]


def build_probe_list() -> list[dict]:
    """The 24 probes, as override dicts. Center-only is excluded (already built)."""
    probes: list[dict] = []
    seen: set[tuple] = set()

    def add(note: str, **overrides):
        key = tuple(sorted(overrides.items()))
        if key in seen or not overrides:
            return
        seen.add(key)
        probes.append({"note": note, "overrides": overrides})

    # Core factorial: corr x holiday_threshold x changepoint_range.
    for corr in CORR_FACTORIAL:
        for hol in HOLIDAY_THRESHOLDS:
            for cpr in CHANGEPOINT_RANGES:
                ov = {}
                if corr != 0.0:
                    ov["seasonality-corr-threshold"] = corr
                if hol != CENTER["holiday-threshold"]:
                    ov["holiday-threshold"] = hol
                if cpr != CENTER["changepoint-range"]:
                    ov["changepoint-range"] = cpr
                add("factorial", **ov)

    # Extra resolution on the primary axis, everything else at center.
    for corr in CORR_FILL:
        add("corr-fill", **{"seasonality-corr-threshold": corr})

    # Validation: at -1.00 every tile is above the cutoff, so this should land
    # essentially on regime=multiplicative. If it does not, the axis is wrong.
    add("validate-endpoint", **{"seasonality-corr-threshold": -1.0})

    # Opposite endpoint, to bound the chart on the additive side.
    add("additive-endpoint", **{"seasonality-regime": "additive"})

    # Seasonality amplitude at a mid-interior threshold. Known to trade summer
    # against December ~1:1.4 under auto; worth one measurement at this center.
    for sps in (0.006, 0.012):
        add("sps", **{"seasonality-corr-threshold": -0.15,
                      "seasonality-prior-scale": sps})

    # Conditional weekly-seasonality window.
    add("recent-weeks", **{"seasonality-corr-threshold": -0.15, "recent-weeks": 8})

    return probes


def probe_command(overrides: dict, results_dir: Path) -> list[str]:
    args = dict(CENTER)
    args.update(overrides)
    cmd = [sys.executable, str(SCAN_SCRIPT),
           "--forecast-start-date", FORECAST_START,
           "--raw-cache-dir", str(CANONICAL_DIR),
           "--results-dir", str(results_dir)]
    for flag, value in args.items():
        cmd += [f"--{flag}", str(value)]
    return cmd


def resolve_slug(overrides: dict, results_dir: Path) -> str:
    """Ask run_param_scan for the slug via --dry-run, so it stays the one source."""
    cmd = probe_command(overrides, results_dir) + ["--dry-run"]
    out = subprocess.run(cmd, capture_output=True, text=True).stdout
    for line in out.splitlines():
        if line.startswith("Slug"):
            return line.split(":", 1)[1].strip()
    raise RuntimeError(f"could not resolve slug from --dry-run output:\n{out}")


def run_probe(index: int, total: int, overrides: dict, results_dir: Path,
              log_dir: Path) -> tuple[str, bool, float, str]:
    """Run one probe. Returns (label, ok, seconds, log_path)."""
    label = ",".join(f"{k}={v}" for k, v in sorted(overrides.items())) or "center"
    log_path = log_dir / f"probe_{index:02d}.log"
    started = time.time()
    with open(log_path, "w") as fh:
        fh.write(f"# probe {index}/{total}: {label}\n")
        fh.flush()
        proc = subprocess.run(probe_command(overrides, results_dir),
                              stdout=fh, stderr=subprocess.STDOUT)
    return label, proc.returncode == 0, time.time() - started, str(log_path)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    p.add_argument("--workers", type=int, default=3,
                   help="Concurrent probes (3 is proven safe on this machine).")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the probe plan and exit.")
    p.add_argument("--force", action="store_true",
                   help="Re-run probes whose output parquet already exists.")
    args = p.parse_args()

    probes = build_probe_list()
    results_dir = args.results_dir
    log_dir = results_dir / "logs"

    print(f"Summer-trough grid: {len(probes)} probes, {args.workers} at a time")
    print(f"  results : {results_dir}")
    print(f"  raw cache: {CANONICAL_DIR.name}")
    print(f"  objective: raise Aug-25 min, hold Dec-15 in [48,622,970, 48,722,970]\n")

    for i, probe in enumerate(probes, 1):
        label = ",".join(f"{k}={v}" for k, v in sorted(probe["overrides"].items()))
        print(f"  [{i:2d}/{len(probes)}] {probe['note']:18s} {label}")

    if args.dry_run:
        print("\n[dry-run] nothing executed.")
        return 0

    results_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Skip probes already on disk so an interrupted grid resumes cheaply.
    pending = []
    for i, probe in enumerate(probes, 1):
        slug = resolve_slug(probe["overrides"], results_dir)
        parquet = (results_dir / slug
                   / f"mozaic_daily_forecast.{FORECAST_START}.ld-D.adj-lo.parquet")
        if parquet.exists() and not args.force:
            print(f"  skip (exists): {slug}")
            continue
        pending.append((i, probe))

    print(f"\nrunning {len(pending)} of {len(probes)} probes\n")
    if not pending:
        return 0

    started = time.time()
    failures = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(run_probe, i, len(probes), probe["overrides"],
                        results_dir, log_dir): (i, probe)
            for i, probe in pending
        }
        for done, future in enumerate(as_completed(futures), 1):
            label, ok, secs, log_path = future.result()
            elapsed = time.time() - started
            rate = elapsed / done
            eta = rate * (len(pending) - done)
            status = "ok  " if ok else "FAIL"
            sys.stdout.write(
                f"[{done}/{len(pending)}] {status} {secs/60:.1f}m  {label}\n"
                f"    elapsed {elapsed/60:.1f}m | mean {rate/60:.1f}m/probe "
                f"| ETA {eta/60:.1f}m\n"
            )
            sys.stdout.flush()
            if not ok:
                failures.append((label, log_path))

    print(f"\ndone in {(time.time()-started)/60:.1f}m")
    if failures:
        print(f"{len(failures)} FAILED:")
        for label, log_path in failures:
            print(f"  {label}\n    {log_path}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
