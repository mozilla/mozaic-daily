#!/usr/bin/env python3
"""Trend-only grid at the s01 center. Holiday parameters are PERMANENTLY excluded.

Why holidays are out (Brendan, 2026-07-29)
------------------------------------------
Holiday parameters produce **strictly local** effects -- a few days around detected
holiday dates. Using them to move a whole-season quantity like the Aug trough or
the Dec-15 28d-MA means compensating for an overall trend with a small regional
fix, which is overfitting. They are excluded from trough searches from now on
regardless of how large their measured slope looks.

This is not a soft preference: the driver REFUSES holiday overrides rather than
merely omitting them, so the exclusion cannot be reintroduced by accident. All
four stay at their package defaults, which is what both the canonical August build
and s01 already use -- so excluding them costs nothing relative to the current
best result.

(For the record, the gradient measured holiday_max_radius as the second-largest
trough lever at this center. That is exactly the temptation being declined.)

Searchable set -- 6 numeric knobs plus the regime enum
-----------------------------------------------------
    changepoint_prior_scale, changepoint_range, recent_weeks, n_changepoints,
    seasonality_prior_scale, seasonality_corr_threshold  (+ seasonality_regime)

Objective
---------
Raise the Aug-25 trough minimum (28d-MA, post-headwind) while holding Dec-15 in
[48,622,970, 48,722,970] (+-50,000 of the canonical 48,672,970). Headwind fixed at
-1,245,000.

Center = July's s01 rebuilt on August data, which already satisfies both:
    trough 45,193,561 · Dec-15 48,678,612 (+5,642) · seam kink -20,604/day
Room at the center: 55,642 DOWN, 44,358 UP on Dec-15.

Design
------
``changepoint_range`` dominates (+102,862 trough per +10% of center) and moves
Dec-15 *down* while raising the trough -- favourable in both objectives at once, and
pointing at the larger side of the remaining Dec-15 room. But its curvature is 2.4x
its own slope, so the gradient cannot be extrapolated; this grid MEASURES the
response instead of trusting the slope.

``n_changepoints`` raises both (ratio 1.16) while ``changepoint_range`` raises trough
and lowers Dec (ratio 0.39). Non-parallel, so they are the natural compensating
pair: push ncp up to buy trough with Dec-15's upward room, and cpr up to buy trough
*and* recover Dec-15. Hence a cpr x ncp factorial as the core, with the second-tier
knobs swept singly.

Usage
-----
    source .venv/bin/activate
    python scripts/run_trend_only_grid.py --dry-run
    python scripts/run_trend_only_grid.py --workers 3
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
DEFAULT_RESULTS_DIR = REPO_ROOT / "research/param-scans/summer-trough-v2/trend_only"

# Flags this driver refuses. Holiday effects are local; see the module docstring.
FORBIDDEN_FLAGS = {
    "holiday-threshold", "holiday-max-radius",
    "holiday-min-radius", "holiday-effect-floor",
}

# s01, trend knobs only. Holiday knobs are deliberately absent -> package defaults.
CENTER = {
    "seasonality-regime": "multiplicative",
    "changepoint-prior-scale": 0.1849,
    "changepoint-range": 0.734,
    "recent-weeks": 17,
    "n-changepoints": 35,
    "seasonality-prior-scale": 0.00825,
}

# Core factorial. cpr is the dominant lever and lowers Dec-15; ncp raises both.
CPR_VALUES = [0.734, 0.78, 0.82, 0.86]
NCP_VALUES = [35, 42, 50]
# Second tier, swept singly at the center.
CPS_VALUES = [0.13, 0.26, 0.32]
CPR_EXTENDED = [0.90, 0.94]
RECENT_VALUES = [20, 24]
SPS_VALUES = [0.02, 0.05]
NCP_EXTENDED = [60]


def build_probe_list() -> list[dict]:
    probes: list[dict] = []
    seen: set[tuple] = set()

    def add(note: str, **overrides):
        overrides = {k: v for k, v in overrides.items()
                     if CENTER.get(k) != v}          # drop no-op overrides
        key = tuple(sorted(overrides.items()))
        if not overrides or key in seen:
            return
        seen.add(key)
        probes.append({"note": note, "overrides": overrides})

    for cpr in CPR_VALUES:
        for ncp in NCP_VALUES:
            add("cpr x ncp", **{"changepoint-range": cpr, "n-changepoints": ncp})
    for cps in CPS_VALUES:
        add("cps", **{"changepoint-prior-scale": cps})
    for cpr in CPR_EXTENDED:
        add("cpr extended", **{"changepoint-range": cpr})
    for rw in RECENT_VALUES:
        add("recent_weeks", **{"recent-weeks": rw})
    for sps in SPS_VALUES:
        add("sps", **{"seasonality-prior-scale": sps})
    for ncp in NCP_EXTENDED:
        add("ncp extended", **{"n-changepoints": ncp})
    return probes


def probe_command(overrides: dict, results_dir: Path) -> list[str]:
    bad = FORBIDDEN_FLAGS & set(overrides)
    if bad:
        raise ValueError(
            f"holiday parameters are permanently excluded from this search "
            f"(local effects; see module docstring) -- refused: {sorted(bad)}")
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
    out = subprocess.run(probe_command(overrides, results_dir) + ["--dry-run"],
                         capture_output=True, text=True).stdout
    for line in out.splitlines():
        if line.startswith("Slug"):
            return line.split(":", 1)[1].strip()
    raise RuntimeError(f"could not resolve slug:\n{out}")


def run_probe(index: int, overrides: dict, results_dir: Path,
              log_dir: Path) -> tuple[str, bool, float, str]:
    label = ",".join(f"{k}={v}" for k, v in sorted(overrides.items())) or "center"
    log_path = log_dir / f"probe_{index:02d}.log"
    started = time.time()
    with open(log_path, "w") as fh:
        fh.write(f"# probe {index}: {label}\n")
        fh.flush()
        proc = subprocess.run(probe_command(overrides, results_dir),
                              stdout=fh, stderr=subprocess.STDOUT)
    return label, proc.returncode == 0, time.time() - started, str(log_path)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    probes = build_probe_list()
    log_dir = args.results_dir / "logs"

    print(f"Trend-only grid: {len(probes)} probes (holiday knobs excluded by design)")
    print(f"  center : {CENTER}")
    print(f"  results: {args.results_dir}\n")
    for i, probe in enumerate(probes, 1):
        ov = ", ".join(f"{k}={v}" for k, v in sorted(probe["overrides"].items()))
        print(f"  [{i:2d}/{len(probes)}] {probe['note']:14s} {ov}")

    if args.dry_run:
        print("\n[dry-run] nothing executed.")
        return 0

    args.results_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Slug collisions silently overwrite and read back as "no effect" -- refuse.
    resolved: dict[str, str] = {}
    collisions = []
    for probe in probes:
        slug = resolve_slug(probe["overrides"], args.results_dir)
        label = ",".join(f"{k}={v}" for k, v in sorted(probe["overrides"].items()))
        if slug in resolved:
            collisions.append((label, resolved[slug], slug))
        resolved[slug] = label
    if collisions:
        print("\nABORT: probes share an output slug and would overwrite each other:")
        for a, b, slug in collisions:
            print(f"  {a}  <->  {b}\n    both -> {slug}")
        return 2

    pending = []
    for i, probe in enumerate(probes, 1):
        slug = resolve_slug(probe["overrides"], args.results_dir)
        parquet = (args.results_dir / slug
                   / f"mozaic_daily_forecast.{FORECAST_START}.ld-D.adj-lo.parquet")
        if parquet.exists() and not args.force:
            print(f"  skip (exists): {slug}")
            continue
        pending.append((i, probe))

    print(f"\nrunning {len(pending)} of {len(probes)}\n")
    if not pending:
        return 0

    started, failures = time.time(), []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_probe, i, pr["overrides"], args.results_dir,
                              log_dir): (i, pr) for i, pr in pending}
        for done, future in enumerate(as_completed(futures), 1):
            label, ok, secs, log_path = future.result()
            elapsed = time.time() - started
            rate = elapsed / done
            sys.stdout.write(
                f"[{done}/{len(pending)}] {'ok  ' if ok else 'FAIL'} "
                f"{secs/60:.1f}m  {label}\n"
                f"    elapsed {elapsed/60:.1f}m | mean {rate/60:.1f}m/probe "
                f"| ETA {rate*(len(pending)-done)/60:.1f}m\n")
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
