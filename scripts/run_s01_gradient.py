#!/usr/bin/env python3
"""Central-difference sensitivity gradient at July's s01 config, on August data.

Purpose is **sensitivity, not optimisation**. The center is expected to fail the
Dec-15 criterion; the deliverable is the local slope of each numeric parameter so
the shape of the phase space around this point is known.

Center = July's best-trough candidate s01 (`research/param-scans/aug22-retune/`,
28-point LHS winner), rebuilt at the August anchor:

    regime=multiplicative, cps=0.1849, cpr=0.734, recent_weeks=17,
    n_changepoints=35, sps=0.00825, holiday knobs at package defaults

Method
------
Each numeric parameter gets a +delta and a -delta probe with everything else held
at center, giving a two-sided estimate  dKPI/dparam ~ (f(x+h) - f(x-h)) / 2h.

Deltas are deliberately **small** (~5-12% of center) so these are local
derivatives. July's own gradient used medium deltas (cps +-17%, recent +-23%) and
its script flags that those are secants blending slope with curvature -- this run
is meant to be the local-slope counterpart, not a repeat.

Two irreducible coarseness caveats, both reported rather than hidden:
- Integer knobs cannot go below +-1. For the holiday radii that is +-20% and
  +-33% of center, so their "slopes" remain secants.
- ``seasonality_corr_threshold`` is ignored when ``seasonality_regime`` is forced,
  so its gradient is taken at the *equivalent* center expressed as
  ``regime=auto, corr=-0.35`` (which reproduced forced-multiplicative to within
  26 DAU at the previous center). Both center forms are run so the substitution
  is verified rather than assumed.

Usage
-----
    source .venv/bin/activate
    python scripts/run_s01_gradient.py --dry-run
    python scripts/run_s01_gradient.py --workers 3
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
DEFAULT_RESULTS_DIR = REPO_ROOT / "research/param-scans/summer-trough-v2/s01_gradient"

# July's s01, as CLI flags. The gradient center.
CENTER = {
    "seasonality-regime": "multiplicative",
    "changepoint-prior-scale": 0.1849,
    "changepoint-range": 0.734,
    "recent-weeks": 17,
    "n-changepoints": 35,
    "seasonality-prior-scale": 0.00825,
    "holiday-threshold": -0.032,
    "holiday-max-radius": 5,
    "holiday-min-radius": 3,
    "holiday-effect-floor": -0.6,
}

# The corr-threshold value that stands in for forced multiplicative. Verified at the
# previous center to reproduce it within 26 DAU; re-verified here by running both forms.
CORR_EQUIV = -0.35

# (flag, center value, delta, kind). Delta sizes are ~5-12% of center except the
# integer knobs, which bottom out at +-1.
GRADIENT_AXES = [
    ("changepoint-prior-scale", 0.1849, 0.0185, "float"),   # ~10%
    ("changepoint-range", 0.734, 0.025, "float"),           # ~3.4%
    ("recent-weeks", 17, 2, "int"),                         # ~12%
    ("n-changepoints", 35, 3, "int"),                       # ~8.6%
    ("seasonality-prior-scale", 0.00825, 0.000825, "float"),  # 10%
    ("holiday-threshold", -0.032, 0.003, "float"),          # ~9.4%
    ("holiday-max-radius", 5, 1, "int"),                    # 20% (floor)
    ("holiday-min-radius", 3, 1, "int"),                    # 33% (floor)
    ("holiday-effect-floor", -0.6, 0.05, "float"),          # ~8.3%
]


def build_probe_list() -> list[dict]:
    """Two center forms, then a +/- pair per numeric axis."""
    probes: list[dict] = [
        {"label": "center_mult", "note": "center (s01, forced multiplicative)",
         "overrides": {}},
        {"label": "center_auto_corr", "note": "center (equivalent, regime=auto)",
         "overrides": {"seasonality-regime": "auto",
                       "seasonality-corr-threshold": CORR_EQUIV}},
    ]

    for flag, center, delta, kind in GRADIENT_AXES:
        for sign, tag in ((-1, "minus"), (+1, "plus")):
            value = center + sign * delta
            value = int(round(value)) if kind == "int" else round(value, 6)
            probes.append({
                "label": f"{flag}__{tag}",
                "note": f"d/d({flag})",
                "overrides": {flag: value},
            })

    # corr_threshold is only live under regime=auto, so its pair sits on the
    # equivalent auto center rather than the forced-multiplicative one.
    for sign, tag in ((-1, "minus"), (+1, "plus")):
        probes.append({
            "label": f"seasonality-corr-threshold__{tag}",
            "note": "d/d(corr_threshold), on the auto center",
            "overrides": {"seasonality-regime": "auto",
                          "seasonality-corr-threshold": round(CORR_EQUIV + sign * 0.05, 4)},
        })

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
    out = subprocess.run(probe_command(overrides, results_dir) + ["--dry-run"],
                         capture_output=True, text=True).stdout
    for line in out.splitlines():
        if line.startswith("Slug"):
            return line.split(":", 1)[1].strip()
    raise RuntimeError(f"could not resolve slug:\n{out}")


def run_probe(label: str, overrides: dict, results_dir: Path,
              log_dir: Path) -> tuple[str, bool, float, str]:
    log_path = log_dir / f"{label.replace('/', '_')}.log"
    started = time.time()
    with open(log_path, "w") as fh:
        fh.write(f"# {label}: {overrides}\n")
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

    print(f"s01 sensitivity gradient: {len(probes)} probes "
          f"({len(GRADIENT_AXES) + 1} axes x 2, plus 2 center forms)")
    print(f"  center  : {CENTER}")
    print(f"  results : {args.results_dir}\n")
    for i, probe in enumerate(probes, 1):
        ov = ", ".join(f"{k}={v}" for k, v in probe["overrides"].items()) or "(center)"
        print(f"  [{i:2d}/{len(probes)}] {probe['label']:38s} {ov}")

    if args.dry_run:
        print("\n[dry-run] nothing executed.")
        return 0

    args.results_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Defence in depth. The slug is the output directory, so two probes resolving to
    # the same slug race and overwrite each other -- and the colliding probes then
    # read back identical numbers, which looks like "no effect" rather than an error.
    # to_slug() is tested for injectivity upstream; refuse to run if it ever regresses.
    resolved: dict[str, str] = {}
    collisions = []
    for probe in probes:
        slug = resolve_slug(probe["overrides"], args.results_dir)
        if slug in resolved:
            collisions.append((probe["label"], resolved[slug], slug))
        resolved[slug] = probe["label"]
    if collisions:
        print("\nABORT: probes share an output slug and would overwrite each other:")
        for a, b, slug in collisions:
            print(f"  {a}  <->  {b}\n    both -> {slug}")
        print("\nFix ModelConfig.to_slug() so it is injective over the varied fields.")
        return 2

    pending = []
    for probe in probes:
        slug = resolve_slug(probe["overrides"], args.results_dir)
        parquet = (args.results_dir / slug
                   / f"mozaic_daily_forecast.{FORECAST_START}.ld-D.adj-lo.parquet")
        if parquet.exists() and not args.force:
            print(f"  skip (exists): {probe['label']}")
            continue
        pending.append(probe)

    print(f"\nrunning {len(pending)} of {len(probes)}\n")
    if not pending:
        return 0

    started, failures = time.time(), []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_probe, pr["label"], pr["overrides"],
                              args.results_dir, log_dir): pr for pr in pending}
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
