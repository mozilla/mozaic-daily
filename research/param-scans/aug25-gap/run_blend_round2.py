"""Round 2 of the Aug-25 search: blended interpolation along the s01 -> July-params axis.

At our anchor, July's shipped config sits 1,389,575 BELOW s01 at Aug-25 while costing only
30,990 at Dec-15 -- a 44.8:1 lever against the 3.92:1 we need. The target is 14.1% of the way
along that axis.

Every trend knob is interpolated linearly at fraction f (0 = s01, 1 = July). Two knobs are
not interpolated:

  seasonality_prior_scale -- both ends are already 0.00825 (July did not set it and the CLI
      default is 0.00825), so there is no axis to travel.
  seasonality_regime      -- an ENUM, not a continuous knob. It cannot be fractionally
      interpolated, so each fraction is run under BOTH regimes. This is the deviation from
      "switch at the midpoint": every f here is <= 0.30, so a midpoint rule would pin all 8
      runs to multiplicative, where Round 1 and the prior art both cap the reachable Aug-25
      move near -150K -- short of the -196,183 required. Running both branches also measures
      how tall the regime step is at OUR center, which decides whether this axis is a ramp we
      can land on or a cliff that needs seasonality_corr_threshold instead.

Usage:
    python research/param-scans/aug25-gap/run_blend_round2.py [--dry-run] [--parallel N]
"""

from __future__ import annotations

import argparse
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
RESULTS_DIR = HERE / "runs"
LOG_DIR = HERE / "logs"
RAW_CACHE = (
    REPO_ROOT / "data-official/2026-08/desktop_baseline_2026-07-28"
    / "cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825"
)

FORECAST_START = "2026-07-28"
FRACTIONS = [0.10, 0.15, 0.20, 0.30]
REGIMES = ["multiplicative", "auto"]

# knob -> (s01 value, July shipped value, is_integer)
AXIS = {
    "changepoint-prior-scale": (0.1849, 0.08983, False),
    "changepoint-range": (0.734, 0.65, False),
    "n-changepoints": (35, 25, True),
    "recent-weeks": (17, 13, True),
}


def blend(fraction: float) -> dict[str, float | int]:
    """Linear interpolation from s01 (f=0) toward July's shipped config (f=1)."""
    out: dict[str, float | int] = {}
    for knob, (s01, july, is_int) in AXIS.items():
        value = s01 + fraction * (july - s01)
        out[knob] = int(round(value)) if is_int else round(value, 5)
    return out


def command(fraction: float, regime: str) -> tuple[str, list[str]]:
    knobs = blend(fraction)
    label = f"f{int(fraction * 100):02d}_{regime[:4]}"
    argv = [
        "python", "scripts/run_param_scan.py",
        "--forecast-start-date", FORECAST_START,
        "--results-dir", str(RESULTS_DIR.relative_to(REPO_ROOT)),
        "--raw-cache-dir", str(RAW_CACHE.relative_to(REPO_ROOT)),
        "--seasonality-prior-scale", "0.00825",
        "--seasonality-regime", regime,
        # Holiday knobs pinned to package defaults on every run: standing policy is that
        # strictly local effects must never be used to move a whole-season quantity.
        "--holiday-threshold", "-0.032",
        "--holiday-max-radius", "5",
        "--holiday-min-radius", "3",
        "--holiday-effect-floor", "-0.6",
    ]
    for knob, value in knobs.items():
        argv += [f"--{knob}", str(value)]
    return label, argv


def run(job: tuple[float, str]) -> tuple[str, bool, float]:
    fraction, regime = job
    label, argv = command(fraction, regime)
    log = LOG_DIR / f"round2_{label}.log"
    started = time.time()
    print(f"START  {label}", flush=True)
    with open(log, "w") as handle:
        ok = subprocess.run(argv, cwd=REPO_ROOT, stdout=handle,
                            stderr=subprocess.STDOUT).returncode == 0
    elapsed = time.time() - started
    print(f"{'DONE  ' if ok else 'FAILED'} {label}  ({elapsed / 60:.1f}m)", flush=True)
    return label, ok, elapsed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--parallel", type=int, default=3)
    args = parser.parse_args()

    if not RAW_CACHE.is_dir():
        raise SystemExit(f"FATAL: raw cache missing: {RAW_CACHE}")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    jobs = [(f, r) for f in FRACTIONS for r in REGIMES]

    print(f"{'f':>5}  {'regime':<15} " + "  ".join(f"{k:>24}" for k in AXIS))
    for fraction in FRACTIONS:
        knobs = blend(fraction)
        print(f"{fraction:>5.2f}  {'both':<15} " + "  ".join(f"{knobs[k]:>24}" for k in AXIS))
    print(f"\n{len(jobs)} runs, {args.parallel} concurrent.")

    if args.dry_run:
        for fraction, regime in jobs:
            print(" ".join(command(fraction, regime)[1]))
        return 0

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        results = list(pool.map(run, jobs))

    failed = [label for label, ok, _ in results if not ok]
    print(f"\nFinished. {len(results) - len(failed)}/{len(results)} succeeded.")
    if failed:
        print("FAILED: " + ", ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
