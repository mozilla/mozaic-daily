"""Round 4: dense sweep inside (-0.2, 0.0), then bisect the step that straddles the target.

Round 3 found that corr_threshold's entire 1,355,877 transition is compressed into t in
(-0.2, 0.0): every t from -1.0 to -0.4 is byte-identical to s01, and t=0.0 is the full drop.
So essentially every tile's corr(|y|,|dy|) lies in that 0.2-wide window, and the sweep that
stepped at 0.2 resolution jumped clean over it.

This round samples 8 points inside the window, then -- if the target lands inside a step rather
than on a sampled point -- bisects that step up to MAX_BISECTIONS times.

The response is a per-tile STAIRCASE, not a curve: each threshold flips some set of tiles from
additive to multiplicative. Bisection is therefore not guaranteed to converge. If one large tile
carries a step taller than the accept band, no threshold lands inside it, and the correct outcome
is to report that -- not to keep subdividing. The loop stops and says so.

Usage:
    python research/param-scans/aug25-gap/run_dense_round4.py [--dry-run] [--parallel N]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))
sys.path.insert(0, str(HERE))

from run_corr_round3 import RAW_CACHE, RESULTS_DIR, S01, FORECAST_START, LOG_DIR  # noqa: E402

SWEEP = [-0.18, -0.16, -0.14, -0.12, -0.10, -0.08, -0.05, -0.02]
MAX_BISECTIONS = 3


def command(threshold: float) -> tuple[str, list[str]]:
    label = f"corr{threshold:+.4f}".replace(".", "p")
    argv = [
        "python", "scripts/run_param_scan.py",
        "--forecast-start-date", FORECAST_START,
        "--results-dir", str(RESULTS_DIR.relative_to(REPO_ROOT)),
        "--raw-cache-dir", str(RAW_CACHE.relative_to(REPO_ROOT)),
        "--seasonality-regime", "auto",
        "--seasonality-corr-threshold", str(threshold),
        "--holiday-threshold", "-0.032",
        "--holiday-max-radius", "5",
        "--holiday-min-radius", "3",
        "--holiday-effect-floor", "-0.6",
    ]
    for knob, value in S01.items():
        argv += [f"--{knob}", value]
    return label, argv


def run(threshold: float) -> bool:
    label, argv = command(threshold)
    started = time.time()
    print(f"START  {label}", flush=True)
    with open(LOG_DIR / f"round4_{label}.log", "w") as handle:
        ok = subprocess.run(argv, cwd=REPO_ROOT, stdout=handle,
                            stderr=subprocess.STDOUT).returncode == 0
    print(f"{'DONE  ' if ok else 'FAILED'} {label}  ({(time.time() - started) / 60:.1f}m)",
          flush=True)
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--parallel", type=int, default=3)
    args = parser.parse_args()

    print(f"Dense sweep inside (-0.2, 0.0): {SWEEP}")
    if args.dry_run:
        for threshold in SWEEP:
            print(" ".join(command(threshold)[1]))
        return 0

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        results = list(pool.map(run, SWEEP))
    print(f"\nSweep finished. {sum(results)}/{len(results)} succeeded.\n")

    # Import scoring only now: it reads every dial parquet on disk, so it must run after the
    # sweep has written them.
    from score_corr import AUG25_TARGET, AUG25_TOLERANCE, collect, solve

    for iteration in range(1, MAX_BISECTIONS + 1):
        df, _ = collect()
        best = df.iloc[(df["aug25"] - AUG25_TARGET).abs().argmin()]
        print(f"[bisect {iteration}] closest sampled: t={best.corr_threshold:+.4f}  "
              f"Aug-25 {best.aug25:,.0f}  ({best.aug25 - AUG25_TARGET:+,.0f} vs target)")

        if abs(best["aug25"] - AUG25_TARGET) <= AUG25_TOLERANCE:
            print(f"[bisect {iteration}] IN BAND -- stopping.")
            break

        crossing, height, bracket = solve(df)
        if bracket is None:
            print(f"[bisect {iteration}] target not bracketed by any sampled pair -- stopping.")
            break

        midpoint = round((bracket[0] + bracket[1]) / 2, 4)
        if any(abs(midpoint - t) < 1e-6 for t in df["corr_threshold"]):
            print(f"[bisect {iteration}] midpoint {midpoint} already sampled; the step is "
                  f"{height:,.0f} DAU tall and cannot be subdivided further -- stopping.")
            break

        print(f"[bisect {iteration}] step across [{bracket[0]}, {bracket[1]}] is {height:,.0f} "
              f"DAU tall; sampling midpoint {midpoint}")
        if not run(midpoint):
            print(f"[bisect {iteration}] run failed -- stopping.")
            break
    else:
        print(f"Bisection budget ({MAX_BISECTIONS}) exhausted.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
