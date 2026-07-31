"""Round 7: extend past the grid edges where every Pareto-optimal cell was pinned.

Round 6's 243-cell grid produced a 7-point Pareto frontier (Aug-25 accuracy vs seam kink) and
EVERY point sat at cpr=0.814 and ncp=40 -- both upper edges of that design -- with the best-kink
points also at recent=20, a third upper edge. Three of five knobs pinned at a boundary says the
optimum is outside the box, so this round extends past all three rather than only refining inside.

    cps     0.1749 / 0.1849 / 0.1949     half-step refinement (0.1849 is on 4 of 7 frontier points)
    cpr     0.814  / 0.829  / 0.844      extends 2 steps past the edge
    ncp     40 / 43 / 46 / 49            extends 3 steps past the edge
    recent  20 / 23 / 26                 extends 2 steps past the edge
    sps     0.00825                      HELD -- see below

3 x 3 x 4 x 3 x 1 = 108 cells.

sps is held rather than varied: Round 6's exact decomposition put it among the weakest factors
(correlation +0.007 with Aug-25, absent from the top main effects), and the two adjacent frontier
points that differ only in sps are 224 apart in kink penalty. Spending the budget on cpr/ncp/recent
-- which carry 40.6% / 31.3% / 18.5% (with cpr:ncp) of Aug-25 variance -- buys far more. If a winner
emerges, sps 0.0065 can be tested on it in 1-2 follow-up runs.

cpr stops at 0.844 deliberately: Round 5 measured cpr 0.86 at Dec-15 +284,733, far outside the cap.

SELECTION RULE for this round (changed by the user 2026-07-30):
    hard  Aug-25 within +/-75,000 of 45,027,066
    hard  Dec-15 within +/-50,000 of 48,703,960
    prefer Dec-15 margin -- cells above ~80% of cap (|delta| > 40,000) are flagged, not preferred
    minimise the seam-kink increase vs s01's -9,554     <- the objective

Usage:
    python research/param-scans/aug25-gap/run_finesweep_round7.py [--dry-run] [--parallel N]
"""

from __future__ import annotations

import argparse
import itertools
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from run_corr_round3 import FORECAST_START, LOG_DIR, RAW_CACHE, RESULTS_DIR  # noqa: E402
from run_grid_round6 import AUTH_MARKERS, PARQUET_NAME, check_credentials  # noqa: E402

LEVELS = {
    "cps": (0.1749, 0.1849, 0.1949),
    "cpr": (0.814, 0.829, 0.844),
    "ncp": (40, 43, 46, 49),
    "recent": (20, 23, 26),
    "sps": (0.00825,),
}

_print_lock = threading.Lock()
_durations: list[float] = []
_auth_failed = threading.Event()


def slug_for(cps, cpr, ncp, recent, sps) -> str:
    return (f"cps{cps}_thresh032_recent{recent}_cpr{cpr}_ncp{ncp}"
            f"_clip0.6_sps{sps}_regimemultiplicative")


def command(cps, cpr, ncp, recent, sps) -> list[str]:
    return [
        "python", "scripts/run_param_scan.py",
        "--forecast-start-date", FORECAST_START,
        "--results-dir", str(RESULTS_DIR.relative_to(REPO_ROOT)),
        "--raw-cache-dir", str(RAW_CACHE.relative_to(REPO_ROOT)),
        "--seasonality-regime", "multiplicative",
        "--changepoint-prior-scale", str(cps),
        "--changepoint-range", str(cpr),
        "--n-changepoints", str(ncp),
        "--recent-weeks", str(recent),
        "--seasonality-prior-scale", str(sps),
        "--holiday-threshold", "-0.032",
        "--holiday-max-radius", "5",
        "--holiday-min-radius", "3",
        "--holiday-effect-floor", "-0.6",
    ]


def run_cell(payload) -> tuple[str, bool]:
    index, total, combo, started_at = payload
    slug = slug_for(*combo)
    slug_dir = RESULTS_DIR / slug
    began = time.time()

    if (slug_dir / PARQUET_NAME).exists():
        return slug, True
    if _auth_failed.is_set():
        return slug, False

    log_path = LOG_DIR / f"round7_{slug}.log"
    with open(log_path, "w") as handle:
        ok = subprocess.run(command(*combo), cwd=REPO_ROOT, stdout=handle,
                            stderr=subprocess.STDOUT).returncode == 0

    for pkl in slug_dir.glob("mozaic_objects.*.pkl"):
        pkl.unlink()

    elapsed = time.time() - began
    with _print_lock:
        _durations.append(elapsed)
        done, wall = len(_durations), time.time() - started_at
        rate = done / wall if wall else 0
        sys.stdout.write(
            f"[{done}/{total}] {slug[:50]:<50} {elapsed:5.1f}s | "
            f"median {statistics.median(_durations[-30:]):5.1f}s | "
            f"elapsed {wall / 60:5.1f}m | ETA {((total - done) / rate) / 60 if rate else 0:5.1f}m\n")
        sys.stdout.flush()

    if not ok:
        if any(marker in log_path.read_text()[-4000:] for marker in AUTH_MARKERS):
            _auth_failed.set()
            with _print_lock:
                sys.stdout.write("  AUTH FAILURE -- ABORTING. Re-run "
                                 "`gcloud auth application-default login`, then relaunch.\n")
                sys.stdout.flush()
        else:
            with _print_lock:
                sys.stdout.write(f"  FAILED {slug}\n")
                sys.stdout.flush()
    return slug, ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--parallel", type=int, default=4)
    args = parser.parse_args()

    combos = list(itertools.product(*LEVELS.values()))
    total = len(combos)
    existing = sum((RESULTS_DIR / slug_for(*c) / PARQUET_NAME).exists() for c in combos)
    print(f"Round 7 extension grid: {total} cells "
          f"({' x '.join(str(len(v)) for v in LEVELS.values())})")
    for knob, levels in LEVELS.items():
        print(f"  {knob:<7} {levels}")
    print(f"Already on disk: {existing}.  To run: {total - existing}.\n")
    if args.dry_run:
        return 0

    check_credentials()
    print("ADC check passed.\n")

    started_at = time.time()
    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        results = list(pool.map(
            run_cell, [(i, total, c, started_at) for i, c in enumerate(combos, 1)]))

    failed = [s for s, ok in results if not ok]
    print(f"\nRound 7 finished in {(time.time() - started_at) / 60:.1f}m. "
          f"{len(results) - len(failed)}/{len(results)} succeeded.")
    if _auth_failed.is_set():
        print("*** ABORTED ON EXPIRED CREDENTIALS -- grid incomplete. ***")
    if failed:
        print("FAILED:\n  " + "\n  ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
