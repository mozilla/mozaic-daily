"""Round 6: full 3^5 factorial around the best config, to map the local response surface directly.

Centre is the Round-5 winner: cps 0.1849, cpr 0.784, ncp 35, recent 17, sps 0.00825, all under
regime=multiplicative. Every knob takes -delta / centre / +delta, all combinations => 243 cells.

Why 3 levels and not 2: a 2-level factorial reports a main effect of ~0 at a local extremum and
cannot see the curvature that is actually there. Round 1 hit exactly that at s01 (both +/- arms
moved Aug-25 the same direction for three knobs). Three levels give main effects, curvature, and
every interaction up to 5-way from measurement rather than inference.

regime is held at multiplicative: Rounds 2-4 established that auto overshoots by ~1.16M at every
trend-knob setting tested, so including it would double the grid to run cells already known to fail.

cpr uses a tighter delta (+/-0.03) than Round 1's +/-0.05 because the centre moved to 0.784 and
cpr=0.86 was measured at Dec-15 +284,733 -- +/-0.05 would push a third of the grid over the cliff.

DISK: run_param_scan has no flag to suppress the fitted mozaic pickle, and 243 x 634MB would be
~154GB. The driver deletes each pickle once its run completes, keeping only parquet + sidecar +
parameters.json (~1.5MB/cell). Pickles are regenerable from the recorded command.

Usage:
    python research/param-scans/aug25-gap/run_grid_round6.py [--dry-run] [--parallel N]
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

PARQUET_NAME = "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet"

# knob -> (low, centre, high). Centre row is the Round-5 best config.
LEVELS = {
    "cps": (0.1649, 0.1849, 0.2049),
    "cpr": (0.754, 0.784, 0.814),
    "ncp": (30, 35, 40),
    "recent": (14, 17, 20),
    "sps": (0.0065, 0.00825, 0.01),
}

_print_lock = threading.Lock()
_durations: list[float] = []
# Set when any cell dies on expired credentials. The first grid run lost 58 of 243 cells that
# way: each cell failed fast and independently, so the run looked like it was progressing while
# a quarter of it quietly evaporated. Auth is a run-wide condition, not a per-cell one, so the
# whole grid stops instead.
_auth_failed = threading.Event()

AUTH_MARKERS = ("Reauthentication is needed", "RefreshError", "ACCESS_TOKEN_TYPE_UNSUPPORTED",
                "invalid authentication credentials", "DefaultCredentialsError")


def check_credentials() -> None:
    """Fail in the first second, not after two hours, if ADC is expired.

    run_param_scan issues a BigQuery pre-flight check per cell even though the raw pull is
    cached, so every cell needs live credentials for the whole run.
    """
    import google.auth
    from google.auth.transport.requests import Request

    try:
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        credentials.refresh(Request())
    except Exception as exc:  # noqa: BLE001 - any auth failure is equally fatal here
        raise SystemExit(
            f"FATAL: Application Default Credentials are not usable: {type(exc).__name__}: {exc}\n"
            f"Run:  gcloud auth application-default login\n"
            f"Every cell issues a BigQuery pre-flight check, so the grid cannot start without it."
        ) from exc


def slug_for(cps, cpr, ncp, recent, sps) -> str:
    """Reproduce DesktopModelConfig.to_slug() for these knobs, to detect already-run cells."""
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
        # Holiday knobs pinned to package defaults on every cell: strictly local effects must
        # never be used to move a whole-season quantity.
        "--holiday-threshold", "-0.032",
        "--holiday-max-radius", "5",
        "--holiday-min-radius", "3",
        "--holiday-effect-floor", "-0.6",
    ]


def report(index: int, total: int, slug: str, elapsed: float, started_at: float) -> None:
    """Progress line with a median-of-last-30 ETA."""
    with _print_lock:
        _durations.append(elapsed)
        window = _durations[-30:]
        median = statistics.median(window)
        done = len(_durations)
        wall = time.time() - started_at
        # Remaining wall time scales by concurrency, approximated by observed throughput.
        rate = done / wall if wall else 0
        eta = (total - done) / rate if rate else 0
        sys.stdout.write(
            f"[{done}/{total}] {slug[:52]:<52} took {elapsed:5.1f}s | "
            f"median {median:5.1f}s | elapsed {wall / 60:5.1f}m | ETA {eta / 60:5.1f}m\n")
        sys.stdout.flush()


def run_cell(args) -> tuple[str, bool]:
    index, total, combo, started_at = args
    cps, cpr, ncp, recent, sps = combo
    slug = slug_for(cps, cpr, ncp, recent, sps)
    slug_dir = RESULTS_DIR / slug
    began = time.time()

    if (slug_dir / PARQUET_NAME).exists():
        return slug, True

    if _auth_failed.is_set():
        return slug, False  # credentials died; don't burn a subprocess to fail again

    log_path = LOG_DIR / f"round6_{slug}.log"
    with open(log_path, "w") as handle:
        ok = subprocess.run(command(*combo), cwd=REPO_ROOT, stdout=handle,
                            stderr=subprocess.STDOUT).returncode == 0

    # Drop the fitted-model pickle immediately: 243 of them would be ~154GB.
    for pkl in slug_dir.glob("mozaic_objects.*.pkl"):
        pkl.unlink()

    report(index, total, slug, time.time() - began, started_at)
    if not ok:
        tail = log_path.read_text()[-4000:]
        is_auth = any(marker in tail for marker in AUTH_MARKERS)
        if is_auth:
            _auth_failed.set()
        with _print_lock:
            sys.stdout.write(
                f"  {'AUTH FAILURE -- ABORTING GRID' if is_auth else 'FAILED'} {slug}\n")
            if is_auth:
                sys.stdout.write("  Credentials expired mid-run. Re-run "
                                 "`gcloud auth application-default login`, then relaunch this "
                                 "script -- completed cells are skipped.\n")
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

    print(f"Full factorial {' x '.join(str(len(v)) for v in LEVELS.values())} = {total} cells")
    for knob, levels in LEVELS.items():
        print(f"  {knob:<7} {levels}")
    print(f"Already on disk: {existing}.  To run: {total - existing}.  "
          f"Concurrency: {args.parallel}.")
    print("Pickles deleted per cell; ~1.5MB retained each.\n")

    if args.dry_run:
        return 0

    check_credentials()
    print("ADC check passed.\n")

    started_at = time.time()
    payload = [(i, total, c, started_at) for i, c in enumerate(combos, 1)]
    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        results = list(pool.map(run_cell, payload))

    failed = [slug for slug, ok in results if not ok]
    print(f"\nGrid finished in {(time.time() - started_at) / 60:.1f}m. "
          f"{len(results) - len(failed)}/{len(results)} succeeded.")
    if _auth_failed.is_set():
        print("\n*** RUN ABORTED ON EXPIRED CREDENTIALS -- the grid is INCOMPLETE. ***")
        print("The variance decomposition requires a complete balanced grid; analyze_grid.py "
              "will refuse to run until every cell is present.")
    if failed:
        print("FAILED cells:\n  " + "\n  ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
