"""Round 3: calibrate seasonality_corr_threshold as a continuous auto <-> multiplicative dial.

Round 2 established that seasonality_regime is a CLIFF: the auto-minus-multiplicative step is
~-1.35M at Aug-25 regardless of the trend knobs, leaving a 1,317,589-wide gap with the target
(45,027,066) sitting inside it -- 168,826 below the multiplicative floor.

corr_threshold is the only continuous crossing. Under regime=auto it is the per-tile cutoff on
corr(|y|,|dy|): a tile runs multiplicative+linear ABOVE the cutoff, so LOWER threshold => more
tiles multiplicative. It dials the per-tile MIXTURE instead of flipping a global switch.

Trend knobs are held at s01 on every run so the dial is the only moving part.

Two endpoints double as sanity anchors:
  t = -1.0  every tile has corr >= -1, so all tiles go multiplicative. Should reproduce forced
            multiplicative (s01, Aug-25 45,223,249). If it does not, the dial does not mean what
            the help text says and the rest of the sweep is uninterpretable.
  t =  0.0  the legacy hardcoded cutoff -- plain production `auto`. Round 2 measured configs near
            here at ~43.87M.

Usage:
    python research/param-scans/aug25-gap/run_corr_round3.py [--dry-run] [--parallel N]
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
THRESHOLDS = [-1.0, -0.8, -0.6, -0.4, -0.2, 0.0]

# s01 trend knobs, held fixed across the whole sweep.
S01 = {
    "changepoint-prior-scale": "0.1849",
    "changepoint-range": "0.734",
    "n-changepoints": "35",
    "recent-weeks": "17",
    "seasonality-prior-scale": "0.00825",
}


def command(threshold: float) -> tuple[str, list[str]]:
    label = f"corr{threshold:+.1f}".replace(".", "p")
    argv = [
        "python", "scripts/run_param_scan.py",
        "--forecast-start-date", FORECAST_START,
        "--results-dir", str(RESULTS_DIR.relative_to(REPO_ROOT)),
        "--raw-cache-dir", str(RAW_CACHE.relative_to(REPO_ROOT)),
        # corr_threshold is only meaningful under regime=auto -- multiplicative forces every
        # tile regardless, which would make the whole sweep a constant.
        "--seasonality-regime", "auto",
        "--seasonality-corr-threshold", str(threshold),
        # Holiday knobs pinned to package defaults: strictly local effects must never be used
        # to move a whole-season quantity.
        "--holiday-threshold", "-0.032",
        "--holiday-max-radius", "5",
        "--holiday-min-radius", "3",
        "--holiday-effect-floor", "-0.6",
    ]
    for knob, value in S01.items():
        argv += [f"--{knob}", value]
    return label, argv


def run(threshold: float) -> tuple[str, bool, float]:
    label, argv = command(threshold)
    started = time.time()
    print(f"START  {label}", flush=True)
    with open(LOG_DIR / f"round3_{label}.log", "w") as handle:
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

    print(f"corr_threshold sweep, regime=auto, s01 trend knobs held: {THRESHOLDS}")
    print(f"{len(THRESHOLDS)} runs, {args.parallel} concurrent.\n")

    if args.dry_run:
        for threshold in THRESHOLDS:
            print(" ".join(command(threshold)[1]))
        return 0

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        results = list(pool.map(run, THRESHOLDS))

    failed = [label for label, ok, _ in results if not ok]
    print(f"\nFinished. {len(results) - len(failed)}/{len(results)} succeeded.")
    if failed:
        print("FAILED: " + ", ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
