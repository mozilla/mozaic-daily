"""Round 5: aggressive changepoint_range under multiplicative, then stack it with cps/sps.

The last untested direction. Round 1 moved cpr by only +/-delta (cpr=0.784 gave -27,436); the
prior art swept cpr to 0.90 at its own center and saw ~-114K. Everything here stays on the
MULTIPLICATIVE branch, because Rounds 2-4 established the auto branch overshoots by ~1.16M and
there is no interior point between the two.

Phase 1 -- cpr ladder at 0.82 / 0.86 / 0.90 / 0.94, s01 elsewhere. Maps how far cpr alone reaches
and whether it turns over.

Phase 2 -- take the best (lowest Aug-25) cpr that still holds Dec-15 inside +/-50,000, and stack
it with the two other downward movers Round 1 found:
    stack A = best_cpr + cps 0.1649
    stack B = best_cpr + cps 0.1649 + sps 0.01
Two runs rather than one so the sps contribution is separable. Cross-terms are unmeasured by the
one-at-a-time rounds, so a stack must be RUN, never predicted by summing single-knob deltas.

Usage:
    python research/param-scans/aug25-gap/run_cpr_round5.py [--dry-run] [--parallel N]
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

from run_corr_round3 import FORECAST_START, LOG_DIR, RAW_CACHE, RESULTS_DIR  # noqa: E402

CPR_LADDER = [0.82, 0.86, 0.90, 0.94]
S01_CPS, S01_CPR, S01_NCP, S01_RECENT, S01_SPS = "0.1849", "0.734", "35", "17", "0.00825"
CPS_LO, SPS_HI = "0.1649", "0.01"


def command(cps: str, cpr: str, sps: str, tag: str) -> tuple[str, list[str]]:
    argv = [
        "python", "scripts/run_param_scan.py",
        "--forecast-start-date", FORECAST_START,
        "--results-dir", str(RESULTS_DIR.relative_to(REPO_ROOT)),
        "--raw-cache-dir", str(RAW_CACHE.relative_to(REPO_ROOT)),
        "--seasonality-regime", "multiplicative",
        "--changepoint-prior-scale", cps,
        "--changepoint-range", cpr,
        "--n-changepoints", S01_NCP,
        "--recent-weeks", S01_RECENT,
        "--seasonality-prior-scale", sps,
        # Holiday knobs pinned to package defaults throughout: strictly local effects must never
        # be used to move a whole-season quantity.
        "--holiday-threshold", "-0.032",
        "--holiday-max-radius", "5",
        "--holiday-min-radius", "3",
        "--holiday-effect-floor", "-0.6",
    ]
    return tag, argv


def run(spec: tuple[str, list[str]]) -> bool:
    tag, argv = spec
    started = time.time()
    print(f"START  {tag}", flush=True)
    with open(LOG_DIR / f"round5_{tag}.log", "w") as handle:
        ok = subprocess.run(argv, cwd=REPO_ROOT, stdout=handle,
                            stderr=subprocess.STDOUT).returncode == 0
    print(f"{'DONE  ' if ok else 'FAILED'} {tag}  ({(time.time() - started) / 60:.1f}m)",
          flush=True)
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--parallel", type=int, default=3)
    args = parser.parse_args()

    ladder = [command(S01_CPS, str(c), S01_SPS, f"cpr{c}") for c in CPR_LADDER]
    if args.dry_run:
        for _, argv in ladder:
            print(" ".join(argv))
        print("\n(phase 2 stacks depend on phase 1 results)")
        return 0

    print(f"Phase 1: cpr ladder {CPR_LADDER} under multiplicative, s01 elsewhere.")
    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        results = list(pool.map(run, ladder))
    print(f"Ladder finished. {sum(results)}/{len(results)} succeeded.\n")

    from leaderboard import DEC15_BUDGET, collect_all  # noqa: E402

    df = collect_all()
    # Select on the config, not the label: Round 1's cpr probes (0.684/0.784) also produce
    # labels starting "cpr0.", and folding them in would let a Round-1 point win phase 2.
    ladder_rows = df[df["cpr"].isin(CPR_LADDER) & (df["changed"] == ("cpr",))]
    eligible = ladder_rows[ladder_rows["dec15_vs_base"].abs() <= DEC15_BUDGET]
    if eligible.empty:
        print("No ladder point holds Dec-15 inside budget; skipping the stacks.")
        print(ladder_rows.to_string(index=False))
        return 0

    best = eligible.loc[eligible["aug25"].idxmin()]
    best_cpr = f"{best['cpr']:g}"
    print(f"Phase 2: best in-budget cpr = {best_cpr} "
          f"(Aug-25 {best['aug25']:,.0f}, Dec-15 {best['dec15_vs_base']:+,.0f})\n")

    stacks = [
        command(CPS_LO, best_cpr, S01_SPS, f"stackA_cpr{best_cpr}_cpsLo"),
        command(CPS_LO, best_cpr, SPS_HI, f"stackB_cpr{best_cpr}_cpsLo_spsHi"),
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        stack_results = list(pool.map(run, stacks))
    print(f"\nStacks finished. {sum(stack_results)}/{len(stack_results)} succeeded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
