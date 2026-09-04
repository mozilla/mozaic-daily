#!/usr/bin/env python3
"""Round-1 central-difference sensitivity gradient for the August 2026 mobile forecast.

Purpose is **sensitivity, not optimisation**. The center is the currently shipped mobile
config and is known to miss the target by -322,714; the deliverable is the local first and
second derivative of Dec-15 with respect to each numeric knob, so the shape of the phase
space around this point is known before committing to a wider search.

Center = the shipped August mobile lock (`data-official/2026-08/mobile_organic_2026-07-28/`),
which is itself July's `grad_moderate` grid-search winner:

    cps=0.035, cpr=0.75, n_changepoints=25, recent_weeks=13, sps=0.1,
    regime=auto, holiday_threshold=-0.055, holiday_effect_floor=-0.6

Target: **17,923,869 +- 50,000** (July's delivered mobile Dec-15). See `mobile_scoring.py`
for why the gap exists and what closing it costs.

Method
------
Each axis gets a `+delta` and a `-delta` probe with everything else held at center. The
three-point stencil gives both derivatives:

    f'  ~ (f(x+h) - f(x-h)) / 2h
    f'' ~ (f(x+h) - 2 f(x) + f(x-h)) / h^2

`f''` is what tells us whether the linear extrapolation to a target value is trustworthy.
Every prior search on this codebase has found cross-parameter non-linearity, so treat the
single-knob extrapolations printed at the end as **starting points for round 2, not
predictions**.

Scope decisions (from the 2026-07-31 search brief)
--------------------------------------------------
- **Holiday knobs are excluded** by standing policy — strictly local effects must not be
  used to move a whole-season quantity. `holiday_threshold` stays pinned at the shipped
  -0.055 rather than being reset to the -0.032 default, so the center is the build actually
  in production.
- **`seasonality_regime` is set by `--regime` and held fixed across the gradient** (default
  `auto`, the shipped value). On mobile the regime sets `seasonality_mode` only — growth
  stays volume-driven, unlike desktop — and under `auto` a tile is multiplicative iff
  `max(DAU) <= 2e6`, so the large tiles including the world headline are **additive**.
  Re-running the same 11 probes under `--regime multiplicative` yields both the regime's
  own effect (center vs center) and the local gradient at that new center. Results share
  one dir; `to_slug()` appends `_regime<value>` so nothing collides.
- **`seasonality_corr_threshold` is not an axis.** `MobileModelConfig` raises on any
  non-zero value: mobile's regime switch is volume-driven, so there is no correlation
  cutoff for it to move. Desktop-only.

That leaves five numeric axes -> 11 probes (1 center + 5 x 2).

Deltas are deliberately **small** (~10% of center) so these are local derivatives rather
than secants. Two coarseness caveats, reported rather than hidden:
- `n_changepoints` and `recent_weeks` are integers, so their deltas quantise; `recent_weeks`
  at +-2 on a center of 13 is +-15% and is a secant, not a slope.
- `changepoint_range` is bounded above by 1.0 and is the most non-linear knob in every prior
  scan, so +-0.025 is kept tighter in relative terms than the others.

Usage
-----
    source .venv/bin/activate
    python scripts/run_mobile_gradient.py --dry-run
    python scripts/run_mobile_gradient.py --workers 3
    python scripts/run_mobile_gradient.py --workers 3 --regime multiplicative

Each probe takes ~2 minutes and writes an ~838 MB mozaic pickle, so a full round-1 run is
~8 minutes wall-clock and ~9 GB of disk under `--results-dir`.
"""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SCAN_SCRIPT = REPO_ROOT / "scripts/run_mobile_param_scan.py"

_spec = importlib.util.spec_from_file_location(
    "mobile_scoring", REPO_ROOT / "scripts" / "mobile_scoring.py")
mobile_scoring = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mobile_scoring)

FORECAST_START = "2026-07-28"
#: The shipped build's slug dir holds the real raw BQ pull every probe symlinks.
RAW_CACHE_DIR = (REPO_ROOT / "data-official/2026-08/mobile_uac_meta_2026-07-28"
                 / "cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1")
DEFAULT_RESULTS_DIR = REPO_ROOT / "research/param-scans/mobile-aug/results"

#: The shipped August mobile lock, as CLI flags. The gradient center.
CENTER = {
    "changepoint-prior-scale": 0.035,
    "changepoint-range": 0.75,
    "n-changepoints": 25,
    "recent-weeks": 13,
    "seasonality-prior-scale": 0.1,
    "seasonality-regime": "auto",
    "holiday-threshold": -0.055,
    "holiday-effect-floor": -0.6,
}

#: (flag, center, delta, kind). See the module docstring for why each delta is this size.
GRADIENT_AXES = [
    ("changepoint-prior-scale", 0.035, 0.0035, "float"),   # 10%
    ("changepoint-range", 0.75, 0.025, "float"),           # 3.3% -- bounded, most non-linear
    ("n-changepoints", 25, 3, "int"),                      # 12%
    ("recent-weeks", 13, 2, "int"),                        # 15% -- secant, quantised
    ("seasonality-prior-scale", 0.1, 0.01, "float"),       # 10%
]


def build_probe_list() -> list[dict]:
    """Center, then a +/- pair per numeric axis."""
    probes: list[dict] = [{"label": "center", "axis": "(center)", "sign": 0,
                           "value": None, "overrides": {}}]
    for flag, center, delta, kind in GRADIENT_AXES:
        for sign, tag in ((-1, "minus"), (+1, "plus")):
            value = center + sign * delta
            value = int(round(value)) if kind == "int" else round(value, 6)
            probes.append({"label": f"{flag}__{tag}", "axis": flag, "sign": sign,
                           "value": value, "overrides": {flag: value}})
    return probes


def probe_command(overrides: dict, results_dir: Path, center: dict) -> list[str]:
    """Build the scan invocation for one probe.

    ``center`` is passed in rather than read from the module-level CENTER because these run
    inside ProcessPoolExecutor workers. On macOS the pool spawns rather than forks, so a
    worker re-imports this module and would see the pristine CENTER — any regime override
    applied in main() would be silently dropped and the probe would run the wrong config.
    """
    args = dict(center)
    args.update(overrides)
    cmd = [sys.executable, str(SCAN_SCRIPT),
           "--forecast-start-date", FORECAST_START,
           "--raw-cache-dir", str(RAW_CACHE_DIR),
           "--results-dir", str(results_dir)]
    for flag, value in args.items():
        cmd += [f"--{flag}", str(value)]
    return cmd


def resolve_slug(overrides: dict, results_dir: Path, center: dict) -> str:
    out = subprocess.run(probe_command(overrides, results_dir, center) + ["--dry-run"],
                         capture_output=True, text=True).stdout
    for line in out.splitlines():
        if line.startswith("Slug"):
            return line.split(":", 1)[1].strip()
    raise RuntimeError(f"could not resolve slug:\n{out}")


def parquet_for(slug: str, results_dir: Path) -> Path:
    return (results_dir / slug
            / f"mozaic_daily_forecast.{FORECAST_START}.gm-D.adj-p.parquet")


def run_probe(label: str, overrides: dict, results_dir: Path,
              log_dir: Path, center: dict) -> tuple[str, bool, float, str]:
    log_path = log_dir / f"{label.replace('/', '_')}.log"
    started = time.time()
    with open(log_path, "w") as handle:
        handle.write(f"# {label}: center={center} overrides={overrides}\n")
        handle.flush()
        proc = subprocess.run(probe_command(overrides, results_dir, center),
                              stdout=handle, stderr=subprocess.STDOUT)
    return label, proc.returncode == 0, time.time() - started, str(log_path)


def score_all(probes: list[dict], results_dir: Path, center: dict) -> pd.DataFrame:
    """Score every probe that produced a parquet."""
    rows = []
    for probe in probes:
        slug = resolve_slug(probe["overrides"], results_dir, center)
        parquet = parquet_for(slug, results_dir)
        if not parquet.exists():
            print(f"  [missing] {probe['label']} -> {parquet}")
            continue
        scores = mobile_scoring.score_forecast(parquet)
        scores.pop("config", None)
        rows.append({"label": probe["label"], "axis": probe["axis"],
                     "sign": probe["sign"], "value": probe["value"], **scores})
    return pd.DataFrame(rows)


def derivatives(scored: pd.DataFrame) -> pd.DataFrame:
    """First and second derivative of dec15_post per axis, from the 3-point stencil."""
    center_rows = scored[scored["axis"] == "(center)"]
    if center_rows.empty:
        raise SystemExit("center probe missing or unscored — cannot form derivatives.")
    f0 = float(center_rows["dec15_post"].iloc[0])

    rows = []
    for flag, center, delta, _kind in GRADIENT_AXES:
        pair = scored[scored["axis"] == flag]
        minus = pair[pair["sign"] == -1]
        plus = pair[pair["sign"] == +1]
        if minus.empty or plus.empty:
            continue
        f_minus, f_plus = float(minus["dec15_post"].iloc[0]), float(plus["dec15_post"].iloc[0])
        # Use the ACTUAL probed values, not the nominal delta: integer axes round.
        h_minus = center - float(minus["value"].iloc[0])
        h_plus = float(plus["value"].iloc[0]) - center
        first = (f_plus - f_minus) / (h_plus + h_minus)
        second = (f_plus - 2 * f0 + f_minus) / (h_plus * h_minus)
        gap = mobile_scoring.TARGET_DEC15 - f0
        rows.append({
            "axis": flag, "center": center, "delta": delta,
            "f_minus": f_minus, "f_center": f0, "f_plus": f_plus,
            "d1": first, "d2": second,
            # Linear single-knob extrapolation to close the whole gap. A starting point for
            # round 2, NOT a prediction -- d2 says how fast this estimate goes wrong, and
            # cross-parameter non-linearity is not captured at all.
            "value_to_close_gap": center + gap / first if first else float("nan"),
            "effect_per_10pct": first * 0.10 * center,
        })
    return pd.DataFrame(rows)


def report(scored: pd.DataFrame, deriv: pd.DataFrame, results_dir: Path,
           tag: str = "round1") -> None:
    scored_path = results_dir.parent / f"{tag}_scores.csv"
    deriv_path = results_dir.parent / f"{tag}_derivatives.csv"
    scored.to_csv(scored_path, index=False)
    deriv.to_csv(deriv_path, index=False)

    f0 = float(scored.loc[scored["axis"] == "(center)", "dec15_post"].iloc[0])
    gap = mobile_scoring.TARGET_DEC15 - f0

    print("\n" + "=" * 100)
    print(f"Center Dec-15 (post-headwind): {f0:,.0f}    "
          f"target {mobile_scoring.TARGET_DEC15:,.0f} +-{mobile_scoring.TOLERANCE:,}    "
          f"gap {gap:+,.0f}")
    print("=" * 100)
    print(f"{'axis':<28}{'f(-d)':>14}{'f(+d)':>14}{'d1 /unit':>16}"
          f"{'d2 /unit^2':>18}{'effect @10%':>14}")
    for _, r in deriv.reindex(
            deriv["effect_per_10pct"].abs().sort_values(ascending=False).index).iterrows():
        print(f"{r['axis']:<28}{r['f_minus']:>14,.0f}{r['f_plus']:>14,.0f}"
              f"{r['d1']:>16,.0f}{r['d2']:>18,.3g}{r['effect_per_10pct']:>+14,.0f}")

    print("\nSingle-knob linear extrapolation to close the gap (round-2 starting points, "
          "NOT predictions):")
    for _, r in deriv.iterrows():
        print(f"  {r['axis']:<28} {r['center']:>10} -> {r['value_to_close_gap']:>12,.4f}")

    print("\nSeam handoff per probe (reported, NOT scored):")
    print(f"{'label':<34}{'dec15_post':>14}{'seam step':>12}{'kink':>10}{'YoY %':>9}")
    for _, r in scored.iterrows():
        print(f"{r['label']:<34}{r['dec15_post']:>14,.0f}{r['seam_step']:>+12,.0f}"
              f"{r['seam_slope_kink']:>+10,.0f}{r['yoy_dec15_pct']:>9.2f}")

    print(f"\nWrote {scored_path}\n      {deriv_path}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--regime", choices=["auto", "additive", "multiplicative"],
                        default="auto",
                        help="seasonality_regime held at the center for the whole gradient "
                             "(default 'auto', the shipped value). Re-running the same 11 "
                             "probes under a different regime gives both the regime's own "
                             "effect (its center vs the auto center) and the local gradient "
                             "at that new center. Probes land in the SAME results dir — "
                             "to_slug() appends '_regime<value>', so they cannot collide.")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true",
                        help="Re-run probes whose parquet already exists.")
    parser.add_argument("--score-only", action="store_true",
                        help="Skip running; score and report the parquets already on disk.")
    args = parser.parse_args()

    # Resolved once here and threaded explicitly through every call — see probe_command().
    center = dict(CENTER, **{"seasonality-regime": args.regime})
    tag = "round1" if args.regime == "auto" else f"round1_{args.regime[:4]}"

    probes = build_probe_list()
    log_dir = args.results_dir / "logs" / tag

    print(f"August mobile round-1 gradient [regime={args.regime}]: {len(probes)} probes "
          f"({len(GRADIENT_AXES)} axes x 2, plus the center)")
    print(f"  center  : {center}")
    print(f"  raw     : {RAW_CACHE_DIR}")
    print(f"  results : {args.results_dir}\n")
    for i, probe in enumerate(probes, 1):
        overrides = ", ".join(f"{k}={v}" for k, v in probe["overrides"].items()) or "(center)"
        print(f"  [{i:2d}/{len(probes)}] {probe['label']:34s} {overrides}")

    if args.dry_run:
        print("\n[dry-run] nothing executed.")
        return 0

    if not RAW_CACHE_DIR.exists():
        raise SystemExit(f"Raw cache dir missing: {RAW_CACHE_DIR}")
    args.results_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Defence in depth. The slug IS the output directory, so two probes resolving to the same
    # slug overwrite each other and then read back identical numbers -- which looks like "this
    # knob has no effect" rather than an error. Refuse to run if to_slug() is not injective
    # over the varied fields.
    resolved: dict[str, str] = {}
    collisions = []
    for probe in probes:
        slug = resolve_slug(probe["overrides"], args.results_dir, center)
        if slug in resolved:
            collisions.append((probe["label"], resolved[slug], slug))
        resolved[slug] = probe["label"]
    if collisions:
        print("\nABORT: probes share an output slug and would overwrite each other:")
        for a, b, slug in collisions:
            print(f"  {a}  <->  {b}\n    both -> {slug}")
        return 2

    if not args.score_only:
        pending = []
        for probe in probes:
            parquet = parquet_for(resolve_slug(probe["overrides"], args.results_dir, center),
                                  args.results_dir)
            if parquet.exists() and not args.force:
                print(f"  skip (exists): {probe['label']}")
                continue
            pending.append(probe)

        print(f"\nrunning {len(pending)} of {len(probes)} at {args.workers} workers\n")
        started, failures = time.time(), []
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(run_probe, p["label"], p["overrides"],
                                   args.results_dir, log_dir, center): p for p in pending}
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

    print("\nScoring...")
    scored = score_all(probes, args.results_dir, center)
    report(scored, derivatives(scored), args.results_dir, tag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
