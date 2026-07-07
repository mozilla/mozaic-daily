#!/usr/bin/env python3
"""Round-1 one-at-a-time (OAT) sensitivity probe for the July mobile forecast.

From the current mobile params (= ``MobileModelConfig`` defaults = the June params),
perturb each of six knobs symmetrically by a small delta — holding the others at
center — so ``(net(+d) - net(-d)) / 2d`` estimates a clean local slope of the
Dec-15 net (vs June) with respect to that knob. This tells us which knobs move the
headline and in which direction before committing to a wider search.

13 cells total: 1 center + 2 x 6 perturbations. Each cell runs
``run_mobile_param_scan.py`` (raw-cached, marketing applied), then is scored by
``mobile_sensitivity.score_forecast``. Idempotent: a cell whose adj-m parquet
already exists is scored, not re-run.

    source .venv/bin/activate
    python scripts/mobile_grid_search.py \\
        --raw-cache-dir tmp/mobile_holidayskip_2026-06-29

Outputs ``research/param-scans/mobile-july/round1_results.csv`` and prints a
per-knob slope table.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
for p in (str(SRC_PATH), str(REPO_ROOT / "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

from mozaic.models import MobileModelConfig  # noqa: E402
from mobile_sensitivity import score_forecast  # noqa: E402

FORECAST_START = "2026-06-29"

# Each entry: (config-field, CLI-flag, center, minus, plus). Center values are the
# MobileModelConfig defaults (June params); deltas are small for local-slope estimation.
PARAM_GRID = [
    ("prophet_changepoint_prior_scale", "--changepoint-prior-scale", 0.02, 0.015, 0.025),
    ("prophet_changepoint_range", "--changepoint-range", 0.82, 0.79, 0.85),
    ("prophet_n_changepoints", "--n-changepoints", 25, 22, 28),
    ("prophet_recent_weeks", "--recent-weeks", 13, 11, 15),
    ("holiday_threshold", "--holiday-threshold", -0.032, -0.028, -0.036),
    ("holiday_effect_floor", "--holiday-effect-floor", -0.6, -0.55, -0.65),
]


def config_for(overrides: dict) -> MobileModelConfig:
    return MobileModelConfig(**overrides)


def slug_dir_for(results_dir: Path, overrides: dict) -> Path:
    return results_dir / config_for(overrides).to_slug()


def adjm_parquet_for(slug_dir: Path) -> Path:
    return slug_dir / f"mozaic_daily_forecast.{FORECAST_START}.gm-D.adj-m.parquet"


PARAM_FLAG_TO_FIELD = {flag: field for field, flag, *_ in PARAM_GRID}
FIELD_TO_FLAG = {field: flag for field, flag, *_ in PARAM_GRID}


def run_overrides(overrides: dict, raw_cache_dir: Path, results_dir: Path) -> Path:
    """Run one config (any number of knob overrides). Idempotent: skip if parquet exists."""
    slug_dir = slug_dir_for(results_dir, overrides)
    parquet = adjm_parquet_for(slug_dir)
    if parquet.exists():
        print(f"  [skip] {slug_dir.name} (exists)")
        return parquet
    cmd = [sys.executable, str(REPO_ROOT / "scripts/run_mobile_param_scan.py"),
           "--forecast-start-date", FORECAST_START,
           "--raw-cache-dir", str(raw_cache_dir),
           "--results-dir", str(results_dir)]
    for field, value in overrides.items():
        cmd += [FIELD_TO_FLAG[field], str(value)]
    print(f"  [run ] {slug_dir.name}")
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        raise SystemExit(f"scan cell failed (exit {result.returncode}): {' '.join(cmd)}")
    if not parquet.exists():
        raise SystemExit(f"scan cell produced no parquet at {parquet}")
    return parquet


def run_cell(flag: str | None, value, raw_cache_dir: Path, results_dir: Path) -> Path:
    """Round-1 OAT helper: run one single-knob cell (or center when flag is None)."""
    overrides = {} if flag is None else {PARAM_FLAG_TO_FIELD[flag]: value}
    return run_overrides(overrides, raw_cache_dir, results_dir)


# --- Round 2 -----------------------------------------------------------------
# Wider single-knob sweeps in the favorable directions from round 1 (cps up, cpr
# down, threshold more negative, plus larger recent_weeks steps), plus a few
# gradient-guessed configs. Gradient guesses use round-1 central-difference slopes
# to linearly extrapolate the knob value(s) that would close the +113,138 gap from
# center — flagged as guesses because these knobs are non-linear. cpr stays <= 0.82
# (only ranged down), so changepoints never enter the last ~2 months (user limit).
ROUND2_SINGLE = [
    ("prophet_changepoint_prior_scale", [0.03, 0.04, 0.047, 0.05]),   # 0.047 = gradient single-knob target
    ("prophet_changepoint_range", [0.78, 0.75, 0.70]),                # 0.70 = gradient single-knob target
    ("holiday_threshold", [-0.045, -0.06, -0.076]),                   # -0.076 = gradient single-knob target
    ("prophet_recent_weeks", [6, 8, 20, 26]),                         # larger steps both directions
]
ROUND2_COMBINED = [
    # even split of +113k across the three strong knobs (linear extrapolation)
    ("grad_even", {"prophet_changepoint_prior_scale": 0.029,
                   "prophet_changepoint_range": 0.781, "holiday_threshold": -0.047}),
    # moderate combined overshoot to bracket the target
    ("grad_moderate", {"prophet_changepoint_prior_scale": 0.035,
                       "prophet_changepoint_range": 0.75, "holiday_threshold": -0.055}),
]


def run_round2(raw_cache_dir: Path, results_dir: Path, out: Path) -> None:
    rows = []
    center_parquet = run_overrides({}, raw_cache_dir, results_dir)
    center_net = score_forecast(center_parquet)["net_vs_june"]
    print(f"\nCenter net vs June: {center_net:+,.0f}   (target +400,000; gap {400_000 - center_net:+,.0f})")

    print("\nSingle-knob wider sweeps:")
    for field, values in ROUND2_SINGLE:
        for value in values:
            pq = run_overrides({field: value}, raw_cache_dir, results_dir)
            s = score_forecast(pq)
            rows.append({"label": f"{field}={value}", "kind": "single",
                         "slug": pq.parent.name, **s})
    print("\nGradient-guessed combined configs:")
    for label, overrides in ROUND2_COMBINED:
        pq = run_overrides(overrides, raw_cache_dir, results_dir)
        s = score_forecast(pq)
        rows.append({"label": label, "kind": "combined", "slug": pq.parent.name, **s})

    df = pd.DataFrame(rows)
    df["abs_gap"] = df["gap_to_target"].abs()
    df = df.sort_values("abs_gap")
    df.to_csv(out, index=False)
    print(f"\nWrote {out}")

    print("\n" + "=" * 88)
    print(f"Ranked by distance to target (+400,000). Center = {center_net:+,.0f}")
    print("=" * 88)
    print(f"{'config':<52}{'net vs June':>14}{'gap to +400k':>16}")
    for _, r in df.iterrows():
        print(f"{r['label']:<52}{r['net_vs_june']:>+14,.0f}{r['gap_to_target']:>+16,.0f}")


def run_round1(raw_cache_dir: Path, results_dir: Path, out: Path) -> None:
    rows = []

    # Center cell (defaults, no override).
    print("Center cell:")
    center_parquet = run_cell(None, None, raw_cache_dir, results_dir)
    center_score = score_forecast(center_parquet)
    rows.append({"param": "(center)", "direction": "center", "value": None,
                 "slug": center_parquet.parent.name, **center_score})
    center_net = center_score["net_vs_june"]

    # Perturbation cells.
    slopes = []
    for field, flag, center, minus, plus in PARAM_GRID:
        print(f"\n{field}:")
        scored = {}
        for direction, value in (("minus", minus), ("plus", plus)):
            parquet = run_cell(flag, value, raw_cache_dir, results_dir)
            s = score_forecast(parquet)
            scored[direction] = (value, s)
            rows.append({"param": field, "direction": direction, "value": value,
                         "slug": parquet.parent.name, **s})
        (v_minus, s_minus), (v_plus, s_plus) = scored["minus"], scored["plus"]
        slope = (s_plus["net_vs_june"] - s_minus["net_vs_june"]) / (v_plus - v_minus)
        slopes.append({
            "param": field, "center": center,
            "net_minus": s_minus["net_vs_june"], "net_center": center_net,
            "net_plus": s_plus["net_vs_june"],
            "d_net_per_unit": slope,
        })

    df = pd.DataFrame(rows)
    df.to_csv(out, index=False)
    print(f"\nWrote {out}")

    print("\n" + "=" * 78)
    print(f"Center net vs June: {center_net:+,.0f}   (target +400,000; "
          f"gap {400_000 - center_net:+,.0f})")
    print("=" * 78)
    print(f"{'param':<34}{'net(-d)':>12}{'net(+d)':>12}{'d net/unit':>16}")
    for s in sorted(slopes, key=lambda r: -abs(r["d_net_per_unit"] * (0.01 if 'prior' in r['param'] else 1))):
        print(f"{s['param']:<34}{s['net_minus']:>12,.0f}{s['net_plus']:>12,.0f}"
              f"{s['d_net_per_unit']:>16,.0f}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--round", type=int, choices=[1, 2], default=1,
                        help="1 = OAT slope probe (default); 2 = wider sweeps + gradient-guessed configs")
    parser.add_argument("--raw-cache-dir", type=Path,
                        default=REPO_ROOT / "tmp/mobile_holidayskip_2026-06-29")
    parser.add_argument("--results-dir", type=Path,
                        default=REPO_ROOT / "research/param-scans/mobile-july/results")
    parser.add_argument("--out", type=Path, default=None,
                        help="Results CSV (default: round{N}_results.csv)")
    args = parser.parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)
    out = args.out or (REPO_ROOT / f"research/param-scans/mobile-july/round{args.round}_results.csv")
    if args.round == 1:
        run_round1(args.raw_cache_dir, args.results_dir, out)
    else:
        run_round2(args.raw_cache_dir, args.results_dir, out)


if __name__ == "__main__":
    main()
