"""Rank every config produced by this search against the Aug-25 target.

Scores s01 plus every run under runs/, labels each by how its config differs from s01, and
ranks by |Aug-25 - target| among configs that hold Dec-15 inside +/-50,000.

Used two ways: as the round-by-round leaderboard, and by run_cpr_round5.py to pick which
ladder point to stack on.

OBJECTIVE: Aug-25 -> 45,027,066 (+/-25,000), Dec-15 within +/-50,000 of 48,703,960, seam kink
reported (baseline -9,554). Sibling scans under research/param-scans/ aimed the OPPOSITE way;
the `target band : 45M-46M` line printed by scripts/score_near_horizon.py belongs to them.

Usage:
    python research/param-scans/aug25-gap/leaderboard.py [--csv] [--top N]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))
sys.path.insert(0, str(HERE))

from score_near_horizon import score_parquet  # noqa: E402

from score_blend import PARQUET_NAME  # noqa: E402
from score_gradient import (  # noqa: E402
    AUG25_TARGET, AUG25_TOLERANCE, DEC15_BASELINE, DEC15_BUDGET, S01_PARQUET, _config_of,
)

S01_KINK = -9_553.78

# short name -> (parameters.json field, s01 value)
KNOBS = {
    "cps": ("prophet_changepoint_prior_scale", 0.1849),
    "cpr": ("prophet_changepoint_range", 0.734),
    "ncp": ("prophet_n_changepoints", 35),
    "recent": ("prophet_recent_weeks", 17),
    "sps": ("prophet_seasonality_prior_scale", 0.00825),
    "regime": ("seasonality_regime", "multiplicative"),
    "corr": ("seasonality_corr_threshold", 0.0),
}


def _describe(config: dict) -> tuple[str, tuple[str, ...]]:
    """Label a config by how it differs from s01, plus the tuple of changed knob names."""
    changed = []
    for name, (field, s01_value) in KNOBS.items():
        value = config.get(field)
        # corr_threshold only bites under regime=auto; under multiplicative every tile is
        # forced regardless, so a non-default corr there is not a real difference.
        if name == "corr" and config.get("seasonality_regime") != "auto":
            continue
        if value != s01_value:
            changed.append((name, value))
    if not changed:
        return "s01", ()
    label = "_".join(f"{name}{value}" for name, value in changed)
    return label, tuple(name for name, _ in changed)


def collect_all() -> pd.DataFrame:
    rows = []
    for parquet in [S01_PARQUET, *sorted(HERE.glob(f"runs/*/{PARQUET_NAME}"))]:
        if not parquet.exists():
            continue
        config = _config_of(parquet)
        label, changed = _describe(config)
        scored = score_parquet(parquet, target_date="2026-08-25")
        rows.append({
            "label": label,
            "changed": changed,
            "n_changed": len(changed),
            **{name: config.get(field) for name, (field, _) in KNOBS.items()},
            "aug25": scored["global_target_post"],
            "dec15": scored["global_dec15_post"],
            "kink": scored["seam_slope_kink_model"],
            "trough_date": scored["trough_min_date"],
            "parquet": str(parquet.relative_to(REPO_ROOT)),
        })

    df = pd.DataFrame(rows)
    df["aug25_vs_s01"] = df["aug25"] - 45_223_249.05
    df["aug25_vs_target"] = df["aug25"] - AUG25_TARGET
    df["dec15_vs_base"] = df["dec15"] - DEC15_BASELINE
    df["kink_vs_s01"] = df["kink"] - S01_KINK
    df["dec15_ok"] = df["dec15_vs_base"].abs() <= DEC15_BUDGET
    df["hits"] = (df["aug25_vs_target"].abs() <= AUG25_TOLERANCE) & df["dec15_ok"]
    # Rank by closeness to target, but only configs that respect the Dec-15 cap are eligible:
    # a config that moves Aug-25 further by blowing Dec-15 is not "closer", it is invalid.
    df["rank_key"] = df["aug25_vs_target"].abs().where(df["dec15_ok"], float("inf"))
    return df.sort_values(["rank_key", "label"]).reset_index(drop=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", action="store_true")
    parser.add_argument("--top", type=int, default=12)
    args = parser.parse_args()

    df = collect_all()
    pd.set_option("display.width", 240)

    print(f"Aug-25 target {AUG25_TARGET:,} (+/-{AUG25_TOLERANCE:,})   "
          f"Dec-15 {DEC15_BASELINE:,} (+/-{DEC15_BUDGET:,})   s01 kink {S01_KINK:,.0f}")
    print(f"{len(df)} configs scored.  Hitting the target: {int(df['hits'].sum())}\n")

    eligible = df[df["dec15_ok"]]
    print(f"TOP {args.top} by |Aug-25 - target|, among the {len(eligible)} holding Dec-15 "
          f"in budget:\n")
    show = eligible.head(args.top)[
        ["label", "aug25", "aug25_vs_s01", "aug25_vs_target", "dec15_vs_base",
         "kink", "kink_vs_s01", "trough_date"]]
    print(show.to_string(index=False, float_format=lambda v: f"{v:,.0f}"))

    excluded = df[~df["dec15_ok"]]
    if not excluded.empty:
        print(f"\nExcluded for Dec-15 (|delta| > {DEC15_BUDGET:,}): "
              + ", ".join(f"{r.label} ({r.dec15_vs_base:+,.0f})" for r in excluded.itertuples()))

    best = eligible.iloc[0]
    print(f"\nBEST SO FAR: {best.label}")
    print(f"  Aug-25 {best.aug25:,.0f}   {best.aug25_vs_s01:+,.0f} vs s01   "
          f"{best.aug25_vs_target:+,.0f} vs target "
          f"({100 * -best.aug25_vs_s01 / 196_183:.1f}% of the required move)")
    print(f"  Dec-15 {best.dec15_vs_base:+,.0f} vs base   kink {best.kink:,.0f} "
          f"({best.kink_vs_s01:+,.0f} vs s01)")
    print(f"  {best.parquet}")

    if args.csv:
        out = HERE / "leaderboard.csv"
        df.drop(columns=["changed"]).to_csv(out, index=False)
        print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
