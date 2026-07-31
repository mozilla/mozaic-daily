"""Select the recommended config under the triple constraint (rule set by the user 2026-07-30).

    HARD    Aug-25 within +/-75,000 of 45,027,066
    HARD    Dec-15 within +/-50,000 of 48,703,960
    PREFER  Dec-15 margin -- above 80% of cap (|delta| > 40,000) is flagged, not preferred
    MINIMISE  the seam-kink increase vs s01's -9,554        <- the objective

This supersedes leaderboard.py, which ranked by |Aug-25 - target| under a +/-25,000 band and used
|delta Dec-15| as the tie-break. Both are kept: leaderboard.py is the record of how rounds 1-6 were
judged, this is how round 7 onward is judged.

Scores every multiplicative run on disk, not just one round's, because the selection is global.

Usage:
    python research/param-scans/aug25-gap/select_candidate.py [--rescore] [--top N]
"""

from __future__ import annotations

import argparse
import json
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
from score_gradient import S01_PARQUET  # noqa: E402

AUG25_TARGET = 45_027_066
AUG25_HARD = 75_000
DEC15_BASELINE = 48_703_960
DEC15_HARD = 50_000
DEC15_MARGIN = 40_000          # 80% of cap
S01_KINK = -9_553.78
S01_AUG25 = 45_223_249.05
REQUIRED_MOVE = 196_183
CACHE = HERE / "all_scores.csv"

FIELDS = {
    "cps": "prophet_changepoint_prior_scale",
    "cpr": "prophet_changepoint_range",
    "ncp": "prophet_n_changepoints",
    "recent": "prophet_recent_weeks",
    "sps": "prophet_seasonality_prior_scale",
}
FACTORS = list(FIELDS)


def collect(rescore: bool) -> pd.DataFrame:
    if CACHE.exists() and not rescore:
        return pd.read_csv(CACHE)

    rows = []
    for parquet in [S01_PARQUET, *sorted(HERE.glob(f"runs/*/{PARQUET_NAME}"))]:
        if not parquet.exists():
            continue
        config = json.loads((parquet.parent / "parameters.json").read_text())["config"]
        # Only the multiplicative branch is selectable: rounds 2-4 established that auto
        # overshoots Aug-25 by ~1.16M at every trend-knob setting, far outside the hard band.
        if config.get("seasonality_regime") != "multiplicative":
            continue
        scored = score_parquet(parquet, target_date="2026-08-25")
        rows.append({
            **{name: config[field] for name, field in FIELDS.items()},
            "aug25": scored["global_target_post"],
            "dec15": scored["global_dec15_post"],
            "kink": scored["seam_slope_kink_model"],
            "trough_date": scored["trough_min_date"],
            "parquet": str(parquet.relative_to(REPO_ROOT)),
        })

    df = pd.DataFrame(rows).drop_duplicates(subset=FACTORS)
    df.to_csv(CACHE, index=False)
    return df


def apply_rule(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["aug_vs_target"] = df["aug25"] - AUG25_TARGET
    df["dec_vs_base"] = df["dec15"] - DEC15_BASELINE
    df["kink_pen"] = df["kink"].abs() - abs(S01_KINK)
    df["move_pct"] = 100 * (S01_AUG25 - df["aug25"]) / REQUIRED_MOVE
    df["aug_ok"] = df["aug_vs_target"].abs() <= AUG25_HARD
    df["dec_ok"] = df["dec_vs_base"].abs() <= DEC15_HARD
    df["dec_margin_ok"] = df["dec_vs_base"].abs() <= DEC15_MARGIN
    df["feasible"] = df["aug_ok"] & df["dec_ok"]
    return df


def pareto(df: pd.DataFrame) -> pd.DataFrame:
    """Cells not dominated on BOTH |Aug-25 - target| and kink penalty."""
    keep = []
    for _, row in df.iterrows():
        better = ((df["aug_vs_target"].abs() <= abs(row["aug_vs_target"]))
                  & (df["kink_pen"] <= row["kink_pen"])
                  & ((df["aug_vs_target"].abs() < abs(row["aug_vs_target"]))
                     | (df["kink_pen"] < row["kink_pen"])))
        if not better.any():
            keep.append(row)
    return pd.DataFrame(keep).sort_values("kink_pen")


def show(df: pd.DataFrame, columns: list[str]) -> str:
    out = df[columns].copy()
    # Factor columns need their own formatting: a thousands-no-decimals float_format renders
    # cps 0.1749 and sps 0.00825 both as "0", making distinct configs look like duplicates.
    for factor in FACTORS:
        if factor in out:
            out[factor] = out[factor].map(lambda v: f"{v:g}")
    return out.to_string(index=False, float_format=lambda v: f"{v:,.0f}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rescore", action="store_true")
    parser.add_argument("--top", type=int, default=12)
    args = parser.parse_args()

    df = apply_rule(collect(args.rescore))
    pd.set_option("display.width", 240)

    print(f"RULE  Aug-25 {AUG25_TARGET:,} +/-{AUG25_HARD:,} (hard) | "
          f"Dec-15 {DEC15_BASELINE:,} +/-{DEC15_HARD:,} (hard), prefer |d|<={DEC15_MARGIN:,} | "
          f"minimise kink vs s01 {S01_KINK:,.0f}")
    print(f"{len(df)} multiplicative configs scored | Aug-25 band {int(df.aug_ok.sum())} | "
          f"+Dec-15 hard {int(df.feasible.sum())} | "
          f"+Dec-15 margin {int((df.feasible & df.dec_margin_ok).sum())}\n")

    cols = FACTORS + ["aug25", "aug_vs_target", "dec_vs_base", "kink", "kink_pen", "move_pct"]

    preferred = df[df.feasible & df.dec_margin_ok].sort_values("kink_pen")
    print(f"--- FEASIBLE WITH Dec-15 MARGIN, ranked by kink increase ({len(preferred)}) ---")
    print(show(preferred.head(args.top), cols) if len(preferred) else "  none")

    near_cap = df[df.feasible & ~df.dec_margin_ok].sort_values("kink_pen")
    if len(near_cap):
        print(f"\n--- feasible but Dec-15 above 80% of cap ({len(near_cap)}), not preferred ---")
        print(show(near_cap.head(6), cols))

    front = pareto(df[df.feasible])
    print(f"\n--- Pareto frontier, Aug-25 accuracy vs kink ({len(front)}) ---")
    print(show(front, cols + ["dec_margin_ok"]))

    if len(preferred):
        best = preferred.iloc[0]
        print(f"\nRECOMMENDED: " + "  ".join(f"{f}={best[f]:g}" for f in FACTORS))
        print(f"  Aug-25 {best.aug25:,.0f}  ({best.aug_vs_target:+,.0f} vs target, "
              f"{best.move_pct:.1f}% of the required move)")
        print(f"  Dec-15 {best.dec_vs_base:+,.0f}  "
              f"({100 * abs(best.dec_vs_base) / DEC15_HARD:.0f}% of cap)")
        print(f"  kink   {best.kink:,.0f}  (+{best.kink_pen:,.0f} vs s01, "
              f"{abs(best.kink) / abs(S01_KINK):.2f}x)")
        print(f"  {best.parquet}")
    else:
        print("\nNo config satisfies the rule with Dec-15 margin.")

    print(f"\nCache: {CACHE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
