"""Analyse the Round-6 3^5 factorial: exact variance decomposition over all 31 effects.

A balanced full factorial with one observation per cell decomposes total variance EXACTLY into
the 5 main effects, 10 two-way, 10 three-way, 5 four-way and 1 five-way interaction -- no
model fitting, no assumptions, and the parts sum to the whole. That is what makes this a direct
measurement of the interactions rather than an inference from one-at-a-time probes.

Method (classic ANOVA decomposition by iterated subtraction):
    effect(S) = mean of y over all axes NOT in S, minus the sum of effect(T) for every proper
                subset T of S.
    SS(S)     = (cells per S-level-combination) * sum(effect(S)^2)
    sum over all non-empty S of SS(S) == sum((y - grand mean)^2)      [asserted]

Scoring 243 parquets takes a few minutes, so results are cached to grid_scores.csv.

Usage:
    python research/param-scans/aug25-gap/analyze_grid.py [--rescore] [--top N]
"""

from __future__ import annotations

import argparse
import itertools
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))
sys.path.insert(0, str(HERE))

from score_near_horizon import score_parquet  # noqa: E402

from run_grid_round6 import LEVELS, PARQUET_NAME, slug_for  # noqa: E402
from score_gradient import (  # noqa: E402
    AUG25_TARGET, AUG25_TOLERANCE, DEC15_BASELINE, DEC15_BUDGET,
)

FACTORS = list(LEVELS)
CACHE = HERE / "grid_scores.csv"
S01_KINK = -9_553.78
CENTRE_AUG25 = 45_195_814.0  # the Round-5 best config, this grid's centre cell


def score_grid(rescore: bool) -> pd.DataFrame:
    if CACHE.exists() and not rescore:
        return pd.read_csv(CACHE)

    rows, combos = [], list(itertools.product(*LEVELS.values()))
    for i, combo in enumerate(combos, 1):
        parquet = HERE / "runs" / slug_for(*combo) / PARQUET_NAME
        if not parquet.exists():
            continue
        scored = score_parquet(parquet, target_date="2026-08-25")
        rows.append({
            **dict(zip(FACTORS, combo)),
            "aug25": scored["global_target_post"],
            "dec15": scored["global_dec15_post"],
            "kink": scored["seam_slope_kink_model"],
            "trough_date": scored["trough_min_date"],
        })
        if i % 25 == 0:
            sys.stdout.write(f"  scored {i}/{len(combos)}\n")
            sys.stdout.flush()

    df = pd.DataFrame(rows)
    df.to_csv(CACHE, index=False)
    return df


def decompose(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Exact ANOVA decomposition of `metric` over all 31 effects of the 5-factor design."""
    shape = tuple(len(LEVELS[f]) for f in FACTORS)
    ordered = df.sort_values(FACTORS)
    if len(ordered) != int(np.prod(shape)):
        raise SystemExit(
            f"decomposition needs the complete grid: have {len(ordered)}, "
            f"need {int(np.prod(shape))}. Re-run the missing cells first.")
    y = ordered[metric].to_numpy(dtype=float).reshape(shape)

    grand = y.mean()
    effects: dict[tuple[int, ...], np.ndarray] = {}
    rows = []
    for size in range(1, len(FACTORS) + 1):
        for subset in itertools.combinations(range(len(FACTORS)), size):
            others = tuple(a for a in range(len(FACTORS)) if a not in subset)
            marginal = y.mean(axis=others)  # mean over axes not in the subset
            effect = marginal - grand
            # Subtract every proper subset's effect, broadcast into this subset's shape.
            for lower_size in range(1, size):
                for lower in itertools.combinations(subset, lower_size):
                    broadcast = [np.newaxis] * size
                    for pos, axis in enumerate(subset):
                        if axis in lower:
                            broadcast[pos] = slice(None)
                    effect = effect - effects[lower][tuple(broadcast)]
            effects[subset] = effect
            cells_per_combo = int(np.prod([shape[a] for a in others])) if others else 1
            rows.append({
                "effect": ":".join(FACTORS[a] for a in subset),
                "order": size,
                "ss": cells_per_combo * float((effect ** 2).sum()),
                "range": float(effect.max() - effect.min()),
            })

    table = pd.DataFrame(rows)
    total_ss = float(((y - grand) ** 2).sum())
    # The decomposition is exact for a balanced full factorial; if this fails, the grid is not
    # balanced or a cell is duplicated, and every percentage below would be wrong.
    assert abs(table["ss"].sum() - total_ss) < max(1.0, total_ss * 1e-9), (
        f"decomposition does not close: {table['ss'].sum():,.1f} vs {total_ss:,.1f}")
    table["pct_var"] = 100 * table["ss"] / total_ss
    return table.sort_values("pct_var", ascending=False).reset_index(drop=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rescore", action="store_true")
    parser.add_argument("--top", type=int, default=15)
    args = parser.parse_args()

    df = score_grid(args.rescore)
    df["aug25_vs_target"] = df["aug25"] - AUG25_TARGET
    df["aug25_vs_centre"] = df["aug25"] - CENTRE_AUG25
    df["dec15_vs_base"] = df["dec15"] - DEC15_BASELINE
    df["dec15_ok"] = df["dec15_vs_base"].abs() <= DEC15_BUDGET
    df["hits"] = (df["aug25_vs_target"].abs() <= AUG25_TOLERANCE) & df["dec15_ok"]
    pd.set_option("display.width", 220)

    print(f"Grid cells scored: {len(df)} / 243")
    print(f"Aug-25 range: {df.aug25.min():,.0f} .. {df.aug25.max():,.0f} "
          f"(spread {df.aug25.max() - df.aug25.min():,.0f})")
    print(f"Dec-15 in budget: {int(df.dec15_ok.sum())}   |   HITTING TARGET: "
          f"{int(df.hits.sum())}\n")

    for metric in ("aug25", "dec15"):
        table = decompose(df, metric)
        print(f"--- variance decomposition of {metric} "
              f"(all {len(table)} effects, exact, sums to 100%) ---")
        print(table.head(args.top).to_string(
            index=False, float_format=lambda v: f"{v:,.2f}"))
        by_order = table.groupby("order")["pct_var"].sum()
        print("  share by interaction order: " + "  ".join(
            f"{o}-way {p:5.1f}%" for o, p in by_order.items()))
        print(f"  main effects {by_order.get(1, 0):.1f}%  vs  "
              f"interactions {100 - by_order.get(1, 0):.1f}%\n")

    print("--- correlation of coded factor level with each outcome ---")
    coded = df.copy()
    for factor in FACTORS:
        low, _, high = LEVELS[factor]
        coded[factor] = coded[factor].map(
            lambda v, lo=low, hi=high: -1 if v == lo else (1 if v == hi else 0))
    print(coded[FACTORS + ["aug25", "dec15", "kink"]].corr()
          .loc[FACTORS, ["aug25", "dec15", "kink"]]
          .to_string(float_format=lambda v: f"{v:+.3f}"))

    eligible = df[df["dec15_ok"]].sort_values("aug25")
    print(f"\n--- lowest Aug-25 among the {len(eligible)} cells holding Dec-15 in budget ---")
    # Format factor columns separately: a thousands-no-decimals float_format renders cps 0.1649
    # and sps 0.00825 both as "0", which makes distinct cells look like duplicate rows.
    show = eligible.head(args.top)[
        FACTORS + ["aug25", "aug25_vs_centre", "aug25_vs_target", "dec15_vs_base",
                   "kink", "hits"]].copy()
    for factor in FACTORS:
        show[factor] = show[factor].map(lambda v: f"{v:g}")
    print(show.to_string(index=False, float_format=lambda v: f"{v:,.0f}"))

    if not eligible.empty:
        best = eligible.iloc[0]
        print(f"\nBEST IN GRID: " + "  ".join(f"{f}={best[f]:g}" for f in FACTORS))
        print(f"  Aug-25 {best.aug25:,.0f}  ({best.aug25_vs_centre:+,.0f} vs grid centre, "
              f"{best.aug25_vs_target:+,.0f} vs target)")
        print(f"  Dec-15 {best.dec15_vs_base:+,.0f}   kink {best.kink:,.0f} "
              f"({best.kink - S01_KINK:+,.0f} vs s01)")
        moved = -(best.aug25 - 45_223_249.05)
        print(f"  total move from canonical s01: {-moved:+,.0f} "
              f"({100 * moved / 196_183:.1f}% of the required 196,183)")

    print(f"\nCache: {CACHE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
