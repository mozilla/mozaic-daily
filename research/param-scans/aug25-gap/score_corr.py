"""Score Round 3: the seasonality_corr_threshold dial, and the kink curve that rides along with it.

Answers three things:
  1. Does t = -1.0 reproduce forced-multiplicative? If not, the dial is not what the help text
     claims and nothing downstream is interpretable.
  2. Where on the dial does Aug-25 hit 45,027,066 +/- 25,000, and what does Dec-15 do there?
  3. How does the seam kink vary across the dial -- requested explicitly, because Round 2 showed
     the kink tracks the REGIME (~-9.5K multiplicative vs ~-64K auto), so crossing the gap may
     drag the kink with it.

Usage:
    python research/param-scans/aug25-gap/score_corr.py [--csv]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from score_near_horizon import score_parquet  # noqa: E402

from run_corr_round3 import S01 as S01_KNOBS  # noqa: E402
from score_blend import PARQUET_NAME  # noqa: E402
from score_gradient import (  # noqa: E402
    AUG25_TARGET, AUG25_TOLERANCE, DEC15_BASELINE, DEC15_BUDGET, S01_PARQUET, _config_of,
)

FIELD = {
    "changepoint-prior-scale": "prophet_changepoint_prior_scale",
    "changepoint-range": "prophet_changepoint_range",
    "n-changepoints": "prophet_n_changepoints",
    "recent-weeks": "prophet_recent_weeks",
}


def _on_sweep(config: dict) -> bool:
    """True if this run holds s01 trend knobs under regime=auto -- i.e. it is a dial point."""
    if config.get("seasonality_regime") != "auto":
        return False
    return all(str(config[field]) == S01_KNOBS[flag] for flag, field in FIELD.items())


def collect() -> tuple[pd.DataFrame, dict]:
    rows = []
    for parquet in sorted(HERE.glob(f"runs/*/{PARQUET_NAME}")):
        config = _config_of(parquet)
        if not _on_sweep(config):
            continue
        scored = score_parquet(parquet, target_date="2026-08-25")
        rows.append({
            "corr_threshold": float(config["seasonality_corr_threshold"]),
            "aug25": scored["global_target_post"],
            "dec15": scored["global_dec15_post"],
            "kink": scored["seam_slope_kink_model"],
            "trough_date": scored["trough_min_date"],
        })

    reference = score_parquet(S01_PARQUET, target_date="2026-08-25")
    df = pd.DataFrame(rows).sort_values("corr_threshold").reset_index(drop=True)
    df["aug25_vs_target"] = df["aug25"] - AUG25_TARGET
    df["dec15_vs_base"] = df["dec15"] - DEC15_BASELINE
    df["hits"] = (df["aug25_vs_target"].abs() <= AUG25_TOLERANCE) & \
                 (df["dec15_vs_base"].abs() <= DEC15_BUDGET)
    return df, reference


def solve(df: pd.DataFrame) -> tuple[float | None, float, tuple[float, float] | None]:
    """Threshold where Aug-25 crosses the target, by linear interpolation between samples.

    Returns (threshold, bracket_height, bracket). ``bracket_height`` is how far apart the two
    bracketing samples are in DAU. The interpolation assumes the response is roughly linear
    between them; corr_threshold flips whole tiles from additive to multiplicative, so the true
    response is a STAIRCASE. When the bracket is much taller than the accept tolerance, the
    interpolated crossing is a guess about the interior of an unsampled step and must not be
    reported as a solution -- callers should check the height.
    """
    s = df.sort_values("corr_threshold")
    x, y = s["corr_threshold"].to_numpy(), s["aug25"].to_numpy()
    for i in range(len(x) - 1):
        lo, hi = sorted((y[i], y[i + 1]))
        if lo <= AUG25_TARGET <= hi and y[i] != y[i + 1]:
            frac = (AUG25_TARGET - y[i]) / (y[i + 1] - y[i])
            return (float(x[i] + frac * (x[i + 1] - x[i])),
                    abs(y[i + 1] - y[i]), (float(x[i]), float(x[i + 1])))
    return None, 0.0, None


def plot(df: pd.DataFrame, reference: dict) -> Path:
    fig, (top, bottom) = plt.subplots(2, 1, figsize=(12, 9), sharex=True)

    top.plot(df["corr_threshold"], df["aug25"], marker="o", ms=9, lw=2, color="tab:purple")
    top.axhspan(AUG25_TARGET - AUG25_TOLERANCE, AUG25_TARGET + AUG25_TOLERANCE,
                color="crimson", alpha=0.22)
    top.axhline(AUG25_TARGET, color="crimson", ls="--", lw=1.8,
                label=f"target {AUG25_TARGET:,.0f} ±{AUG25_TOLERANCE:,.0f}")
    top.axhline(reference["global_target_post"], color="tab:blue", ls=":", lw=1.8,
                label=f"s01 forced multiplicative {reference['global_target_post']:,.0f}")
    crossing, height, bracket = solve(df)
    if crossing is not None:
        top.axvline(crossing, color="black", lw=1.4, ls="-.",
                    label=f"nominal crossing t ≈ {crossing:.3f}")
    if bracket is not None and height > 5 * AUG25_TOLERANCE:
        top.axvspan(*bracket, color="grey", alpha=0.25,
                    label=f"UNSAMPLED STEP — {height:,.0f} DAU tall")
    top.set_ylabel("Aug-25 DAU (28d MA, post-headwind)")
    top.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
    top.set_title("seasonality_corr_threshold is ONE RISER, not a dial\n"
                  "regime=auto, s01 trend knobs held fixed", fontsize=13)
    top.legend(fontsize=9)
    top.grid(alpha=0.3)

    bottom.plot(df["corr_threshold"], df["kink"], marker="s", ms=9, lw=2, color="tab:orange",
                label="seam kink (model-only)")
    bottom.axhline(reference["seam_slope_kink_model"], color="tab:blue", ls=":", lw=1.8,
                   label=f"s01 kink {reference['seam_slope_kink_model']:,.0f}")
    if crossing is not None:
        bottom.axvline(crossing, color="black", lw=1.4, ls="-.")
        kink_at = float(np.interp(crossing, df["corr_threshold"], df["kink"]))
        bottom.plot([crossing], [kink_at], marker="*", ms=20, color="crimson", ls="none",
                    label=f"interpolated kink ≈ {kink_at:,.0f} (inside the step)")
    if bracket is not None and height > 5 * AUG25_TOLERANCE:
        bottom.axvspan(*bracket, color="grey", alpha=0.25)
    bottom.set_xlabel("seasonality_corr_threshold   (lower ⇒ more tiles multiplicative)")
    bottom.set_ylabel("seam slope kink (DAU/day)")
    bottom.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e3:,.0f}K"))
    bottom.set_title("Kink curve across the dial", fontsize=13)
    bottom.legend(fontsize=9)
    bottom.grid(alpha=0.3)

    fig.tight_layout()
    out = HERE / "plots" / "corr_dial_full.png"
    fig.savefig(out, dpi=140, bbox_inches="tight")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", action="store_true")
    args = parser.parse_args()

    df, reference = collect()
    if df.empty:
        raise SystemExit("No dial points found under runs/.")
    pd.set_option("display.width", 200)

    print(f"Aug-25 target {AUG25_TARGET:,} (+/-{AUG25_TOLERANCE:,})   "
          f"Dec-15 {DEC15_BASELINE:,} (+/-{DEC15_BUDGET:,})\n")
    show = df.copy()
    show["corr_threshold"] = show["corr_threshold"].map(lambda v: f"{v:+.4f}")
    print(show.to_string(index=False, float_format=lambda v: f"{v:,.0f}"))

    anchor = df[df["corr_threshold"] == -1.0]
    if not anchor.empty:
        drift = float(anchor.iloc[0]["aug25"]) - reference["global_target_post"]
        verdict = "VALID" if abs(drift) < 25_000 else "SUSPECT"
        print(f"\nSanity anchor t=-1.0 vs forced multiplicative: drift {drift:+,.0f}  [{verdict}]")
        print("  (t=-1.0 puts every tile above the cutoff, so it should reproduce s01.)")

    crossing, height, bracket = solve(df)
    print(f"\nAug-25 target is bracketed at t ≈ "
          f"{'%.3f' % crossing if crossing is not None else 'NOT BRACKETED by this sweep'}")
    if bracket is not None and height > 5 * AUG25_TOLERANCE:
        print(f"  *** NOT A SOLUTION *** the bracketing samples t={bracket[0]} and t={bracket[1]} "
              f"are {height:,.0f} DAU apart ({height / AUG25_TOLERANCE:.0f}x the tolerance).")
        print("  corr_threshold flips whole tiles, so the response is a staircase, not a line.")
        print("  This interpolation guesses the interior of an unsampled step. Sample inside it.")
    if crossing is not None:
        for metric in ("dec15", "kink"):
            value = float(np.interp(crossing, df["corr_threshold"], df[metric]))
            extra = f"  (vs base {value - DEC15_BASELINE:+,.0f})" if metric == "dec15" else \
                    f"  (s01 {reference['seam_slope_kink_model']:,.0f})"
            print(f"  interpolated {metric:<6} {value:>14,.0f}{extra}")

    hits = df[df["hits"]]
    print(f"\nSampled points satisfying BOTH constraints: {len(hits)}")
    if not hits.empty:
        print(hits.to_string(index=False, float_format=lambda v: f"{v:,.0f}"))

    print(f"\nWrote {plot(df, reference)}")
    if args.csv:
        out = HERE / "corr_dial_scores.csv"
        df.to_csv(out, index=False)
        print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
