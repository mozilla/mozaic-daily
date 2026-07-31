"""Score Round 2: the s01 -> July-params blend axis, both seasonality regimes.

Reports each blend fraction under both regimes, plus the two axis endpoints (s01 at f=0 and
July's shipped config at f=1), and answers the question the round was run to settle: is the
regime a continuous ramp we can land the target on, or a step too tall to land on?

OBJECTIVE: Aug-25 -> 45,027,066 (+/-25,000), Dec-15 within +/-50,000 of 48,703,960.

Usage:
    python research/param-scans/aug25-gap/score_blend.py [--csv]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from score_near_horizon import score_parquet  # noqa: E402

from run_blend_round2 import AXIS, FRACTIONS, blend  # noqa: E402
from score_gradient import (  # noqa: E402
    AUG25_TARGET, AUG25_TOLERANCE, DEC15_BASELINE, DEC15_BUDGET, S01_PARQUET, _config_of,
)

HERE = Path(__file__).resolve().parent
PARQUET_NAME = "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet"

# July's shipped config re-run at OUR anchor on OUR data -- the f=1.0 end of the axis.
# Carries LOL 180K rather than the locked 200K, so its Dec-15 reads ~7K low; noted in the
# report rather than silently corrected, since the correction is a pinned ledger constant
# and not a measurement of this build.
JULY_PARAMS_PARQUET = (
    REPO_ROOT / "data-official/2026-08/desktop_baseline_2026-07-28"
    / "cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825" / PARQUET_NAME
)

# CLI flag name -> parameters.json field
FIELD = {
    "changepoint-prior-scale": "prophet_changepoint_prior_scale",
    "changepoint-range": "prophet_changepoint_range",
    "n-changepoints": "prophet_n_changepoints",
    "recent-weeks": "prophet_recent_weeks",
}


def _blend_fraction(config: dict) -> float | None:
    """Return the blend fraction this config sits at, or None if it is not on the axis."""
    for fraction in FRACTIONS:
        knobs = blend(fraction)
        if all(config[FIELD[k]] == knobs[k] for k in AXIS):
            return fraction
    return None


def collect() -> pd.DataFrame:
    rows = []

    def add(parquet: Path, fraction: float, regime: str) -> None:
        scored = score_parquet(parquet, target_date="2026-08-25")
        rows.append({
            "f": fraction,
            "regime": regime,
            "aug25": scored["global_target_post"],
            "dec15": scored["global_dec15_post"],
            "kink": scored["seam_slope_kink_model"],
            "trough_date": scored["trough_min_date"],
        })

    add(S01_PARQUET, 0.0, "multiplicative")
    if JULY_PARAMS_PARQUET.exists():
        add(JULY_PARAMS_PARQUET, 1.0, "auto")

    for parquet in sorted(HERE.glob(f"runs/*/{PARQUET_NAME}")):
        config = _config_of(parquet)
        fraction = _blend_fraction(config)
        if fraction is None:
            continue  # a Round-1 single-knob probe, not on this axis
        add(parquet, fraction, config["seasonality_regime"])

    df = pd.DataFrame(rows).sort_values(["regime", "f"]).reset_index(drop=True)
    df["aug25_vs_s01"] = df["aug25"] - float(df.loc[(df.f == 0.0), "aug25"].iloc[0])
    df["aug25_vs_target"] = df["aug25"] - AUG25_TARGET
    df["dec15_vs_base"] = df["dec15"] - DEC15_BASELINE
    df["hits"] = (df["aug25_vs_target"].abs() <= AUG25_TOLERANCE) & \
                 (df["dec15_vs_base"].abs() <= DEC15_BUDGET)
    return df


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", action="store_true")
    args = parser.parse_args()

    df = collect()
    pd.set_option("display.width", 200)

    print(f"Aug-25 target {AUG25_TARGET:,} (+/-{AUG25_TOLERANCE:,})   "
          f"Dec-15 {DEC15_BASELINE:,} (+/-{DEC15_BUDGET:,})\n")
    # Format f separately: a thousands-with-no-decimals float_format applied to the whole
    # frame renders 0.10 and 0.30 both as "0", which silently makes every blend point look
    # like the s01 endpoint.
    show = df[["f", "regime", "aug25", "aug25_vs_s01", "aug25_vs_target",
               "dec15", "dec15_vs_base", "kink", "trough_date", "hits"]].copy()
    show["f"] = show["f"].map(lambda v: f"{v:.2f}")
    print(show.to_string(index=False, float_format=lambda v: f"{v:,.0f}"))

    # The question this round exists to answer: how tall is the regime step?
    print("\nRegime step at matched trend knobs (auto - multiplicative):")
    for fraction in sorted(set(df["f"])):
        pair = df[df["f"] == fraction]
        if set(pair["regime"]) != {"auto", "multiplicative"}:
            continue
        a = pair[pair.regime == "auto"].iloc[0]
        m = pair[pair.regime == "multiplicative"].iloc[0]
        print(f"  f={fraction:<5.2f}  Aug-25 {a.aug25 - m.aug25:>+12,.0f}   "
              f"Dec-15 {a.dec15 - m.dec15:>+12,.0f}")

    hits = df[df["hits"]]
    print(f"\nCandidates satisfying BOTH constraints: {len(hits)}")
    if not hits.empty:
        print(hits.to_string(index=False, float_format=lambda v: f"{v:,.0f}"))

    if args.csv:
        out = HERE / "round2_scores.csv"
        df.to_csv(out, index=False)
        print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
