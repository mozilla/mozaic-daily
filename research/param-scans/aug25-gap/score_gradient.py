"""Score the Round-1 +/- delta probe and build the Aug-25 gradient / curvature table.

Reads the s01 center point (data-official/2026-08/desktop_locked/) plus every run under
runs/, scores each with the shared scorer, and reports per-knob central differences at the
two scored dates.

OBJECTIVE (this search only): move Aug-25 DOWN by 196,183 to 45,027,066 (+/-25,000) while
holding Dec-15 within +/-50,000 of 48,703,960. Sibling scans under research/param-scans/
were tuned to move the trough UP; their targets do not apply here. In particular the
`in_band` / `gap_to_band_low` fields emitted by scripts/score_near_horizon.py refer to a
45M-46M band from that earlier objective and are deliberately not used.

Usage:
    python research/param-scans/aug25-gap/score_gradient.py [--csv]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from score_near_horizon import score_parquet  # noqa: E402

HERE = Path(__file__).resolve().parent
RUNS_DIR = HERE / "runs"
SCORES_CSV = HERE / "scores.csv"

TARGET_DATE = "2026-08-25"
AUG25_BASELINE = 45_223_249
AUG25_TARGET = 45_027_066
AUG25_TOLERANCE = 25_000
DEC15_BASELINE = 48_703_960
DEC15_BUDGET = 50_000

S01_PARQUET = (
    REPO_ROOT / "data-official/2026-08/desktop_locked"
    / "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet"
)

# knob -> (parameters.json field, s01 value, delta)
KNOBS = {
    "cps": ("prophet_changepoint_prior_scale", 0.1849, 0.02),
    "cpr": ("prophet_changepoint_range", 0.734, 0.05),
    "ncp": ("prophet_n_changepoints", 35, 5),
    "recent": ("prophet_recent_weeks", 17, 3),
    "sps": ("prophet_seasonality_prior_scale", 0.00825, 0.00175),
}


def _find_run_parquets() -> list[Path]:
    return sorted(RUNS_DIR.glob("*/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet"))


def _config_of(parquet: Path) -> dict:
    return json.loads((parquet.parent / "parameters.json").read_text())["config"]


def _identify(config: dict) -> str:
    """Label a run by which single knob it moved off s01, e.g. 'cps_hi'."""
    moved = [
        (knob, config[field], s01)
        for knob, (field, s01, _) in KNOBS.items()
        if config[field] != s01
    ]
    if not moved:
        return "s01"
    if len(moved) > 1:
        return "MULTI:" + ",".join(k for k, _, _ in moved)
    knob, value, s01 = moved[0]
    return f"{knob}_{'hi' if value > s01 else 'lo'}"


def collect() -> pd.DataFrame:
    rows = []
    for parquet in [S01_PARQUET, *_find_run_parquets()]:
        if not parquet.exists():
            continue
        config = _config_of(parquet)
        scored = score_parquet(parquet, target_date=TARGET_DATE)
        label = "s01" if parquet == S01_PARQUET else _identify(config)
        rows.append({
            "label": label,
            "aug25": scored["global_target_post"],
            "dec15": scored["global_dec15_post"],
            "trough_min": scored["trough_min_post"],
            "trough_date": scored["trough_min_date"],
            "kink_model": scored["seam_slope_kink_model"],
            **{k: config[f] for k, (f, _, _) in KNOBS.items()},
            "parquet": str(parquet.relative_to(REPO_ROOT)),
        })

    df = pd.DataFrame(rows)
    df["aug25_vs_target"] = df["aug25"] - AUG25_TARGET
    df["dec15_vs_base"] = df["dec15"] - DEC15_BASELINE
    df["hits_aug25"] = df["aug25_vs_target"].abs() <= AUG25_TOLERANCE
    df["dec15_ok"] = df["dec15_vs_base"].abs() <= DEC15_BUDGET
    return df


def gradients(df: pd.DataFrame) -> pd.DataFrame:
    """Central-difference first and second derivatives per knob, at both scored dates."""
    by_label = df.set_index("label")
    if "s01" not in by_label.index:
        raise SystemExit("s01 center point missing -- cannot form central differences.")
    center = by_label.loc["s01"]

    out = []
    for knob, (_, s01_value, delta) in KNOBS.items():
        lo_label, hi_label = f"{knob}_lo", f"{knob}_hi"
        if lo_label not in by_label.index or hi_label not in by_label.index:
            continue
        lo, hi = by_label.loc[lo_label], by_label.loc[hi_label]

        row = {"knob": knob, "s01": s01_value, "delta": delta}
        for metric in ("aug25", "dec15"):
            f_lo, f_0, f_hi = lo[metric], center[metric], hi[metric]
            first = (f_hi - f_lo) / (2 * delta)
            second = (f_hi - 2 * f_0 + f_lo) / (delta ** 2)
            row[f"{metric}_d1"] = first
            row[f"{metric}_d2"] = second
            # Elasticity: DAU change per +1% change in the knob. Makes knobs on
            # different scales directly rankable.
            row[f"{metric}_per_pct"] = first * s01_value / 100.0
            # Curvature check: how far the two one-sided slopes disagree, as a
            # fraction of the central slope. Large => a linear solve is unsafe.
            one_sided_lo = (f_0 - f_lo) / delta
            one_sided_hi = (f_hi - f_0) / delta
            row[f"{metric}_asym"] = (
                abs(one_sided_hi - one_sided_lo) / abs(first) if first else float("nan")
            )
        # The number that decides the search: Aug-25 movement bought per unit of
        # Dec-15 movement. Must beat ~4:1 (196,183 needed vs 50,000 allowed).
        row["efficiency"] = (
            abs(row["aug25_per_pct"] / row["dec15_per_pct"])
            if row["dec15_per_pct"] else float("inf")
        )
        # Knob % move implied to close the full 196,183 at Aug-25, linear extrapolation.
        row["pct_to_target"] = (
            (AUG25_TARGET - center["aug25"]) / row["aug25_per_pct"]
            if row["aug25_per_pct"] else float("nan")
        )
        row["implied_dec15"] = row["pct_to_target"] * row["dec15_per_pct"]
        out.append(row)
    return pd.DataFrame(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", action="store_true", help="Write scores.csv")
    args = parser.parse_args()

    df = collect()
    pd.set_option("display.width", 200, "display.max_columns", 50)

    print(f"Aug-25 target {AUG25_TARGET:,} (+/-{AUG25_TOLERANCE:,})   "
          f"Dec-15 {DEC15_BASELINE:,} (+/-{DEC15_BUDGET:,})   "
          f"baseline Aug-25 {AUG25_BASELINE:,}\n")

    show = df[["label", "aug25", "aug25_vs_target", "dec15", "dec15_vs_base",
               "trough_date", "kink_model", "hits_aug25", "dec15_ok"]]
    print(show.to_string(index=False, float_format=lambda v: f"{v:,.0f}"))

    grad = gradients(df)
    if not grad.empty:
        print("\nCentral differences about s01 (DAU per +1% of knob):\n")
        print(grad[["knob", "s01", "delta", "aug25_per_pct", "dec15_per_pct",
                    "efficiency", "aug25_asym", "pct_to_target", "implied_dec15"]]
              .to_string(index=False, float_format=lambda v: f"{v:,.2f}"))
        print("\n  efficiency    = |Aug-25 per %| / |Dec-15 per %|; must beat ~3.92 "
              f"({AUG25_BASELINE - AUG25_TARGET:,} needed / {DEC15_BUDGET:,} allowed)")
        print("  aug25_asym    = disagreement between the two one-sided slopes, "
              "as a fraction of the central slope. >~0.3 means a linear solve is unsafe.")
        print("  pct_to_target = linear extrapolation only; trust it only where aug25_asym is small.")

    if args.csv:
        df.to_csv(SCORES_CSV, index=False)
        grad.to_csv(HERE / "gradients.csv", index=False)
        print(f"\nWrote {SCORES_CSV} and {HERE / 'gradients.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
