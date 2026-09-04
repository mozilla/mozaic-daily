#!/usr/bin/env python3
"""Sizing exercise: what a +276,000 mobile tailwind looks like on the locked cpr0.725 base.

**Exploratory.** Nothing here is wired into the pipeline or the canonical notebook. The spec it
reads (`data-official/2026-08/tailwind/tailwind.json`) is deliberately outside `adjustments/`,
because the canonical notebook's `load_adjustments()` globs that directory and sums every JSON
in it — a spec dropped there would take effect silently.

Reports three things:
  1. where the tailwind lands the Dec-15 KPI, against the target band;
  2. what it costs in interpretive terms (YoY, share of mobile, per-day size, vs the paid level);
  3. how much of it has an evidence base versus how much is planning judgement.

    source .venv/bin/activate
    python research/param-scans/mobile-aug/tailwind_exercise.py
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

REPO = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO / "src"))

_spec = importlib.util.spec_from_file_location(
    "mobile_scoring", REPO / "scripts" / "mobile_scoring.py")
mobile_scoring = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mobile_scoring)

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from mozaic_daily.seam_ma import display_ma  # noqa: E402

BASE = (REPO / "data-official/2026-08/mobile_cpr0725_2026-07-28"
        / "cps0.035_thresh055_recent13_cpr0.725_ncp25_clip0.6_sps0.1"
        / "mozaic_daily_forecast.2026-07-28.gm-D.adj-p.parquet")
TAILWIND_SPEC = REPO / "data-official/2026-08/tailwind/tailwind.json"
PLOTS = REPO / "research/param-scans/mobile-aug/plots"

SEAM = pd.Timestamp("2026-07-28")
DEC15 = pd.Timestamp("2026-12-15")
TARGET = mobile_scoring.TARGET_DEC15
TOL = mobile_scoring.TOLERANCE
DESKTOP_DEC15 = 48_697_603      # locked g01 build, post-headwind
JULY_MOBILE = 17_923_869
JULY_ALL = 66_509_352
PAID_LEVEL = 922_250.47 + 637_226.74
PROTOTYPE_DELTA = 141_637       # independent implementation's measured excess


def ramp(dates: pd.DatetimeIndex, spec: dict, key: str) -> pd.Series:
    """Linear ramp: 0 at start_date, spec[key] at anchor_date, clamped after. 0 before the seam."""
    start, anchor = pd.Timestamp(spec["start_date"]), pd.Timestamp(spec["anchor_date"])
    full, span = float(spec[key]), (pd.Timestamp(spec["anchor_date"]) - start).days
    elapsed = (dates - start).days.to_numpy().clip(0, span)
    return pd.Series(full * elapsed / span, index=dates)


def main() -> int:
    spec = json.loads(TAILWIND_SPEC.read_text())
    headwind = mobile_scoring.load_headwind(mobile_scoring.DEFAULT_HEADWIND)
    tail_dau = float(spec["mobile_dau"])

    df, _ = load_forecast(str(BASE), require_state=["p"])
    daily = mobile_scoring.mobile_daily_series(df)
    ma = display_ma(daily["target_date"], daily["dau"], SEAM)

    hw = ramp(ma.index, headwind, "mobile_dau")
    tw = ramp(ma.index, spec, "mobile_dau")
    hw[ma.index < SEAM] = 0.0
    tw[ma.index < SEAM] = 0.0

    base_post = ma + hw                 # the locked build as published
    with_tw = base_post + tw

    b, w = float(base_post.loc[DEC15]), float(with_tw.loc[DEC15])
    actual = daily[daily["data_type"] == "training"].set_index("target_date")["dau"]
    actual_ma = actual.rolling(28).mean()
    prior = float(actual_ma.loc[DEC15 - pd.DateOffset(years=1)])

    print("=" * 78)
    print("MOBILE TAILWIND SIZING EXERCISE — exploratory, nothing wired")
    print("=" * 78)
    print(f"base (locked cpr0.725, post-headwind) {b:>14,.0f}")
    print(f"tailwind at Dec-15                    {tail_dau:>+14,.0f}")
    print(f"with tailwind                         {w:>14,.0f}")
    print(f"target                                {TARGET:>14,.0f}  +-{TOL:,}")
    print(f"result                                {w - TARGET:>+14,.0f}   "
          f"{'IN BAND' if abs(w - TARGET) <= TOL else 'OUT OF BAND'}")

    print(f"\n--- what +{tail_dau:,.0f} means ---")
    print(f"  share of the Dec-15 mobile total     {tail_dau / w * 100:>8.2f}%")
    print(f"  YoY at Dec-15   {b / prior - 1:>7.2%}  ->  {w / prior - 1:>7.2%}   "
          f"(measured organic rate 11.60%)")
    print(f"  vs the whole paid level ({PAID_LEVEL:,.0f})  {tail_dau / PAID_LEVEL * 100:>7.1f}%")
    print(f"  ramp slope                           {tail_dau / (DEC15 - SEAM).days:>8,.0f} DAU/day")
    print(f"  equivalent to Firefox iOS growing an extra ~{tail_dau / 3_650_000 * 100:.1f}% "
          f"(iOS is ~21.5% of mobile)")

    print("\n--- evidence base ---")
    print(f"  independent implementation (prototype) {PROTOTYPE_DELTA:>+10,.0f}   "
          f"{PROTOTYPE_DELTA / tail_dau * 100:>5.0f}% of the tailwind")
    print(f"  planning judgement, unattributed       {tail_dau - PROTOTYPE_DELTA:>+10,.0f}   "
          f"{(tail_dau - PROTOTYPE_DELTA) / tail_dau * 100:>5.0f}%")
    print("  (two identified production defects — untreated Farsi-locale shutdown craters in")
    print("   ROW tiles, and 3.5 years of backfilled Fenix organic share — both bias DOWN,")
    print("   but neither is measured, so neither can be claimed as a specific quantity.)")

    print("\n--- ALL (desktop locked at 48,697,603) ---")
    for label, mob in [("base only", b), ("with tailwind", w)]:
        allv = DESKTOP_DEC15 + mob
        print(f"  {label:<15} mobile {mob:>12,.0f}   ALL {allv:>12,.0f}   "
              f"vs July ALL {allv - JULY_ALL:>+10,.0f}")

    # --- chart ---------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(13, 6.5))
    ax.axhspan(TARGET - TOL, TARGET + TOL, color="tab:green", alpha=0.12, zorder=0,
               label=f"target band {TARGET:,.0f} +-{TOL:,}")
    ax.axhline(TARGET, color="tab:green", lw=1.2, ls="--", zorder=1)

    a = actual_ma[actual_ma.index >= pd.Timestamp("2026-01-01")].dropna()
    ax.plot(a.index, a.values, color="black", lw=2.0, label="actuals (28d-MA)", zorder=5)

    fc = slice(SEAM, pd.Timestamp("2026-12-31"))
    ax.plot(base_post[fc].index, base_post[fc].values, color="tab:red", lw=1.9,
            label=f"locked base cpr0.725  ->  {b:,.0f}", zorder=4)
    ax.plot(with_tw[fc].index, with_tw[fc].values, color="tab:blue", lw=2.2,
            label=f"+ {tail_dau:,.0f} tailwind  ->  {w:,.0f}", zorder=4)
    ax.fill_between(base_post[fc].index, base_post[fc].values, with_tw[fc].values,
                    color="tab:blue", alpha=0.15, zorder=3, label="tailwind (ramp from seam)")
    for value, color in ((b, "tab:red"), (w, "tab:blue")):
        ax.plot([DEC15], [value], marker="o", color=color, ms=7, zorder=6)

    ax.axvline(SEAM, color="grey", lw=1.0, ls=":", zorder=2)
    ax.set_title("August mobile — locked cpr0.725 base with a +276,000 tailwind (EXPLORATORY)\n"
                 "ALL MOBILE world DAU, 28d-MA, post-headwind", fontsize=12)
    ax.set_ylabel("DAU (28-day MA)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(alpha=0.25)
    ax.legend(loc="upper left", fontsize=9)
    fig.tight_layout()
    out = PLOTS / "tailwind_276k_exercise.png"
    fig.savefig(out, dpi=130)
    print(f"\nsaved {out.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
