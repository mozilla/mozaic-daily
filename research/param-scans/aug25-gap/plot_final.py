"""Final figure: the best-achievable candidate against canonical, July, and the target.

Top: canonical format -- actuals from Jan 1, s01, the candidate, July's delivered curve, target star.
Bottom: the reachable set. Every config scored across five rounds, plotted as Aug-25 against
Dec-15 delta, with the accept band and the Dec-15 cap drawn. Shows at a glance that the target
band is empty and why: the points cluster in two bands ~1.32M apart with nothing between them.

Writes plots/final_candidate.png.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))

from leaderboard import collect_all  # noqa: E402
from plot_round1 import AUG25, DISPLAY_END, DISPLAY_START, curve  # noqa: E402
from score_gradient import (  # noqa: E402
    AUG25_TARGET, AUG25_TOLERANCE, DEC15_BASELINE, DEC15_BUDGET, S01_PARQUET,
)

CANDIDATE = (
    REPO_ROOT / "data-official/2026-08/desktop_candidate_aug25"
    / "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet"
)


def main() -> int:
    s01, actuals = curve(S01_PARQUET)
    candidate, _ = curve(CANDIDATE)
    july = pd.read_csv(
        REPO_ROOT / "data-official/2026-07/csv/july_canonical_curves.csv", parse_dates=["date"]
    ).set_index("date")["desktop_current_july"]

    fig, (top, bottom) = plt.subplots(2, 1, figsize=(15, 12))

    top.plot(actuals.index, actuals.values, color="black", lw=2.2, label="Actuals (28d MA)")
    top.plot(s01.index, s01.values, color="tab:blue", lw=2.4,
             label="s01 canonical — Aug-25 45,223,249")
    top.plot(candidate.index, candidate.values, color="tab:green", lw=2.4,
             label="candidate cpr=0.784 — Aug-25 45,195,814")
    top.plot(july.index, july.values, color="tab:red", lw=1.8, ls="--",
             label="July delivered — Aug-25 43,261,424")
    top.plot([AUG25], [AUG25_TARGET], marker="*", ms=22, color="crimson", ls="none", zorder=6,
             label=f"target {AUG25_TARGET:,.0f} (unreached by 168,748)")
    top.axvline(pd.Timestamp("2026-07-28"), color="grey", lw=1, ls=":")
    top.set_xlim(DISPLAY_START, DISPLAY_END)
    top.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
    top.set_title("Best achievable on Prophet parameters: 14.0% of the requested move\n"
                  "candidate and canonical are nearly coincident at this scale", fontsize=13)
    top.legend(loc="lower right", fontsize=10)
    top.grid(alpha=0.3)

    # -- reachable set -------------------------------------------------------------------
    df = collect_all()
    ok = df[df["dec15_ok"]]
    bad = df[~df["dec15_ok"]]
    bottom.scatter(ok["dec15_vs_base"], ok["aug25"], s=55, color="tab:blue",
                   label=f"Dec-15 in budget ({len(ok)})", zorder=3)
    bottom.scatter(bad["dec15_vs_base"], bad["aug25"], s=55, color="lightgrey",
                   edgecolor="grey", label=f"Dec-15 over budget ({len(bad)})", zorder=2)

    bottom.axhspan(AUG25_TARGET - AUG25_TOLERANCE, AUG25_TARGET + AUG25_TOLERANCE,
                   color="crimson", alpha=0.25, zorder=1)
    bottom.axhline(AUG25_TARGET, color="crimson", ls="--", lw=1.8,
                   label=f"Aug-25 target band (EMPTY — 0 of {len(df)} configs)")
    for edge in (-DEC15_BUDGET, DEC15_BUDGET):
        bottom.axvline(edge, color="black", lw=1.4, ls=":")
    bottom.axvline(0, color="black", lw=0.8, alpha=0.4)

    best = ok.iloc[0]
    bottom.scatter([best["dec15_vs_base"]], [best["aug25"]], s=260, marker="*",
                   color="tab:green", edgecolor="black", zorder=5,
                   label=f"best: cpr=0.784 ({best['aug25']:,.0f})")

    xlim, ylim = (-80_000, 200_000), (43_700_000, 45_450_000)
    bottom.set_xlim(*xlim)
    bottom.set_ylim(*ylim)
    # Matplotlib drops out-of-range points silently, which would leave the legend claiming 42
    # configs while far fewer are drawn. Say how many are off-axis and where they went.
    off = df[~(df["dec15_vs_base"].between(*xlim) & df["aug25"].between(*ylim))]
    if not off.empty:
        worst = off.loc[off["dec15_vs_base"].abs().idxmax()]
        bottom.annotate(
            f"{len(off)} config(s) off-axis, all Dec-15 over budget\n"
            f"(worst: {worst.label} at {worst.dec15_vs_base:+,.0f})",
            xy=(0.985, 0.04), xycoords="axes fraction", ha="right", fontsize=9,
            bbox={"boxstyle": "round", "fc": "lightyellow", "ec": "grey"})
    bottom.set_xlabel("Dec-15 delta vs canonical (dotted lines = ±50,000 cap)")
    bottom.set_ylabel("Aug-25 DAU (28d MA, post-headwind)")
    bottom.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
    bottom.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e3:+,.0f}K"))
    bottom.set_title("The reachable set — 42 configs over five rounds. Two bands ~1.32M apart, "
                     "target band empty between them", fontsize=13)
    bottom.legend(loc="upper right", fontsize=9)
    bottom.grid(alpha=0.3)

    fig.tight_layout()
    out = HERE / "plots" / "final_candidate.png"
    fig.savefig(out, dpi=140, bbox_inches="tight")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
