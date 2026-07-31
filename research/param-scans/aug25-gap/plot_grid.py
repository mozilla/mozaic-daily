"""Round-6 figures: the cpr x ncp interaction, and how isolated the winning cell is.

Three panels:
  (a) cpr x ncp interaction plot. Non-parallel lines ARE the interaction -- ncp's effect reverses
      sign depending on cpr, which is why Round 1's one-at-a-time probe at cpr=0.734 called ncp
      inert.
  (b) variance decomposition, top effects, Aug-25 vs Dec-15 side by side.
  (c) the winner against its ten one-step neighbours -- every one of them is worse.

Writes plots/round6_grid.png.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from analyze_grid import FACTORS, decompose, score_grid  # noqa: E402
from run_grid_round6 import LEVELS  # noqa: E402
from score_gradient import AUG25_TARGET, AUG25_TOLERANCE  # noqa: E402

WINNER = {"cps": 0.1649, "cpr": 0.814, "ncp": 40, "recent": 17, "sps": 0.00825}
WINNER_AUG25 = 45_036_389.0


def main() -> int:
    df = score_grid(rescore=False)
    fig = plt.figure(figsize=(17, 11))
    grid = fig.add_gridspec(2, 2, height_ratios=[1, 1])
    inter = fig.add_subplot(grid[0, 0])
    var = fig.add_subplot(grid[0, 1])
    neigh = fig.add_subplot(grid[1, :])

    # -- (a) cpr x ncp interaction -------------------------------------------------------
    for ncp, colour in zip(LEVELS["ncp"], ["tab:blue", "tab:orange", "tab:green"]):
        means = [df[(df.cpr == cpr) & (df.ncp == ncp)]["aug25"].mean() for cpr in LEVELS["cpr"]]
        inter.plot(LEVELS["cpr"], means, marker="o", ms=10, lw=2.5, color=colour,
                   label=f"ncp = {ncp}")
    inter.axhline(AUG25_TARGET, color="crimson", ls="--", lw=1.8, label="target")
    inter.set_xlabel("changepoint_range")
    inter.set_ylabel("mean Aug-25 DAU")
    inter.set_xticks(list(LEVELS["cpr"]))
    inter.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.3f}M"))
    inter.set_title("cpr × ncp interaction (18.5% of Aug-25 variance)\n"
                    "Non-parallel lines are the interaction: ncp's effect flips sign with cpr",
                    fontsize=12)
    inter.legend(fontsize=10)
    inter.grid(alpha=0.3)

    # -- (b) variance decomposition ------------------------------------------------------
    top_aug = decompose(df, "aug25").head(8).set_index("effect")["pct_var"]
    dec = decompose(df, "dec15").set_index("effect")["pct_var"]
    order = list(top_aug.index)
    y = np.arange(len(order))
    var.barh(y - 0.2, top_aug.values, height=0.4, color="tab:purple", label="Aug-25")
    var.barh(y + 0.2, [dec.get(e, 0) for e in order], height=0.4, color="tab:grey",
             label="Dec-15")
    var.set_yticks(y, order, fontsize=9)
    var.invert_yaxis()
    var.set_xlabel("% of total variance (exact decomposition, 31 effects sum to 100%)")
    var.set_title("Where the variance lives\n"
                  "Aug-25: 73% main / 27% interaction · Dec-15: 45% main / 55% interaction",
                  fontsize=12)
    var.legend(fontsize=10)
    var.grid(alpha=0.3, axis="x")

    # -- (c) winner vs its one-step neighbours -------------------------------------------
    labels, values = [], []
    for factor in FACTORS:
        for level in LEVELS[factor]:
            if level == WINNER[factor]:
                continue
            query = dict(WINNER)
            query[factor] = level
            mask = np.ones(len(df), dtype=bool)
            for key, value in query.items():
                mask &= (df[key] == value).to_numpy()
            labels.append(f"{factor}\n{WINNER[factor]:g}→{level:g}")
            values.append(float(df.loc[mask, "aug25"].iloc[0]))

    positions = np.arange(len(labels))
    neigh.bar(positions, values, color="lightsteelblue", edgecolor="grey")
    neigh.axhline(WINNER_AUG25, color="tab:green", lw=2.5,
                  label=f"winner {WINNER_AUG25:,.0f}")
    neigh.axhspan(AUG25_TARGET - AUG25_TOLERANCE, AUG25_TARGET + AUG25_TOLERANCE,
                  color="crimson", alpha=0.22, label="accept band")
    neigh.set_xticks(positions, labels, fontsize=9)
    neigh.set_ylim(44_950_000, 45_260_000)
    neigh.set_ylabel("Aug-25 DAU")
    neigh.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.3f}M"))
    neigh.set_title("The winner is a spike, not a basin — all ten one-step neighbours are worse "
                    f"(+52,092 to +165,860); only 1 of 243 cells lands in the band",
                    fontsize=12)
    neigh.legend(fontsize=10, loc="upper left")
    neigh.grid(alpha=0.3, axis="y")

    fig.tight_layout()
    out = HERE / "plots" / "round6_grid.png"
    fig.savefig(out, dpi=140, bbox_inches="tight")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
