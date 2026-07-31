"""Round-2 figure: the s01 -> July-params blend axis is discontinuous, and the target is in the gap.

Left: Aug-25 against blend fraction, one series per seasonality regime, with the accept band
shaded. The two branches never approach each other, and the band sits between them.
Right: the 28d-MA curves themselves for the two branch endpoints, so the gap is visible as a
shape difference rather than only as a scalar.

Writes plots/round2_blend_gap.png.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from plot_round1 import AUG25, DISPLAY_END, DISPLAY_START, curve  # noqa: E402
from score_blend import JULY_PARAMS_PARQUET, PARQUET_NAME, _blend_fraction, collect  # noqa: E402
from score_gradient import AUG25_TARGET, AUG25_TOLERANCE, S01_PARQUET, _config_of  # noqa: E402

STYLE = {"multiplicative": ("tab:blue", "o"), "auto": ("tab:red", "s")}


def main() -> int:
    df = collect()
    fig, (left, right) = plt.subplots(1, 2, figsize=(16, 7))

    # -- left: Aug-25 vs blend fraction ------------------------------------------------
    for regime, group in df.groupby("regime"):
        color, marker = STYLE[regime]
        group = group.sort_values("f")
        left.plot(group["f"], group["aug25"], marker=marker, ms=9, lw=2,
                  color=color, label=f"regime={regime}")

    left.axhspan(AUG25_TARGET - AUG25_TOLERANCE, AUG25_TARGET + AUG25_TOLERANCE,
                 color="crimson", alpha=0.22, zorder=0)
    left.axhline(AUG25_TARGET, color="crimson", lw=1.8, ls="--",
                 label=f"target {AUG25_TARGET:,.0f} ±{AUG25_TOLERANCE:,.0f}")

    mult_floor = df[df.regime == "multiplicative"]["aug25"].min()
    auto_ceiling = df[df.regime == "auto"]["aug25"].max()
    left.annotate(
        f"unreachable gap\n{mult_floor - auto_ceiling:,.0f} wide\n"
        f"target sits {mult_floor - AUG25_TARGET:,.0f} below\nthe multiplicative floor",
        xy=(0.55, (mult_floor + auto_ceiling) / 2), ha="center", va="center", fontsize=11,
        bbox={"boxstyle": "round", "fc": "lightyellow", "ec": "grey"})
    left.annotate("", xy=(0.42, mult_floor), xytext=(0.42, auto_ceiling),
                  arrowprops={"arrowstyle": "<->", "lw": 2, "color": "black"})

    left.set_xlabel("blend fraction f   (0 = s01,  1 = July shipped params)")
    left.set_ylabel("Aug-25 DAU, 28d MA, post-headwind")
    left.set_title("The regime is a cliff, not a ramp\n"
                   "Trend knobs move Aug-25 by tens of thousands; the regime moves it by 1.35M",
                   fontsize=12)
    left.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
    left.legend(loc="center right", fontsize=10)
    left.grid(alpha=0.3)

    # -- right: the curves themselves ---------------------------------------------------
    picks = [("s01 (f=0, multiplicative)", S01_PARQUET, "tab:blue", "-"),
             ("July params (f=1, auto)", JULY_PARAMS_PARQUET, "tab:red", "-")]
    for parquet in sorted(HERE.glob(f"runs/*/{PARQUET_NAME}")):
        config = _config_of(parquet)
        if _blend_fraction(config) == 0.10:
            regime = config["seasonality_regime"]
            picks.append((f"f=0.10, {regime}", parquet, STYLE[regime][0], "--"))

    for label, parquet, color, ls in picks:
        ma, _ = curve(Path(parquet))
        right.plot(ma.index, ma.values, lw=2, color=color, ls=ls, label=label, alpha=0.9)

    right.plot([AUG25], [AUG25_TARGET], marker="*", ms=22, color="crimson", ls="none",
               zorder=6, label=f"Aug-25 target {AUG25_TARGET:,.0f}")
    right.set_xlim(pd.Timestamp("2026-06-01"), DISPLAY_END)
    right.set_ylim(43_000_000, 49_500_000)
    right.set_title("Both branches in curve space — the target falls between them",
                    fontsize=12)
    right.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
    right.legend(loc="lower right", fontsize=9)
    right.grid(alpha=0.3)

    fig.tight_layout()
    out = HERE / "plots" / "round2_blend_gap.png"
    fig.savefig(out, dpi=140, bbox_inches="tight")
    print(f"Wrote {out}")
    print(f"multiplicative floor {mult_floor:,.0f} | auto ceiling {auto_ceiling:,.0f} | "
          f"gap {mult_floor - auto_ceiling:,.0f}")
    print(f"target {AUG25_TARGET:,.0f} sits {mult_floor - AUG25_TARGET:,.0f} below the "
          f"multiplicative floor and {AUG25_TARGET - auto_ceiling:,.0f} above the auto ceiling")
    dial = (AUG25_TARGET - auto_ceiling) / (mult_floor - auto_ceiling)
    print(f"=> a continuous auto->multiplicative dial must sit ~{dial:.1%} of the way toward "
          f"multiplicative")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
