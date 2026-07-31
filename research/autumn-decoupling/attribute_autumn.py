"""Attribute the August-vs-July autumn gap to its causes.

The published chart shows August (green) sitting ~250-400K above July (blue) across October and
November. Four things changed between those two curves, and only one of them is the s01 model
retune that the summer lift came from:

  1. data refresh      07-06 seam -> 07-28 seam
  2. LOL ceiling       125K -> the cycle's intermediate ceiling
  3. headwind          -1,345,000 from 2026-04-01  ->  -1,245,000 from the seam
  4. s01 model retune  prev config -> multiplicative / cps 0.1849 / cpr 0.734 / recent 17 / ncp 35
  5. LOL ceiling       intermediate -> 200K (the locked build)

Steps 1+2, 4 and 5 are separable because all three August builds are still on disk. Step 3 is
display-layer, so it can be rendered under either convention on the same parquet.

NOTE (2026-07-30): the intermediate LOL curves were deleted and the s01 build that sits on one of them
lost its `data-official/` copy. This module now reads that build from its surviving twin under
`research/param-scans/summer-trough-v2/s01_gradient/` — same run, verified by sidecar and a hard-linked
.pkl. See `curves.py`. The ladder is therefore unchanged in substance; only rung labels stopped naming
ceilings that no longer exist as files.

Run: python research/autumn-decoupling/attribute_autumn.py
"""

from __future__ import annotations

import pandas as pd

from curves import (
    AUGUST_HEADWIND,
    BUILDS,
    JULY_HEADWIND,
    build_ma,
)

PROBE_DATES = [
    ("Aug-25 trough", pd.Timestamp("2026-08-25")),
    ("Sep-15", pd.Timestamp("2026-09-15")),
    ("Oct-15", pd.Timestamp("2026-10-15")),
    ("Nov-15", pd.Timestamp("2026-11-15")),
    ("Dec-15", pd.Timestamp("2026-12-15")),
]


def probe(series: pd.Series) -> dict[str, float]:
    return {name: float(series.get(date, float("nan"))) for name, date in PROBE_DATES}


def print_table(title: str, rows: list[tuple[str, dict[str, float]]], *, deltas: bool) -> None:
    cols = [name for name, _ in PROBE_DATES]
    width = max(len(label) for label, _ in rows) + 2
    print(f"\n{title}")
    print("-" * (width + 14 * len(cols)))
    print(" " * width + "".join(f"{c:>14}" for c in cols))
    prev = None
    for label, values in rows:
        if deltas and prev is not None:
            cells = "".join(f"{values[c] - prev[c]:>+14,.0f}" for c in cols)
        else:
            cells = "".join(f"{values[c]:>14,.0f}" for c in cols)
        print(f"{label:<{width}}{cells}")
        prev = values


def main() -> None:
    # --- Rung 0: July as delivered, on July's own headwind convention.
    july = probe(build_ma(BUILDS["july_delivered"], JULY_HEADWIND))

    # --- Rung 1: August data + the raised LOL ceiling on the PREVIOUS config, still on July's headwind.
    #     Isolates "refresh the data and raise the LOL ceiling" from everything else.
    aug_prev_julyhw = probe(build_ma(BUILDS["aug_prevconfig"], JULY_HEADWIND))

    # --- Rung 2: same build, August's headwind convention. Pure display-layer step.
    aug_prev_aughw = probe(build_ma(BUILDS["aug_prevconfig"], AUGUST_HEADWIND))

    # --- Rung 3: the s01 retune, ceiling held at the intermediate value.
    s01_prev_ceiling = probe(build_ma(BUILDS["s01_prev_ceiling"], AUGUST_HEADWIND))

    # --- Rung 4: ceiling raised to 200K. The locked build.
    locked = probe(build_ma(BUILDS["s01_200k_locked"], AUGUST_HEADWIND))

    ladder = [
        ("0. July delivered", july),
        ("1. + Aug data & LOL ceiling raise", aug_prev_julyhw),
        ("2. + Aug headwind (re-anchor & -100K)", aug_prev_aughw),
        ("3. + s01 model retune", s01_prev_ceiling),
        ("4. + LOL ceiling -> 200K  [LOCKED]", locked),
    ]

    print_table("28d-MA levels (post-headwind)", ladder, deltas=False)
    print_table("Step-by-step contribution", ladder, deltas=True)

    print("\nTotal August LOCKED minus July delivered")
    print("-" * 60)
    for name, _ in PROBE_DATES:
        print(f"  {name:<16}{locked[name] - july[name]:>+14,.0f}")

    # The question peers actually asked: of the autumn elevation, how much is the retune?
    print("\nShare of the Oct/Nov gap owned by the s01 retune")
    print("-" * 60)
    for name in ("Oct-15", "Nov-15"):
        total = locked[name] - july[name]
        retune = s01_prev_ceiling[name] - aug_prev_aughw[name]
        headwind = aug_prev_aughw[name] - aug_prev_julyhw[name]
        data_lol = aug_prev_julyhw[name] - july[name]
        ceiling = locked[name] - s01_prev_ceiling[name]
        print(f"  {name}: total {total:+,.0f}")
        print(f"      data+LOL raise   {data_lol:+,.0f}   headwind convention {headwind:+,.0f}")
        print(f"      s01 retune       {retune:+,.0f}   LOL -> 200K         {ceiling:+,.0f}")


if __name__ == "__main__":
    main()
