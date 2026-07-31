# `experiment_july_methodology/` — counterfactual: July's methodology on August's values

**NOT WIRED, and must not be.** The canonical August curve is
`../marketing_lift_model.uac_meta_total.2026-07-28.parquet` (anchor-and-subtract). This directory
answers one question only: what would August's mobile number have been if we had kept the prior
cycle's construction?

Self-contained — the production pipeline and the canonical notebook were not modified.

## Contents

| file | role |
|---|---|
| `build_july_methodology_curve.py` | builds the counterfactual curve |
| `marketing_lift_model.july_methodology.2026-07-28.parquet` (+ `.meta.json`) | the curve, Dec-15 lift **830,360** |
| `forecast/` | mobile DAU forecast produced with that curve (`.adj-m.` + sidecar) |
| `experiment_july_methodology.ipynb` | standalone plot notebook |
| `plots/mobile_july_methodology_experiment.png` | the chart |

## The rule

July carried the prior cycle's delivered curve forward, moved only by the change in the marketing
team's own Total-Paid-DAU outlook (`data-official/2026-07/marketing/build_lift.py`). Applied one
cycle on:

    L_aug_JM(d) = L_july_delivered(d) + [aug_total(d) − jul_total(d)],   0 before campaign launch

| curve | Dec-15 lift |
|---|--:|
| August canonical (anchor-and-subtract) | 637,227 |
| July delivered | 778,880 |
| **this experiment** | **830,360** |

## Result — mobile Dec-15 2026, 28d MA, post-headwind

| series | Dec-15 | vs canonical |
|---|--:|--:|
| July 2026 delivered | 17,923,869 | +59,137 |
| August canonical | 17,864,732 | — |
| August experiment | 17,903,366 | **+38,633** |

Keeping July's methodology would have landed the August mobile KPI **+38,633 above** the canonical
build, and 20,503 *below* July's delivered number.

## Pass-through is not a constant

The two curves differ by **+193,133** at Dec-15 but the KPI moved only **+38,633** — 20%
pass-through. The earlier canonical-vs-July comparison passed through 42% (−141,653 → −59,875).

**So the absorption fraction is not a property of the overlay you can reuse.** It depends on how the
curve difference is distributed across the *training* window, not on the Dec-15 endpoint: the more
two curves differ during training, the more Prophet's re-learned trend offsets the change. Any
future estimate of "what will this curve change do to the KPI" has to be run, not scaled.

## Two properties of the methodology this makes visible

1. **The whole Meta contribution books as incremental delta.** July's source predates Meta, so
   `aug_total − jul_total` counts all of it as new. That is the rule's behaviour, not a choice.
2. **It stacks a third cycle of deltas on one measurement.** July inherits June, which rests on a
   single 45-day empirical gap ending 2026-05-20. Escaping that chain is why anchor-and-subtract was
   adopted.

It also faithfully reproduces an inherited defect: July's one-day −63,955 spike at 2026-04-06 (its
builder zeroed only *strictly* before launch) survives, one day inside the training window.

## Reproduction guard

The notebook copies the canonical notebook's series and plot logic rather than importing it, so it
cannot perturb the canonical run. Two assertions in `[compute-series]` guard that copy against
drift: July delivered must rebuild to 17,923,869 and August canonical to 17,864,732. Both currently
reproduce to **0 DAU**. If either fails, every delta here is invalid.

**Where new files go:** nothing. If this methodology is ever readopted it becomes a normal cycle
build under `data-official/{YYYY-MM}/`, not an experiment.
