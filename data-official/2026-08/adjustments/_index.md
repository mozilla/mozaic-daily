# `data-official/2026-08/adjustments/` — display-layer adjustment specs (`h`)

`headwind.json` — the Windows 10 migration headwind. A `linear_ramp` **from the seam (2026-07-28)**
reaching **desktop −1,245,000 / mobile −27,162** at the 2026-12-15 anchor.

Two changes this cycle:

1. **Desktop anchor attenuated +100,000** from July's −1,345,000, in two steps on 2026-07-29
   (−1,295,000, then −1,245,000). An **alternate under evaluation**. Mobile amplitude unchanged.
2. **Ramp re-anchored to the seam** — `start_date` 2026-04-01 → 2026-07-28. A **convention change**, see
   below. Affects both platforms (one `start_date` serves both). **Zero effect on Dec-15.**

## The seam re-anchoring (2026-07-29)

The ramp is applied only from the forecast seam forward — history is actuals and must never move
retroactively. Under the old convention it *started* 2026-04-01, so on the first forecast day it switched
on at 118/258 = 45.7% of full value: a **−569,419 one-day level step** in a 28-day trailing MA, which is
physically impossible for an MA to produce on its own.

Measured in `../desktop_adjustment_ladder.ipynb`: the model output is continuous across the seam (+5,157),
and the headwind accounted for **100.9%** of the −564,262 discontinuity visible in the published charts.
No parameter value could have changed that, because the step never existed inside the model.

The convention encodes a factual claim — whether the April→July Win10 loss is already in the training
data. It is: the model was fit through 2026-07-27. So the old convention charged it twice. Re-anchoring
also makes `h` consistent with the premise used to justify attenuating the anchor ("Prophet has learned
more of the decline" and "the elapsed loss is in the data" are the same statement); previously those two
channels partly cancelled, plausibly part of why the anchor kept needing to come down.

Effect — identical at the anchor, large in the interior:

| date | old ramp | new ramp | lift |
|---|--:|--:|--:|
| 2026-07-28 (seam) | −569,419 | 0 | **+569,419** |
| 2026-08-22 | −690,058 | −222,321 | +467,737 |
| 2026-10-01 | −883,081 | −578,036 | +305,046 |
| 2026-12-15 (anchor) | −1,245,000 | −1,245,000 | **0** |

Desktop Dec-15 stayed at 48,672,970; the Aug-22 trough went 43,453,752 → 43,921,488. Mobile's seam step
went from −12,424 to 0, Dec-15 unchanged.

**June's and July's specs are deliberately untouched**, so the N-1 comparison still reproduces July's
delivered number exactly. The trade-off: between the seam and Dec-15 the cycles are on different
conventions and should not be compared point-for-point there.

⚠️ **Known inconsistency, not fixed.** Three of the four ramp implementations
(`src/mozaic_daily/adjustments.py`, the canonical notebook's helper,
`data-official/2026-06/export_canonical_curves.py`) do **not** clamp at `anchor_date` and keep
extrapolating past it; `scripts/score_near_horizon.py` does clamp. Seam-anchoring shortens the ramp from
258 to 140 days, so it is steeper and the unclamped overshoot at 2026-12-31 grows from ≈−1,322,000 to
≈−1,387,000. This affects only 2026-12-16 → 12-31 and no reported number, but it should be resolved
deliberately.

`adj-h` carries the portion of the Win10 migration decline that Prophet has *not* learned from data. As
the decline lands in actuals the exogenous anchor should shrink, or it double-counts — that is the
standing rationale, reached in June and acted on in July. Five extra weeks of training data (through
2026-07-27) motivated this step.

**The attenuation history deserves scrutiny as a run, not just per step:**

| cycle | desktop anchor | step | cumulative vs June |
|---|--:|--:|--:|
| June delivered | −1,420,000 | — | — |
| July, first pass | −1,370,000 | +50,000 | +50,000 |
| July delivered | −1,345,000 | +25,000 | +75,000 |
| August, first pass | −1,295,000 | +50,000 | +125,000 |
| **August current (alternate)** | **−1,245,000** | **+50,000** | **+175,000** |

Five values, four attenuations, each justified the same way and each raising the reported number.
Cumulatively +175,000, or 12.3% of June's value. **None is measured against a held-out estimate of how
much of the decline Prophet actually absorbed** — they are calibrated judgements. If a cycle ever *needs*
the headwind to attenuate to reach a target, that is the failure mode to catch.

**A validation analysis is queued** (`../../../research/headwinds/`, not yet written). The tractable test:
the ramp starts from zero on 2026-04-01, and training ends 2026-07-27 — 117 of 258 days, 45% along it. So
a −1,245,000 anchor asserts that **≈565,000 of transition-attributable net loss is already observable in
actuals**. That is directly measurable. Note that Win10 → Win11 migration is DAU-neutral: the headwind is
attrition, not the falling Win10 curve. As of 2026-07-27 the split was Win11 26.30M / Win10 15.66M
(`os_version` in the legacy `active_users_aggregates` separates them cleanly).

Unlike `l`/`o`/`m`, this is a **display-layer** adjustment: it is *not* baked into the forecast
parquets. It is applied to the 28-day MA in the canonical notebook (`[compute-series]`) via
`load_adjustments` + `apply_net_adjustment`. The forecast parquets in `../desktop_baseline_*` and
`../mobile_baseline_*` are **pre-headwind**.

The spec has no `applies_to_forecast_start` key — it is picked up by directory, so anything reading
`ADJUSTMENTS_DIR` gets it regardless of anchor date. That also means an empty directory would silently
yield a pre-headwind number, so `load_adjustments` raises on an empty glob rather than returning zeros.

**Why a change here needs no model re-run.** Because `h` never enters the training frame, editing this
file and re-executing the canonical notebook is the complete update path — for the amplitude *and* for
the ramp start. It also means the effect of an amplitude change on the reported Dec-15 number is *exactly*
the anchor delta, with no Prophet interaction — unlike `l`/`o`/`m`, where the bidirectional structure
makes the realised effect an empirical question.

**Where new files go:** additional display-layer adjustment specs for this cycle. They are summed, so
each file must describe a distinct effect — do not add a second file that restates the same headwind.
