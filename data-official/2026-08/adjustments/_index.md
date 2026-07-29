# `data-official/2026-08/adjustments/` — display-layer adjustment specs (`h`)

`headwind.json` — the Windows 10 migration headwind. A `linear_ramp` from 2026-04-01 reaching
**desktop −1,295,000 / mobile −27,162** at the 2026-12-15 anchor.

**Desktop anchor attenuated +50,000 on 2026-07-29** (July's was −1,345,000). Mobile unchanged. The
`start_date` / `anchor_date` / `type` fields are still July's.

`adj-h` carries the portion of the Win10 migration decline that Prophet has *not* learned from data. As
the decline lands in actuals the exogenous anchor should shrink, or it double-counts — that is the
standing rationale, reached in June and acted on in July. Five extra weeks of training data (through
2026-07-27) motivated this step.

**The attenuation history deserves scrutiny as a run, not just per step:**

| cycle | desktop anchor | step |
|---|--:|--:|
| June delivered | −1,420,000 | — |
| July, first pass | −1,370,000 | +50,000 |
| July delivered | −1,345,000 | +25,000 |
| **August current** | **−1,295,000** | **+50,000** |

Four values, three attenuations, each justified the same way and each raising the reported number. None
is measured against a held-out estimate of how much of the decline Prophet actually absorbed — they are
calibrated judgements. A validation pass against realised Win10-cohort DAU is worth doing before the
next step, and if a cycle ever *needs* the headwind to attenuate to reach a target, that is the failure
mode to catch.

Unlike `l`/`o`/`m`, this is a **display-layer** adjustment: it is *not* baked into the forecast
parquets. It is applied to the 28-day MA in the canonical notebook (`[compute-series]`) via
`load_adjustments` + `apply_net_adjustment`. The forecast parquets in `../desktop_baseline_*` and
`../mobile_baseline_*` are **pre-headwind**.

The spec has no `applies_to_forecast_start` key — it is picked up by directory, so anything reading
`ADJUSTMENTS_DIR` gets it regardless of anchor date. That also means an empty directory would silently
yield a pre-headwind number, so `load_adjustments` raises on an empty glob rather than returning zeros.

**Why a change here needs no model re-run.** Because `h` never enters the training frame, editing this
file and re-executing the canonical notebook is the complete update path. It also means the effect on
the reported Dec-15 number is *exactly* the anchor delta (+50,000 this time), with no Prophet
interaction — unlike `l`/`o`/`m`, where the bidirectional structure makes the realised effect an
empirical question.

**Where new files go:** additional display-layer adjustment specs for this cycle. They are summed, so
each file must describe a distinct effect — do not add a second file that restates the same headwind.
