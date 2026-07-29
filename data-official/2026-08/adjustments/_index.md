# `data-official/2026-08/adjustments/` — display-layer adjustment specs (`h`)

`headwind.json` — the Windows 10 migration headwind. A `linear_ramp` from 2026-04-01 reaching
**desktop −1,245,000 / mobile −27,162** at the 2026-12-15 anchor.

**Desktop anchor attenuated +100,000 from July's −1,345,000**, in two steps on 2026-07-29 (−1,295,000,
then −1,245,000). The current value is an **alternate under evaluation**. Mobile unchanged. The
`start_date` / `anchor_date` / `type` fields are still July's.

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
file and re-executing the canonical notebook is the complete update path. It also means the effect on
the reported Dec-15 number is *exactly* the anchor delta (+50,000 this time), with no Prophet
interaction — unlike `l`/`o`/`m`, where the bidirectional structure makes the realised effect an
empirical question.

**Where new files go:** additional display-layer adjustment specs for this cycle. They are summed, so
each file must describe a distinct effect — do not add a second file that restates the same headwind.
