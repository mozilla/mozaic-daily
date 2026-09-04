# `data-official/2026-08/tailwind/` — mobile tailwind (`t`), ADOPTED 2026-08-03

> **ADOPTED AND LIVE.** The spec now lives at `../adjustments/tailwind.json` and is summed into the
> published curves by the canonical notebook. This directory holds the rationale, not the spec.

This is the **rationale record** for adjustment code `t`. Read it before quoting the published
August mobile number, which is **1.67%** this overlay — and which was **sized to land within 1,000 DAU
of July's delivered figure**.

## Where things are

| | |
|---|---|
| spec (live) | `../adjustments/tailwind.json` |
| code registration | `../../adjustment_codes.yaml`, code `t` |
| rationale (this file) | `_index.md` |
| sizing script + chart | `../../../research/param-scans/mobile-aug/tailwind_exercise.py`, `plots/tailwind_276k_exercise.png` |
| search evidence | `../../../research/param-scans/mobile-aug/` |

**`adjustments/` is live by presence.** `load_adjustments()` does `glob("*.json")` and sums every
spec it finds — there is no date gate and no enable flag at the display layer. Adding or removing a
file there changes the published numbers immediately. That is why this spec sat here until it was
deliberately adopted, and why it must not be edited casually now that it is in place.

## The spec

Linear ramp, **0 at the seam (2026-08-02, moved with the 2026-08-03 data refresh) → **+299,000** at the
2026-12-15 anchor**, mobile only,
desktop 0. Combined with `h` (mobile −27,162), the **net mobile display adjustment at Dec-15 is
+248,838**.

**Sign.** Positive. This is a *tailwind*; `h` is a headwind and its `mobile_dau` is **−27,162**. It is
registered as its own code `t` rather than an edit to `headwind.json`, so that an upward judgement
cannot hide inside the headwind line.

**Ramp start.** The seam, not the 2026-08-03 request date. This cycle deliberately re-anchored the
`h` ramp to the seam to remove a seam discontinuity (`../adjustments/_index.md`); starting six days
later would reintroduce a small kink. Dec-15 is unaffected either way — the ramp is anchored there.

## What it produces (as published)

On the refreshed `mobile_cpr0725_2026-08-02` base (the 2026-08-03 data refresh moved the base +500):

| | |
|---|--:|
| base, post-headwind | 17,625,562 |
| + tailwind at Dec-15 | **+299,000** |
| **result** | **17,924,562** |
| target | 17,923,869 ±50,000 |
| **vs target** | **+693 — essentially exact** |

ALL (desktop at 48,703,443): **66,628,005**, which is **+118,653** vs July's ALL of 66,509,352.

## What +276,000 means

| | |
|---|--:|
| share of the Dec-15 mobile total | **1.67%** |
| Dec-15 YoY | 12.33% → **14.24%** (measured organic rate **11.60%**) |
| as a fraction of the entire paid level (1,559,477) | 19.2% |
| ramp slope | 2,215 DAU/day (135-day ramp from the 08-02 seam) |

## Evidence base — this is the part to argue about

| basis | amount | share |
|---|--:|--:|
| Independent implementation (`mobile_organic_aug` prototype) lands this much higher | +141,637 | **47%** |
| Planning judgement, unattributed | +134,363 | 45% |
| **Calibration to July's published Dec-15** (the 2026-08-03 raise) | **+23,000** | **8%** |

⚠️ **The last +23,000 has no evidence behind it other than the target.** It was added so published
mobile lands **+693** from July's delivered 17,923,869 — i.e. sized to make mobile read "flat versus
July". That is a legitimate planning choice, but it is *calibration to a prior published number* and must
never be described as a measurement. It also dilutes the evidence-backed share from ~51% to ~47%.

The prototype half is real evidence: same parameters, same data, same paid curve, a 0.77% MAPE
180-day backtest, and a **−1.4% recent bias** (it has been running *low* against May–July actuals).
Its excess is fully attributed in `../../../research/param-scans/mobile-aug/PROTOTYPE_COMPARISON.md`.

Two identified production defects both bias **downward**, which is directionally consistent with a
positive tailwind, but **neither is measured** and neither can be claimed as a quantity:

1. **Untreated Farsi-locale shutdown craters.** mozaic's Iran fill covers `country='IR'` only. An
   89,677 DAU/day population that collapses in lockstep with IR during every shutdown sits in ROW
   country tiles with no mask and no fill, so ordinary ROW tiles train on cratered series.
2. **Backfilled Fenix organic share.** 63% of the headline's training window carries organic values
   synthesised from a share held flat for 3.5 years before the mirror's 2024-06-01 start.

**The honest framing, now that it has shipped:** ~47% of it is an independent-estimate correction, ~45% is
a planning decision, and the final 8% exists to hit a previously published number. It is the **third**
discretionary upward lever this cycle, after the LOL ceiling (+77,604) and the Win10 anchor (now +30,000,
cut from +125,000 across 2026-08-03 and 08-04) — **+406,604 of discretionary uplift in total**, against data refreshes that moved desktop −64,769 then
+100,840, and mobile +500.

## Reproduce

```bash
source .venv/bin/activate
python research/param-scans/mobile-aug/tailwind_exercise.py
```

Chart: `research/param-scans/mobile-aug/plots/tailwind_276k_exercise.png`.

## Adoption record (2026-08-03) — all four steps done

1. ✅ Code `t` (`mobile_tailwind`) registered in `../../adjustment_codes.yaml`. **Separate from `h`
   on purpose** — folding an upward judgement into the headwind line would hide it from the ledger.
2. ✅ Spec moved to `../adjustments/tailwind.json`.
3. ✅ Canonical notebook repointed at `mobile_cpr0725_2026-07-28/`, with
   `EXPECTED_MOBILE_CONFIG.prophet_changepoint_range` 0.75 → 0.725 and `ORGANIC_MOBILE_DEC15`
   re-pinned (now **17,924,562** after the 299,000 raise). `[mobile-dec15]` now prints an explicit decomposition
   (base → re-lock → tailwind → published) and **asserts it closes**, so the discretionary share
   cannot become implicit.
4. ✅ Attribution ledger and caveats cell updated; the caveats now open with this overlay.

## To revert

One file: `rm ../adjustments/tailwind.json`, then re-execute the notebook and re-pin
`ORGANIC_MOBILE_DEC15 = 17_625_562`. To revert **only the calibration increment**, set `mobile_dau` back
to 276000 and re-pin `17_901_562`. No model re-run — `t` is display-layer, so its Dec-15 effect is
exactly its anchor with no Prophet interaction. Nothing else co-changed with it.
