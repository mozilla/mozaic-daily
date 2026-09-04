# `data-official/2026-08/adjustments/` — display-layer adjustment specs (`h` + `t`)

## ⚠️ TWO specs live here as of 2026-08-03, and this directory is LIVE BY PRESENCE

`load_adjustments()` does `glob("*.json")` and **sums every spec it finds**. There is no date gate
and no enable flag at the display layer, so adding or removing a file here changes the published
numbers immediately, with no code change to review.

| spec | code | desktop @ Dec-15 | mobile @ Dec-15 |
|---|---|--:|--:|
| `headwind.json` | `h` | **−1,315,000** | −27,162 |
| `tailwind.json` | `t` | 0 | **+299,000** |
| **net** | | **−1,315,000** | **+271,838** |

`tailwind.json` is **new and discretionary** — a mobile calibration overlay, **~47%** backed by an
independent implementation and ~53% planning judgement. Raised 276,000 → **299,000** on 2026-08-03 so
published mobile lands within 1,000 DAU of July's delivered number; that last +23,000 is calibration to
a prior published figure, not a measurement. It is deliberately a separate spec from
`headwind.json` so it lands on the attribution ledger under its own name. Rationale and revert
instructions: `../tailwind/_index.md`.

`headwind.json` — the Windows 10 migration headwind. A `linear_ramp` **from the seam (2026-08-02,
moved with the 2026-08-03 data refresh — a 135-day ramp, was 140)** reaching **desktop −1,315,000 /
mobile −27,162** at the 2026-12-15 anchor.

Two changes this cycle:

1. **Desktop anchor now attenuated +30,000 from July's −1,345,000, i.e. −1,315,000.** It went
   −1,295,000 → −1,245,000 (2026-07-29) → −1,220,000 (2026-07-30) → back to −1,295,000 (2026-08-03) →
   −1,314,000 → **−1,315,000 (both 2026-08-04)**. Mobile amplitude unchanged throughout.

   The **2026-08-03** move was the first DOWNWARD step in the run — the anchor had been attenuated five
   times consecutively, every step raising the reported number. It gave back 75,000 of the +100,840 the
   2026-08-02 data refresh added.

   The **2026-08-04** moves continued in the same direction: 19,000 more headwind, then a further 1,000,
   taking desktop Dec-15 to **48,703,443** and the August-vs-July gap to **+117,960**. Together the
   reversals give back **95,000** of the refresh's +100,840, so the published desktop delta now retains
   only **+5,840** of it.

   **The anchor is now BELOW its July delivered value net of only +30,000**, and below the −1,295,000
   first-pass value. This is the first time in the cycle the headwind has been made *more* severe than
   any value August previously used.

   ⚠️ **This lever has now been moved four times in 48 hours** (−1,220,000 → −1,295,000 → −1,276,000 →
   −1,314,000 → −1,315,000). The final 1,000 step is ~0.002% of desktop DAU — three orders of magnitude
   below the ±21,705 the validation found indistinguishable. Steps at that granularity are dialling in a
   presentation number, not calibrating a model input, and the record should not imply otherwise.

   *(Auditability: the 08-04 changes landed in three steps the same day — first −1,276,000, which was the
   wrong direction, corrected by −38,000 to −1,314,000, then −1,000 more. Nothing was distributed at
   −1,276,000.)*

   ⚠️ **This broke a documented coupling.** The +25,000 step (−1,245,000 → −1,220,000) was applied as
   one unit with the **g01** model retune, to absorb most of its −31,357. The headwind has now moved
   without g01 moving, so that pairing no longer holds and
   `../desktop_s01_REVERT_2026-07-29/REVERT.md` **must not be followed as written** — see the warning
   added at the top of it.
2. **Ramp re-anchored to the seam** — `start_date` 2026-04-01 → 2026-07-28, then → **2026-08-02** when
   the data refreshed on 2026-08-03. A **convention change**, see below. Affects both platforms (one
   `start_date` serves both). **Zero effect on Dec-15** either time — the ramp is anchored there.
   It does move the interior, and one consequence needs a decision: the summer trough now falls inside
   the `display_ma` splice zone (see `../_index.md` § "Trough is now convention-dependent").

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

Effect — identical at the anchor, large in the interior. **Measured on 2026-07-29, when the desktop
anchor was −1,245,000 and the model was still on the previous config**; the shape of the finding is
what carries forward, not the absolute levels (the anchor is now −1,315,000 and the model is g01):

| date | old ramp | new ramp | lift |
|---|--:|--:|--:|
| 2026-07-28 (seam) | −569,419 | 0 | **+569,419** |
| 2026-08-22 | −690,058 | −222,321 | +467,737 |
| 2026-10-01 | −883,081 | −578,036 | +305,046 |
| 2026-12-15 (anchor) | −1,245,000 | −1,245,000 | **0** |

On that build desktop Dec-15 stayed at 48,672,970 and the Aug-22 trough went 43,453,752 → 43,921,488;
mobile's seam step went from −12,424 to 0, Dec-15 unchanged. **The zero-at-Dec-15 result is exact and
convention-independent** — both conventions terminate on the same anchor — so it still holds on the
current build.

**June's and July's specs are deliberately untouched**, so the N-1 comparison still reproduces July's
delivered number exactly. The trade-off: between the seam and Dec-15 the cycles are on different
conventions and should not be compared point-for-point there.

⚠️ **Known inconsistency, not fixed.** There are now **five** `linear_ramp` implementations and they
disagree past `anchor_date`:

| implementation | clamps at anchor? |
|---|---|
| `src/mozaic_daily/adjustments.py` (`render_adjustment`) | **no** |
| the canonical notebook's `[helpers]` copy | **no** |
| `data-official/2026-06/export_canonical_curves.py` (frozen) | **no** |
| `scripts/score_near_horizon.py` (`_headwind_ramp`) | yes |
| `scripts/mobile_scoring.py` (`headwind_ramp`) | yes |

*2026-09-04:* `scripts/export_desktop_no_headwind_csv.py` (and `export_desktop_ex_ir_cn_csv.py` through it) now render via the package `render_adjustment` instead of a sixth copy, so they inherit the **no** row.

Seam-anchoring shortens the ramp 258 → 140 → 135 days, so it is steeper and the unclamped overshoot past
the anchor is larger. At 2026-12-31 (**151/135 = 1.1185×** the anchor, since the ramp start moved to
2026-08-02) the unclamped implementations give:

| | anchor (intended) | unclamped at 2026-12-31 |
|---|--:|--:|
| desktop `h` | −1,315,000 | −1,470,852 |
| mobile `h` + `t` net | +271,838 | +304,056 |

*(Corrected 2026-08-05. This table previously read **156/135 = 1.156×**, −1,519,556 and +314,124. When
`start_date` moved 2026-07-28 → 2026-08-02 the denominator was updated 140 → 135 but the numerator was
left at 156, which is the day count from the **old** ramp start to 12-31; from 2026-08-02 it is 151. The
error overstated the overshoot by 48,704 desktop / 10,068 mobile. It affected **no reported number** —
every headline is Dec-15 or earlier, where both conventions terminate on the anchor exactly — and no
published curve, since this table only ever described the defect. Caught by
`tests/test_export_desktop_no_headwind_csv.py::test_overshoots_past_anchor_and_is_not_clamped`, which
locks the live implementation's value.)*

This affects only 2026-12-16 → 12-31 and **no reported number** (every headline is Dec-15 or earlier), but
it should be resolved deliberately. Note that adopting `t` extended the defect to a *positive* overlay:
the mobile curve now over-shoots **upward** past the anchor, where before it only over-shot downward.

Two further divergences between the package and the notebook copy, same family:

- `load_adjustments_from_dir()` in the package **returns zeros for an empty directory**; the notebook's
  `load_adjustments()` **raises `FileNotFoundError`**. The notebook's behaviour is the safe one — an empty
  dir would otherwise silently publish a pre-headwind number — and it is what the guarantee below refers to.
- The notebook's `render_adjustment` copy has no `else: raise` branch, so an unknown `type` silently
  returns zeros instead of failing loudly as the package version does.

Both exist because the notebook duplicates the package helpers rather than importing them. Consolidating
is the right fix; it was not done here because it changes a cycle notebook that is already executed and
published.

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
| August, second pass | −1,245,000 | +50,000 | +175,000 |
| August, third pass (with the g01 retune) | −1,220,000 | +25,000 | +200,000 |
| August, fourth pass (2026-08-03, after the data refresh) | −1,295,000 | **−75,000** | +125,000 |
| August, fifth pass (2026-08-04) | −1,314,000 | **−19,000** | +106,000 |
| **August LIVE** (2026-08-04) | **−1,315,000** | **−1,000** | **+105,000** |

Nine values. Five consecutive attenuations, then **three consecutive reversals** (−75,000, −19,000,
−1,000).

Cumulative attenuation vs June is now **+105,000**, or 7.4% of June's value, against a peak of
+200,000 (14.1%). The anchor sits **95,000 below its peak attenuation** and is the most severe value
August has used. Note the step sizes are now decreasing geometrically — 75,000, 19,000, 1,000 — which
is the signature of converging on a target value rather than of successive re-estimation. **None of these is measured against a held-out estimate of how much of the decline
Prophet actually absorbed** — they are calibrated judgements, and the validation below found the
candidates indistinguishable in telemetry.

The stated failure mode was "if a cycle ever *needs* the headwind to attenuate to reach a target."
Two things about the 2026-08-03 reversal are worth recording against that:

- **It moved the number DOWN**, which is the opposite of the pattern that made the run suspicious. It
  was taken because the data refresh added +100,840 and the resulting +212,960 August-vs-July gap was
  judged too large, not because a target needed hitting from below.
- **But it is still a discretionary lever pulled to land a gap**, and it now cancels almost all of a
  *measured* data-refresh gain. The refresh said +100,840; after the −75,000 and −19,000 reversals the
  published desktop delta keeps only **+5,840** of it. That is a defensible smoothing choice and it is
  *not* a data finding — do not describe the **+117,960** as what the refresh produced.
- **The 2026-08-04 moves have no stated rationale of their own in this record.** They were requested as
  a flat 19,000 and then 1,000 of additional headwind; unlike the 08-03 reversal (which was explicitly
  sized against the refresh gain) there is no measurement or target behind either magnitude. If they
  need defending later, that will have to be supplied.
- **Net effect of the cycle's headwind decisions is now to erase the data refresh almost exactly.** The
  three reversals together (−95,000) are within 5,840 of the refresh's +100,840. Whether that is prudence
  or anchoring on the pre-refresh number is a judgement worth stating explicitly when this is reported —
  and the decreasing step sizes make the second reading harder to rule out.

The third-pass +25,000 was originally sized to offset the g01 retune's −31,357, so a model change and a
headwind change cancelled on the headline; the reversal has now unwound that pairing (see change 1 above).

**The validation analysis has been done** — `../../../research/headwinds/WIN10_ANCHOR_FINDINGS.md`
(read-only: no specs changed, no forecasts re-run). Two verdicts, and both matter here:

1. **Telemetry cannot distinguish the candidate anchors.** Over the ramp's elapsed portion the live
   candidates span 21,705 DAU against a specification envelope **1,488,293** wide — ~69:1 noise to
   signal. Attenuating by another 25,000 is neither supported nor refuted by data. It is a judgement
   call and must be labelled as one, not presented as data-driven.
2. **The April-anchored ramp's elapsed portion was contradicted.** −1,245,000 required ≈540,000 of
   transition-attributable net loss to have accrued by 2026-07-22; **not one of 90 specification
   variants reached it** (most pessimistic −388,058, median mildly *positive* at +134,926, central
   estimate ≈0). The conclusion was not "shave another 25,000" but that the
   *linear-ramp-from-2026-04-01 parameterisation was the wrong shape* — it front-loaded loss that had
   not happened.

**The seam re-anchoring (below) is the fix for finding 2**, and it removes the arithmetic that the
earlier version of this section used. Under the new convention the ramp asserts **zero** un-absorbed
loss at 2026-07-28, so there is no longer an elapsed portion to contradict. What remains is a purely
forward claim:

> Between the seam and 2026-12-15, **1,220,000** of Win10-attributable net attrition will occur that
> Prophet's fitted trend does not already extrapolate.

That is an **unfalsifiable forward judgement** against current data, not a measured quantity — which is
exactly what finding 1 says about its magnitude. Treat it accordingly when quoting desktop.

Note that Win10 → Win11 migration is DAU-neutral: the headwind is attrition, not the falling Win10 curve.
Measured on Win10 + Win11 combined, Apr-1 → Jul-22 ex-IR/CN, Win10 alone shed −1,814,214 while Win11 —
the supposed destination — shed −957,418, because summer drags on everything. Reading the Win10 curve as
the headwind would overstate it several-fold. As of 2026-07-27 the split was Win11 26.30M / Win10 15.66M
(`os_version` in the legacy `active_users_aggregates` separates them cleanly).

Unlike `l`/`o`/`p`, both specs here are **display-layer** adjustments: they are *not* baked into the
forecast parquets. They are applied to the 28-day MA in the canonical notebook (`[compute-series]`) via
`load_adjustments` + `apply_net_adjustment`. Every forecast parquet in this cycle — including the
canonical `../desktop_locked/` and `../mobile_cpr0725_2026-07-28/` — is **pre-headwind and
pre-tailwind**.

Neither spec has an `applies_to_forecast_start` key — they are picked up by directory, so anything reading
`ADJUSTMENTS_DIR` gets them regardless of anchor date. That also means an empty directory would silently
yield a pre-headwind number, so the notebook's `load_adjustments` raises `FileNotFoundError` on an empty
glob rather than returning zeros. **The package's `load_adjustments_from_dir` does not** — see the
divergence note above; do not rely on the guard when calling the package function from a script.

**Why a change here needs no model re-run.** Because neither `h` nor `t` ever enters the training frame,
editing a spec here and re-executing the canonical notebook is the complete update path — for the
amplitude *and* for the ramp start. It also means the effect of an amplitude change on the reported
Dec-15 number is *exactly* the anchor delta, with no Prophet interaction — unlike `l`/`o`, where the
bidirectional structure makes the realised effect an empirical question, or `p`, where the pre-mozaic
split changes what the model fits.

**Where new files go:** additional display-layer adjustment specs for this cycle. They are summed, so
each file must describe a distinct effect — do not add a second file that restates the same headwind.
