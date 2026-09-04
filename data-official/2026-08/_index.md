# `data-official/2026-08/` — August 2026 forecast cycle

Active cycle (branch `august-forecast`, off `clean-slate`).

## Status: DATA-REFRESHED to the **2026-08-02** seam · desktop **g01**, headwind **−1,315,000** · mobile **`p` + cpr 0.725 + `t`**

> **DATA REFRESH, 2026-08-03 — this is the live build.** Both platforms rebuilt at forecast_start
> **2026-08-02** (trained through **2026-08-01**), replacing the 2026-07-28 builds. **Nothing else
> changed:** desktop stayed on **g01**, mobile on **cpr 0.725**, and the `l` / `o` / marketing curves
> were carried forward unchanged, so the move is **pure fresher data**. The measured Fenix
> paid/organic split feeding `p` *was* rebuilt for the new training window (all four producer checks
> pass; shredder-drift trailing edge **0.0000%** against the new production pull). The `h` and `t`
> ramp starts moved with the seam (2026-07-28 → 2026-08-02, a **135-day** ramp, was 140), which
> leaves Dec-15 untouched because both ramps are anchored there.
>
> | Dec-15 28d-MA | 07-28 build | 08-02 refresh | data effect |
> |---|--:|--:|--:|
> | Desktop | 48,697,603 | 48,798,443 | **+100,840** |
> | Mobile | 17,901,062 | **17,901,562** | **+500** |
> | **ALL** | 66,598,665 | 66,700,005 | **+101,340** |
>
> ⚠️ **The desktop and ALL columns above are the refresh measured in isolation, at the −1,220,000
> anchor that was live at the time — they are NOT the published numbers.** On 2026-08-03 the headwind
> anchor was moved to −1,295,000 (giving back 75,000 of that gain) and then to −1,315,000 across two
> steps on 2026-08-04, a net −95,000 versus the refresh-era anchor — so the published delta retains only
> **+5,840** of the refresh. **Published: desktop 48,703,443 ·
> mobile 17,924,562 · ALL 66,628,005.** See the Dec-15 table below.
>
> **The asymmetry is the finding**: five extra days moved desktop +100,840 and mobile +500. Note the
> *previous* refresh (07-06 → 07-28) moved desktop **−64,769** — the sign flipped, so "the data says
> down" is no longer true of desktop this cycle. The 07-28 builds stay on disk as the comparison base.
>
> ⚠️ **One side effect needs a decision: the summer trough is now inside the splice zone.** See
> "Trough is now convention-dependent" below.

> **MOBILE METHODOLOGY CHANGED 2026-07-31.** The `m` marketing-lift overlay is retired; mobile now
> uses **`p` (`paid_organic_split`)** — mozaic forecasts **organic** DAU and marketing's paid curve
> is stacked on as a **level**. Mobile Dec-15 **17,864,732 → 17,601,155 (−263,577)**. Mobile
> parameters are unchanged; desktop is byte-identical (0 delta on all 365 curve days).
> Revert kit: `mobile_adjm_REVERT_2026-07-31/REVERT.md` — **three co-changed inputs, one unit.**
> Method: `organic/_index.md`. Evidence + reproduction: `../../research/mobile-organic/_index.md`.

> **s01 is preserved as a REVERT TARGET at `desktop_s01_REVERT_2026-07-29/`.** The 2026-07-30 swap
> to g01 changed the model config **and** the headwind anchor (+25,000) as one unit; reverting means
> undoing both. See that directory's `REVERT.md`. **Do not delete it while August is the live cycle.**

Current build is at forecast_start **2026-08-02** (trained through 2026-08-01). Seven changes from
July — five desktop, two mobile (`m` → `p` plus the cpr 0.725 re-lock), and the data refresh on top:
the **desktop model retuned — first to s01, then on 2026-07-30 to g01** (`regime=multiplicative, cps=0.1649,
cpr=0.814, recent=17, ncp=40`; s01 was `cps=0.1849,
cpr=0.734, recent=17, ncp=35`), the **launch-on-login (`l`) ceiling raised 125,000 → 200,000 DAU/day**,
the **Win10 headwind desktop anchor net-attenuated +30,000 to −1,315,000** (it went to −1,220,000, was
moved back to −1,295,000 on 2026-08-03, then to −1,315,000 on 2026-08-04), and the **headwind ramp
re-anchored to start at the seam** instead of 2026-04-01. Mobile keeps July's PARAMETER lock but
changes its paid methodology. `o` is an unchanged stale carry-forward from July.

**The s01 retune is the largest *trough* mover of the cycle** (it is not the largest Dec-15 mover — the
headwind attenuation, the LOL raise and the 2026-08-03 data refresh are all larger): summer trough
**+1,359,887** for **+5,642** at Dec-15 (11% of the ±50,000 budget). Canonical build:
`desktop_g01_2026-08-02/`. Delta evidence:
`../../research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb`. Holiday parameters were
excluded from the search on principle — strictly local effects must not be used to move a whole-season
quantity — and the notebook asserts all four are at defaults.

**The headwind seam step is fixed; a smaller display artifact remains.** The original −564,262 drop was
100.9% the headwind switching on at 45.7% of its ramp. Re-anchoring removed it and lifted the near term
without moving Dec-15 at all — the ramp now contributes exactly 0 at the seam. See
`adjustments/_index.md` and `desktop_adjustment_ladder.ipynb`.

**The second, smaller seam artifact is also now FIXED (2026-07-29).** It was a **+102,595 upward** step
from `reconstruct_matched_daily`, which deseasonalized with a 7-day centered mean computed on the forecast
only — at the seam that degenerates to a weekday-only forward window (`min_periods=4`) and read ~10% high,
then got multiplied by the day-of-week factor on top. `Fix A` divides by the forecast's own day-of-week
profile *before* smoothing, so window composition stops mattering. The fixed implementation lives in
`src/mozaic_daily/seam_ma.py`; `data-official/2026-06/export_canonical_curves.py` is untouched so past
cycles cannot move, and code still bound to it is in `_archive/`.

On this build the residual display distortion at the seam is **+102 DAU** (was 211,480), and the curve now
steps **−107,445** — essentially the model's own plain 28d-MA step of −107,547. Dec-15 and everything from
seam+27 onward were byte-identical before and after the fix. The seam-kink figures were re-measured as a
result: s01 **−19,702/day vs −74,237** (pre-fix they read −20,604 vs −72,593). See
`research/ma-seam-turbulence/LOG.md` § Fix A, `seam_step_diagnosis.ipynb`, and
`seam_fix_before_after.ipynb` for the verification.

**Dec-15 2026 28d-MA (headwind applied):**

| platform | Aug current | Jul delivered | delta |
|---|--:|--:|--:|
| Desktop | 48,703,443 | 48,585,483 | +117,960 (+0.24%) |
| Mobile | **17,924,562** | 17,923,869 | **+693 (+0.00%)** |
| **ALL** | **66,628,005** | **66,509,352** | **+118,653 (+0.18%)** |

Mobile is **17,625,562 model + 299,000 discretionary `t` tailwind**. The pre-tailwind, pre-re-lock,
pre-refresh `p` value was 17,601,155.

Mobile's move is the `m` → `p` methodology change, not a change of view on paid acquisition:
marketing's curve is unchanged and `p` consumes that very curve. `m`'s lift is identically zero
before its 2026-03-30 anchor, so Prophet was handed a training series that still contained all the
paid growth from 2024-06 on and extrapolated it as *organic* — then the add-back layered paid on
again. Total mobile DAU grew **+16.12%/yr** over the training window; organic grew **+11.60%/yr**.
The near term moves the other way (**+39,957** at Aug-25) because `p` restores the full measured
paid to recent training rows; it is the slope that flattens.

Superseded mobile values, for reading older text in this file: `m` UAC+Meta **17,864,732**,
`m` July-carried-forward **17,924,607**, and ALL **66,562,335** / **66,622,210** respectively.

**Summer trough minima** (28d-MA, post-adjustment) as exported to `csv/august_dec15_summary.csv`:

| series | trough min | trough date |
|---|--:|---|
| Desktop | **45,220,838** | 2026-08-24 |
| Mobile | 17,063,631 | 2026-08-16 |
| ALL | 62,331,979 | 2026-08-24 |

Aug-22 values, for continuity with earlier builds: Desktop **45,269,694** · Mobile 17,105,137 ·
ALL 62,374,831. All figures above are read from the published CSVs, which are the ground truth for
anything quotable; note the mobile and ALL numbers include the display-layer `t` tailwind.

### ⚠️ Trough is now convention-dependent — needs a decision

**The 2026-08-02 refresh moved the seam, and the trough went with it into the `display_ma` splice
zone.** The transition now spans **2026-08-02 → 2026-08-29**, so the first date whose 28-day window is
entirely forecast — and therefore immune to the splice convention — is **2026-08-30**. The trough
minimum falls on **2026-08-24**, *inside* the zone:

| desktop near-horizon | `display_ma` (published) | plain `rolling(28)` | gap |
|---|--:|--:|--:|
| trough minimum | **45,220,838** (08-24) | 45,285,321 (08-25) | **−64,483** |
| 2026-08-30 (seam+28) | 45,312,866 | 45,312,866 | **0** |
| Dec-15 | 48,703,443 | 48,703,443 | **0** |

This directly violates the rule this cycle adopted for quotable near-horizon numbers: at the 07-28
seam, Aug-25 was chosen over Aug-22 *precisely because* it sat a full window past the seam and so was
convention-independent. That property has now been lost — the published `summer_trough_min` is
**−64,483 sensitive** to a display choice. (Note the two conventions also trough on *different dates*,
08-24 vs 08-25, so this gap is not exactly anchor-independent and must be re-measured after any spec
change rather than carried forward.)

Dec-15 and everything from seam+27 onward are unaffected (both gaps above are exactly 0), so **no
headline number is in question.** But the trough is the KPI the desktop retunes were adopted to move,
so it should not be quoted as a measured minimum until this is settled. The obvious fix is to score at
**2026-08-30** (45,312,866) — the new splice-immune date — rather than at the raw minimum. **Not
applied: that changes a scored KPI definition and is a judgement call.**

Also note the trough is **not** comparable to the 07-28 build's 45,041,389: the seam moved, the ramp
start moved with it (which lifts the interior), and the data refreshed. Those three effects are mixed
in the +194,930 difference and were not separated.

### Attribution ledger

Six changes separate July's delivered number from this build, and **all six are attributed**:

| desktop Dec-15 28d-MA | step | running |
|---|--:|--:|
| July delivered (125K LOL, hw −1,345,000, 07-06 anchor) | — | 48,585,483 |
| + data refresh to the 07-28 anchor | −64,769 | 48,520,714 |
| + LOL ceiling 125,000 → 200,000 | +77,604 | 48,598,318 |
| + Win10 headwind −1,345,000 → **−1,315,000** (net, after three reversals on 08-03 and 08-04) | **+30,000** | 48,628,318 |
| + desktop model retune to s01 | +5,642 | 48,633,960 |
| + desktop model retune s01 → g01 (measured, config-isolated) | −31,357 | 48,602,603 |
| **+ data refresh 07-28 → 08-02 anchor (measured, config-isolated)** | **+100,840** | **48,703,443** |

**Mobile ledger** (added 2026-08-03) — three changes from July's delivered 17,923,869:

| mobile Dec-15 28d-MA | step | running |
|---|--:|--:|
| July delivered (`m`, carried forward) | — | 17,923,869 |
| + data refresh + `m` → `p` methodology swap | −322,714 | 17,601,155 |
| + model re-lock, cpr 0.75 → 0.725 (measured, config-isolated) | +23,907 | 17,625,062 |
| + data refresh 07-28 → 08-02 anchor (measured, config-isolated) | +500 | 17,625,562 |
| **+ `t` discretionary tailwind (display layer, exact)** | **+299,000** | **17,924,562** |

The notebook's `[mobile-dec15]` prints this decomposition and **asserts it closes**, so the
discretionary share can never become implicit.

The headwind line now carries **every** anchor move (−1,345,000 → −1,295,000 → −1,245,000 → −1,220,000,
back to −1,295,000 on 2026-08-03, then −1,315,000 on 2026-08-04), collapsed to a single **+30,000**
ledger row. The row is computed live from the specs rather than pinned, which is why it tracks these
edits automatically while the rest of the chain stays pinned.

One historical note that no longer holds as a live coupling: the +25,000 step (−1,245,000 → −1,220,000)
was originally applied *with* the g01 retune, to absorb most of its −31,357. The anchor has since moved
twice for unrelated reasons, so that pairing is broken — see `adjustments/_index.md` and the warning at
the top of `desktop_s01_REVERT_2026-07-29/REVERT.md`. The notebook asserts the chain closes; it
currently closes to **−0**.

⚠️ **The LOL step was two steps until 2026-07-30.** The ceiling was raised in stages
(125,000 → 180,000 = +52,256, then 180,000 → 200,000 = +25,348), each differenced against its own frozen
build. The intermediate curves and the `desktop_superseded_lol180k_2026-07-28/` comparison build were
deleted at the user's instruction, so the two steps are **merged into one +77,604 line** here. The total
is unchanged and the chain still closes.

**The split is still reproducible, just not from `data-official/`.** The deleted 180K build was the same
run as `research/param-scans/summer-trough-v2/s01_gradient/cps0.1849_thresh032_recent17_cpr0.734_ncp35_clip0.6_sps0.00825_regimemultiplicative/`
— verified identical sidecar (same `model_config`, same `adjustments_applied` incl. the `l` spec sha1
`e23a6267`, same commit) and the two directories' `.pkl` files were hard links to one inode. So
differencing that parquet against `desktop_locked/` still yields +25,348, and against
`desktop_baseline_2026-07-28/` still yields the +5,642 retune. What *is* gone is the ability to rebuild
any of it from scratch, since the intermediate curves no longer exist.

**Each remaining step differences two builds that differ in exactly one input** — except the merged LOL
step, which spans two raises — and each is *pinned* as a constant in `[desktop-dec15]` rather than
recomputed. Two consequences:

- **The ledger is a real check, not a tautology.** Because the steps are pinned independently of the
  canonical parquet, they do not close by construction — so the notebook asserts the chain sums to the
  measured Dec-15 and fails loudly if a constant goes stale or the wrong parquet is loaded. It currently
  closes to a residual of **−0**. A residual ledger cannot test itself.
- **No comparison build was re-run to produce any of these numbers.** **Historical builds are locked** —
  published deltas were quoted against them, and a chain whose links move cannot be audited. The
  intermediate parquets are also gitignored and GCS-bound, which is the second reason the steps are pinned
  rather than loaded.

The LOL step is **+77,604** against a raw curve change of +75,000/day at Dec-15 — a 103% pass-through,
which the notebook asserts falls in a plausible 0.5–1.5× band. Near-zero would mean the bidirectional
add-back leg never ran; far above 1 would mean the training subtraction is reshaping trend.

**The retune's own like-for-like measurement** is in
`research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb`: previous config vs s01 with the
**LOL curve held identical on both sides**, so the model config is the only difference — trough
**+1,359,887** for **+5,642** at Dec-15, 11% of the ±50,000 budget. Both sides of that comparison sit
on a curve that is no longer this cycle's active one, which is *why* it isolates the config; do not
repoint it at the canonical build, and note that its 45,193,561 trough is therefore not this build's
(s01 troughed at 45,223,249; g01 troughs at 45,041,389).

**A ceiling change is not a level shift.** `l` is bidirectional, so a higher ceiling also changes the
training series Prophet fits, and its realised Dec-15 effect depends on the model config — the same
20,000/day raise measured +25,348 under s01 but only ~+7,211 under the previous flatter config. That
config-dependence is why the merged +77,604 cannot be re-derived by scaling the curve delta, and why a
future ceiling change needs a fresh measurement rather than a pro-rata estimate.


**The headwind amplitude step is exactly +100,000 by construction.** `h` is applied to the 28-day MA,
never to the training frame, so its Dec-15 effect is the anchor delta with no Prophet interaction and it
needs no model re-run.

**The ramp re-anchoring is absent from the ledger because its Dec-15 effect is exactly zero** — both
conventions terminate on the same anchor. It is not a KPI change; it is a near-horizon reshaping
(+569,419 at the seam, +467,737 at Aug-22, +305,046 at Oct-1) that also removed the seam discontinuity.
Near-horizon numbers from this build are therefore **not** comparable to the earlier August builds; Dec-15
is.

### Read this before quoting the headline

**Three discretionary levers moved upward this cycle — two on desktop, one on mobile — and on both
platforms they set the sign of the result.**

**Desktop** (sums to the +117,960 delta):

| lever | change | Dec-15 effect | basis |
|---|---|--:|---|
| `l` LOL ceiling | 125,000 → 200,000/day | **+77,604** | extrapolation judgement; unfalsifiable |
| `h` Win10 anchor | −1,345,000 → **−1,315,000** | **+30,000** | calibration judgement; cut to +50,000 on 2026-08-03, then 19,000 + 1,000 more headwind on 2026-08-04 |
| `h` ramp start | 2026-04-01 → seam | **0** | convention correction (measured) |
| data refresh | 07-06 → 07-28 anchor | −64,769 | what the fresher data said |
| **data refresh** | 07-28 → **08-02** anchor | **+100,840** | what the fresher data said (2026-08-03) |
| s01 model retune | previous config → s01 | +5,642 | measured, config-isolated |
| **g01 model retune** | s01 → g01 | **−31,357** | measured, config-isolated; buys −186,860 at Aug-25 |

**The judgement calls still account for most of the headline gain, but the 2026-08-03 refresh changed
the story.** Data and model together now come to **+10,356** (−64,769 first refresh, +5,642 s01,
−31,357 g01, **+100,840 second refresh**); the two discretionary levers add **+107,604** after the
three headwind moves. Net **+117,960**.

Note what those moves did to this accounting: the discretionary contribution fell from a peak of
+202,604 to +107,604, but it did so by **cancelling 95,000 of a measured data gain** (−75,000 on
2026-08-03, then −19,000 and −1,000 on 2026-08-04). Data-and-model is still +10,356 throughout — the
headwind moves did not make the published number more data-driven, they moved its level. Do not read
+117,960 as "what the refresh produced" (that was +100,840 on top of the 07-28 build's +112,120).
**The published delta is now within 5,840 of the pre-refresh +112,120**, which is worth noticing: three
discretionary headwind steps of decreasing size have returned the headline to almost exactly where it
sat before the data refresh moved it.

So the earlier framing — "the data said down, and two judgements reversed it" — **no longer holds for
desktop.** With five more days of data the data-and-model contribution flipped from −90,484 to +10,356.
The judgements are still ~95% of the gain, and they remain unvalidated (`h`'s validation came back
"cannot be distinguished in telemetry, and the elapsed portion is contradicted"), but they are no longer
*rescuing* a decline. Worth re-checking on the next refresh, since one 5-day window flipped it once.

**Mobile** (sums to the +693 delta):

| lever | change | Dec-15 effect | basis |
|---|---|--:|---|
| data refresh + `m` → `p` swap | methodology | **−322,714** | measured; `p` forecasts organic only |
| model re-lock | cpr 0.75 → 0.725 | +23,907 | measured, config-isolated; chosen on seam quality |
| data refresh | 07-28 → 08-02 anchor | **+500** | what the fresher data said — essentially nothing |
| **`t` tailwind** | none → +299,000 | **+299,000** | **~47% independent implementation, ~53% planning judgement; last +23,000 sized to hit July's number** |

**`t` is the single largest discretionary lever of the cycle**, and mobile would be reported ~1.5% lower
without it. Across both platforms discretionary uplift totals **+406,604** — still more than three times
the +118,653 ALL gain versus July.

⚠️ **Mobile is now calibrated to July's number, and that should be stated whenever it is quoted.** The
tailwind was raised 276,000 → 299,000 specifically so published mobile lands +693 from July's delivered
17,923,869. Mobile reading "flat versus July" is therefore **an outcome that was chosen, not measured** —
the underlying model base is 17,625,562, i.e. ~1.7% below the published figure.

That does not make any of them wrong — July's 125K LOL clamp sat *below* the last clean measurement of
130,296, so it was arguably too conservative — but the framing matters when these numbers are quoted, and
it matters more now that the intermediate LOL variants have been deleted: the single remaining curve
should not be mistaken for a measured or consensus value.

**The mobile charts' `DRAFT` watermark was switched OFF on 2026-08-03 when mobile was signed off** —
switched off, not deleted: the helper and its call sites remain, behind a `SHOW_DRAFT_WATERMARK` flag
in each producing notebook, so it can be reapplied by flipping one boolean (see `plots/_index.md`). The
caveat it stood for is unchanged — 1.67% of published mobile is the discretionary `t` overlay, and the
last +23,000 of it was sized to hit July's number — so that caveat now travels only in text. Anyone
circulating the mobile charts is responsible for carrying it: see `tailwind/_index.md`, the
`[mobile-dec15]` decomposition, and the caveats cell.

**Intermediate-variant history was removed on 2026-07-30.** Earlier in the cycle this section carried a
table of every LOL-ceiling / headwind-anchor / ramp-start combination built at the 2026-07-28 anchor
(five rows on the previous model parameters, plus the s01 pair). It was deleted along with the
intermediate curves, because a standing menu of superseded ceilings invited exactly the
over-referencing this cleanup was meant to stop.

**There are two desktop builds for this cycle:** `desktop_locked/` — **g01** parameters, LOL 200,000,
headwind −1,220,000 ramping from the seam (**Dec-15 48,697,603, Aug-25 trough 45,041,389**) — and
`desktop_s01_REVERT_2026-07-29/`, the complete s01 build kept as a **revert target**, not an archive. The
ledger above is now the only record of the path taken to it; the intermediate builds' own numbers are
in git history.

The one path-dependent fact worth keeping out of history: **the ramp re-anchoring changed the trough,
not Dec-15.** Same anchor either way, so Dec-15 is identical, but the Aug-22 trough moved +467,737 and
the seam discontinuity disappeared. Near-horizon numbers from this build are therefore not comparable
to any pre-2026-07-29 August figure.

**Still not the number to publish.** `o` remains stale — now ~6 weeks (`m` is retired, superseded by `p`);
the headwind anchor has now been attenuated **five** times running, and the validation analysis
(`research/headwinds/WIN10_ANCHOR_FINDINGS.md`) found its elapsed portion contradicted and its magnitude
indistinguishable in telemetry; the LOL ceiling is unfalsifiable against current data (measurement stopped
2026-06-23); and mobile carries the discretionary +276,000 `t` overlay, ~49% of which is unattributed
planning judgement. See the caveats cell, `adjustments/_index.md` and `tailwind/_index.md`.

## Mobile: re-locked and given a discretionary tailwind (2026-08-03) — LIVE

**Two mobile changes, and they are different in kind.**

**1. Model re-lock.** `prophet_changepoint_range` **0.75 → 0.725**, everything else unchanged.
Dec-15 **17,601,155 → 17,625,062 (+23,907)**. Build: `mobile_cpr0725_2026-07-28/`. Chosen for **seam
quality**, not for the +23,907: level step −9,989 → −8,304, slope kink −2,954 → −1,004 (−66%), with
smooth measured neighbours either side, so unlike desktop `g01` it is not an isolated optimum.

**2. ⚠️ `t` mobile tailwind, +276,000 at Dec-15 — the largest discretionary lever of the cycle.**
Published mobile is now **17,901,062**, of which **1.54% is this overlay**. It is display-layer like
`h`, so no model re-run and no Prophet interaction.

Why an overlay rather than parameters: a 33-probe search across three seasonality regimes
(`research/param-scans/mobile-aug/`) established that **no exposed non-holiday parameter combination
reaches July's 17,923,869** — the entire envelope spans 63,539 against a 322,714 gap, because mozaic
reconciles **top-down** and the mobile headline is effectively one Prophet fit on the aggregate
(forcing `seasonality_regime=additive` moved 63 of 64 leaf tiles by 27,552 DAU and the total by 1).
So the remainder is carried **visibly on the ledger** instead of being pushed into parameter values
chosen only because they raise the number.

**About half of the tailwind (+141,637) is the measured excess of an independent implementation;
the other half is a planning decision.** Read `tailwind/_index.md` before quoting mobile.

Reverting is one file: `rm adjustments/tailwind.json`, re-execute, re-pin `ORGANIC_MOBILE_DEC15`.

## Current working set

- **Producer / review notebook** — `august_canonical_v2026-07-28.ipynb` (26 cells, executed with
  outputs). The single canonical view: both platform plots, the ex-Iran mobile plot, the Dec-15 table,
  and the caveats. All plots are generated inside the notebook and saved to `plots/`.
  A **"Dec-15 vs the low / baseline / stretch benchmarks"** section (`[desktop-vs-targets]`,
  `[mobile-vs-targets]`) prints absolute Dec-15 values and per-benchmark deltas for each platform.
  Both cells label the benchmarks as the **June-cycle aspirational markers reused in July, not August
  targets** (see the `target`-column note below) — August desktop lands below all three, mobile above
  all three. Values are computed from the curves, not hardcoded, so they follow a rebuild.
  Also carries four **`*_with_2025` reference charts** overlaying 2025 actuals as a faint calendar-aligned
  grey line, built from parquet `training` rows rather than a BQ query (verified equal to actuals by
  `scripts/verify_training_rows_are_actuals.py`). Visual reference only — they feed no number. See
  `plots/_index.md` for why they are separate charts rather than edits to the four originals.
  Its final `[csv-export]` cell writes the two public CSVs in `csv/` and round-trips them: both files
  are re-read from disk and their Dec-15 values re-checked against the in-memory curves, and the
  forecast columns are asserted to begin exactly at the seam.
- **Public CSV exports** — `csv/august_canonical_curves.csv` (365 × 10, full-year daily 28d-MA for
  desktop/mobile/ALL × actuals/prior-July/current-August) and `csv/august_dec15_summary.csv` (3 rows:
  Dec-15 headline + summer trough per platform). Both git-tracked via explicit `.gitignore` exceptions.
  Column reference, what's baked in, and provenance: `csv/README.md`. **No `target` column this cycle** —
  July had a stakeholder desktop target of 48,584,362 but no August target has been set, and carrying
  July's forward under that name would read as an August target. The `summer_trough_*` columns are new:
  the trough is the KPI the s01 retune was adopted to move, so a Dec-15-only summary would omit the
  cycle's largest change.
- **Stakeholder handoff bundle** — `handoff/august_canonical_handoff_2026-07-28.zip` (~1.7 MB): the two
  CSVs, a boss-facing `README.md` with a hand-verification checklist (checkpoint values, expected seam
  step, boundary row counts), the 9 canonical charts, and 5 adjustment-ladder / seam-fix diagnostics.
  See `handoff/_index.md`. The zip and its staging dir are gitignored; regenerate rather than duplicate.
- **KPI workbook DRAFT update** — `kpi_sheet/` (2026-08-10). Appends the August cycle to the "Official
  Forecast Data" tab as a **`FUTURE`** cycle rather than promoting it to `CURRENT`, so a draft dashboard
  can be built without moving the official numbers (July stays `CURRENT`). Full replacement table
  `official_forecast_data.2026-08-10.csv` (6,390 rows — the 5,660 existing rows unchanged and in place,
  asserted field-for-field) plus a 730-row `FUTURE`-only extract. One departure from July's scheme,
  documented in `kpi_sheet/_index.md`: nothing is renamed or demoted. The handoff-gap convention IS
  followed — the JUN→JUL null at 2026-07-05 is added by this build, giving six prior segments and five
  breaks; it keeps the JUN and JUL vintages from connecting across their +895,841 desktop / +414,383
  mobile level disagreement. `kpi_sheet/plot_future_lines.ipynb` renders both lines per product
  (`plots/kpi_sheet_future_{desktop,mobile}.png`). The prior line's Jul 6 → Aug 1 segment is July's
  **as-published** curve from the tab, not
  `csv/`'s regenerated `*_prior_july` column — those disagree on exactly the 27 splice days (up to
  418,345 desktop) and agree from 2026-08-02 on, per `display_ma`'s seam+27 guarantee.
- **Mobile KPI cake chart** — `mobile_kpi_cake.ipynb` → `plots/mobile_kpi_cake.png`. Standalone.
  Three cumulative lines for 2026: organic DAU, + paid marketing, + `h`/`t`. The top line is the
  published KPI, so the gaps are the contributions. At Dec-15: organic **16,097,845** (89.81%), paid
  **1,554,879** (8.67%), `h`+`t` **271,838** (1.52%). Asserts the bands reconstruct the headline and
  that no adjustment leaks into history. Reads the specs live, so it follows a spec edit.
- **Adjustment-ladder diagnostic** — `desktop_adjustment_ladder.ipynb` (+ `adjustment_isolation/`).
  Desktop only. Turns each adjustment on one at a time so its individual effect is visible. **Its main
  finding: the seam discontinuity at 2026-07-28 is 100.9% the display-layer headwind**, not the model —
  the raw model output is continuous across the seam, and no parameter value can change that. Also
  measures the `l`/`o` interaction term (−15,590 at Dec-15) and contrasts the two headwind ramp
  conventions. **Read this before starting a parameter search.**
- **Desktop forecast (CANONICAL)** —
  `desktop_g01_2026-08-02/cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/mozaic_daily_forecast.2026-08-02.ld-D.adj-lo.parquet`
  — g01 params, `l` at the **200K** ceiling + `o`, at the refreshed **2026-08-02** seam.
  Pre-headwind (the parquet carries `l`+`o` only; `h` is applied at the display layer). Published
  Dec-15 after `h` at −1,315,000: **48,703,443**.
  `desktop_locked/` (the 2026-07-28 build, 48,697,603) stays on disk as the refresh comparison base
  and must not be deleted while August is live.
- **Desktop forecast (previous-cycle params, superseded)** — `desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` — kept for two reasons: its raw BQ pull is the shared cache every scan and isolation run symlinks, and its Dec-15 (48,672,970) is the anchor point the ledger's retune step is pinned from. The canonical notebook **no longer loads it** — all ledger steps are pinned constants
  (+ sidecar, `parameters.json`). Pre-headwind; `l` + `o` baked in. **FROZEN — never re-run or rebuilt.**
  It was built on a superseded LOL curve that no longer exists on disk, which is another reason never to
  attempt a rebuild; the frozen artifact is the record. **The directory name is a misnomer** — it held
  the original baseline, which later runs overwrote in place. Kept as-is because the notebook and the
  committed sidecar reference the path.
- **Mobile forecast (CANONICAL)** —
  `mobile_cpr0725_2026-08-02/cps0.035_thresh055_recent13_cpr0.725_ncp25_clip0.6_sps0.1/mozaic_daily_forecast.2026-08-02.gm-D.adj-p.parquet`
  (+ sidecar, `parameters.json`). Pre-headwind **and pre-tailwind**; `p` baked in, at the refreshed
  **2026-08-02** seam. Its own Dec-15 is **17,625,562** — the published 17,901,562 adds the
  display-layer `t` tailwind on top. `mobile_cpr0725_2026-07-28/` (17,625,062) stays on disk as the
  refresh comparison base.
  Three superseded builds remain on disk and must not be deleted while August is live:
  `mobile_organic_2026-07-28/` (the immediate predecessor, cpr 0.75, 17,601,155 — the revert target for
  the re-lock), `mobile_uac_meta_2026-07-28/` (the last `m`-era build, 17,864,732 — its parquet AND its
  838 MB pickle are the revert target for the `m` → `p` swap) and `mobile_baseline_2026-07-28/`
  (`m`-era, 17,924,607). **Not rebuilt for the LOL change** — `l` is desktop-only, so mobile is
  unaffected by it and the notebook asserts the drift is exactly 0.
- **Adjustment specs (wired)** — `adjustments/headwind.json` (`h`, display layer),
  `adjustments/tailwind.json` (`t`, display layer, **new 2026-08-03**),
  `launch_on_login/lol.json` (`l`), `mozillaonline/mozillaonline.json` (`o`),
  `organic/organic.json` (`p`, **replaces `m`**; its measured split was rebuilt for the new training
  window — `organic/fenix_paid_organic.2026-08-02.parquet`). `o` is a byte-identical carry-forward of July's with
  only `applies_to_forecast_start` moved 2026-07-06 → 2026-07-28. **`l` is rebuilt** (200K ceiling — see
  `launch_on_login/_index.md`) and **`h` has changed three times** (desktop amplitude −1,345,000 →
  −1,295,000 → −1,245,000 → −1,220,000 → −1,295,000 → −1,314,000 → −1,315,000, and `start_date`
  2026-04-01 →
  2026-07-28 → 2026-08-02 — see
  `adjustments/_index.md`).
  `marketing/marketing.json` (`m`) is **retired**: its date gate is cleared so it applies to nothing, but
  the directory and its lift parquet stay, because `p` consumes that very curve as the *paid forecast*.
  `m` must also stay registered in `adjustment_codes.yaml` so July's and August's pre-swap `.adj-m.`
  artifacts keep loading.
- **Iran** — queried natively; the shutdown gap is covered by mozaic's built-in counterfactual fill
  (auto-applied by `populate_tiles`). No cycle-local artifact needed.

## How the current build was produced

The **mobile** command below was run once (2026-07-29) and has not been re-run since — `l` is
desktop-only, and mobile's headwind *amplitude* never moved (its ramp start did, but `h` is display-layer
so that needs no model run). The **desktop** command was run repeatedly into the same directory, each run
overwriting the last, as the LOL ceiling was raised to its final 200,000. Runs after the first reused the
cached raw BQ pull already in the slug dir, so no `--raw-cache-dir` was needed and no BigQuery re-query
happened.

⚠️ **The per-ceiling run logs were deleted on 2026-07-30** along with the intermediate curves, so only
`logs/aug_baseline_{desktop,mobile}_2026-07-28.log` (the first desktop run) survives. The commands below
are the reproducible record; the intermediate invocations differed only in which curve `lol.json` named.
Note that the *canonical* build lives in `desktop_locked/`, not in the `desktop_baseline_*` directory
these commands write to — see that directory's `README.md` for the final invocation.

**Headwind and tailwind changes need none of this.** `h` and `t` are display-layer, so the three anchor
steps (−1,345,000 → −1,295,000 → −1,245,000 → −1,220,000 → −1,295,000 → −1,314,000 → −1,315,000) and the adoption of `t` were spec edits plus a
notebook re-execution, with no model rebuild.


```bash
source .venv/bin/activate

python scripts/run_param_scan.py --forecast-start-date 2026-07-28 \
    --results-dir data-official/2026-08/desktop_baseline_2026-07-28 \
    --changepoint-prior-scale 0.08983 --changepoint-range 0.65 --n-changepoints 25 \
    --recent-weeks 13 --holiday-threshold -0.032 --holiday-max-radius 5 \
    --holiday-min-radius 3 --holiday-effect-floor -0.6

python scripts/run_mobile_param_scan.py --forecast-start-date 2026-07-28 \
    --results-dir data-official/2026-08/mobile_organic_2026-07-28 \
    --changepoint-prior-scale 0.035 --changepoint-range 0.75 --n-changepoints 25 \
    --recent-weeks 13 --holiday-threshold -0.055 --holiday-effect-floor -0.6
```

`run_main.py` **cannot** reproduce these — it has no parameter flags and would use package defaults.
The two param-scan runners are the real producers, and they apply the overlays whose spec's
`applies_to_forecast_start` matches the run date. Logs: `logs/aug_baseline_{desktop,mobile}_2026-07-28.log`,
and `logs/aug_mobile_organic_2026-07-28.log` for the `p` rebuild. **The mobile command above now
produces `.adj-p.`, not `.adj-m.`** — the runner derives the code from whichever spec gates the run,
and `marketing.json`'s date gate is cleared. Both scan runners now pass their config through
`main(model_configs=...)` instead of monkeypatching `process_data_source`.

**The date gate is the trap.** Overlay specs are matched by exact string equality on
`applies_to_forecast_start`. A run at a date no spec claims applies **no** overlays and silently emits
`.raw.` instead of `.adj-lo` / `.adj-p`. The notebook defends against this: `load_all_level_dau` passes
`require_state=["l","o"]` / `["p"]`, so a mis-gated run fails loudly at load instead of producing a
plausible wrong headline.

**Note the two display-layer specs are gated differently — they are not gated at all.**
`adjustments/` is **live by presence**: `load_adjustments` globs `*.json` and sums everything it finds,
with no `applies_to_forecast_start` key and no enable flag. Adding or deleting a file there changes the
published numbers immediately. That is the mechanism behind both `h` and `t`.

## Verification built into the notebook

Three checks run as assertions, not eyeballs:

1. **Config lock** — desktop's sidecar is compared field-by-field against the **g01** lock (and its four
   holiday knobs against package defaults, enforcing the exclusion policy); mobile's against July's locked
   values (8 params each). Any drift aborts.
2. **State markers** — `load_forecast(..., require_state=...)` pins which adjustments must be present.
3. **Prior-curve reproduction** — July's delivered Dec-15 numbers (48,585,483 / 17,923,869) are
   hardcoded and the rebuilt prior curve must match within 1,000 DAU. Both reproduce at **drift 0**,
   which is what licenses quoting the August-vs-July deltas at all.

## Expected layout (populate as the cycle progresses)

```
2026-08/
  august_canonical_v<date>.ipynb   # producer/review notebook (present)
  desktop_g01_2026-08-02/          # present — CANONICAL desktop (g01, LOL 200K, 08-02 seam)
  desktop_locked/                  # present — the 07-28 g01 build; refresh comparison base
  desktop_s01_REVERT_2026-07-29/   # present — s01 REVERT TARGET (config + headwind); see its REVERT.md
  desktop_baseline_2026-07-28/     # present — superseded (July's params); ledger baseline + raw cache
  desktop_candidate_aug25/         # present — g01 candidate build kept for the Aug-25 gap search
  mobile_cpr0725_2026-08-02/       # present — CANONICAL mobile (adj-p, cpr 0.725, 08-02 seam)
  mobile_cpr0725_2026-07-28/       # present — the 07-28 build; refresh comparison base
  mobile_rawpull_2026-08-02/       # present — raw BQ mobile pull for the 08-02 split's drift check
  mobile_organic_2026-07-28/       # superseded (cpr 0.75) — REVERT TARGET for the re-lock
  mobile_uac_meta_2026-07-28/      # superseded adj-m — REVERT TARGET, do not delete
  mobile_baseline_2026-07-28/      # superseded adj-m
  mobile_adjm_REVERT_2026-07-31/   # revert kit for the m -> p swap
  adjustments/headwind.json        # present (h, display layer)
  adjustments/tailwind.json        # present (t, display layer) — NEW 2026-08-03
  tailwind/                        # present — `t` rationale + evidence/judgement split
  organic/                         # present — `p` spec + measured paid/organic split
  launch_on_login/lol.json         # present (l, 200K ceiling)
  mozillaonline/mozillaonline.json # present (o, stale carry-forward)
  marketing/marketing.json         # present but RETIRED (m) — date gate cleared; parquet still fed to `p`
  adjustment_isolation/            # present — per-adjustment isolation builds for the ladder notebook
  desktop_adjustment_ladder.ipynb  # present — adjustment isolation diagnostic
  seam_fix_before_after.ipynb      # present — Fix A verification
  mobile_kpi_cake.ipynb            # present — mobile KPI cumulative "cake" decomposition
  plots/                           # present
  csv/august_canonical_curves.csv  # present — + august_dec15_summary.csv, README.md
  handoff/                         # present — _index.md + the gitignored bundle zip
  _backup_mobile_methodology_2026-07-31/  # present — pre-`p` notebook/CSV/plot snapshot + RESTORE.md
  kpi_sheet/                       # present — DRAFT (`FUTURE`) workbook update, not a promotion
  TODO_factors.md                  # NOT YET — start it as a diff against ../2026-07/TODO_factors.md
```

The CSV export was deferred earlier in the cycle on the grounds that the build was baseline-only. It
is now cut, but **the "still not the number to publish" caveats above have not been resolved** — `o`
remains stale, the headwind anchor's elapsed portion was contradicted by measurement, and mobile now
carries the +276,000 `t` overlay. Both the CSV README and the handoff bundle README state this
prominently, so the export is a snapshot of the current build rather than a sign-off. Re-cut both when
the overlays land.

## Next up

- **Re-measure and swap the `o` MozillaOnline curve.** Now a ~6-week-stale carry-forward (it was
  deliberately held through the 2026-08-03 refresh so the refresh delta stayed isolated) and needs a
  fresh build against data through 2026-08-01 — now the last stale overlay, and the main remaining reason
  the desktop forecast is not deliverable. (`m` marketing is **retired**, not pending: `p` replaced it and
  consumes its lift curve directly, so there is no separate `m` swap to do. What *is* worth re-measuring is
  the **paid curve `p` reads** — see `organic/_index.md`.) `l` is **rebuilt** (200K, 2026-07-29) and is now
  the cycle's only LOL curve, though its ceiling remains an unfalsifiable judgement — deleting the
  alternates removed the menu, not the uncertainty.
  Change one overlay at a time: two in one run makes the Dec-15 delta uninterpretable.
- **The Win10 headwind anchor has now been validated — and the result is not comfortable.** Now at
  **−1,315,000**, a net +30,000 attenuation from July's −1,345,000. It was attenuated to −1,220,000 in
  three steps (two on 2026-07-29, the last on 2026-07-30 alongside the g01 retune), **moved back to
  −1,295,000 on 2026-08-03** — the first downward step in the run — then to −1,314,000 and −1,315,000 on
  2026-08-04. **Nine values across three cycles**, four of them within 48 hours, and the live value is
  the most severe August has used. **The three reversals together (−95,000) erase the data refresh's
  +100,840 almost exactly** — worth stating plainly when this is reported, since the published desktop
  delta retains only +5,840 of a measured gain, and the step sizes (75,000 → 19,000 → 1,000) look like
  convergence on a target rather than re-estimation.
  `research/headwinds/WIN10_ANCHOR_FINDINGS.md` found that (a) telemetry **cannot** distinguish the
  candidate anchors — they span 21,705 DAU against a 1,488,293-wide specification envelope — and (b) the
  April-anchored ramp's already-elapsed portion was **contradicted**: none of 90 specification variants
  reached the ≈540,000 of loss it implied, central estimate ≈0. The seam re-anchoring fixed the shape
  problem; the **magnitude remains an unfalsifiable forward judgement**. Do not attenuate further on the
  old rationale — the analysis has already retired it. See `adjustments/_index.md`.
- **Decide how the summer trough is scored now that it sits inside the splice zone.** The published
  minimum (2026-08-24) is −64,483 sensitive to the `display_ma` convention; 2026-08-30 is the first
  splice-immune date. See "Trough is now convention-dependent" above. Untouched pending a decision.
- **Re-check the desktop data-refresh direction on the next refresh.** 07-06 → 07-28 read −64,769;
  07-28 → 08-02 read **+100,840**. A single 5-day window flipped the sign of the data contribution, so
  neither reading should be treated as the settled trend.
- **`TODO_factors.md`** — begin as a diff against July's.
- **Open, needs go/no-go — summer-trough overlay.** `research/param-scans/aug22-retune/` established
  that no exposed parameter combination lifts the Aug trough to target while holding Dec-15 (best
  sampled point 0.385M short; `seasonality_regime=multiplicative` gets ~71% of the lift but plateaus).
  Its recommendation is a bidirectional overlay in the `l`/`o`/`m` family tapering to ~0 by Nov/Dec.
  **Nothing here implements it and nothing was tuned toward the trough.** Target shape:
  `research/summer-slump/`.
- **`../2026-06/` is retained on purpose** even though it is N-2. This notebook still imports
  `display_ma` from `../2026-06/export_canonical_curves.py`, and July's `m` chain reaches into it. The
  September roll-forward should give August its own copy. See `../_index.md`.

## Dependency note

The `seasonality_prior_scale` / `seasonality_regime` knobs now appear in every config slug
(`_sps0.00825` / `_sps0.1`) because they were exposed in `mozaic-forecasting-official` @
`configurable-model-params` (`126fe14`, `6f02912`) after July's build. **Their defaults reproduce the
values July hardcoded**, so the baseline is behaviour-comparable to July despite the slug change. Those
commits are **not pushed** to that repo's origin — reproducing this needs the local checkout.

## Present vs Archived

Cycle archived at the September button-down (2026-09-04) to
`gs://moz-data-science-brwells-bucket/mozaic-daily-archive/august-2026/` — `data-official/2026-08/` in
full (198 files, 9.4 GB before verification; final counts recorded in the archive README), the three
August parameter searches under `param-scans/` (`summer-trough-v2/` 40 GB incl. 68 probe pkls,
`mobile-aug/` 26 GB incl. 33 probe pkls, `aug25-gap/` 1.7 GB), `research/headwinds/aug-post-seam-retune/`
(586 MB refit pkl), and the repo-root `mozaic_daily_forecast.*.gd-D.parquet` intermediates. The full
tree also remains in the `august-forecast` branch. **Fitted pickles are first-class artifacts** — every
one, including per-probe scan pickles, is in the archive.

- **Present (on disk, through the 3-month retention window):** the canonical parquets + sidecars
  (`desktop_g01_2026-08-02/`, `mobile_cpr0725_2026-08-02/`), every `parameters.json` / `.meta.json`
  sidecar of every build listed in the layout above, `csv/`, `plots/`, `kpi_sheet/`, `tailwind/`, all
  wired specs (`adjustments/`, `organic/`, `launch_on_login/`, `mozillaonline/`, `marketing/*.json`),
  the notebooks, `handoff/_index.md`, and the REVERT/RESTORE documents.
- **Archived to GCS, removed from disk:** every `mozaic_objects.*.pkl`, every raw pull
  (`mozaic_parts.raw.*`), the superseded and baseline build parquets (`desktop_locked/`,
  `desktop_baseline_2026-07-28/`, `desktop_candidate_aug25/`, `desktop_s01_REVERT_2026-07-29/`,
  `mobile_*_2026-07-28/`, `mobile_adjm_REVERT_2026-07-31/`), `adjustment_isolation/` run blobs,
  the `marketing/experiment_july_methodology/forecast/` build blobs (the tracked lift-model parquets stay), `_backup_mobile_methodology_2026-07-31/` blobs,
  and the `handoff/` zip + staging dir. The REVERT directories' revert window closed when September
  became the live cycle; their READMEs stay so the record of what a revert meant is legible.

Pull a blob back with `gcloud storage cp -r gs://…/august-2026/data-official/2026-08/<dir> data-official/2026-08/`.

## Where new files go

Month-scoped artifacts (this cycle's producer/diagnostic notebooks, adjustment specs, parquets,
canonical CSVs) live here. Cross-month or topic-anchored work (mechanism diagnostics, parameter
searches, validation against actuals over time) goes to `research/{topic}/`.
