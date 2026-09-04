# August 2026 Canonical Forecast Curves — CSV exports

Static, plot-ready exports of the August 2026 canonical forecast review
(`../august_canonical_v2026-07-28.ipynb`). Read them into pandas and plot — no
BigQuery, no model code needed. This is the set to hand off / upload.

| File | Scope | Display adjustments (`h` + `t`) applied? |
|---|---|---|
| `august_canonical_curves.csv` | Desktop, Mobile, and ALL (Desktop+Mobile) world totals | **Yes** |
| `august_dec15_summary.csv` | The Dec-15 headline + summer trough per platform | **Yes** |
| `august_canonical_curves.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.csv` | Desktop only | **`h` REMOVED**; `t` is mobile-only so N/A |
| `august_dec15_summary.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.csv` | Desktop only | **`h` REMOVED** |
| `august_canonical_curves.DESKTOP_ONLY.EX_IR_CN.csv` | Desktop only, **ex-Iran, ex-China** | Yes, full anchor |
| `august_dec15_summary.DESKTOP_ONLY.EX_IR_CN.csv` | Desktop only, **ex-Iran, ex-China** | Yes, full anchor |
| `august_canonical_curves.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.EX_IR_CN.csv` | Desktop only, **ex-Iran, ex-China** | **`h` REMOVED** |
| `august_dec15_summary.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.EX_IR_CN.csv` | Desktop only, **ex-Iran, ex-China** | **`h` REMOVED** |

⚠️ **Only the first two files are the published forecast.** The other six are scoped or
counterfactual views — see [§ The WIN10_HEADWIND_REMOVED
files](#the-win10_headwind_removed-files) and [§ The EX_IR_CN files](#the-ex_ir_cn-files)
below. Everything the canonical forecast reports comes from the first two.

All curve values are **28-day moving averages of daily active users (DAU)**, in
absolute user counts, daily from **2026-01-01 through 2026-12-31**.

**Rounding policy:** every published figure is derived from already-rounded values, never
from the underlying floats. So `all_* == desktop_* + mobile_*` exactly on every row, and
`delta_vs_july` is exactly the difference of the two columns beside it. Deriving from the
floats is marginally more accurate but leaves the file self-contradictory — a delta column
that prints one unit away from the difference of the two columns beside it. At 10⁻⁸ relative
error that accuracy buys nothing and the off-by-one costs real trust. The assertions in the
producing cell are therefore exact equality, not tolerance-based.

- **Desktop** = Firefox Desktop only (`legacy_desktop`; Glean desktop is excluded).
- **Mobile** = Fenix (Android) + Firefox iOS + Focus Android + Focus iOS (`glean_mobile`).
- **ALL** = Desktop + Mobile, summed date-by-date.

Iran is included **natively** in every column (the mozaic package auto-applies a
counterfactual "what Iran would have been with no shutdown" fill during training).
There is no `plus_iran` / `no_iran` split.

---

## Read this before quoting the headline

August lands **+118,653 ALL** above July. That number is **not** "what the fresher
data says" — on both platforms the data and the model pointed **down**, and three
upward judgement calls totalling **+478,604** more than reversed it.

**Desktop** (sums to +117,960):

| lever | change | Dec-15 desktop effect | basis |
|---|---|--:|---|
| data refresh (07-06 → 07-28 anchor) | — | **−64,769** | what the fresher data said |
| **data refresh (07-28 → 08-02 anchor)** | — | **+100,840** | what the fresher data said (2026-08-03) |
| `l` launch-on-login ceiling | 125,000 → 200,000/day | **+77,604** | extrapolation judgement; **unfalsifiable** |
| `h` Win10 headwind anchor | −1,345,000 → **−1,315,000** | **+30,000** | calibration judgement; cut in three steps (2026-08-03, then twice on 08-04) that together give back 95,000 of the refresh's +100,840 |
| desktop model retune to s01 | — | +5,642 | measured like-for-like |
| desktop model retune s01 → g01 | — | −31,357 | measured, config-isolated |
| `h` ramp start (2026-04-01 → seam) | — | 0 | convention correction |

**Mobile** (sums to +693):

| lever | change | Dec-15 mobile effect | basis |
|---|---|--:|---|
| data refresh + `m` → `p` methodology swap | — | **−322,714** | measured; mozaic now forecasts **organic** DAU only |
| mobile model re-lock | cpr 0.75 → 0.725 | +23,907 | measured; chosen on seam quality, not size |
| data refresh (07-28 → 08-02 anchor) | — | **+500** | what the fresher data said — essentially nothing |
| **`t` mobile tailwind** | none → +299,000 | **+299,000** | **~47% independent implementation, ~53% planning judgement** |

Four caveats on the current build:

- **⚠️ Mobile carries a +299,000 discretionary overlay (`t`) — 1.67% of the published
  mobile number — and it was sized to land mobile within 1,000 DAU of July's figure.**
  So "mobile is flat versus July (+693)" is a *chosen* outcome, not a measured one; the
  underlying model base is 17,625,562. Quote it with that stated. It exists because the `m` → `p` methodology swap cost 322,714 at
  Dec-15 and a 33-probe parameter search across three seasonality regimes established
  that **no** exposed non-holiday parameter combination recovers it (the whole probe
  envelope spans 63,539). Rather than push the gap into parameter values chosen only
  because they raise the number, it is carried visibly as its own adjustment code.
  About half is the measured excess of an independent implementation; the rest is a
  planning decision. Full argument: `../tailwind/_index.md`.
  **The mobile charts carried a `DRAFT` watermark until 2026-08-03, when mobile was signed off and it
  was switched off** (behind a `SHOW_DRAFT_WATERMARK` flag, so it can be reapplied). The caveat did not
  go away with it — it now travels only in text, including this
  bullet, so keep it attached whenever these curves or charts are circulated.
- **The Win10 headwind anchor has been validated, and the result is not comfortable.**
  `research/headwinds/WIN10_ANCHOR_FINDINGS.md` found the candidate anchors
  indistinguishable in telemetry (they span 21,705 DAU against a 1,488,293-wide
  specification envelope) and the April-anchored ramp's already-elapsed portion
  **contradicted** — none of 90 specification variants reached the ≈540,000 of loss it
  implied. Re-anchoring to the seam fixed the shape; the magnitude remains an
  unfalsifiable forward judgement.
- **`o` (MozillaOnline) is a ~4–5 week-stale carry-forward from July**, unchanged.
  Re-measuring it is the main remaining reason the upstream cycle notes describe this
  build as not final. (`m` is **retired**, superseded by `p` — not pending.)
- **The launch-on-login ceiling cannot be validated against data.** The holdback
  control received the feature on 2026-06-23, so the counterfactual is permanently
  gone and no fresh telemetry can adjudicate the ceiling. At a ~20K/day haircut
  against a ~220K convolution model, 200,000 is the least conservative end of the
  range that was considered. The lower variants were deleted on 2026-07-30, which
  removed the menu but not the uncertainty — do not read the single remaining
  curve as a measured or consensus value.

Full attribution ledger (it closes to a residual of −0, and the notebook asserts it):
`../_index.md` § Attribution ledger.

---

## Prompt for an AI agent

> I have a CSV of Firefox DAU forecast curves. The first column is `date` (daily,
> 2026-01-01 through 2026-12-31). Every other column is a **28-day moving average
> of DAU**. Columns are grouped into `desktop_*`, `mobile_*`, and `all_*` (all_ =
> desktop + mobile). Load it into pandas (parse `date` as datetime) and make line
> charts — one per platform group — with date on the x-axis and DAU (formatted in
> millions) on the y-axis. Some columns are intentionally blank in part of the
> year: the `*_current_august` forecast columns only start at the forecast date
> **2026-08-02** and are empty before it; the `*_actuals` columns stop a couple of
> days before today because the most recent telemetry day is still landing. Let
> those gaps render as gaps. Format y-axis ticks to two decimal places of a million
> — the ranges are narrow enough that a whole-million formatter renders adjacent
> ticks identically.

---

## `august_canonical_curves.csv`

Nine series: 3 platforms (desktop / mobile / ALL) × 3 curves (actuals / prior-July
forecast / current-August forecast). Forecast curves **include both display-layer
adjustments — the `h` headwind and the new `t` mobile tailwind** (see "What's baked in").

| Column | Platform | Meaning |
|---|---|---|
| `date` | — | Calendar date (daily), `YYYY-MM-DD`. 2026-01-01 .. 2026-12-31. |
| `desktop_actuals` | Desktop | Observed DAU. Ends 2026-08-01. |
| `desktop_prior_july` | Desktop | **July** forecast (prior cycle). Full-year. |
| `desktop_current_august` | Desktop | **August** forecast (current cycle). Blank before 2026-08-02. |
| `mobile_actuals` | Mobile | Observed DAU. Ends 2026-08-01. |
| `mobile_prior_july` | Mobile | July forecast (prior cycle). Full-year. |
| `mobile_current_august` | Mobile | August forecast (current cycle). Blank before 2026-08-02. |
| `all_actuals` | ALL | `desktop_actuals + mobile_actuals`. |
| `all_prior_july` | ALL | `desktop_prior_july + mobile_prior_july`. |
| `all_current_august` | ALL | `desktop_current_august + mobile_current_august`. |

### Key Dec-15 numbers (28-day MA of DAU)

| Series | July (prior) | August (current) | Δ vs July | Δ % |
|---|---:|---:|---:|---:|
| Desktop | 48,585,483 | 48,703,443 | +117,960 | +0.24% |
| Mobile | 17,923,869 | 17,924,562 | +693 | +0.00% |
| **ALL** | **66,509,352** | **66,628,005** | **+118,653** | **+0.18%** |

**Mobile is nearly flat for a non-obvious reason: three large changes very nearly
cancel.** It is not a quiet cycle for mobile. The paid/organic methodology swap (`m` →
`p`) took −322,714, a model re-lock added +23,907, and the discretionary `t` tailwind
added +276,000. Reading the −22,807 as "mobile barely moved" is the single most likely
misreading of this file — see the lever tables above.

### What's baked in (`current_august` forecast columns)

- **Win10 headwind (`h`)** — a linear ramp added on top of the raw model output over
  the forecast horizon, **ramping from the seam (2026-08-02)** to its Dec-15 anchor:
  - Desktop anchor: **−1,315,000 DAU** at 2026-12-15 (ramping from the **2026-08-02** seam, 135 days)
  - Mobile anchor: **−27,162 DAU** at 2026-12-15

  The seam start is new this cycle. Under July's convention the ramp began 2026-04-01
  but was only *applied* from the seam forward, so it switched on at 45.7% of full
  value (−569,419) as a one-day level step — which accounted for 100.9% of the seam
  discontinuity visible in July's chart. Ramping from the seam hits the identical
  Dec-15 anchor starting from zero, so **the KPI is untouched and the near term lifts**.
- **⚠️ Mobile tailwind (`t`)** — **new this cycle**, and the largest discretionary lever
  in the file. A linear ramp, **0 at the 2026-08-02 seam → +299,000 DAU at 2026-12-15**, mobile
  only. **Sign is positive**, unlike `h`. Display-layer like `h`, so it is *not* in the
  parquet and has no Prophet interaction — its Dec-15 effect is exactly its anchor.
  Combined with `h`, the **net mobile display adjustment at Dec-15 is +271,838**.
  See the caveat above and `../tailwind/_index.md`.
- **Desktop tailwinds** (applied per-tile inside the forecast, already in the parquet):
  - **Launch-on-login (`l`)** — **200,000 DAU/day** ceiling (July: 125,000). See the
    caveat above: this is extrapolation, not measurement.
  - **MozillaOnline migration (`o`)** — CN distribution-partner migration onto
    mainline Firefox. **Unchanged carry-forward from July; stale.**
- **Mobile paid/organic split (`p`)** — **replaces the retired `m` marketing-lift
  overlay this cycle.** Paid Fenix DAU is *measured*, not modelled: training rows are
  multiplied by a measured organic share so mozaic forecasts **organic** DAU only, and
  paid is then stacked back on as a **level**. Because the add-back is additive and
  post-mozaic, paid contributes exactly its own value. This is the change that cost
  −322,714 at Dec-15: under `m`, Prophet was handed a training series that still
  contained all the paid growth and extrapolated a **+16.12%/yr total** rate as organic;
  the measured organic rate is **+11.60%/yr**. Applied per-tile, so it is in the parquet.
- **Desktop model** — retuned twice this cycle, ending on the **g01** config
  (`seasonality_regime=multiplicative, cps=0.1649, cpr=0.814, recent=17, ncp=40`).
  The first retune (s01) lifted the summer trough by +1,359,887 for only +5,642 at
  Dec-15; g01 then bought a further −186,860 at the Aug-25 trough for −31,357 at Dec-15.
- **Mobile model** — July's lock with **one** change: `prophet_changepoint_range`
  0.75 → 0.725 (`cps=0.035, ncp=25, recent=13, sps=0.1, regime=auto`). Chosen for seam
  handoff quality (slope kink −66%), not for its +23,907.
- **Iran** — native counterfactual fill applied during training inside the mozaic
  package, so every column is already Iran-inclusive.

The `prior_july` columns are last cycle's published July forecast, carrying **July's
own** headwind spec (−1,345,000 desktop, ramping from 2026-04-01) applied from July's
own seam (2026-07-06). They span the entire window. Because the two cycles use
different ramp conventions, **only the Dec-15 comparison is apples-to-apples** — between
the seam and Dec-15 the two curves sit on different conventions by construction.

---

## `august_dec15_summary.csv`

| Column | Meaning |
|---|---|
| `series` | `Desktop`, `Mobile`, or `ALL`. |
| `measurement_date` | `2026-12-15`. |
| `current_august` | August forecast Dec-15 28d-MA DAU (incl. `h` headwind and, for mobile, the `t` tailwind). |
| `prior_july` | July forecast Dec-15 28d-MA DAU. |
| `delta_vs_july` | `current_august − prior_july`. |
| `delta_pct_vs_july` | Same as a percentage, 3 dp. |
| `summer_trough_min` | Minimum of the current curve over 2026-07-28 .. 2026-10-15. |
| `summer_trough_date` | Date that minimum falls on. |

| Series | Dec-15 | Summer trough | on |
|---|---:|---:|---|
| Desktop | 48,703,443 | 45,220,838 | 2026-08-24 |
| Mobile | 17,924,562 | 17,063,631 | 2026-08-16 |
| ALL | 66,628,005 | 62,331,979 | 2026-08-24 |

**There are no `target` / `vs_target` columns this cycle.** July carried a stakeholder
desktop target of 48,584,362, but **no August target has been set**. Carrying July's
number in a column named `target` would read as an August target, so it is omitted
rather than shipped stale. (The gold low/baseline/stretch markers on the plots are
June-cycle aspirational benchmarks, reused only so the vertical scale stays comparable
across cycles — they are likewise not August targets.)

The trough columns are new this cycle. The summer trough is the near-horizon KPI the
desktop retunes were adopted to move, so a summary reporting only Dec-15 would omit the
largest desktop change of the cycle.

⚠️ **The trough now sits INSIDE the seam-splice zone, and is convention-dependent.** With the seam at
2026-08-02 the `display_ma` transition runs 2026-08-02 → 2026-08-29, so the first date whose 28-day
window is entirely forecast is **2026-08-30**. The published desktop minimum falls on **2026-08-24**,
inside that zone, and reads **−64,483** below a plain `rolling(28)` (which troughs at 45,285,321 on
08-25). At the previous 07-28 seam this was deliberately avoided by scoring Aug-25, which was then a
full window past the seam.

**Dec-15 is unaffected** — it is byte-identical under both conventions, as is everything from seam+27
onward — so no headline number is in question. But **do not quote `summer_trough_min` as a measured
minimum** until the scoring date is settled. The splice-immune alternative is 2026-08-30
(desktop 45,312,866 · mobile 17,145,428 · ALL 62,458,294). For continuity with earlier August builds,
Aug-22 reads Desktop 45,269,694 · Mobile 17,105,137 · ALL 62,374,831.

---

## The `WIN10_HEADWIND_REMOVED` files

Two extra files answer one question: **what would the August desktop forecast read if the Win10
migration headwind (`h`) were not applied at all?** They exist because `h` is an
[unfalsifiable forward judgement](../adjustments/_index.md) whose anchor moved four times in 48
hours, so the size of its contribution to the headline is worth being able to see directly.

| | file | rows × cols |
|---|---|---|
| curves | `august_canonical_curves.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.csv` | 365 × 4 |
| summary | `august_dec15_summary.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.csv` | 1 × 10 |

### ⚠️ These are NOT the published forecast

Nothing in these files should ever be quoted as an August figure. The published desktop Dec-15 is
**48,703,443** (in `august_dec15_summary.csv`). These files read **50,018,443**, which is that
number plus the full 1,315,000 headwind anchor.

Three defences against mixing them up, all deliberate:

1. **The filename** says `WIN10_HEADWIND_REMOVED`.
2. **Every value column** carries the `_NO_WIN10_HEADWIND` suffix, so a column pulled out of
   context still says what it is. There is no column named `desktop_current_august` in these files —
   a script written against the canonical schema will `KeyError` rather than silently read the
   counterfactual.
3. **The summary carries the amounts that were added back** (`win10_headwind_added_back_august`,
   `win10_headwind_added_back_july`), so a reader can recover the published figures by subtraction
   without opening another file.

### Columns

`august_canonical_curves.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.csv`:

| Column | Meaning |
|---|---|
| `date` | daily, 2026-01-01 → 2026-12-31 |
| `desktop_actuals` | **unchanged** — byte-identical to the canonical file. Actuals are telemetry and carry no adjustment |
| `desktop_prior_july_NO_WIN10_HEADWIND` | July's delivered forecast with **July's own** headwind (−1,345,000, ramping from 2026-04-01, applied from July's 2026-07-06 seam) stripped |
| `desktop_current_august_NO_WIN10_HEADWIND` | August's forecast with the **August** headwind (−1,315,000, ramping from the 2026-08-02 seam) stripped. Blank before the seam |

Mobile and `ALL` columns are **absent by design** — this view was requested desktop-only, and
`all_*` cannot be carried without mobile. August's other display-layer spec, the `t` mobile
tailwind, has `desktop_dau: 0`, so it never touched these curves.

### What the comparison to July becomes

Both forecast columns are stripped, so the August-vs-July comparison stays like-for-like — but it
is **a different number from the published one**:

| | published (`h` applied) | `h` removed | why they differ |
|---|--:|--:|---|
| August desktop Dec-15 | 48,703,443 | **50,018,443** | +1,315,000 |
| July desktop Dec-15 | 48,585,483 | **49,930,483** | +1,345,000 |
| August − July | **+117,960** | **+87,960** | the two anchors differ by 30,000 |

That last row is the useful reading: **30,000 of the published +117,960 desktop delta is the
headwind attenuation from July's anchor to August's**, not forecast movement. It matches the
`h` line in the attribution ledger above exactly.

Note the seam behaves as the re-anchoring predicts: on 2026-08-02 the August column is
**identical** to the published one (the ramp is 0 at the seam), and diverges by one day of ramp
(1,315,000 / 135 = 9,741 DAU/day) thereafter. July's column diverges immediately at its own seam,
because July's spec still uses the old 2026-04-01 ramp start and so switches on at 123/258 of full
value.

Summer trough moves too: **45,435,134 on 2026-08-24**, versus the published 45,220,838 on the same
date. The convention-dependence warning under
[§ `august_dec15_summary.csv`](#august_dec15_summarycsv) — the trough sits inside the seam-splice
zone — applies here identically, so do not quote it as a measured minimum either.

### What is *not* removed

Only `h`. The desktop curve still carries `l` (launch-on-login, 200K/day ceiling) and `o`
(MozillaOnline migration). Those are per-tile bidirectional overlays **baked into the forecast
parquet**, so they are not reversible at the display layer — removing them would need a model
re-run, not arithmetic.

### Provenance

Produced by `scripts/export_desktop_no_headwind_csv.py`, which reads the two published CSVs plus
`../adjustments/headwind.json` and `../../2026-07/adjustments/headwind.json` and subtracts each
ramp back out. It does **not** read the parquets and does **not** modify July's frozen artifacts.

```bash
python scripts/export_desktop_no_headwind_csv.py            # write both files + verify
python scripts/export_desktop_no_headwind_csv.py --dry-run   # print Dec-15 only
```

Because `h` is applied to the 28-day MA in the notebook's `[compute-series]` and never enters the
training frame, `published − ramp` is exact — no re-run required. The one caveat: the published
columns are already rounded to integers, so each reconstructed value can sit up to **1 DAU** from
the true unrounded pre-headwind curve. Deriving from the published file rather than the parquet is
the deliberate trade — it guarantees the output is exactly the published number minus the
documented adjustment, which is the property a reader checks by hand.

The script asserts, after re-reading what it wrote: the actuals column is unchanged; adding each
cycle's ramp back reproduces the published columns to ≤1 DAU; the prior-July column is untouched
before July's seam; the August column starts at the seam; subtracting the
`win10_headwind_added_back_*` columns reproduces the published Dec-15 figures **exactly**; and no
mobile or `ALL` column is present. It also raises if the adjustments directory ever grows a
non-headwind spec that moves desktop, so the file cannot become mislabelled by a future spec edit.

---

## The `EX_IR_CN` files

Four files giving the desktop forecast with **Iran and China removed**, in both headwind states.
IR and CN are the two markets whose desktop numbers are driven by something other than ordinary
product performance — a national internet shutdown and a distribution-partner migration — so the
ex-IR/CN scope is what to look at when the question is "how is the rest of the world doing."

| | `h` applied | `h` removed |
|---|---|---|
| curves | `august_canonical_curves.DESKTOP_ONLY.EX_IR_CN.csv` | `…WIN10_HEADWIND_REMOVED.EX_IR_CN.csv` |
| summary | `august_dec15_summary.DESKTOP_ONLY.EX_IR_CN.csv` | `…WIN10_HEADWIND_REMOVED.EX_IR_CN.csv` |

Each is 365 × 4. Column names carry the scope, and the headwind state where it applies:

```
date
desktop_actuals_EX_IR_CN                                  # both files, identical
desktop_prior_july_EX_IR_CN                               # h-applied file
desktop_current_august_EX_IR_CN                           #   "
desktop_prior_july_NO_WIN10_HEADWIND_EX_IR_CN             # h-removed file
desktop_current_august_NO_WIN10_HEADWIND_EX_IR_CN         #   "
```

The actuals column carries only `EX_IR_CN` in both files, deliberately: actuals never carry an
adjustment, so tagging them `NO_WIN10_HEADWIND` would assert something meaningless. Unlike the
world no-headwind file, actuals here **do** change — the scope applies to them too.

### ⚠️ Read this first: ex-IR/CN, August is BELOW July

This is the headline result of the scope, and it is the opposite sign to the published number.

| | published (world) | ex-IR/CN |
|---|--:|--:|
| August Dec-15, `h` applied | 48,703,443 | **45,785,757** |
| July Dec-15, `h` applied | 48,585,483 | **45,847,274** |
| **August − July** | **+117,960** | **−61,517** |
| August − July, `h` removed | +87,960 | **−91,517** |

**China accounts for essentially all of it.** Decomposed at Dec-15 on the 28-day MA:

| country | August | July | delta |
|---|--:|--:|--:|
| CN | 2,003,699 | 1,828,286 | **+175,412** |
| IR | 913,988 | 909,922 | +4,065 |
| ex-IR/CN | 47,100,757 | 47,192,274 | **−91,518** |
| world | 50,018,443 | 49,930,483 | +87,960 |

(Pre-headwind, so the two anchors don't muddy it; the identity closes exactly:
−91,518 + 179,477 = +87,960.)

So the published *"August is +117,960 above July"* is, ex-China, **August is below July**. Two
things to keep straight when quoting this:

- **What drives CN's +175,412 is not decomposed here.** Candidates are the `o` MozillaOnline
  overlay behaving differently under the later seam and fresher CN telemetry. This file measures the
  *size* of the CN contribution, not its cause — do not attribute it to `o` on the strength of these
  files alone.
- The world figure is not wrong. Both are true statements about different scopes; the ex-IR/CN one is
  just the one that answers "is the product growing."

Summer trough, for completeness: **42,615,232 on 2026-08-24** (`h` applied) / 42,829,528 (`h`
removed). Same convention-dependence caveat as
[§ `august_dec15_summary.csv`](#august_dec15_summarycsv) — the trough sits in the splice zone.

### Excluding CN also removes ~93% of the `o` MozillaOnline tailwind

This is a consequence of the scope, not a choice. `o` is allocated by **fixed geo shares that are
~93% CN** and is baked into the forecast parquet per-tile, so dropping the CN tile drops nearly all
of it. These curves are **not "world minus 5.8%"** — they are also curves with almost no
MozillaOnline migration in them. Given `o` is a China-distribution-partner artifact that is
[~4–5 weeks stale](#read-this-before-quoting-the-headline), that is arguably the point of the scope,
but it must be stated whenever these are circulated.

Two smaller consequences, both benign:

- **Excluding IR removes the counterfactual gap-fill from training and the shutdown crater from
  actuals**, so the ex-IR/CN series is entirely real telemetry. The actuals line loses its
  Mar–May 2026 dip.
- **`l` (launch-on-login) is allocated by trailing-DAU share**, so IR/CN's slice of it leaves
  automatically. No special handling.

### The headwind is applied at its full, unscaled anchor

The `h`-applied files carry **−1,315,000** (August) and **−1,345,000** (July) — the same anchors as
the world files, *not* scaled down by IR/CN's 5.83% share. Two reasons:

1. **The Win10 mechanism was measured ex-IR/CN in the first place** (see
   [`../adjustments/_index.md`](../adjustments/_index.md): *"Measured on Win10 + Win11 combined,
   Apr-1 → Jul-22 ex-IR/CN"*), so the anchor is already an ex-IR/CN quantity. If anything it is the
   *world* application that is loose.
2. `scripts/score_near_horizon.py`'s `ex_cn_ir` scope already does exactly this, so the two agree.

No geo allocation of `h` exists and none was invented. The summaries carry
`win10_headwind_applied_*` / `win10_headwind_added_back_*` so the amount is on the face of each file
and the pair cross-checks by subtraction.

### Why these need the parquets

Two reasons this could not be derived from the published CSVs:

1. Those files carry only world (`country == "ALL"`) totals.
2. It cannot be done by subtracting per-country 28-day MAs either, because `display_ma`'s
   variance-matched seam splice is **non-linear** — subtracting MAs differs from the MA of the
   ex-IR/CN daily series by up to **~2,900 DAU** inside the splice window. The daily series is
   differenced first and `display_ma` recomputed on the result.

`ALL − IR − CN` is exact because `ALL` reconciles to the sum of its 16 country tiles: measured
residual **3.7e-08 DAU** (August) / 3.0e-08 (July), i.e. pure float64 noise. Mozaic reconciles
top-down, so this is a property of the build and is asserted, not assumed.

### Provenance

```bash
python scripts/export_desktop_ex_ir_cn_csv.py            # write all four + verify
python scripts/export_desktop_ex_ir_cn_csv.py --dry-run   # print Dec-15 only
```

Reads the August build (`../desktop_g01_2026-08-02/`) and July's delivered build
(`../../2026-07/desktop_locked/`) via `load_forecast(require_state=["l","o"])`, plus both cycles'
`headwind.json`. It reuses the ramp helper from `export_desktop_no_headwind_csv.py` rather than
adding a seventh `linear_ramp` implementation to the five
[already documented as diverging](../adjustments/_index.md).

**Actuals come from the parquet's `training` rows**, which is valid only because those rows are real
telemetry — verified via the sharpest available probe, IR's shutdown crater (~6K DAU on 2026-03-01;
the counterfactual fill is smooth there). Consequence: **the actuals column ends 2026-08-01, one day
earlier than the two published files**, whose column came from BigQuery. Asserted, not just noted.

The load-bearing check is that **the world curve rebuilt from each parquet reproduces the published
CSV** — that is what licenses the whole parquet → `display_ma` → ramp path, leaving the country
subtraction as the only difference. The script also asserts that the two scopes differ by exactly
the `h` ramp on every row, that actuals are identical across the pair, that IR and CN are materially
gone, and that each summary re-derives from its own curves file. Tests:
`tests/test_export_desktop_ex_ir_cn_csv.py`.

---

## Why some columns are blank in part of the year

- The **`*_current_august`** columns are the new forecast, drawn forecast-only: they
  start on the forecast date (**2026-08-02**) and are empty before it.
- The **`*_actuals`** columns are observed data and stop before the file was generated
  (telemetry lands with a lag — the export uses a
  `CURRENT_DATE("America/Los_Angeles") - 2` cutoff; last actual day is **2026-08-01**).
  The LA-time clamp matters: plain `CURRENT_DATE()` is UTC, so after ~5pm PDT it bumps
  to tomorrow and `-2` lands on a still-partial day, which poisons the trailing MA.
- The **`*_prior_july`** columns span the entire window.

This is expected — leave the gaps as gaps when plotting.

---

## Seam smoothing (forecast 28dMA)

The forecast 28-day moving averages are smoothed across the actuals→forecast seam. A
naive trailing 28dMA blends raw actuals into its window for the first 27 forecast days,
which makes the curve oscillate for ~a month before settling. The export uses the
variance-matched `display_ma` from `src/mozaic_daily/seam_ma.py`. **Every date from
forecast-start +27 days onward (including the Dec-15 headline) is byte-identical to the
naive average** — only the visual seam transition changed.

Two seam artifacts were fixed for this cycle, and it is worth knowing what changed
because the curve now *looks* different at the seam:

1. **The headwind step** (−564,262 in earlier charts) was the ramp-convention bug
   described above. Re-anchoring to the seam removed it.
2. **A +102,595 upward display artifact** in the MA reconstruction (`Fix A`,
   2026-07-29): the trend estimator deseasonalized with a 7-day centered mean computed
   on the forecast only, which at the seam degenerates to a weekday-only forward window
   and read ~10% high. The fix divides by the forecast's own day-of-week profile before
   smoothing. Residual distortion on this build is **+102 DAU**.

**The curve therefore leaves the actuals heading slightly DOWN (−107,445), and that is
correct** — the old upward step was masking a real decline. The model's own plain 28d-MA
steps −107,547, so the display now tracks it to ~100 DAU. Dec-15 and everything from
seam+27 onward were byte-identical before and after the fix.

Full diagnosis: `research/ma-seam-turbulence/LOG.md` § Fix A, and
`../seam_fix_before_after.ipynb`.

---

## Minimal plotting recipe

```python
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

df = pd.read_csv("august_canonical_curves.csv", parse_dates=["date"]).set_index("date")
millions = FuncFormatter(lambda v, _: f"{v/1e6:.2f}M")

for platform in ["desktop", "mobile", "all"]:
    cols = [c for c in df.columns if c.startswith(platform)]
    ax = df[cols].plot(figsize=(14, 6), title=f"2026 {platform.title()} DAU — 28-day MA")
    ax.set_ylabel("DAU")
    ax.yaxis.set_major_formatter(millions)
    plt.tight_layout()
    plt.show()
```

`df[cols].plot` skips NaNs automatically, so blank early-year cells render as gaps.

---

## Provenance / regenerating

Generated by the `# [csv-export]` cell in `../august_canonical_v2026-07-28.ipynb`.
Both platforms share the forecast seam **2026-08-02** (trained through 2026-08-01).

- **Forecast parquets (current August):**
  - Desktop: `../desktop_g01_2026-08-02/cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/mozaic_daily_forecast.2026-08-02.ld-D.adj-lo.parquet`
    (legacy_desktop DAU; **g01** params; overlays `l` at the 200K ceiling + `o`).
  - Mobile: `../mobile_cpr0725_2026-08-02/cps0.035_thresh055_recent13_cpr0.725_ncp25_clip0.6_sps0.1/mozaic_daily_forecast.2026-08-02.gm-D.adj-p.parquet`
    (glean_mobile DAU; July's lock with `cpr` 0.725; overlay **`p`**, whose measured split was rebuilt
    for the 2026-08-01 training end).
  - `new_profiles` is not exported here — these are DAU-only curves.
- **Prior forecast (July):** `../../2026-07/desktop_locked/` and
  `../../2026-07/mobile_refresh_2026-07-06/`, with July's own headwind spec from
  `../../2026-07/adjustments/`.
- **Adjustment specs (display layer):** `../adjustments/` — **both files in that
  directory are summed**, with no date gate:
  - `headwind.json` (`h`): desktop **−1,315,000** / mobile −27,162 at the 2026-12-15
    anchor, ramping from 2026-08-02.
  - `tailwind.json` (`t`): desktop 0 / mobile **+299,000** at the same anchor and ramp
    start (2026-08-02). Net mobile at Dec-15: **+271,838**.
- **Actuals:** `telemetry.active_users_aggregates` (desktop) and
  `glean_telemetry.active_users_aggregates` (mobile) in BigQuery, through
  `CURRENT_DATE("America/Los_Angeles") - 2`.

Each daily series is converted to a 28-day moving average; the **net** `h` + `t` ramp
value at each date is then added to the forecast MA over the forecast horizon. The mobile
`p` split and the desktop `l`/`o` overlays are applied per-tile inside the forecast, so
they are already in the parquets.

### Checks that run as assertions, not eyeballs

The producing notebook aborts rather than emitting a plausible wrong number:

1. **Config lock** — each sidecar's model config is compared field-by-field against its
   lock (desktop: **g01**; mobile: July's with `cpr` 0.725, 8 params), plus desktop's four
   holiday knobs against package defaults (they are excluded from tuning by policy).
2. **State markers** — `load_forecast(..., require_state=["l","o"])` / **`["p"]`** pins
   which adjustments must be present. A run whose spec date failed to match would have
   emitted `.raw.` and fails loudly here.
3. **Prior-curve reproduction** — July's delivered Dec-15 values (48,585,483 /
   17,923,869) are hardcoded and the rebuilt prior curve must match within 1,000 DAU.
   Both reproduce at **drift 0**, which is what licenses quoting the deltas at all.
4. **Attribution-ledger closure** — the six pinned desktop steps must sum to the measured
   Dec-15. Residual is **−0**. The mobile decomposition (base → re-lock → tailwind →
   published) is likewise printed and asserted to close, so the discretionary share of the
   mobile number can never become implicit.
5. **CSV round-trip** — both files are re-read from disk after writing and their Dec-15
   values re-checked against the in-memory curves, and the forecast columns are asserted
   to start exactly at the seam.

Re-run with:

```bash
# Rebuilds csv/august_canonical_curves.csv + csv/august_dec15_summary.csv
# and all nine plots under plots/. Needs BigQuery access for the actuals.
source .venv/bin/activate
jupyter nbconvert --to notebook --execute --inplace \
  data-official/2026-08/august_canonical_v2026-07-28.ipynb
```
