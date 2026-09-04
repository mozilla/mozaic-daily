# `research/mobile-organic/` — organic-only mobile forecast + external paid layer

Topic directory for the 2026-08 mobile methodology change: stop modelling paid acquisition as
an *increment since an anchor* (`m`) and start **measuring** it from the client-level
paid/organic flag, forecasting organic only, and stacking marketing's paid **level** on top
(`p`, `paid_organic_split`).

The production machinery lives in `src/mozaic_daily/organic.py` and
`data-official/{YYYY-MM}/organic/`. **This directory holds the evidence and the arguments** —
the probes that licensed the design, the reproduction against the external prototype, and the
open methodological question about the paid seam.

## Why the change

`m` treats paid as "incremental DAU since 2026-03-30". Desktop's `l` and `o` can do that
honestly because launch-on-login and the MozillaOnline migration have hard start dates. Mobile
paid acquisition has run continuously for years, so the anchor is an accounting choice and
everything downstream inherits it. Two consequences:

- The bidirectional overlay **absorbs 58%** of any change to the curve: a −141,653 change in the
  Dec-15 lift moved the mobile KPI only −59,875, because a smaller subtraction leaves Prophet a
  higher organic trend that partly offsets the smaller add-back. *A lift-curve comparison is not
  a KPI estimate.*
- Marketing's source carries no cohort dimension, so the lift understates post-anchor cohort DAU
  by an amount that source cannot measure.

Under `p`, paid contributes **exactly its own value** with no Prophet interaction — the same
property `h` has, and the reason `h` needs no model re-run.

## Origin: the external prototype

`~/work/product-data-science-core/scratch/brwells/mobile_organic_aug/` — read its `SPEC.md`
before quoting anything. It is **read-only from here**; we do not port it, we reproduce against
it. It fits three global single-tile Mozaics (Fenix organic ex-IR, other apps ex-IR, Iran masked)
and stacks marketing's paid level on the sum.

### Reproduction target

From that project's `variants/all_apps/_index.md`, the **4-app / canonical-params** row — the one
whose scope matches production (`Fenix, Firefox iOS, Focus Android, Focus iOS`):

| quantity | value |
|---|---|
| organic incl. Iran, Dec-15 28d MA | **16,215,080** |
| paid level, Dec-15 (anchor 922,250.47 + lift 637,226.74) | **1,559,477** |
| **total, Dec-15 28d MA** | **17,769,950** |

Its 6-app headline (17,769,730) differs by only −224 — Klar is 37 DAU/day and immaterial.

**These numbers were produced at `holiday_threshold = -0.032`, not the `-0.055` the config
claims.** The prototype's `fit_organic_forecast` never forwards `holiday_threshold` /
`holiday_max_radius` / `holiday_min_radius` to `populate_tiles`, nor `holiday_effect_floor` to
`Mozaic`; production forwards all four. Its reported "holiday_threshold effect = 0.00%" is
therefore tautological. `reproduce_prototype.py` runs a bug-for-bug arm and a plumbed arm, writing
`reproduction_results.json`.

## Phase 0 probe results (2026-07-31)

All four probes ran read-only against BigQuery. **Every one came back favourable.**

### 1. The two `active_users_aggregates` tables are identical for mobile

The prototype reads `mozdata.telemetry.active_users_aggregates`; production reads
`moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates`. Over 2026-07-01..07-27:

| app_name | glean_telemetry | mozdata.telemetry | delta |
|---|--:|--:|--:|
| Fenix | 347,969,250 | 347,969,250 | **0** |
| Firefox iOS | 99,710,442 | 99,710,442 | **0** |
| Focus Android | 7,136,731 | 7,136,731 | **0** |
| Focus iOS | 5,960,152 | 5,960,152 | **0** |
| Klar Android / iOS | 695 / 420 | 695 / 420 | **0** |

**This residual term is exactly zero.** (Mobile has no Legacy equivalent — both candidates are
Glean-backed, so the standing Legacy-only rule has no applicable alternative here.)

### 2. Relabelled traffic is already excluded

`Fenix MozillaOnline` (2,057,931 over 27 days ≈ **76.2K/day**) and `Fenix BrowserStack` (496) are
**separate `app_name` values in both tables**, so production's
`app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS")` filter already drops them —
matching how the mirror's source view (`mozdata.fenix.active_users`) relabels them. Mirror share
and production level are therefore computed over the same population.

### 3. Mirror health — `moz-fx-data-bq-data-science.brwells.fenix_dau_growth_source_v1`

- Coverage **2024-06-01 → 2026-07-01** (761 days), 248 countries, `date × country ×
  growth_source` grain. **A 26-day tail extension is required** to reach the August
  `training_end_date` of 2026-07-27.
- Partition present and populated: `organic` / `paid_rolling_12mo` / `paid_prior_1yr`.
- Expires **2027-04-01** — which is why the per-cycle pinned parquet, not the table, is the
  pipeline's input.

**The organic share is strongly time-varying**, ex-IR, monthly:

| month | organic | paid | | month | organic | paid |
|---|--:|--:|---|---|--:|--:|
| 2024-06 | 0.98898 | 1.10% | | 2025-10 | 0.94599 | 5.40% |
| 2024-12 | 0.97458 | 2.54% | | 2026-01 | 0.93210 | 6.79% |
| 2025-06 | 0.95766 | 4.23% | | 2026-04 | 0.91367 | 8.63% |
| 2025-08 | 0.95388 | 4.61% | | 2026-07 | **0.88420** | **11.58%** |

Iran runs **98.7–99.9% organic** throughout, confirming the prototype's ~99% figure and the
decision to exclude IR from the split.

**Consequence for the pre-2024-06 backfill — better than feared.** At the oldest measured month
paid was only **1.10%**, so holding that share flat back to 2020-12-31 bounds the error at
**≤1.1pp of Fenix DAU**, and the true early paid share was almost certainly *lower* (the ramp is
monotone), so the residual is smaller still. The induced artifact is at most ~1.1pp of apparent
extra growth spread over 3.5 years (~0.2pp/yr), against a `changepoint_range` of 0.75 that makes
the extrapolated slope a recent-history quantity. Truncating training to 2024-06-01 instead would
cost Firefox iOS and Focus 3.5 years of history and land ~55 days above Prophet's
730-observation yearly-seasonality gate. **Flat backfill is the cheaper assumption by a wide
margin** — but it is still an assumption, and it is stated in `data-official/*/organic/_index.md`.

### 4. Shredder drift reproduces against the production level source

Mirror total vs `glean_telemetry` `app_name='Fenix'`:

| date | mirror | production | rel gap |
|---|--:|--:|--:|
| 2024-06-01 | 9,160,189 | 9,429,437 | **−2.855%** |
| 2024-12-01 | 10,055,568 | 10,287,120 | −2.251% |
| 2025-06-01 | 10,460,425 | 10,655,022 | −1.826% |
| 2025-12-01 | 11,850,490 | 11,993,192 | −1.190% |
| 2026-04-01 | 11,586,328 | 11,648,395 | −0.533% |
| 2026-06-25 | 12,969,783 | 12,969,783 | **0.000%** |
| 2026-07-01 | 13,235,380 | 13,235,380 | **0.000%** |

Monotone, and **−2.855%** matches the prototype's −2.8554% measured against the other table. The
exact zeros at the trailing edge are a second confirmation of finding 2: the two sources cover
the same Fenix population once shredder attrition has not yet bitten.

**This is why the split takes the *share* from the mirror and the *level* from the production
query.** Taking the level from the mirror would read as ~+3.3pp of fake growth.

### 5. Firefox iOS has no paid signal — A1 confirmed, and strengthened

`mozdata.firefox_ios.new_profile_clients`, monthly:

| month | new clients | gclid-labelled | Adjust network | Adjust paid | Apple Search Ads |
|---|--:|--:|--:|--:|--:|
| 2024-08 | 3,669,680 | **0** | 2,242,021 | 105,896 | 100,774 |
| 2024-09 | 3,555,677 | **0** | 28,746 | 130 | 121 |
| 2025-07 | 3,688,515 | **0** | 7,490 | 38 | 36 |
| 2026-06 | 3,814,892 | **0** | 3,700 | 4 | 4 |
| 2026-07 | 3,660,533 | **0** | 3,139 | **0** | **0** |

- `paid_vs_organic_gclid` is **100% NULL in every month** — gclid is a Play-Store mechanism and
  does not exist on iOS.
- Adjust attribution collapsed in **September 2024** and never recovered.
- Over the last 12 months, paid-attributed clients run **0–51 per month out of ~3.6M** —
  under **0.0015%**.

So there is no usable signal to subtract, and modelling iOS as 100% organic is the only option
the data supports. **This measures attribution, not spend** — absence of signal is not absence of
spend, so the ask to marketing stands and is recorded as open below.

## Reproduction result (2026-07-31) — PASSES, and the residual is fully attributed

`reproduce_prototype.py` rebuilds the prototype's *recipe* (three global single tiles, summed, with
marketing's paid level stacked on) using **mozaic-daily's own pinned inputs and code**. Run it with:

```bash
source .venv/bin/activate
python research/mobile-organic/reproduce_prototype.py --arm both \
    --json research/mobile-organic/reproduction_results.json
```

### Headline

| arm | organic | paid | total | vs prototype 17,769,950 |
|---|--:|--:|--:|--:|
| **buggy** (holiday knobs suppressed — prototype-equivalent) | 16,112,985 | 1,559,477 | **17,672,462** | **−97,488 (−0.55%)** |
| plumbed (knobs forwarded, as production does) | 16,122,594 | 1,559,477 | 17,682,071 | −87,879 (−0.49%) |

**`holiday_threshold` −0.032 → −0.055 is worth +9,609 (+0.05%)** on this series. The prototype
reported 0.00% for it; that was tautological, since it never plumbed the knob through at all.

**Paid reproduces exactly: 1,559,477**, from anchor 922,250.47 + lift 637,226.74. Pinned by
`tests/test_organic.py::test_real_august_marketing_level_matches_the_published_anchor_and_lift`.

### Where the −97,488 lives: entirely in the Iran tile

| tile | ours (buggy arm) | prototype | delta |
|---|--:|--:|--:|
| **ex-Iran organic** | 15,517,619 | 15,509,650 | **+7,969 (+0.05%)** |
| **Iran** | 595,366 | ~705,200 | **−109,834 (−15.6%)** |

**The ex-Iran halves agree to 0.05%.** That is the substantive result: across two independently
written pipelines, different tile architectures and different repos, the organic series and its fit
land within 8K of each other on a 15.5M base.

The Iran gap is a **known, measured definitional difference**, not an error. The prototype's Iran
tile is `IR-geo + Farsi-locale-outside-IR`, masked; ours is `IR-geo`, corrected by mozaic's
built-in 2026 counterfactual fill. Measured directly (Phase 4a probe, 28-day mean at training end):

| | DAU/day |
|---|--:|
| IR-geo (our Iran tile) | 554,463 |
| Farsi-locale outside IR (theirs, not ours) | **89,677** |
| prototype's Iran = IR-geo + fa | 644,140 |

**The Farsi-locale transfer alone accounts for ~82% of the 109,834 Iran gap.** The remainder is
consistent with fill-vs-mask, which the prototype independently measured at +1.98% on Iran (≈12K).

**We cannot close this term, and should not try.** Production's mobile `QuerySpec` has no `locale`
column, so the reassignment is unavailable without adding a dimension to the production query. It
also *cancels in the global total* by construction — the term the ex-IR series subtracts is the
term the Iran series adds — so it only perturbs how the two tiles are split and fitted.

### Terms measured at zero (so they are not hiding anything)

| term | measured |
|---|--:|
| `mozdata.telemetry` vs `glean_telemetry` AUA, all mobile apps | **exactly 0** |
| per-country share vs one global share, **on the global total** | **+9 DAU** |
| Klar Android + Klar iOS (in their 6-app scope, not our 4) | −224 (their own grid) |

The per-country/global result is worth reading carefully: a single global share gives the same
*global* organic level (because `Σ y_c·share_c ≈ (Σ y_c)·global_share` when the level and share
sources cover the same population). It differs materially **per tile**, which is what matters for a
per-country forecast — paid intensity runs 0.2% in RU against 27.6% in ID.

### The other half: tile architecture

| | Dec-15, pre-headwind |
|---|--:|
| prototype (3 global tiles) | 17,769,950 |
| our global-tile arm | 17,672,462 |
| **our production build (16 countries × 4 apps, reconciled)** | **17,628,317** |

So **−44,145 (−0.25%)** separates the global-tile arm from the per-country production build. That is
the tile-architecture-plus-reconciliation term, isolated. Post-headwind the published number is
**17,601,155**.

**Verdict: pass.** Every term is named and measured; the unexplained remainder is under 0.05% of the
total, well inside the 0.25% bar.

## Open items

- **Confirm iOS paid spend with marketing.** The only remaining way to test A1. If material spend
  exists it is missing from *both* halves of `organic + paid`, so the total is unaffected but the
  attribution between them is wrong for ~21.5% of mobile DAU.
- **The paid seam — DECISION REQUIRED.** Training rows get the *measured* paid added back (so they
  stay equal to raw actuals); forecast rows get *marketing's modelled* level. They disagree by
  **−36,674 (−0.21% of total)** at the 2026-07-28 seam. ⚠️ **Re-measured at the refreshed 2026-08-02
  seam this collapses to +1,903 (+0.01% of total)** — see the note at the end of this section, which
  changes how urgent this decision is. `paid_seam_methods.ipynb` contrasts three treatments
  and **recommends method 1 (the honest splice, what ships today)** — but this is a recommendation,
  not a decision, and it is waiting on you.

  The decisive measurement is that **the disagreement is not a constant**: over the 177-day
  historical overlap the ratio moves **−8.1%**, from **+11.6%** early to **+3.5%** late. An offset
  would be fitting a fixed calibration to a moving relationship, so it is not licensed by the data.
  The direction is reassuring — the two series *converge*, because marketing's early-2026 curve is
  nearly all forecast while its recent weeks are actuals (UAC through 2026-07-20, Meta through
  2026-07-13). The value that matters at the seam is the late one.

  **The step does not wash out of the 28-day MA** — it ramps in over 28 days and then persists at
  full size for the rest of the horizon, Dec-15 included. So this is a headline decision worth
  36,674, not a cosmetic one: methods 1 and 3 keep marketing's level and land on the same Dec-15,
  method 2 substitutes a continuation of our own measurement and lands 36,674 lower (all at the
  2026-07-28 seam). Plot:
  `plots/paid_seam_three_methods.png`, `plots/paid_measured_vs_marketing_overlap.png`.
- **Productionize the mirror.** It is a scratch table expiring 2027-04-01. The pinned per-cycle
  parquet insulates the pipeline, but the *producer* still depends on it.

## What the change was worth

The August mobile rebuild moved Dec-15 from **17,864,732** (`m`) to **17,601,155** (`p`),
**−263,577 (−1.48%)**. Same parameters, same raw data, same marketing curve — so the move is
entirely the methodology.

**It is the removal of a double-count.** `m`'s lift is identically zero before its 2026-03-30
anchor, so the training series it handed Prophet still contained all the paid growth from 2024-06
onward; Prophet extrapolated that as *organic* trend, and the add-back then layered paid on again.
Measured on the training window:

| | annualised growth, 2024-06-28 → 2026-07-27 |
|---|--:|
| **total** mobile DAU (what `m` effectively extrapolated) | **+16.12%/yr** |
| **organic** mobile DAU (what `p` extrapolates) | **+11.60%/yr** |

Paid went **99,938 → 1,478,129** over that window — a 14.8× increase, from 0.8% to 8.7% of mobile
DAU. Under `m` most of that ramp was being read as underlying growth.

The near term moves the *other* way: **+39,957 at Aug-25**, because `p` restores the full measured
paid to recent training rows. It is the extrapolated slope that flattens, not the level.

## Contents

- `_index.md` — this file: the evidence, the probes, the reproduction verdict.
- `reproduce_prototype.py` — re-runnable harness. Rebuilds the prototype's three-global-tile recipe
  on mozaic-daily's pinned inputs, in a bug-for-bug arm and a plumbed arm.
- `reproduction_results.json` — its output, plus the separately measured terms.
- `paid_seam_methods.ipynb` — the deferred seam decision. Ends in a human go/no-go.
- `build_paid_seam_notebook.py` — regenerates that notebook from cell sources, so it can be edited
  and re-run without hand-editing ipynb internals.
- `plots/` — `paid_seam_three_methods.png`, `paid_measured_vs_marketing_overlap.png`.

## Where new code goes

Re-runnable machinery → `src/mozaic_daily/organic.py` (with a test in `tests/test_organic.py`).
Per-cycle data artifacts → `data-official/{YYYY-MM}/organic/`. Analysis narrative and anything
that argues for a choice → a notebook here.

### ⚠️ Re-measured at the 2026-08-02 seam: the step is now ~zero (2026-08-03)

The 2026-08-03 data refresh moved the seam 2026-07-28 → 2026-08-02 and rebuilt the measured split
for the new training window. The paid seam step — marketing's first forecast day minus our last
measured day — is now **+1,903 DAU, +0.01% of total mobile DAU**, down from −36,674 / −0.21%.

**This substantially de-risks the open decision.** The three treatments differ by roughly the size of
the step, so at +1,903 they are all within noise of each other and none of them can move Dec-15
materially. The decision should stay open on principle — a future refresh could widen the gap again,
and the *mechanism* (our measurement handing over to marketing's model) is unchanged — but it is no
longer a blocker on publishing mobile.

**The step must be re-measured every refresh, and the measurement must be seam-derived.** A
hardcoded `2026-07-27` in the canonical notebook survived the refresh and silently reported
−41,798 (a six-day-offset comparison) instead of the true one-day step. Fixed 2026-08-03 by deriving
`LAST_MEASURED_DAY = MOBILE_FORECAST_START - 1 day`.
