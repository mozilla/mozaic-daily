# Why production's mobile forecast reads 17.60M and the `mobile_organic_aug` prototype reads 17.77M

Comparison run 2026-07-31 against
`~/work/product-data-science-core/scratch/brwells/mobile_organic_aug/` (checkpoint 2026-07-31).

## The premise correction first

**The prototype does not reach the target either.** Its 17.77M is quoted pre-headwind, and it
was benchmarked against two references that are not this search's target.

| | value |
|---|--:|
| Prototype, 4 apps / canonical params, pre-headwind | 17,769,954 |
| Mobile headwind at Dec-15 (`h`) | −27,162 |
| **Prototype, post-headwind** | **17,742,792** |
| Target band | 17,873,869 – 17,973,869 |
| **Short of the band floor by** | **131,077** |

So yes — the headwind is small (−27,162), and it does not change the conclusion. Adopting the
prototype's architecture wholesale moves production from 17,601,155 to 17,742,792, closing
**44% of the +322,714 gap** and still missing the band by 131,077.

The prototype's own docs benchmark it against (a) a **17.8M planning target**, which it hits to
0.17%, and (b) mozaic-daily's **17,924,607** — the **`m`-era** number, superseded by the `m` → `p`
swap on 2026-07-31. Neither is the ±50,000 band around July's delivered 17,923,869.

## What is identical

The gap is **not** parameters, data, or paid. All three match:

- **Model parameters** — the prototype pins `constants.MOBILE_MODEL_PARAMS` from
  `data-official/2026-08/mobile_baseline_2026-07-28/<slug>/parameters.json`. Same ten values.
- **Organic definition** — both take organic as the *residual* of the mirror's three-way
  `growth_source` partition (`organic / total`), not the positively-labelled
  `paid_vs_organic_gclid='Organic'`.
- **Shredder correction** — both take the *share* from the client-level mirror and the *level*
  from the canonical aggregate.
- **Paid** — both stack marketing's August curve as a level: anchor 922,250 + Dec-15 lift
  637,227 = 1,559,477. The 4,598 difference in the paid line is MA-of-level vs level-then-MA,
  not a different view of paid.
- **App scope** — prototype 6 apps, production 4; Klar is 37 DAU/day and the prototype's own
  grid puts 4-app vs 6-app at **−224 DAU**. Immaterial.
- **Iran actuals** — both read IR 28d-MA = **554,463** at 2026-07-27. Exact agreement.

So the entire +141,637 is **architectural**.

## Where the gap actually sits

The two projects cut their buckets differently, so the reported ex-Iran / Iran lines are not
directly comparable: the prototype moves **Farsi-locale clients geolocating outside IR** into its
Iran tile, and production leaves them in the ROW country tiles. That population is **89,677**
(28d-MA) at 2026-07-27 and rising through 2026. Re-cutting production the prototype's way:

| | prototype | production | delta |
|---|--:|--:|--:|
| ROW organic (ex-IR, ex-Farsi) | 15,509,650 | ~15,379,554 | **+130,096** |
| Iran organic (IR + Farsi-outside-IR) | 705,200 | ~689,286 | +15,914 |
| **total organic** | **16,215,075** | **16,068,840** | **+146,235** |

**~89% of the gap is in the ordinary ex-Iran organic forecast; only ~11% is Iran.** (The Iran
line uses an assumed ~95,000 for the Farsi population at Dec-15; the split is robust to a wide
range of that assumption, the ROW share never drops below ~85%.)

This is the opposite of the first read. Iran looks like the culprit on the raw bucket numbers
(+110,914) purely because of the Farsi reallocation, which nets to zero at the total.

## The four structural differences

### 1. Tile count and reconciliation — the big one

| | prototype | production |
|---|---|---|
| tiles | **3** global tiles, plain sum | **64** country × app leaf tiles, top-down reconciled |
| composition | `fenix_organic` (ex-IR), `other_apps_organic` (ex-IR), `iran_organic` | every (country, app) pair |

### 2. Volume gating puts them in different model forms — from the same config

mozaic's mobile model is **volume-gated on `max(DAU)` per tile**. Under `seasonality_regime='auto'`
a tile is multiplicative iff `max ≤ 2e6`. Measured on the production parquet:

| tile max DAU | production tiles |
|---|--:|
| < 1e6 | 59 |
| 1e6 – 2e6 | 3 |
| 2e6 – 10e6 | 2 |

**62 of 64 production tiles (96.9%, 64.4% of DAU) take the multiplicative branch.** The
prototype's fit log shows the opposite:

```
fenix         789 obs   linear    additive        ROW
other apps   2037 obs   linear    additive        ROW
iran         1920 obs   logistic  multiplicative  IR   (116 days masked)
```

Its two large tiles are both above 2e6 and get **additive seasonality with linear growth**. So
the identical `MOBILE_MODEL_PARAMS` produce a mostly-multiplicative fit in production and a
mostly-additive fit in the prototype, purely because of how the population is partitioned.

This explains a round-1 result: forcing `multiplicative` moved production only **+17,542**,
because production was already 96.9% multiplicative at leaf level. But the leaf-level picture
turned out **not** to be what drives the headline — see the additive section at the end, which
supersedes this framing.

### 3. Fenix organic's training window differs 2.6×

Both hit the same mozaic constraint — `Mozaic.__post_init__` requires every tile to share one
date grid — and resolved it in **opposite directions**:

- **Prototype**: one Mozaic per app, so Fenix organic trains on its natural window,
  **2024-06-01 → 2026-07-29 (789 obs)**. iOS keeps full history in its own Mozaic.
- **Production**: one Mozaic, common grid **2020-12-31 → 2026-07-27 (2,034 days)**, with Fenix's
  pre-2024-06 organic share **held flat backwards** over 3.5 years (`build_share_lookup`).

`changepoint_range` and `n_changepoints` are both *fractions of the window*, so the same nominal
values mean different things:

| | prototype Fenix | production Fenix |
|---|--:|--:|
| training days | 789 | 2,034 |
| last changepoint at `cpr=0.75` | 2026-01-14 | 2025-03-06 |
| recent history with no changepoint | 197 d | 508 d |
| changepoint spacing at `ncp=25` | ~24 d | ~61 d |

**Caution — the sign does not go the obvious way.** Production's own gradient has
`d(Dec-15)/d(cpr) = −820,815` (raising cpr *lowers* Dec-15), while the prototype reports
cpr 0.82 → 0.75 costing it **−2.07%** (lowering cpr *lowers* Dec-15). The knob has **opposite
sign in the two architectures**, so this difference cannot be reasoned about by analogy — it
has to be measured.

### 4. Iran and the Farsi population

- **Production**: IR is queried natively and the shutdown gap is covered by mozaic's built-in
  counterfactual **fill**, on per-country IR tiles inside the reconciled hierarchy.
- **Prototype**: Iran is a single **independent tile with the 116 outage days masked out of
  training** (written as 0 → NaN → Prophet drops), trained back to 2021-01-01, and summed rather
  than reconciled.

The two agree well where they can be checked: the prototype's like-for-like counterfactual over
the 90-day 2026 outage matches mozaic's fill to **+1.98%**. The forward paths differ more —
prototype Iran grows +9.5% from the seam to Dec-15, production's IR-only grows +7.2% — but as the
table above shows this is only ~11% of the gap.

**One production gap this exposes.** mozaic's Iran fill covers `country='IR'` only. The
Farsi-locale-outside-IR population — 89,677 DAU/day and rising — **collapses in lockstep with IR
during every shutdown** but sits in ROW country tiles with no masking and no fill. Production
therefore trains ordinary ROW tiles on series containing three unexplained craters. That is a
defect-shaped difference rather than a judgement call, and it is worth fixing independently of
this search.

## Why we cannot simply mimic it

1. **It does not reach the target.** 17,742,792 post-headwind, 131,077 below the band floor.
   Mimicking it is not a route to 17.92M; it is a route to 17.74M.
2. **It is a different pipeline, not a different configuration.** The gap is 3-tiles-summed
   versus 64-tiles-reconciled, plus the training-window resolution, plus the Iran treatment.
   None of it is reachable from `MobileModelConfig` — there is no flag that collapses production
   to three global tiles.
3. **Production's architecture is load-bearing for the published KPI.** Per-country × per-app
   tiles are what `validation.py` checks row counts against, what the `ALL MOBILE` training rows
   (byte-identical to raw actuals) are built from, and what every downstream consumer of
   `mart_mozaic_daily_forecast_v2` expects. The prototype is explicit that productionalising it
   was deliberately not done and that the scoping doc's Permanence Checklist has to clear first.
4. **The prototype is not validated as more accurate.** Its own backtest scores the canonical
   and package-default parameter sets as a wash (1.09% vs 1.10% MAPE), and it carries an open
   assumption — all iOS DAU treated as organic — that it flags as the largest in the project.

## The additive test was run — and it sharpened the diagnosis

`--regime additive` was run as a full 11-probe gradient on 2026-07-31. **Effect at Dec-15: −0 DAU.**

Not a failed override: 11 fresh runs, 0 skips, sidecars record `seasonality_regime: additive`, and
**63 of 64 leaf tiles move by 27,552 DAU in total** — while the world headline moves **+1**.

The reason completes the picture above. mozaic reconciles **top-down**
(`metric_mozaics[m].reconcile_top_down()`): the metric-level Mozaic forecasts the **aggregate**
series and rescales the leaves to sum to it. Leaf model form changes *allocation*, not the total.
The aggregate is ~16M, above the 2e6 gate, so the top-level fit is **already additive under
`auto`** — forcing additive cannot move it, while forcing multiplicative flips it (+17,542).

> **Production's mobile headline is, to first order, a single Prophet fit on the aggregate
> organic series.** The 64 leaf tiles set allocation only.

This makes the comparison with the prototype much simpler than "3 tiles vs 64 tiles reconciled".
Both projects' headlines are **aggregate Prophet fits**. The difference that survives is *which
aggregate, over which window*:

| | prototype | production |
|---|---|---|
| what is fit | Fenix organic (789 obs, from 2024-06-01) **+** other apps (2037 obs) **+** Iran (1920 obs, masked), summed | **one** aggregate organic series, 2,034 obs from 2020-12-31 |
| Fenix organic pre-2024-06 | not in the fit at all | **present, synthesised** from a held-flat share for 3.5 years |

So ~63% of production's headline training window contains Fenix organic values derived from a
constant backfilled share, and the prototype's Fenix component contains none of it. That — not
tile count, and not the regime — is the live candidate for the remaining +130,096 in ROW organic.

**It is testable but not cheap**, and the trade-off is the one `build_share_lookup`'s docstring
already names: truncating mobile training to 2024-06-01 would cost Firefox iOS and Focus 3.5
years of history and land ~55 days above Prophet's 730-observation yearly-seasonality gate.
