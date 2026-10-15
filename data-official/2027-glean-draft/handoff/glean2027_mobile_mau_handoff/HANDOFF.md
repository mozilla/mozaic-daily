# HANDOFF — Firefox Mobile MAU forecast through 2027 (organic + paid) (PRE-DRAFT)

Generated 2026-10-15 11:59 PDT by `scripts/render_mobile_total_mau_handoff.py` (mozaic-daily `glean-2027-forecast` @ `8fcdc44`, build `mobile_total_mau_2026-10-10`). Data as of 2026-10-09;
paid plan generated 2026-10-14. Owner: Brendan Wells (brwells@mozilla.com). This file briefs an AI agent working with this package; `index.html` is the human version.

## Read this first: this is a PRE-DRAFT, and a very preliminary one

This is an **early, exploratory attempt**, not a forecast anyone has signed off on. It is earlier than the regular monthly forecast drafts: a new metric
(MAU), a new split (organic vs paid), two separately built halves, and judgement calls made over a few days. Treat every number as **"roughly this shape
and magnitude"**, never as a plan, target, or commitment.

When you summarise, quote, chart, or build on anything here, you must:

- **Label it PRE-DRAFT / preliminary** every time, including in tables, slides, and derived files.
- **Quote the all-apps TOTAL only, with its band** (below). Don't present per-app or per-country values as findings, and don't quote the organic or paid
  half on its own without saying it is a half.
- **Not add it to the desktop forecast** (`glean2027_desktop_mau_handoff`): a person active on both devices is counted in both. The side-by-side file next to
  this package exists for comparison, not for summing.
- **Not combine it with other forecasts** (the regular monthly KPI forecast) as if they were one consistent forecast.
- **Check with Brendan before** anything goes into a plan, OKR, deck for leadership, or external doc.

### Why it is preliminary

1. **It is two forecasts added together.** The organic part is our model; the paid part is the marketing data-science team's 2027 spend plan
   (scenario *Optimized for Dec-15 MAU*), taken as delivered. Their plan is a hypothetical (flat weekly spend, no seasonality, no cross-channel
   interaction, which they call an upper bound), and its MAU is paid DAU × 4.792, a global multiplier rather than a model.
   Most of the total's growth in 2027 (+14.0% Dec→Dec) is that plan: paid goes from 8,980,862 at the data edge to
   14,341,804; the organic part grows +7.3%. A different budget decision changes the headline directly
   (the flat-budget scenario gives paid 13,327,527 instead).
2. **Neither half is backtested.** The organic band is the model's own predictive band (uncalibrated); the paid band is the plan's own, which its authors
   call indicative. The total band assumes the two are independent. Read all three as floors on the real uncertainty.
3. **India and Indonesia carry a provisional correction.** Organic Firefox Android there outgrew the same country's Firefox iOS during 2026 as paid share
   rose past 30%, consistent with campaign installs arriving outside the Play Store and being counted organic. The history was split into an authentic part
   (held to the country's own iOS growth; this is what the model forecasts) and an inauthentic part (440,081 MAU at the edge, ~0.8% of
   the world total), which is **carried flat** through 2027 in its own column. Flat is a documented no-information choice (Brendan, 2026-10-15); the question is
   with the marketing DS team and the correction may be reversed or re-sized.
4. **The seam**: the total opens -1.4% below the last week of history (7-day means). The organic model deliberately does not carry all of the
   2026 rise into 2027; the first weeks are the model's view of the underlying level, not a continuation of the last data point.
5. **Per-app numbers are weaker than the all-apps number**: the model reconciles top-down, which flatters the app split. Paid lands on Firefox Android only.
6. **The 2026 Iran shutdown** is bridged in the training data only; the forecast carries Iran as the model sees it.

## The numbers (raw daily value of the rolling 28-day MAU)

| | Total 2026-12-15 | Total 2027-12-15 | Dec → Dec | authentic organic (2027-12-15) | inauthentic organic (flat) | paid |
|---|---:|---:|---:|---:|---:|---:|
| Mobile, all apps | 58,188,189 | 66,327,324 | +14.0% | 51,545,439 | 440,081 | 14,341,804 |
| Firefox Android (Fenix) | 43,756,809 | 50,499,253 | +15.4% | 35,717,368 | 440,081 | 14,341,804 |
| Firefox iOS | 12,316,392 | 13,433,603 | +9.1% | 13,433,603 | 0 | 0 |
| Focus Android | 1,197,278 | 1,428,514 | +19.3% | 1,428,514 | 0 | 0 |
| Focus iOS | 917,709 | 965,954 | +5.3% | 965,954 | 0 | 0 |

**Headline: world mobile MAU on 2027-12-15 ≈ 66.3M (PRE-DRAFT), 90% band 61,554,555 to 71,576,697.**
Of which: authentic organic 51,545,439 (band 46,802,528 to 56,760,479), inauthentic organic 440,081 (flat), paid 14,341,804
(band 13,808,775 to 14,941,206). Read the bands as lower bounds on the real uncertainty.

## Files

| file | contents |
|---|---|
| `glean2027_mobile_mau_forecast.csv` | one row per day, 2020-12-31 → 2027-12-31: `date`, `data_type` (`training` = actual history, `forecast`), then for `world`, `fenix_android`, `firefox_ios`, `focus_android`, `focus_ios` the columns `total_`, `organic_` (authentic), `organic_inauthentic_`, `paid_`; `total = organic + organic_inauthentic + paid` on every row. Plus `total_world_low_90/high_90`, `organic_world_low_90/high_90`, `paid_world_low_90/high_90` on forecast rows. Whole people |
| `glean2027_mobile_mau_by_country.csv` | total per country (all apps), 2024-01-01 → 2027-12-31, plus `world`. Forecast paid is split across countries by each country's share of measured paid over the last 28 days of history |
| `plots/*.png` | world and per-app charts for 2026 and 2027 (grey = prior year, black = history, green = forecast, dotted red = Dec 15) and `mobile_mau_components.png` (organic, inauthentic, paid, total with bands) |
| `index.html` | the human-readable page (same numbers, charts embedded) |
| `../glean2027_mau_desktop_and_mobile.csv` | desktop world MAU (the desktop package's headline) beside mobile total / organic / paid, one row per day. **Do not sum the two devices** |

How to read the CSVs:
- History ends 2026-10-09; filter on `data_type`, not on a date. On history rows total is the real actual and paid is the measured paid.
- MAU is already a rolling 28-day quantity: use the daily value as is and do not smooth it again.
- **Mobile only** (Firefox Android, Firefox iOS, Focus Android, Focus iOS; no desktop anywhere in this package). No adjustments for launches or headwinds; the
  monthly forecast carries those, this one does not. Paid acquisition IS included, via the marketing plan.

## What not to do

- Don't present it as "the 2027 mobile forecast"; it is a preliminary exploration of what a 2027 mobile MAU forecast could look like under one spend plan.
- Don't quote the organic half as "organic growth" without the inauthentic column and the India/Indonesia caveat.
- Don't swap in another paid scenario by hand; ask for a recomposition (the paid columns replace one for one, and the bands must be redone).
- Don't refit, re-tune, or "fix" it from these files. The full build record (`LOCK.md`, `PROCEDURE.html` §15–16, `organic_mau/ios_rate_cap/IOS_RATE_CAP.md`) lives in the
  mozaic-daily repo under `data-official/2027-glean-draft/`; ask Brendan for it.
