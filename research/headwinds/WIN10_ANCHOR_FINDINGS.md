# Win10 headwind anchor — magnitude validation

Analysis: [`win10_anchor_validation.ipynb`](win10_anchor_validation.ipynb) ·
Data + SQL: [`extracts/`](extracts/) · Plots: [`plots/`](plots/)
Read-only: no specs changed, no forecasts re-run.

## Verdict

**On choosing between −1,295,000 and −1,270,000: the data cannot distinguish them, and no honest
analysis of this telemetry can.** Over the ramp's elapsed portion the two differ by ~11,000 DAU
(all three live candidates span 21,705) against a specification envelope **1,488,293** wide — a
noise-to-signal ratio near 69:1. Attenuating −1,295,000 → −1,270,000 is neither supported nor
refuted by telemetry. It is a judgement call, and it should be labelled as one rather than presented
as data-driven.

**On whether either magnitude is defensible: the ramp's already-elapsed portion is contradicted.**
Both candidates — and the −1,245,000 now live at HEAD — require ≈**−540,000 to −562,000** of
transition-attributable net loss to have already accrued between 2026-04-01 and 2026-07-22 (112 of
258 days = 43%). **Not one of 90 specification variants reaches that.** The most pessimistic reaches
−388,058; the median is mildly *positive* (+134,926). Central estimate: **≈ 0**.

So the answer is *not* "shave another 25,000." It is that the **linear-ramp-from-2026-04-01
parameterisation is the wrong shape** — it front-loads loss that has not happened. The Dec-15 total
may still be defensible as a forecast of *future* acceleration, but that is an unfalsifiable forward
judgement, not a measured quantity.

| Anchor | Implied by 2026-07-22 | Inside observed envelope? |
|---|---|---|
| −1,295,000 (prior cycle) | −562,171 | **No** |
| −1,270,000 (proposed) | −551,318 | **No** |
| −1,245,000 (live at HEAD) | −540,465 | **No** |
| observed | −388,058 … +1,100,235 (median +134,926) | — |

## How it was measured

The crux was respected: Win10 → Win11 migration is DAU-neutral, so the falling Win10 curve is not the
headwind. Everything is measured on **Win10 + Win11 combined**. On the same basis as the rest of this
analysis (28d MA, ex-IR/CN, Apr-1 → Jul-22) Win10 alone shed −1,814,214, Win11 — supposedly the
destination — shed −957,418 because summer drags on everything, and the two together shed −2,771,631,
in a period when Mac/Linux fell −7.75% against the combined cohort's −7.15%. Reading the Win10 curve
as the headwind would overstate it several-fold.

Legacy telemetry only (`firefox_desktop_derived.active_users_aggregates_v4`, the desktop-only table
behind `telemetry.active_users_aggregates`; the view's mobile UNION defeats partition pruning —
1.39 TB vs 515 GB). Scope matches production exactly: `app_name = 'Firefox Desktop'`, all countries.
The extract reproduces the supplied controls exactly (2026-07-27 Win10 15,663,337 / Win11 26,300,886 /
desktop total 51,669,205), asserted in-notebook.

Three corrections were necessary and materially change the answer:

1. **The Apr→Jul window is dominated by summer seasonality, not attrition.** The raw combined delta
   is −2.77M, but Mac/Linux (zero Win10 exposure) fell −7.75% against the test cohort's −7.15%. The
   naive delta is uninformative; difference-in-differences is mandatory.
2. **IR and CN must be excluded.** Iran's shutdown recovery (2026-05-26) adds **+772,102** and the
   MozillaOnline CN migration **+782,065** of modern_windows DAU inside the exact measurement window
   — **+1.55M** of confound against a −540K signal.
3. **The `l` and `o` tailwinds are in the actuals** and were netted out (+222,723). At the time of this
   analysis the active `l` spec was an intermediate ceiling, not the 165K stated in the brief —
   verified by SHA1 against the baseline forecast's sidecar.

   ⚠️ **Stale as of 2026-07-30.** The `l` ceiling has since been raised to **200,000** and every lower
   curve was deleted, so the +222,723 netted here reflects a curve that no longer exists. The netted
   value would now be larger. This does **not** change the finding: the conclusion is that the live
   candidates span ~21,705 DAU against a ~1,488,000 specification envelope, so a shift of this size in
   the `l` netting is far inside the noise floor the analysis already establishes. Left as measured
   rather than recomputed — historical analyses are not regenerated.

Two independent counterfactuals, with the 2026 side measured in all-week 28-day MA (the units the
anchor and the overlay curves are expressed in) and 2025 contributing only a unit-free percentage:

| | counterfactual | excess vs it |
|---|---|---|
| Win10+Win11 underlying | −7.73% | — |
| own cohort, 2025 same window (pre-EOL) | −7.84% | **+43,378** |
| Mac/Linux, 2026 concurrent | −7.75% | **+7,219** |

All three land near −7.8%. The Mac/Linux control is the methodologically cleaner one (same year, same
calendar, no cross-method step) and is tighter across the sweep: median +49,701, range
[−325,603, +311,032].

An independent check with no cross-cohort control agrees: the cohort's year-over-year decline was
−5.77% at the ramp start and −4.99% at the window end, i.e. **+0.78pp better**. A real −1,245,000
anchor requires it to have deteriorated by **−1.39pp**.

## What this does not show — read before quoting the number

- **Win10 attrition is not cleanly separable from organic desktop decline with this data.** The claim
  here is narrow and negative: there is no *excess* decline in the Win10-exposed cohort relative to
  its own prior-year seasonal analogue or to concurrent Mac/Linux. It is **not** a claim that
  attrition is zero.
- **A flat YoY rate has two readings.** Either attrition is absent, or it is steady and already
  embedded in the ~5% YoY decline Prophet trains on. Under the second reading the correct additional
  headwind is still ~0, because Prophet already extrapolates the embedded rate forward. The anchor is
  only justified if attrition **accelerates** beyond that. Nothing in four months of data supports
  acceleration — and nothing rules out future acceleration either. That part is genuinely a forecast.
- **The interval is wide and endpoint-sensitive.** The YoY series swings −2.78% to −7.15% over six
  months, wider than the 1.39pp effect being tested, and dips momentarily *to* the anchor-implied
  level in early June before recovering. The direction of the estimate depends on the window
  endpoint. This is why the envelope spans ~1.5M and why no point estimate is offered.
- **Scope.** Measured on modern_windows ex-IR/CN; the anchor is applied to total desktop DAU. Since
  the mechanism is Win10-specific this is the right cohort, but the mapping is not exact.
- The **winX** (Win7/8/8.1) cohort is *not* a usable seasonal counterfactual — it decays ~2× faster
  (−15.9% vs −6.6% over the window) and is excluded from the headline range. It is informative as a
  bound: a genuinely abandoned Windows cohort sheds twice what Win10+Win11 sheds, in both years, and
  Win10+Win11 is not trending toward it.

## Secondary: the seam step double-counts (flagged, not fixed)

`apply_net_adjustment_to_series` applies the ramp only where `index >= forecast_start`, at its
**already-ramped** value. With the live −1,245,000 the ramp reads 0 on 2026-07-27 and −569,419 on
2026-07-28, so the composite takes a **−569,419 step on day one of the forecast**. (The brief cited
≈−592,000, which corresponds to the superseded −1,295,000.)

This is wrong under either reading:

- If the Apr→Jul loss *is* in the training actuals, Prophet has fitted it into the level at the seam,
  and subtracting the ramped value again **double-counts** it.
- This analysis finds the loss is *not* in the actuals — so the step instead asserts a ~569K
  instantaneous drop with no empirical support at all.

If the Dec-15 magnitude is retained on forward-looking grounds, moving `start_date` to the forecast
seam would both remove this discontinuity and stop the ramp asserting unobserved historical loss.
Not implemented — flagged per instructions.

## Reproducing

`extracts/win_version_dau.csv` (515 GB scan) and `extracts/win_version_dau_ircn.csv` (31 GB; `country` is the
table's first clustering key, so the filtered pull is cheap) are committed with their SQL, so the
notebook runs end-to-end without touching BigQuery.
