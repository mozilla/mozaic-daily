# Investigation log — per-country 28dMA early-horizon turbulence

Append-only. One entry per hypothesis: statement → test → result → verdict. **Refuted
hypotheses are kept, not pruned** — they are the audit trail for the HTML report's
"what it is NOT" section.

**Phenomenon:** per-country desktop DAU forecasts, plotted as 28-day moving averages,
oscillate for ~1 month after the forecast-start date then smooth out. Horizon-anchored
(not calendar-anchored): the April forecast wobbles Apr–May; the June forecast wobbles
late-May–Jun (where April is already smooth). Strong for small noisy countries (AR),
absent for large smooth ones (US). Source plots:
`data-official/2026-06/csv/per_country/plots/desktop/<CC>.png`.

All Phase-1 tests read only the canonical parquet
`data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-26.ld-D.raw.parquet`
(daily per-country `dau`, `data_type` ∈ {training=raw actuals, forecast=model median}).
No model re-run, no BigQuery. Tools: `diagnose_seam.py`, `weekly_amplitude.py`.

---

## H1 — "Overfit conditional weekly seasonality" (the first exploration's theory) — **REFUTED**

*Statement:* the `weekly_recent` conditional seasonality (`mozaic/models.py:91-137`,
`recent_weeks=13`) overfits to noisy recent data and is propagated into the forecast,
producing the oscillation.

*Test / reasoning:*
- **Physics:** a 28-day trailing MA = exactly 4 weeks, so it *cancels* a stationary
  period-7 cycle regardless of amplitude. A Prophet additive/multiplicative weekly term
  is periodic with **constant** amplitude into the future — it cannot produce a *damping*
  oscillation, and it cannot survive a 28-day MA at all.
- **Empirical:** the forecast-only 28dMA (no actuals in the window, `ma_forecast_only`)
  is smooth at steady state for *every* country (e.g. AR forecast-only |2nd diff| =
  1,874 ppm and visually smooth; US = 839 ppm), confirming the weekly cycle *is* cancelled
  once the window is fully inside the forecast.

*Verdict:* **Refuted as the cause of the MA wobble.** Conditional seasonality is real and
does shape the daily forecast (see H3/H4), but it cannot itself produce the plotted
28dMA turbulence.

## H2 — MA-window straddling the actuals→forecast seam — **CONFIRMED (proximate cause)**

*Statement:* the plotted forecast 28dMA is computed (`export_canonical_curves.py`
`daily_to_28ma`) over a continuous daily series = raw-actuals training rows + forecast
rows; only afterward are pre-seam dates blanked. So the first 27 forecast-dated MA points
each blend trailing **raw actuals** with forecast. The wobble is that blend transient.

*Test (`diagnose_seam.py`):* compare blended-MA |2nd diff| over forecast weeks 0–3 vs the
forecast-only MA, per country; AR vs US decisive figures (`plots/decisive_AR.png`,
`plots/decisive_US.png`).

*Result:*
- Wobble is confined to forecast days 0–27 — **exactly the 28-day MA window length** —
  then the curve merges with the smooth forecast-only MA.
- blend / forecast-only ratio: **2.7–9.2×** for noisy countries (BR 5.6×, CN 7.3×, DE 7.7×,
  RU 7.6×, MX 9.2×), **~1×** for smooth ones (US 1.1×, CA 1.0×, JP 1.3×). Median 4.0×.
- US control: identical MA method, large daily weekly swings in *both* actuals and
  forecast, yet **no wobble** — so the method isn't wrong per se; the seam *contents* matter.

*Verdict:* **Confirmed.** The wobble lives in the MA window while it straddles the seam,
not in the daily forecast itself. This is a display/measurement artifact in the sense that
the steady-state MA and Dec-15 are unaffected.

## H3 — Weekly-pattern discontinuity at the seam is the country-dependent driver — **CONFIRMED (dominant: amplitude)**

*Statement:* the 28dMA cancels a *stationary* weekly cycle, but during the seam transition
the window holds (actuals weeks) + (forecast weeks). If the weekly *pattern* (amplitude,
phase, or level) differs across the seam, cancellation is incomplete → a weekly-period
residual that slides as the window advances = the wobble.

*Test (`weekly_amplitude.py`):* weekly peak-to-trough amplitude (% of mean) of the last
90 days of actuals vs the first 90 days of forecast, per country
(`plots/weekly_amplitude.csv`).

*Result:* the forecast weekly amplitude is **damped relative to recent actuals** for the
wobbly countries, and **matched** for the smooth ones:
- AR 61.1% → 40.9% (0.67×), BR 79.5% → 44.8% (0.56×), IN 62.9% → 34.2% (0.54×),
  IT 66.0% → 41.1% (0.62×), ID 64.0% → 41.1% (0.64×), MX 72.3% → 51.0% (0.71×).
- US 31.1% → 32.3% (1.04×), CA 24.1% → 25.9% (1.07×), JP 29.1% → 28.8% (0.99×).
The amplitude mismatch magnitude correlates with the H2 blend turbulence.

*Caveat / partial:* **CN** has matched amplitude (58.0% → 59.7%, 1.03×) yet still wobbles
(blend/fc-only 7.3×). So amplitude mismatch is the *dominant* but not the *sole* seam
discontinuity channel — a smaller level/phase-discontinuity contributor exists. Does not
change the verdict or the fix (a display-side fix handles all channels uniformly).

*Verdict:* **Confirmed dominant driver.** The damped forecast weekly amplitude (vs higher
actuals amplitude) for high-day-of-week-swing countries is what makes the seam transition
visible in the 28dMA.

## H4 — Why the forecast weekly amplitude is damped (model-level) — **PLAUSIBLE, out of scope for the fix**

*Statement:* the fitted weekly seasonality under-represents true weekly amplitude for
high-swing countries because `fourier_order=3` (`_add_conditional_weekly_seasonality`)
plus a tight `seasonality_prior_scale=0.00825` (desktop, `models.py:156`) cannot reproduce
a near-square weekday/weekend drop (AR weekends ≈ 50% of weekdays), and regularizes the
amplitude down.

*Test:* observed directly in the daily forecast median (`decisive_AR.png`: orange daily
swings ~370K–412K, narrower than the actuals' Fri≈460K vs Sun≈224K swing).

*Verdict:* **Plausible underlying enabler, but explicitly out of scope for remediation:**
it does **not** affect the steady-state 28dMA or Dec-15 (the weekly term cancels), so the
stakeholder curve does not require a model change. Recorded for completeness.

## Refuted/eliminated secondary candidates (seeded list)

- **Logistic cap/floor curvature** (`tail(426)`, `models.py:187-196`): the forecast-only
  MA is smooth and monotone from the seam onward (no early bend toward a cap) → not the
  wobble source.
- **Reconciliation per-date `delta`** (`core.py:131-166`): would inject structure into the
  daily forecast itself and thus the forecast-only MA, which is smooth → not the source.
- **Median-of-1000-`predictive_samples` jitter** (`models.py:208-216`): would be
  high-frequency daily noise, annihilated by the 28dMA; the wobble is ~weekly-period and
  seam-localized → not the source.
- **Partial / bad last training day** (AR +86% seam step): checked — AR's last training
  day is a Monday (241K) and weekends are ~50% of weekdays (Sat 279K / Sun 224K vs
  Fri 460K). Real day-of-week swing, not a data defect. The +86% metric is a DOW red herring.

---

## VERDICT (Phase-1 gate)

**Primarily an MA-seam artifact (H2), enabled by a weekly-pattern discontinuity at the
actuals→forecast seam (H3, dominantly a damped forecast weekly amplitude; CN shows a
smaller level/phase channel).** The daily forecast is sound at steady state, the 28dMA
and the Dec-15 headline numbers are unaffected, and US/CA are correctly smooth. The fix
is therefore **display-side** (Phase 2A) — make the plotted forecast curve smooth across
the seam without touching the model or the far-horizon values. Model-level weekly
amplitude damping (H4) is noted but out of scope.

---

## Remediation & verification

**Fix (display-side, `data-official/2026-06/export_canonical_curves.py`):** new `display_ma()`
replaces the naive `daily_to_28ma` for all *forecast* curves (current June at `FORECAST_START`,
prior April at `PREV_FORECAST_START`, both ALL-level and per-country; actuals series keep the
plain MA). It computes the forecast-only 28dMA (no actuals in the window) and, for the first 27
forecast days where that is undefined, draws a **linear bridge** from the seam value (= trailing
actuals MA, continuous with the displayed actuals line) to the first clean forecast-only point.
User chose the bridge over a +27-day gap (stakeholder-facing; no gap).

**Why far-horizon is safe:** for dates ≥ seam+27 the window is entirely forecast, so the
forecast-only MA equals the old blend exactly → those dates (incl. Dec-15) are byte-identical.

**Verification (`verify_fix.py`, parquet-only):** AR/BR/IN seam |2nd diff| (wk 0-3) drops to 0
(linear bridge), US/CA unchanged; Dec-15 delta = 0.0 and *all* dates ≥ seam+27 byte-identical
for AR, US, BR, IN, CA. Regenerated CSVs + per-country PNGs: AR/grid now smooth, US visually
unchanged (`data-official/2026-06/csv/per_country/plots/desktop/`).

**Regression lock:** `tests/test_export_canonical_curves.py` (5 tests) — synthetic seam with a
deliberate weekend-amplitude drop; asserts the naive blend *does* wobble (anti-tautology), the
bridge is smooth, the seam anchor is continuous, the actuals region is untouched, and every
date ≥ seam+27 (incl. Dec-15) is byte-identical. All pass.

**Report:** `report.html` (self-contained) — executive summary, before/after AR & US, the
"what it is NOT" refuted list, the amplitude-mismatch root-cause chart (CN flagged as the
level/phase exception), the fix, and the regression test.

**Pre-existing unrelated failures:** `tests/test_forecasting.py::test_get_forecast_dfs_returns_metric_dataframes`
and `tests/test_queries.py::test_additional_holidays_only_on_legacy_desktop` fail on this branch
independent of this work (they exercise `src/mozaic_daily/{forecast,queries}.py`, untouched here;
GLEAN_MOBILE/additional-holidays WIP state). 208 pass.

---

## Phase 2B — variance-matched seam transition (replaces the linear bridge)

**Why the linear bridge was rejected.** The Phase-2A straight bridge is zeroth-order: it draws a
line from the seam anchor to the first clean forecast-only MA point, ignoring the forecast's true
trend *curvature* over those 27 days. It is guaranteed to diverge from realized data whenever the
forecast trend bends during the transition. User asked for a transition that follows the forecast's
real trajectory.

**The fix (Option A — variance-matched transition).** `display_ma` now factors the seam region
into `reconstruct_matched_daily(pre, fc, forecast_start, window)`:
1. `trend_fc = fc.rolling(7, center=True, min_periods=4).mean()` — deseasonalized forecast trend.
2. Recent-actuals multiplicative day-of-week profile over the last 13 weeks (mozaic's recency
   window): detrend actuals by the same 7d centered MA, take the daily ratio, `groupby(dayofweek).mean()`,
   normalize present buckets to **mean 1**, fill any missing weekday with 1.0.
3. `matched = trend_fc * dow_act[weekday]` — the forecast trend carrying the *actuals'* weekly amplitude.
Then `transition_ma = concat([pre, matched]).rolling(28).mean()` for forecast days 1..27; day 28
onward stays the clean forecast-only MA (the **splice constraint** — byte-identical to before, Dec-15
exact). Both sides of the seam now share the same weekly amplitude, so the trailing 28d window
cancels the weekly cycle and the transition rides the true forecast trend (curvature and all).

**Level-preservation algebra.** Over an aligned 28-day window (4 of each weekday) with `trend_fc≈T`:
`Σ trend_fc·dow_act[wd] ≈ T·4·Σ_wd dow_act[wd] = T·28·mean(dow_act) = T·28` since mean(dow_act)=1.
So the window mean = T = the trend; the reconstruction does not bias the level. The **mean-1
normalization is load-bearing** (validated to ~0.01% on the real June parquet for smooth DOW).

**April backtest (the decision gate, `backtest_seam.py`).** Realized April actuals live in the June
parquet's training rows (no BQ). Decision metric = bias-removed (shape) MAE vs realized over April
forecast days 1..27. Per the user (2026-05-29), **the gate is decided on the global ALL-level
transition** (the headline deliverable), not per country:
- **Desktop ALL: shape MAE 302,518 → 90,660 (+70.0%)**, raw 568,479 → 112,578 (+80.2%). PASS.
- **Mobile ALL: shape MAE 54,885 → 44,790 (+18.4%)**, raw +/−. PASS (sanity).
- Per-country (diagnostic, non-gating): 10/15 desktop markets improve, often dramatically
  (AR +69%, ID +93%, MX +86%, ROW +76%, US +73%, CA +65%, DE +47%, IT +48%, PL +42%, BR +18%);
  mobile improves across the board.

**Known limitations (v1 — documented, not chased; this is a global-curve bandaid, not a per-country fix):**
- *Per-country shape regressions* (desktop IN −89%, CN −21%, FR −26%, JP −36%, RU −4%): in these
  markets the **April forecast's own trend curvature diverged from what happened**, so the
  variance-matched transition faithfully tracks a curve that was wrong in hindsight, while a straight
  line happened to sit closer to the (smooth, monotonic) realized decline. NEW's *raw* MAE for IN
  actually improves (28.8k → 18.2k); only demeaned *shape* worsens. These cancel in the ALL aggregate.
- *Per-country splice kink*: for the highest-swing countries the day-27 hand-off to the forecast-only
  MA leaves a small step — AR ~0.99%, BR ~0.74% of level. The global hand-off is smooth (ALL ~0.086%,
  US ~0.031%). A seam-aware centered trend (computing `trend_fc` on the concatenated pre+fc series)
  was tested and **rejected** — it *worsened* the global splice (ALL 0.086% → 0.698%) while only
  helping BR/IN, a net loss for the headline curve.
- No winsorization of the 13-week DOW estimate in v1.

**Tests (`tests/test_export_canonical_curves.py`, 7 pass).** Kept: naive-blend-wobbles (anti-tautology),
far-horizon byte-identical (Dec-15), actuals-region-unchanged. Loosened: seam-anchor continuity (small
step, not byte-equal). Replaced: smooths-the-seam (relative `display_ppm < blend_ppm/3`, no absolute
floor — a curved transition has nonzero 2nd diff). Added: splice smoothness across days 26→27→28→29
(step < 1% level, local 2nd diff < 2%); curved-trend-beats-a-straight-line (variance-matched tracks a
curved realized MA better than a line between endpoints, with a guard that the truth is genuinely curved).

**Verification (all pass):** `pytest tests/test_export_canonical_curves.py -v` (7/7);
`verify_fix.py` (Dec-15 delta 0, all dates ≥ seam+27 byte-identical); `backtest_seam.py` (global gate
PASSED); regenerated `report.html`, ALL + per-country CSVs, per-country PNGs; notebook re-run with
`display_ma` imported from `export_canonical_curves` (single source of truth) — Dec-15 unchanged
(46,893,112 / 47,834,362 / 16,911,773 / 17,511,100).

---

## Phase 3 — the reconstruction edge bias (2026-07-30)

Context: `HANDOFF_recon_edge_bias.md`. The August s01 desktop curve steps **+102,595 up** at
the seam. Cause located: `trend_fc = fc.rolling(7, center=True, min_periods=4).mean()` is
forecast-only, so at the seam it averages the first four forecast days — for a Tue seam,
four consecutive weekdays. Harness: `recon_variants.py` (estimator variants + a fidelity
assertion that `current` reproduces the shipped function exactly),
`eval_recon_edge_fix.py`, `diagnose_splice_metric.py`, `eval_splice_correction_load.py`,
`backtest_recon_variants.py`, `check_delivered_numbers.py`. All read-only, no BQ, no re-run.

### H5 — a DoW-complete forward-7 window at the incomplete edge fixes it — **REJECTED on the stated criteria, but the criteria are partly wrong**

*Statement (handoff §5):* keep the estimator forecast-only — so it cannot re-trigger the
day-27 regression that killed the concatenated-trend fix — but make every window
day-of-week complete: where the centered window is short (positions 0-2 and the last 3),
use a one-sided 7-day mean.

*Result against the handoff's 7 acceptance criteria:*

| # | criterion | verdict |
|---|---|--:|
| 1 | Aug-25 / Dec-15 byte-identical | **PASS** — exactly +0 on both builds; trough minimum and its date also unchanged (45,193,561 @ 2026-08-25) |
| 2 | \|seam step\| reduced | **FAIL** desktop: 102,595 → 112,730. Mobile mixed (June 16,574→10,903, July 9,405→1,466, Aug −1,498→−8,212) |
| 3 | day-27 splice not worsened | **FAIL** — June's own metric: ALL 0.085% → 0.394% |
| 4 | June/July delivered numbers reproduce | **PASS** — all six to within 0.42 DAU |
| 5 | tests pass / tighten tolerance | n/a, no change made |
| 6 | `backtest_seam.py` gate | **could not run** — April parquets archived to GCS; substituted a 4-seam realized backtest (below) |
| 7 | notebooks re-executed | n/a |

*But criteria 2 and 3 are each calibrated against a coincidence, and I wrote them:*

- **Criterion 2 is unachievable by fixing the bug.** The shipped +102,595 is
  (plain-MA step −108,884) + (reconstruction bias +211,479). forward7 returns −112,730,
  i.e. within 3,846 of the plain 28d rolling MA's step. The bias was *masking* a genuine
  −109K downward step in the s01 model. Removing the artifact cannot shrink \|step\|; it
  reveals the true value. Same on mobile: August's −1,498 was bias +7,619 against a plain
  −9,117, and forward7 exposes −8,212. Shrinking the step now requires either a
  compensating display hack or a model change — not a bug fix.
- **Criterion 3's 0.086% baseline is itself a cancellation.** `diagnose_splice_metric.py`
  decomposes the visible day-27 step as `visible = -landing + one-day slope`. June desktop
  ALL under the shipped estimator: landing **−0.445%**, slope **+0.360%**, visible
  −0.085%. The handoff looks smooth only because a large landing error nearly cancels the
  curve's genuine one-day slope. Under forward7 the landing error collapses to **+0.032%**
  (14× better) — and precisely because it no longer cancels the slope, the *visible* step
  grows to +0.394%. The slope-invariant corner on the uncorrected curve agrees with
  `visible` (ALL 0.011% → 0.490%), because at a single junction a level step and a slope
  change are not distinguishable from the displayed series alone.

  Metric validated before use: `visible` reproduces every published June figure — ALL
  0.085% (LOG: ~0.086%), AR 0.981% (~0.99%), BR 0.716% (~0.74%), US 0.031% (~0.031%), and
  the rejected concat fix at 0.696% (~0.698%).

- **In the configuration that actually ships, criterion 3 is neutral.** `continuous_splice`
  (added July, after June's rejection) forces the transition onto the forecast-only MA, so
  the shipped handoff corner is ALL 0.2850% → 0.2882% (+1.1%). The bend the correction must
  apply moves the *opposite* way by cycle: August s01 238,537 → 44,199 (5.4× less
  distortion), June ALL 200,712 → 358,987 (worse, slope-residual dominated).

*Realized backtest — cannot discriminate.* April is archived, so scored four `.raw.`
June-cycle desktop seams (2026-05-17 Sun, 05-21 Thu, 05-26 Tue, 05-28 Thu) against real
actuals from the raw 2026-07-28 build's training rows, using June's bias-removed shape MAE
and ALL-level gate. Mean ALL shape MAE: current 343,662 vs forward7 342,234 — **0.4%,
noise**, and the sign flips by seam: forward7 wins on the Sun and Tue seams, loses on both
Thu seams, i.e. exactly along the bias-regime split. Side finding, not chased: on these four
May seams *every* variance-matched variant is ~8% **worse** than the OLD straight bridge,
against April's +70%. Consistent with the known "levels up, refit lower" divergence in the
May builds; it does not bear on the estimator comparison.

*Weekday dependence — the strongest evidence for the fix, and in none of the criteria.*
Sweeping the seam across seven consecutive days on the s01 build, the shipped estimator's
bias at the seam ranges **−5,586,815 (Mon) to +4,428,872 (Fri)** — the sign flips because a
Mon/Tue seam samples four weekdays and reads high, while a Thu/Fri seam takes 2 of 4 days
from the weekend and reads low. The published seam step therefore spans **380K** purely as a
function of which weekday the cycle happens to start on (+126,139 Mon … −253,905 Thu).
forward7 narrows that to **79K** (−33,265 … −112,730), a 4.8× reduction.

*Verdict:* forward7 is the correct estimator — it is DoW-complete for a seam on any weekday,
it collapses the landing error 14×, and it removes a ±5M weekday artifact — but it **fails
the stated criteria 2 and 3**, and the realized backtest cannot tell it apart from the
status quo. Paused for a human decision rather than shipped: the criteria need to be reset
onto quantities that are not cancellations, and criterion 2 in particular has to be
abandoned or reinterpreted, because the seam step it asks to shrink is real.

### H6 — the same `min_periods=4` defect biases the 13-week day-of-week profile — **CONFIRMED, second-order**

*Statement (handoff §6):* `recent_trend = recent.rolling(7, center=True, min_periods=4).mean()`
runs on `recent` *after* slicing to 13 weeks, so 3 rows at each end are detrended by a
DoW-incomplete window and their ratios still enter `dow_act`.

*Test:* compute the detrending mean on `pre` (which extends years further back) *before*
slicing, so every ratio comes from a full 7-day window (`recon_variants._dow_profile`).

*Result:* real but small. Max profile shift 0.0107 (June desktop Mon −0.0102 / Tue +0.0107,
~1%); August desktop 0.0083; June mobile 0.0012. On top of forward7 it leaves Aug-25,
Dec-15 and the trough byte-identical and moves the seam step 13,608 (−112,730 → −99,122).

*Verdict:* **Confirmed** — same defect, same one-line class of fix, worth taking with H5
since it is free. Not load-bearing on its own.
