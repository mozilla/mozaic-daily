# Iran Partial-Recovery Forecast: Methodology (v2, 150k shipping cap)

Procedural reference for the Iran Firefox Desktop DAU forecast under the
post-shutdown tiered-internet regime. Documents *how* to produce the model.
For *why* the cap is 150k, see [`iran_cap_reasoning.md`](iran_cap_reasoning.md).

**Status:** Two-stage hybrid (weekly + cap-pinned logistic) approved by
leadership 2026-05-15. v3 extends with yearly seasonality + holiday cluster
overlays drawn from pre-shutdown Prophet fit (added 2026-05-15+).

**Notebook:** [`iran_partial_recovery_model.ipynb`](iran_partial_recovery_model.ipynb).
Earlier free-cap version retained at `iran_partial_recovery_model.draft1.ipynb`.

---

## 1. Problem framing

- Iran's internet was shut down ~2026-02-28.
- It re-opened in March 2026 under a tiered, gatekept regime ("Internet Pro" +
  "White SIM" tiers); see [`iran_cap_reasoning.md`](iran_cap_reasoning.md) for details.
- The post-shutdown user base is structurally different (vetted professionals,
  paying users). Pre-shutdown Iran data is not transferable.
- The new regime has only weeks of clean data, which is insufficient to fit a
  saturation cap from data alone — and the early-period curvature reflects
  *supply rationing*, not demand saturation.

## 2. Why a multi-stage hybrid

The fundamental tension:

- **Weekly seasonality** wants a *wide* fit window to estimate day-of-week
  multipliers reliably (need multiple cycles of each day-of-week, robust to
  single-day noise).
- **The logistic trend** wants a *narrow* fit window restricted to the
  post-major-supply-expansion period, because the early-period growth curve
  reflects supply mechanics, not demand.
- **Yearly seasonality** wants *multiple years* of fit data — not available
  in the partial-recovery period. We lift it from pre-shutdown Iran data
  and accept the population-mismatch caveat.
- **Holidays** want known event dates and historical patterns at each event
  — also lifted from pre-shutdown years.

The solution: estimate each component on its own appropriate window.

The trick that makes this work: **multiplicative seasonality can be extracted
without any global trend model** — only a local detrend is needed. So in Stage 1
we fit a flexible quadratic-in-log trend purely as a local-detrend device for
the seasonality fit, then discard it. The yearly and holiday components are
similarly fit on detrended pre-shutdown data, then the cap-pinned logistic
(Stage 4) is fit on the fully deseasonalized partial-recovery series.

## 3. Stage 1 — Weekly seasonality via log-space OLS

**Fit window:** `2026-04-10` → most recent date, **excluding 2026-04-14** (visible
outlier; small unstable population in the days before this date).

**Model:**

```
log(DAU_t) = β₀ + β₁ t + β₂ t² + Σ_{d∈{Tue,…,Sun}} α_d · 1[dow(t)=d] + ε_t
```

- Monday is the reference day-of-week (α_Mon = 0).
- The quadratic `β₁t + β₂t²` is a flexible local-detrend; it absorbs the
  supply-driven growth shape so the day-of-week residuals reflect only
  seasonality.
- **The fitted quadratic is discarded** — its forecast properties are
  meaningless (super-exponential blowup). Only the `α_d` are retained.

**Extraction:**

```python
dow_log_effects[Mon] = 0
dow_log_effects[Tue..Sun] = OLS coefficients
weekly_factor_raw = exp(dow_log_effects)
weekly_factor = weekly_factor_raw / geomean(weekly_factor_raw)   # normalize to geomean = 1
```

This produces a `pd.Series` indexed 0..6 (Mon..Sun) with mean(log) = 0,
suitable for multiplicative application.

**Sanity check:** the diagnostic cell plots OLS residuals over time and broken
out by dow per week. Look for:

- Residual std < ~0.15 (less than ±15% in linear space) per week.
- Similar by-dow residual *pattern* across weeks (no week's dow profile should
  look qualitatively different from the others).

## 4. Stage 4 — Cap-pinned logistic on deseasonalized data

> **Note on execution order:** Stage 4 runs *after* Stages 1, 2, and 3 in the
> notebook because its deseasonalization requires all three factors. It is
> presented here (alongside Stage 1) because Stage 1 + Stage 4 form the
> conceptual core of the original two-stage model. Stages 2 and 3 (§§ 6-7
> below) are overlay enhancements that extend the core to handle yearly and
> holiday patterns.

**Fit window:** `2026-05-01` → most recent date. Rationale: April was still
supply-constrained (the "500 Rule" period plus the late-April registration
freeze). May 1 is the cleanest post-major-supply-expansion data we have.

**Deseasonalize first:** `dau_des_t = dau_t / weekly_factor[dow(t)]`.

**Fit:**

```
trend_t = effective_cap / (1 + exp(-k · (t - t₀)))
```

- Only `k` (growth rate) and `t₀` (inflection offset) are free parameters.
- `effective_cap = cap / max(weekly_factor)` — see "Cap convention" below.
- `cap` is **pinned** at three sensitivity scenarios: 100k / 150k / 200k.
- The shipping forecast uses `cap = 150k`. Edit `SHIPPING_CAP` in the setup
  cell to change.

**Bounds and initial values:**

- `p0 = [0.1, 7.0]` (k ≈ 10%/day, inflection ~1 week out)
- `bounds = ([0.001, -50.0], [2.0, 300.0])`

Curve fit is via `scipy.optimize.curve_fit`. Confidence intervals on `k` and
`t₀` come from the covariance matrix's diagonal.

## 5. Cap convention (important)

**`cap` represents the peak weekly forecast at saturation**, i.e., the forecast
value on the highest-seasonality day-of-week once the logistic has saturated.

Why this matters: the seasonal multiplier on the peak weekday is `max(weekly_factor)`
(typically ~1.4). If we set `cap` as the logistic asymptote directly, the peak
forecast would reach `cap × 1.4` — overshooting the demographic ceiling we
intended to enforce.

So instead:

- Logistic asymptotes at `effective_cap = cap / max(weekly_factor)`.
- At saturation: peak-day forecast = `effective_cap × max(weekly_factor) = cap` ✓
- At saturation: trough-day forecast = `effective_cap × min(weekly_factor)`.

This is implemented in `make_pinned_logistic(cap_value)` — the function takes
the user-facing peak cap and divides internally.

## 6. Stage 2 — Yearly seasonality from pre-shutdown Prophet

Iran has only ~2.5 months of partial-recovery data — not enough to fit yearly
seasonality directly. We instead lift the yearly shape from 6 years of
pre-shutdown Iran data (2020-01-01 → 2026-01-07; stops 1 day before the
January 2026 blackout). The lift assumes the *shape* of the annual rhythm
transfers between the pre-shutdown general population and the partial-recovery
Internet Pro cohort, even if amplitudes don't.

### 6.1 Custom Iran holiday calendar

Built locally in `# [iran-calendar]` rather than via
`mozaic.holiday_smart.get_calendar()` because the latter adds `GlobalHolidays`
(Christmas Eve, NYE, NYD) for every country — wrong for Iran. The custom
calendar combines:

- `holidays.IR(years=...)` — Nowruz (4 days), Eid al-Fitr (2 days), Ashura,
  Tasua, Eid al-Adha, Arbaeen, Islamic Revolution Day, 15 Khordad, ~15 more.
- Local `IranHolidaysV2` class — Shab-e Yalda + the 2025 + 2026 internet
  blackouts (kept as outliers so detrend smooths them).

### 6.2 Pre-shutdown Prophet fit

`# [yearly-fit]` runs `mozaic.holiday_smart.detrend()` on pre-shutdown DAU with
the custom calendar, then fits Prophet:

- `yearly_seasonality=True`, `weekly_seasonality=True`, `daily=False`
- `seasonality_mode='multiplicative'`, `growth='linear'`, `cps=0.05`
- Trend and weekly are *discarded* — only the yearly component is kept.

### 6.3 Yearly factor extraction

`# [yearly-extract]` predicts Prophet on reference non-leap year 2023, pulls
`yhat['yearly']` (multiplicative deviation from 1.0), and builds a doy-indexed
`pd.Series yearly_factor` (length 366, leap day interpolated). Normalized so
geomean(log) = 0.

## 7. Stage 3 — Holiday cluster effects from pre-shutdown

`# [holiday-effects]` estimates per-cluster, per-offset multiplicative
reduction factors from pre-shutdown data:

```
factor[cluster, offset] = mean_y(observed_y / detrend_expected_y)
                          on date_y = anchor(cluster, y) + offset
```

Where `detrend_expected_y` is the kinematic counterfactual produced by
`mozaic.holiday_smart.detrend()` — the "expected value if this day were
holiday-free." Factors clipped to ≤1.0 (holidays only decrease usage).

### 7.1 Cluster grouping (priority order)

Holidays are grouped into curated clusters; priority order prevents
double-counting when windows overlap (e.g., when Eid al-Fitr drifts into the
Nowruz window).

| Priority | Cluster | Anchor substring | Offset window | Days |
|---|---|---|---|---|
| 1 | Nowruz | "Nowruz" | -2 to +14 | 17 |
| 2 | Eid al-Fitr | "Eid al-Fitr" | -1 to +2 | 4 |
| 3 | Tasua + Ashura | "Tasua" | -1 to +2 | 4 |
| 4 | Eid al-Adha | "Eid al-Adha" | 0 to +1 | 2 |
| 5 | 15 Khordad | "15 Khordad" | -1 to +1 | 3 |
| 6 | Islamic Revolution Day | "Islamic Revolution Day" | 0 | 1 |
| 7 | Yalda | "Shab-e Yalda" | -1 to +1 | 3 |

Higher-priority clusters claim dates first in both *estimation* (so each
year contributes one ratio per offset per cluster, never double-counted) and
*forecast application* (so each future date receives at most one
holiday-factor lookup).

### 7.2 Holiday factor build

`# [holiday-factor-build]` walks the forecast horizon, finds each year's
cluster anchors in `iran_calendar`, and populates `holiday_factor: pd.Series`
indexed by date (default 1.0).

## 8. Forecast construction

```
forecast_t = trend_t × weekly_factor[dow(t)] × yearly_factor[doy(t)] × holiday_factor[t]
```

Trend is computed from `LOGISTIC_FIT_START` through `FORECAST_END = 2027-12-31`.
Plots are zoomed to `FORECAST_PLOT_END = 2027-01-01` to keep early dynamics
legible.

The Stage 4 logistic fit *deseasonalizes by all three factors* before
`curve_fit`, so trend estimation is robust to yearly variation and holiday
windows that fall inside the fit window.

### 8.1 Cap convention under overlays

The original cap convention (`cap = peak weekly forecast at saturation`)
held when the only seasonal factor was weekly. With yearly and holiday
overlays added:

- `effective_cap = cap / max(weekly_factor)` (unchanged)
- holidays only ever decrease the forecast (hard floor at 1.0), so peak
  non-holiday days are bounded above by `cap × yearly_factor[doy]`
- in months with `yearly_factor > 1`, peak days can slightly exceed `cap`

The 150k shipping cap is therefore a **guideline for typical-month peaks**,
not an absolute ceiling. Observed peaks may run ~5-10% over `cap` in
favorable months without invalidating the cap-pinning argument.

## 9. Outputs (notebook cells)

| Cell | Purpose |
|------|---------|
| `# [setup]` | Constants. `SHIPPING_CAP`, `CAP_SCENARIOS`, fit windows, `HOLIDAY_CLUSTERS`, pre-shutdown window. |
| `# [query]` | BigQuery pull (post-shutdown IR Desktop DAU). |
| `# [raw-plot]` | Linear + log scatter, fit-window markers, Apr 14 outlier highlighted. |
| `# [seasonality-fit]` | Stage 1 OLS + weekly-factor bar chart + parameter table. |
| `# [seasonality-diagnostic]` | Residuals over time + by dow per week. |
| `# [logistic-fit]` | Stage 4 cap-pinned 2-parameter fit at 3 cap scenarios (deseasonalized by weekly × yearly × holiday). |
| `# [iran-calendar]` | Build custom Iran holiday DataFrame. |
| `# [preshutdown-query]` | BigQuery pull of pre-shutdown IR Desktop DAU (2020-01-01 → 2026-01-07). |
| `# [yearly-fit]` | Detrend pre-shutdown + fit Prophet (multiplicative). |
| `# [yearly-extract]` | Extract Prophet's yearly component as doy-indexed `yearly_factor`. |
| `# [yearly-diagnostic]` | Yearly factor curve with Persian-calendar landmarks. |
| `# [holiday-effects]` | Estimate per-cluster, per-offset reduction factors. |
| `# [holiday-effects-diagnostic]` | One panel per cluster; offset-by-offset bars with n-years. |
| `# [holiday-factor-build]` | Build `holiday_factor` date-indexed series across forecast horizon. |
| `# [forecast-plot]` | Matplotlib static plot, three scenarios, shipping bolded. Composition includes overlays. |
| `# [overlay-decomposition-plot]` | Stacked layers (trend / +weekly / +yearly / +holiday) over Nowruz 2027 window. |
| `# [interactive-plot]` | Plotly version, zoomable, hover unified. |
| `# [text-summary]` | Pasteable monospace summary: params, weekly factors, yearly peaks, top holidays, sensitivity, weekly trajectory. |
| `# [fit-start-sweep]` | Robustness check: re-fit at logistic-window-start ±7d; show k and inflection trajectories. |
| `# [sweep-text]` | Text version of the sweep. |

## 10. Configuration parameters (current values)

| Constant | Value | Meaning |
|----------|-------|---------|
| `SHIPPING_CAP` | `150_000` | Peak weekly forecast in typical month (shipping) |
| `CAP_SCENARIOS` | `[100k, 150k, 200k]` | Sensitivity scenarios |
| `SEASONALITY_FIT_START` | `2026-04-10` | Stage 1 fit window start |
| `SEASONALITY_OUTLIERS` | `[2026-04-14]` | Days masked from Stage 1 |
| `LOGISTIC_FIT_START` | `2026-05-01` | Stage 4 fit window start |
| `LOGISTIC_SWEEP_HALFWIDTH_DAYS` | `7` | Sweep range for robustness check |
| `PRE_SHUTDOWN_START` | `2020-01-01` | Stage 2/3 pre-shutdown fit window start |
| `PRE_SHUTDOWN_END` | `2026-01-07` | Stage 2/3 pre-shutdown fit window end (1 day before Jan blackout) |
| `HOLIDAY_CLUSTERS` | (list) | Cluster definitions for holiday-effect estimation |
| `PLOT_START` | `2026-03-22` | Plot x-axis start |
| `PLOT_END` | `2026-08-01` | Raw-data plot end |
| `FORECAST_PLOT_END` | `2027-01-01` | Forecast plot end |
| `FORECAST_END` | `2027-12-31` | Forecast computed through here |

## 11. Known limitations

1. **Cap is opinion, not data.** The 150k value is defensible (see
   `iran_cap_reasoning.md`) but cannot be validated from the current data
   window. Should be revisited if observed DAU breaks above 150k.
2. **Stage 1 outlier handling is manual.** Apr 14 was identified by eye and
   added to `SEASONALITY_OUTLIERS`. No automatic outlier detection.
3. **Stage 4 fit window is short.** As more partial-recovery data accumulates,
   `k` and `t₀` estimates will tighten. The fit-start sweep cell shows
   current stability.
4. **Yearly amplitude is biased by pre-shutdown population.** `yearly_factor`
   is fit on pre-shutdown general-population data. The partial-recovery
   Internet Pro / White SIM cohort likely has *attenuated* holiday and
   vacation patterns (paying users on $10/GB plans skip vacation less). We
   transfer the *shape* (peaks and troughs) and accept that magnitudes may
   be overstated. Revisit when ≥1 year of post-shutdown data exists.
5. **Holiday-effect runup contamination.** `detrend()`'s kinematic expected
   for a Nowruz day uses same-DOW values from the prior 1-3 weeks. Those
   lag values fall in the *runup* to Nowruz when activity is already mildly
   depressed, so `observed / expected` slightly understates the Nowruz
   day-of effect. Acceptable second-order error for v1; the curated wide
   Nowruz window (-2 to +14) partly mitigates by smearing the effect across
   neighbors.
6. **Cluster-overlap handling is priority-based, not joint.** When two
   clusters' windows overlap (e.g., Eid al-Fitr drifting into the Nowruz
   window in some years), the higher-priority cluster claims the date.
   Alternative would be a joint log-space regression over all
   (cluster, offset) dummies, but with 6 pre-shutdown years and ~30 offsets
   the design matrix can become rank-deficient.
7. **Holiday calendar is limited to `holidays.IR` + Yalda + blackouts.**
   Notably missing: Ramadan as a 30-day sustained effect (the daily fasting
   shifts internet usage patterns). Ramadan's smoothed average is captured
   by yearly seasonality but the within-month shape is not. Revisit if
   Ramadan-period residuals look systematic.

## 12. Reproduction procedure (clean run)

1. Activate `.venv` and confirm `plotly`, `statsmodels`, `google-cloud-bigquery`,
   `prophet`, and the mozaic-forecasting package are installed.
2. Open `iran_partial_recovery_model.ipynb`.
3. Run cells in order. Query cells require GCP application-default
   credentials (`gcloud auth application-default login` if needed). The
   pre-shutdown query pulls ~6 years of data and takes ~30s; the Prophet fit
   takes ~30-60s.
4. Inspect `# [seasonality-diagnostic]` — verify residual stability across
   weeks before trusting Stage 4.
5. Inspect `# [yearly-diagnostic]` — verify the yearly curve shape matches
   intuition (Nowruz dip, summer trough, year-end recovery).
6. Inspect `# [holiday-effects-diagnostic]` — verify Nowruz cluster shows a
   clear dip; check n-years column for sample sufficiency.
7. Inspect `# [text-summary]` — note shipping forecast parameters, yearly
   amplitude, and top-3 holiday clusters.
8. Inspect `# [fit-start-sweep]` — verify `k` and inflection are stable
   across the ±7d sweep.

## 13. Next steps (planned)

- **Mobile mirror.** Replicate the entire methodology on glean_mobile Iran
  data. Requires re-anchoring the cap demographically (mobile Firefox share
  in Iran is different from desktop). See
  `iran_partial_recovery_mobile.TODO.md`.
- **Integration into the broader Mozaic Iran forecast.** Currently this is
  a standalone supplement; the main pipeline still excludes Iran and adds
  synthetic Iran via summation from `generate_iran_synthetic.py`. Should
  consume this partial-recovery model's output instead.

## 14. References

- [`iran_cap_reasoning.md`](iran_cap_reasoning.md) — demographic anchor argument for 150k cap.
- [`iran_partial_recovery_mobile.TODO.md`](iran_partial_recovery_mobile.TODO.md) — mobile mirror, deferred.
- `iran_partial_recovery_model.draft1.ipynb` — original free-cap logistic
  approach; deprecated by the supply-rationing insight. Useful for the
  phase-space-sweep analysis that originally motivated pinning the cap.
