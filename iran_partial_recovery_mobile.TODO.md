# Iran Partial-Recovery Mobile Model — TODO

Mirror notebook of `iran_partial_recovery_model.ipynb` for `glean_mobile`
Iran data. Deferred from the desktop session (2026-05-15) because mobile
needs a separate cap analysis from scratch — the desktop demographic
argument doesn't transfer.

**Target notebook:** `iran_partial_recovery_model_mobile.ipynb`

## 1. Pre-work: data check

Before committing to the same model shape, verify the data justifies it.

- Pull glean_mobile DAU for `country=IR`, Firefox Android/iOS + Focus
  Android/iOS, post-shutdown window (2026-02-28 → present).
- Plot. Answer:
  - Is there an obvious recovery curve, or is mobile flat / near-zero?
  - What's the order of magnitude? Desktop is ~50-70k; mobile likely a
    fraction of that. If <1k, a logistic model may be overkill.
  - Does the day-of-week pattern look similar to desktop, or different?

**Decision gate:** if mobile shows a clear ramp similar to desktop in
*shape* (even at smaller absolute scale), proceed with the desktop
methodology mirror. If mobile is essentially flat or noise, use a simple
extrapolation instead and skip Stages 1-2.

## 2. Re-anchor the cap demographically

`iran_cap_reasoning.md` is Desktop-specific. For mobile:

- Firefox mobile share in Iran (pre-shutdown vs. global): check pre-shutdown
  data. Firefox mobile is usually a tiny fraction of mobile users (~1-2%
  vs. ~3-4% on desktop).
- Mobile devices on Internet Pro: do these users carry a mobile device on
  Internet Pro at all? Internet Pro is IMEI-bound; a user might have it on
  laptop only. Investigate via the pre-shutdown Firefox Mobile / Firefox
  Desktop ratio for IR.
- Likely mobile cap ranges (rough): 5k - 30k DAU. Build the demographic
  table analogous to `iran_cap_reasoning.md` §"Putting it together."

**Output:** `iran_cap_reasoning_mobile.md` (or extend existing doc).

## 3. Stage 1: weekly seasonality on mobile

- Re-fit log-OLS on partial-recovery mobile data (Apr 10+).
- DOW pattern may differ from desktop (more weekend usage on mobile?).
- Check residual stability before trusting Stage 2.

## 4. Stage 2: cap-pinned logistic on mobile

- Same `make_pinned_logistic(cap_value)` shape.
- Cap scenarios driven by Step 2 demographic anchor (not 100/150/200k —
  probably 10k/20k/30k or similar).
- Same fit window (May 1 → present).

## 5. Stage 3: yearly seasonality from pre-shutdown mobile Prophet

- Pre-shutdown glean_mobile data: starts ~2020-12-31 (~5 years vs. 6 for
  desktop). Still over the 2-year threshold for Prophet yearly.
- Use same `iran_calendar` (Iran holidays don't differ by platform).
- Run `mozaic.holiday_smart.detrend()` then Prophet fit. Extract
  `yhat['yearly']` as doy-indexed factor.
- **Caveat:** mobile use during holidays might *increase* (vacation = more
  mobile time) while desktop *decreases*. Watch the sign of the yearly
  factor's peaks/troughs to confirm pattern. If mobile shows a
  *Nowruz spike* (positive), our "holidays only decrease" hard floor needs
  reconsidering for mobile.

## 6. Stage 4: holiday cluster effects on mobile

- Same `HOLIDAY_CLUSTERS` list and windows.
- Re-estimate ratios on mobile pre-shutdown data.
- **Hard-floor question:** if mobile sees Nowruz *bumps*, do we clip to
  ≤1.0 (matching desktop convention) or allow upward effects? Decide
  before estimating. Probably keep ≤1.0 for consistency; revisit if
  residuals are persistently above 1 on holidays.

## 7. Forecast composition + visualization

- Same composition: `forecast = trend × weekly × yearly × holiday`.
- Same overlay-decomposition plot, fit-start sweep, text summary.

## 8. Integration into main pipeline

Once mobile is shipped:

- `scripts/generate_iran_synthetic.py` currently runs the full mozaic
  pipeline on IR-only data to produce desktop + mobile synthetic Iran
  forecasts. Replace its mobile section with the partial-recovery output
  (similar to desktop replacement).
- Update `scripts/add_iran_to_forecast.py` to consume the new mobile
  partial-recovery output.

## 9. Documentation

- `iran_partial_recovery_methodology.md` — add §"Mobile differences" or
  fork to `iran_partial_recovery_methodology_mobile.md`.
- `iran_cap_reasoning_mobile.md` — new doc per Step 2.
- Update the `iran-partial-recovery-forecast` memory file with mobile status.

## 10. Dependencies / blockers

- None known. Mobile data is available in BigQuery same as desktop.
- Mobile Prophet may fail on very low-volume segments (Stan optimization).
  If observed, consider aggregating across apps or using a simpler model.
