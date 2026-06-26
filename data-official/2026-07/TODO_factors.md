# July 2026 Forecast — Factors & TODO

Working doc for the July forecast cycle. Tracks every factor we want to consider,
its modeling approach, the data we need, and status. Update status inline as we go.

- **Branch:** `july-forecast` (off `june-forecast`)
- **Forecast start date:** TBD — confirm the run date (June ran with `forecast_start_date = 2026-05-26`; July is likely ~2026-06-26/30).
- **Created:** 2026-06-26

Status legend: `TODO` · `INVESTIGATING` · `MODELING` · `BLOCKED(reason)` · `DECIDED` · `DONE` · `DROPPED`

---

## 0. Top structural decision — Iran treatment (BLOCKS desktop & mobile)

Iran's internet was shut down ~2026-02-28; Feb–June ran the **no-Iran + synthetic-Iran-add-back** workflow (IR excluded from queries, modeled separately, summed back). **Iran has now returned**, which forces a decision before any forecast runs:

- **(a) Keep no-Iran + modeled add-back** — safest if the returned signal is still ramping/noisy. Iran model would shift from "shutdown" to a recovery/normalization curve.
- **(b) Return IR to native queries + gap holiday** — the path the user described for desktop: fill the shutdown hole in training data with a mozaic-forecasting holiday so Prophet doesn't read the zero-period as real signal, then let real Iran data drive the forecast again.

> **Decision needed from user.** The user's notes lean toward (b) ("fill the gap with a holiday"). Confirm we're retiring the synthetic-Iran add-back this cycle, or running both treatments side-by-side to compare.

Sub-items once decided:
- [ ] Define the exact gap window (shutdown start `~2026-02-28` → recovery date — confirm from data when IR DAU resumes non-zero).
- [ ] If (b): add holiday(s) covering the gap in the mozaic holiday config; verify it suppresses the gap from trend/seasonality fitting.
- [ ] Model the **return ramp** — Iran users likely don't snap back instantly; decide whether to model the recovery shape or let real data carry it.
- [ ] Keep desktop and mobile Iran treatment **consistent** (both reference `Iran has returned`).

---

## Desktop

### D1. Iran return + gap holiday — `TODO`
See §0. Desktop-specific: the gap holiday must cover the IR shutdown window in the legacy_desktop training series.

### D2. MozillaOnline → Firefox desktop migration overlay — `TODO`
MozillaOnline (the China distribution) is migrating onto Firefox desktop. Model as an **overlay**, same bidirectional pattern as the marketing-lift `m` adjustment:
1. Build a migration model (ramp/curve of migrated DAU over time).
2. Subtract it from CN training rows before mozaic so Prophet learns the pre-migration dynamic.
3. Add it back to the per-tile forecast.

- [ ] Confirm migration **timing** (start date, ramp shape, expected steady-state magnitude) and **data source** (is it already visible in telemetry, or a projection?).
- [ ] Decide adjustment style — per-tile bidirectional (like `m`) is the right fit since it should shift the model's view of recent CN history.
- [ ] Register a new one-letter adjustment code in `data-official/adjustment_codes.yaml` (e.g. `o` for mozillaonline) + applier in `src/mozaic_daily/adjustments.py` + test.
- [ ] Likely affects **New Profiles** too (migrated installs look like new profiles) — scope that.

### D3. Windows 10 migration headwinds — `TODO`
Existing `adj-h` headwind models Win10→Win11 attrition. **Revisit the anchor magnitude** based on observed Win10 performance.
- [ ] Pull recent Win10 vs modern_windows DAU trajectory; compare realized attrition vs the June headwind anchor (June desktop anchor `-1,408,000`; see `project_june_thresh_aligned_build` / `project_june_gap_resolution`).
- [ ] June concluded the residual ~75–82k Dec-15 gap was a real Win10 headwind Prophet was absorbing, and that `adj-h` should **attenuate as the headwind shows up in data** — check whether it now appears in the actuals and resize/retire accordingly.
- [ ] Update `data-official/2026-07/adjustments/headwind.json`.

### D4. Telemetry opt-out via deletion-request rate — `INVESTIGATING (scope)`
Look at users turning telemetry off, proxied by the rate of deletion requests.
- [ ] Identify the signal: deletion-request ping / shredder counts (Glean `deletion-request` ping). Confirm the right table/probe (use `mozdata:probe-discovery` / glean-dictionary).
- [ ] Caveat: deletion request ≠ telemetry-off necessarily; clarify what we actually want to measure (true opt-out vs account deletion vs uninstall).
- [ ] Decide whether this becomes a **structural DAU headwind** (a measured downward drift) or just context. Only fold into the forecast if it's a persistent, quantifiable effect not already captured by the trend.

### D5. Desktop marketing check — `TODO`
- [ ] Check the marketing/campaign calendar for any **desktop** efforts in the forecast window. (June's `m` was Fenix Android only.)
- [ ] If none of note → record "no desktop marketing" explicitly and move on. If yes → model like `m`.

### D6. Usage-experiment DAU movement — `INVESTIGATING`
Reported DAU movement from a usage experiment.
- [ ] Identify the experiment (slug, enrollment size, branches).
- [ ] Determine **permanence**: does it ship to 100% (permanent lift, fold in) or end during the horizon (transient, exclude or model the rollback)? Only incorporate effects that persist across the forecast window.
- [ ] Quantify the DAU delta attributable to the experiment vs noise.

---

## Mobile

### M1. Iran return — `TODO`
See §0. Keep treatment consistent with desktop. Mobile gap holiday over the glean_mobile IR shutdown window if going native.

### M2. Marketing ramp-up — `TODO`
Mobile marketing has ramped up beyond the June Fenix Android campaign.
- [ ] Refresh the marketing-lift model with current spend/lift data (source: STMO 118452 per `feedback_marketing_lift_data_source` — NOT the geo_testing tables).
- [ ] Validate against the **Fenix Android gap**, not ALL MOBILE (iOS partially cancels — `project_fenix_gap_target`).
- [ ] Produce a new `marketing_lift_model.*.parquet` + spec under `data-official/2026-07/marketing/`; point `marketing.json` `applies_to_forecast_start` at the July date.

### M3. Mobile telemetry work — `INVESTIGATING (scope)`
Possible telemetry opt-out / measurement effect on mobile, parallel to D4.
- [ ] Same deletion-request investigation, mobile side. Confirm whether it's material before modeling.

---

## Cross-cutting / suggested additions (proposed by Claude — confirm)

### S1. Validate June forecast vs realized actuals before locking params — `TODO` *(strongly recommended)*
We now have ~Apr–June actuals. Before choosing July parameters, backtest June's delivered curve against realized DAU (the `csv-vs-actuals` research topic; `project_actuals_vs_april_overlap`). This is the empirical input to the "same params as last month?" decision.

### S2. Parameters decision — `TODO`
Per the monthly-forecast-update skill: decide whether July reuses June's params (`forecast-parameters/2026-05-26.md`: desktop cps=0.15983/thresh=-0.05/recent13, mobile cps=0.02/thresh=-0.032/recent13) or changes them. If unchanged, copy to `forecast-parameters/2026-06-26.md`. Iran-synthetic regeneration only needed if params change AND we keep the add-back path.

### S3. Holiday calendar / year-end refresh — `TODO`
Confirm the 2026 holiday calendar is current (esp. anything affecting the **Dec-15 measurement headline**) and that the MozillaOnline/Iran holidays don't collide with real seasonal holidays.

### S4. New Profiles & Existing Engagement metrics — `TODO`
The user's factors are DAU-centric, but the pipeline forecasts 4 metrics. MozillaOnline migration (D2) and marketing (D5/M2) plausibly move **New Profiles**. Decide per-factor whether the overlay applies to non-DAU metrics.

### S5. Pipeline / data-landing health check — `TODO`
Pre-flight check that training data has landed through `training_end_date` for all tables before the ~20–30 min runs (skill pre-flight + GCP ADC token check).

### S6. Button down June to GCS — `TODO`
End-of-cycle: archive `data-official/2026-06/` large artifacts (pkl/parquet/zip) to `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/june-2026/` (single-process gsutil; see CLAUDE.md GCS section). Do once June review is fully closed.

### S7. Adjustment-codes registry hygiene — `TODO`
New adjustments this month (MozillaOnline overlay, possibly a telemetry-opt-out headwind) each need: a code in `adjustment_codes.yaml`, an applier in `adjustments.py`, and a test in `test_adjustments.py`. Combined-marker filenames will grow (e.g. `adj-hmo`).

---

## Open questions for the user
1. **Iran (§0):** retire synthetic add-back and go native+gap-holiday, or keep modeled add-back / run both?
2. **MozillaOnline (D2):** is the migration already in telemetry, and do you have a timing+magnitude estimate, or do we need to build the projection from scratch?
3. **Usage experiment (D6):** which experiment — and is it shipping to 100% or ending in-horizon?
4. **Parameters (S2):** any reason to expect July params differ from June, or default to reuse?
5. **Telemetry opt-out (D4/M3):** is this meant to land in *this* forecast, or is it an exploratory investigation feeding a future cycle?
