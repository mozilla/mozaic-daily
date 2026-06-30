# July 2026 Forecast — Factors & TODO

Working doc for the July forecast cycle. Tracks every factor we want to consider,
its modeling approach, the data we need, and status. Update status inline as we go.

- **Branch:** `july-forecast` (off `june-forecast`)
- **Forecast start date:** TBD — confirm the run date (June ran with `forecast_start_date = 2026-05-26`; July is likely ~2026-06-26/30).
- **Created:** 2026-06-26

Status legend: `TODO` · `INVESTIGATING` · `MODELING` · `BLOCKED(reason)` · `DECIDED` · `DONE` · `DROPPED`

---

## 0. Iran treatment — DONE: native data + counterfactual *fill* over the gap

Iran's internet was shut down; Feb–June ran the **no-Iran + synthetic-Iran-add-back** workflow (IR excluded from queries, modeled separately, summed back). **Iran has now returned and fully recovered** to pre-shutdown levels (recovery 2026-05-26).

**Decision (user, 2026-06-30):** Return IR to native queries and **fill the shutdown hole with a synthetic counterfactual** — "what Iran would have been with no shutdown" — fed to mozaic as ordinary training data. (An earlier 2026-06-26 note proposed a NaN-mask "gap holiday"; that was **not** the user's intent and is retired — see the handoff banner.) **Retire / archive the synthetic add-back machinery** for this use case.

**Mechanism (built):** propagate the mozaic model forward (train on clean pre-shutdown IR, FORECAST_START=2026-02-28, July params), harvest the **per-population** IR forecast, then **re-seasonalize** to restore the real weekday→weekend amplitude Prophet damps (peak/trough was ~46% of real). The package ingests the fill via "Approach A" (replace IR gap rows before `populate_tiles()`).

**Exact gap window (BQ-confirmed):** shutdown cliff **2026-03-01** (last real day Feb 28), full restoration **2026-05-26** (last bad day May 25), both platforms same days. Fill window 2026-02-28 → 2026-05-25 (DAU/NP/EED); **MAU 2026-02-28 → 2026-06-21** (rolling-28 stays contaminated ~28d past recovery).

Status:
- [x] Exact gap window from BQ (desktop + mobile).
- [x] Producer + artifacts + spec: `scripts/generate_iran_fill.py`; `data-official/2026-07/iran_fill/` (`FILL_FORMAT_SPEC.md` is the package contract); tests `tests/test_iran_fill.py`.
- [x] Handoff rewritten for the fill-ingestion ask: `iran_gap_holiday_mozaic_handoff.md`.
- [x] **Package-side ingestion** — the fill now ships **inside the mozaic package**
  (`mozaic/fills/iran_2026/*.parquet`, branch `configurable-model-params`) and is **auto-applied** by
  `populate_tiles(data_source=...)`. (Not the splice-from-mozaic-daily route the earlier handoff drafted.)
- [x] **mozaic-daily wiring**: removed the `country != 'IR'` SQL exclusion, added `IR` back to
  `top_DAU_markets` (so it surfaces natively, not folded into ROW), and threaded `data_source` through
  `forecast.py` → `populate_tiles`. Tests: `tests/test_iran_fill_integration.py` +
  `test_queries.py::test_build_query_includes_iran_natively`. Note left for the package agent:
  `~/work/mozaic-forecasting-official/NOTE_TO_MOZAIC_PKG_from_daily.md` (the surface-IR-as-a-market trap +
  a package-side opt-out request, since `--no-iran-fill` can't be honored consumer-side).
- [ ] **Archive the synthetic-Iran use case** (`data-official/iran_synthetic/`, `scripts/generate_iran_synthetic.py`, `scripts/add_iran_to_forecast.py`, `research/iran/`) as retired-for-this-purpose (keep on disk + GCS).
- Return ramp: real recovered data carries it; no separate modeling needed. Desktop + mobile treated consistently (one window, both platforms).

---

## Desktop

### D1. Iran return + counterfactual fill — `DONE (built-in + wired)`
See §0. Desktop fills for **both** `legacy_desktop` and `glean_desktop` ship in the mozaic package and
auto-apply; mozaic-daily passes `data_source` so the right desktop fill is selected (the two share a
schema). Pending: a real-data forecast run to confirm IR `actuals` show the crater and IR/World forecast
is smooth (blocked momentarily on a `gcloud auth application-default login` refresh).

### D2. MozillaOnline → Firefox desktop migration overlay — `TODO` (model exists)
MozillaOnline is migrating onto Firefox desktop. **Brad has reportedly built a migration model — Brendan is locating it** (use it rather than building from scratch). Model as an **overlay**, same bidirectional pattern as the marketing-lift `m` adjustment:
1. Take Brad's migration curve (migrated DAU over time).
2. Subtract it from training rows before mozaic so Prophet learns the pre-migration dynamic.
3. Add it back to the per-tile forecast.

- **Geography:** >90% of the migration is **China (CN)**; the remainder is spread across other countries, **potentially VPN users**. So the overlay can't be applied to CN alone — it needs a small distributed tail across other markets. Confirm how Brad's model splits this.
- **Validation expectation (user):** our migration ramp was **deliberately conservative** — slower than what actuals already show — so the forecast will sit *below* realized data and we **do not expect them to match**. That's by design; don't treat the gap as a modeling error.
- **Placeholder model:** while waiting for Brad's official model, build a stand-in so the pipeline runs end-to-end and the official model is a drop-in swap. Handoff written: `data-official/2026-07/mozillaonline/PLACEHOLDER_MODEL_HANDOFF.md` (point a fresh Claude at it).
- [ ] Locate + ingest Brad's model; confirm its geo split and ramp shape.
- [ ] Decide adjustment style — per-tile bidirectional (like `m`) since it should shift the model's view of recent history. Desktop allocates by **fixed country shares** (not an app flag); within a country split across `modern_windows`/`winX`/other OS rows proportional to DAU.
- [ ] Register a new one-letter adjustment code `o` in `data-official/adjustment_codes.yaml` + applier in `src/mozaic_daily/adjustments.py` + test (Part B of the handoff).

### D3. Windows 10 migration headwinds — `TODO`
Existing `adj-h` headwind models Win10→Win11 attrition. **Revisit the anchor magnitude** based on observed Win10 performance.
- [ ] Pull recent Win10 vs modern_windows DAU trajectory; compare realized attrition vs the June headwind anchor (June desktop anchor `-1,408,000`; see `project_june_thresh_aligned_build` / `project_june_gap_resolution`).
- [ ] June concluded the residual ~75–82k Dec-15 gap was a real Win10 headwind Prophet was absorbing, and that `adj-h` should **attenuate as the headwind shows up in data** — check whether it now appears in the actuals and resize/retire accordingly.
- [ ] Update `data-official/2026-07/adjustments/headwind.json`.

### D4. Telemetry opt-out via deletion-request rate — `HANDED OFF (exploratory, likely out of July timebox)`
Users turning telemetry off may be artificially depressing measured DAU. Set up as a standalone investigation project.
- **Handoff written:** `~/work/experiments/telemetry-optout-dau-impact/HANDOFF.md` (created 2026-06-26).
- **Timebox (user):** exploratory — "probably affecting our data" but **may fall out of this round** given fewer working days before the deadline (July 4th holiday). Not blocking July; if it produces a credible effect size it becomes a headwind in a later cycle.
- **Open frame question** carried in the handoff: observational analysis (assumed) vs. a designed Nimbus/Jetstream study — Brendan to confirm.

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

### M1. Iran return — `DONE (built-in + wired)`
See §0. `glean_mobile` fill ships in the package and auto-applies (mobile schema is unambiguous, but we
pass `data_source` anyway for determinism). All 4 metrics, per-population. Mobile recovery date matches
desktop's (2026-05-26); mobile's real weekly cycle is near-flat (re-seasonalization barely changes it,
correctly matching reality); mobile recovered slightly *above* pre-shutdown (+36% DAU at the seam) — left
unscaled per the go/no-go.

### M2. Marketing ramp-up — `TODO`
Mobile marketing has ramped up beyond the June Fenix Android campaign.
- [ ] Refresh the marketing-lift model with current spend/lift data (source: STMO 118452 per `feedback_marketing_lift_data_source` — NOT the geo_testing tables).
- [ ] Validate against the **Fenix Android gap**, not ALL MOBILE (iOS partially cancels — `project_fenix_gap_target`).
- [ ] Produce a new `marketing_lift_model.*.parquet` + spec under `data-official/2026-07/marketing/`; point `marketing.json` `applies_to_forecast_start` at the July date.

### M3. Mobile telemetry work — `HANDED OFF` (folded into D4)
Mobile is in scope of the same telemetry-opt-out investigation (`~/work/experiments/telemetry-optout-dau-impact/HANDOFF.md` covers desktop + Android + iOS). Same timebox caveat.

---

## Cross-cutting

### S1. Validate June forecast vs realized actuals — `TODO` (note: hard to interpret)
We now have ~Apr–June actuals; sanity-check June's delivered curve against realized DAU (`csv-vs-actuals` research topic; `project_actuals_vs_april_overlap`).
- **User caveat:** the forecast is *expected* to sit **below** actuals because the **MozillaOnline migration ramp was deliberately conservative** (slower than what the data already shows). We knew this and don't expect a match — so validation is genuinely hard here; a forecast-below-actual gap is not by itself a defect. Focus validation on shape/trend rather than level-matching where the MozillaOnline overlay dominates (i.e. CN).

### S2. Parameters decision — `TODO` (expected stable)
**User:** params "should be mostly stable, though we'll see what we see." Default: **reuse June** (`forecast-parameters/2026-05-26.md` — desktop cps=0.15983/thresh=-0.05/recent13, mobile cps=0.02/thresh=-0.032/recent13), copy to `forecast-parameters/<DATE>.md`. Revisit only if S1 surfaces something. (No Iran-synthetic regeneration needed — that path is being retired, §0.)

### S3. Holiday calendar refresh — `TODO` (notes in ~/work/holiday-corrected)
**User:** refresh the calendar; lots of notes in `~/work/holiday-corrected`. Findings from that repo:
- The corrected calendar isn't an exported artifact — it lives in code:
  - `src/holiday_corrected/calendars/tables.py` — year-by-year moving-holiday dicts through 2026 (Eid al-Fitr/Adha, Lunar New Year, Vesak, Mid-Autumn, Hung Kings, Ashura, Arbaeen, Mawlid, Tazaungdaing, Thadingyut, Durga Puja).
  - `src/holiday_corrected/calendars/classes.py` — AI-generated `HolidayBase` subclasses for VN, BD, MM, IQ (where native `holidays`/mozaic calendars are sparse/broken).
  - `src/holiday_corrected/calendars/direct_calendar.py` — `APPROVED_DIRECT_HOLIDAYS` aggregate-level additions (≥3% measured dip).
- mozaic-daily currently inherits holidays from mozaic's native `get_calendar()` via `populate_tiles()`. Decide whether to (a) pull holiday-corrected's corrections into the July run, or (b) confirm mozaic's native calendar already covers 2026 H2 incl. the **Dec-15 measurement headline** window.
- Verify the warmup-clamp fix (mozaic SHA `e97413b9`) is in the installed mozaic — it materially changed year-end holiday lift for leading-zero series.
- [ ] Decide scope of the refresh and execute before the forecast run.

### S4. Pipeline / data-landing health check — `TODO`
Pre-flight that training data has landed through `training_end_date` for all tables before the ~20–30 min runs (skill pre-flight + GCP ADC token check).

### S5. Button down June to GCS — `TODO`
Archive `data-official/2026-06/` large artifacts (pkl/parquet/zip) to `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/june-2026/` (single-process gsutil; CLAUDE.md GCS section). Once June review is closed.

### S6. Adjustment-codes registry hygiene — `TODO`
New adjustments need a code in `adjustment_codes.yaml` + applier in `adjustments.py` + test. Expected new code: `o` (mozillaonline). Combined markers will grow (e.g. `adj-hmo`).

> **Dropped:** New Profiles / Existing-Engagement scoping — user confirmed **DAU is fine for now**.

---

## Deliverables produced this setup pass
- `iran_gap_holiday_mozaic_handoff.md` (this dir) — feature request to `mozaic-forecasting` for the training-exclusion / gap-holiday mechanism (§0, D1, M1).
- `~/work/experiments/telemetry-optout-dau-impact/HANDOFF.md` — telemetry opt-out investigation spec (D4/M3).

## Open questions for the user
1. **MozillaOnline (D2):** where is Brad's model, and how does it split the ~10% non-China tail across countries (VPN attribution)?
2. **Usage experiment (D6):** which experiment — and is it shipping to 100% or ending in-horizon?
3. **Iran archive:** OK to mark `data-official/iran_synthetic/` + `research/iran/` retired-for-this-purpose (kept, not deleted) now, or wait until the gap-holiday path is validated?
4. **Holiday refresh (S3):** pull holiday-corrected's corrections into July, or just verify mozaic's native calendar covers 2026 H2?
