# July 2026 Forecast — Factors & TODO

Working doc for the July forecast cycle. Tracks every factor we want to consider,
its modeling approach, the data we need, and status. Update status inline as we go.

- **Branch:** `july-forecast` (off `june-forecast`)
- **Forecast start date:** TBD — confirm the run date (June ran with `forecast_start_date = 2026-05-26`; July is likely ~2026-06-26/30).
- **Created:** 2026-06-26

Status legend: `TODO` · `INVESTIGATING` · `MODELING` · `BLOCKED(reason)` · `DECIDED` · `DONE` · `DROPPED`

---

## 0. Iran treatment — DECIDED: native data + gap holiday; retire synthetic machinery

Iran's internet was shut down ~2026-02-28; Feb–June ran the **no-Iran + synthetic-Iran-add-back** workflow (IR excluded from queries, modeled separately, summed back). **Iran has now returned.**

**Decision (user, 2026-06-26):** Return IR to native queries and handle the shutdown hole with a **mozaic-forecasting "holiday"** (training-exclusion mask over the gap) so Prophet doesn't read the zero-period as real signal. Real recovered Iran data then drives the forecast. **Retire / archive the synthetic add-back machinery** for this use case.

Mechanism (from mozaic source review): mozaic does holidays via custom detrending + proportional effects, *not* Prophet-native holidays. It already has an `IranHolidays` class with blackout ranges and a "blackout"→-1.0 forecast rule — but those are for *predicting* dips, and the detrend radius (≤5 days) is far too narrow to absorb a multi-month gap. The right primitive is a **training-exclusion (NaN-mask) over the contiguous shutdown range** so Prophet interpolates across it. That capability does not exist upstream yet → handoff to mozaic-forecasting (see deliverable below).

Sub-items:
- [ ] **Determine the exact gap window** from data: shutdown start `~2026-02-28` → the date IR DAU resumes non-zero/normal. Needs a quick BQ pull of IR daily DAU (desktop + mobile).
- [ ] **Write the mozaic-forecasting handoff** proposing the training-exclusion feature (`excluded_date_ranges` threaded `detrend()` → `Tile` → `populate_tiles()`/`ModelConfig`). Align with any in-flight upstream PR (see `~/work/holiday-corrected/MOZAIC_UPSTREAM_PR.md`).
- [ ] **Archive the synthetic-Iran use case**: document `data-official/iran_synthetic/`, `scripts/generate_iran_synthetic.py`, `scripts/add_iran_to_forecast.py`, and `research/iran/` as retired-for-this-purpose (keep on disk + GCS; mark in `_index.md` that the gap-holiday path supersedes it). Don't delete — recovery-curve modeling may still be referenced.
- [ ] Decide whether the **return ramp** needs modeling or real data carries it (likely the latter once the gap is masked).
- [ ] Keep desktop and mobile Iran treatment **consistent**.

---

## Desktop

### D1. Iran return + gap holiday — `TODO`
See §0. Desktop-specific: the gap holiday must cover the IR shutdown window in the legacy_desktop training series.

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

### M1. Iran return — `TODO`
See §0 (DECIDED: native + gap holiday). Mobile applies the same `excluded_date_ranges` shutdown mask over the glean_mobile IR window. Pull the mobile recovery date separately — it may differ from desktop's.

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
