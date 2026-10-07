# `data-official/2026-10/` — October 2026 forecast cycle

Active cycle (branch `october-forecast`, off `clean-slate` @ `60836f3`, which carries every September
tooling change). Opened 2026-10-05 by the button-down skill; **first build 2026-10-06**.

## Status: DRAFT — first October build exists (seam 2026-10-05); `../2026-09/` is still the delivered number

Built 2026-10-06 as the **starting point of an iterative cycle**: fresh data through 2026-10-04, September's model
locks unchanged, every September overlay carried with its September curve, and `e` applied for the first time. The
adjustment curves are known to be stale (data edges 2026-08-31 .. 2026-09-08) and the paid curve is September's.
Possibly only a reference build for a colleague learning the forecast (Brendan, 2026-10-06).

### Current working set

| | path | marker |
|---|---|---|
| desktop canonical | `desktop_g01_2026-10-05/cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/mozaic_daily_forecast.2026-10-05.ld-D.adj-eijlo.parquet` | `.adj-eijlo.` |
| mobile canonical | `mobile_cpr0725_paid1006_2026-10-05/cps0.035_thresh055_recent13_cpr0.725_ncp25_clip0.6_sps0.1/mozaic_daily_forecast.2026-10-05.gm-D.adj-p.parquet` (paid curve re-pulled 2026-10-06; the Med-curve build `mobile_cpr0725_paid0915med_2026-10-05/` is the revert target) | `.adj-p.` |
| raw mobile pull | `mobile_rawpull_2026-10-05/` (the desktop raw pull sits beside the desktop parquet) | |
| notebook | `october_canonical.ipynb` (executed; DRAFT watermark on) | |
| published curves | `csv/october_canonical_curves.csv`, `csv/october_dec15_summary.csv` + the `DESKTOP_ONLY.*` twins | |
| plots | `plots/*.png` (14; `*_vs_september*` compare with September delivered) | |

### Dec-15 2026, 28-day MA, display layer applied (from `csv/october_dec15_summary.csv`)

| | October (this build) | September delivered | delta | near-horizon min (seam..Oct-15) |
|---|--:|--:|--:|--:|
| desktop | **50,189,791** | 49,332,443 | **+857,348** (+1.74%) | 50,080,133 on 2026-10-15 |
| mobile | **18,395,089** | 18,214,594 | **+180,495** (+0.99%) | 17,890,445 on 2026-10-05 |
| ALL | **68,584,880** | 67,547,037 | **+1,037,843** (+1.54%) | 67,970,888 on 2026-10-15 |

Desktop pre-`h` (model + overlays) Dec-15: 51,207,068 (`h` −1,017,277). Mobile model base (parquet, `p` applied)
18,123,251, + `t` 299,000 − `u` 27,162 = 18,395,089 (the first build of the day, on September's Med paid curve, was 17,960,225 → 18,232,063). Both prior curves reproduce September's published Dec-15 to the
DAU (drift 0). Desktop vs targets: +676,634 above Baseline, +417,403 above Stretch.

### Attribution ledger, September delivered → this build (what changed)

| change | kind | Dec-15 effect |
|---|---|---|
| data refresh 2026-09-08 → 2026-10-04 (both platforms), seam 2026-09-09 → 2026-10-05 | model re-run | not isolated (bundled with `e` on desktop; the whole +17,469 on mobile) |
| **`e` launch_at_login_existing_users applied** (was WITHHELD in September) | per-tile overlay, desktop | nominal **736,935** at Dec-15; realized effect not isolated this build (needs an `e`-off run; the combinatorics record would give it) |
| `p` measured split rebuilt for the new window | input rebuild | share series carries the 2026-07-02 join step (+0.41pp, see `organic/_index.md`) |
| **`p` paid curve re-pulled** from GMIO feed `_20261002` (point estimate, actuals through the week of 2026-09-21), replacing September's workbook Med scenario | input + mobile rerun | **+163,026** at mobile Dec-15, exactly the 28d-MA difference of the two curves (additive stack); of the mobile +180,495 vs September, +17,469 is the data refresh and +163,026 the curve |
| `h`, `t`, `u` ramps re-gated to start 2026-10-05 | display layer | anchors unchanged (−1,017,277 / +299,000 / −27,162), exact at Dec-15 |
| `i`, `j`, `l`, `o` curves, model params | carried unchanged | 0 by construction |

Desktop +857,348 vs September is therefore ≈ `e` (nominal 736,935 × an unknown pass-through) + the data refresh;
the two cannot be separated without the `e`-off run. **Paid seam step** after the re-pull: measured paid on 2026-10-04 vs
the new curve on 2026-10-05 is **−850/day (−0.00% of mobile)**; on 28-day means (trailing measured vs the first 28 forecast
days) **+38,233 (+2.1% of paid)**. On the Med curve it had been −115,065/day and −89,383 on means, which is what made the
paid 28d-MA fall for four weeks after the seam in the first build's plots; the new curve's level at the seam (1,799,164)
matches the measured 28-day mean (1,799,331) to within 200 DAU, so that artefact is gone.

### How this build was produced (2026-10-06)

```bash
# gates: every overlay spec + organic.json -> applies_to_forecast_start 2026-10-05; e: `withheld` key deleted;
#        adjustments/{headwind,tailwind,tou_mobile_headwind}.json start_date -> 2026-10-05
python scripts/fetch_raw_pull.py --forecast-start-date 2026-10-05 --data-source glean_mobile --metric DAU \
    --output-dir data-official/2026-10/mobile_rawpull_2026-10-05
python scripts/run_param_scan.py --forecast-start-date 2026-10-05 --results-dir data-official/2026-10/desktop_g01_2026-10-05 \
    --changepoint-prior-scale 0.1649 --changepoint-range 0.814 --n-changepoints 40 --recent-weeks 17 \
    --seasonality-prior-scale 0.00825 --seasonality-regime multiplicative \
    --holiday-threshold -0.032 --holiday-max-radius 5 --holiday-min-radius 3 --holiday-effect-floor -0.6
python scripts/build_fenix_organic_split.py --forecast-start-date 2026-10-05 --overlap-tolerance 0.01 \
    --production-raw data-official/2026-10/mobile_rawpull_2026-10-05/mozaic_parts.raw.glean.mobile.DAU.parquet \
    --out-dir data-official/2026-10/organic      # failed at the 0.005 default first; see organic/_index.md
python scripts/run_mobile_param_scan.py --forecast-start-date 2026-10-05 --raw-cache-dir data-official/2026-10/mobile_rawpull_2026-10-05 \
    --results-dir data-official/2026-10/mobile_cpr0725_paid0915med_2026-10-05 --changepoint-prior-scale 0.035 \   # first build, Med curve
    # ... then: scripts/pull_paid_dau_curve.py --sql <template with _20261002 suffixes> --cycle 2026-10 --forecast-start 2026-10-05,
    #           organic.json paid_forecast.data_file -> the pull2026-10-06 parquet, and the same mobile command with
    #           --results-dir data-official/2026-10/mobile_cpr0725_paid1006_2026-10-05 (the canonical mobile build)
    --changepoint-range 0.725 --n-changepoints 25 --recent-weeks 13 --seasonality-prior-scale 0.1 \
    --holiday-threshold -0.055 --holiday-effect-floor -0.6
jupyter nbconvert --to notebook --execute --inplace data-official/2026-10/october_canonical.ipynb
python scripts/export_desktop_no_headwind_csv.py && python scripts/export_desktop_ex_ir_cn_csv.py && python scripts/verify_forecast_states.py
```

Verified: both parquets load with `require_state`; `parameters.json` matches September's lock field by field;
`verify_training_rows_are_actuals.py` exact on 61/61 probed days for both platforms; `mobile_app_breakdown.py`
`ALL MOBILE` = sum of four apps. Note the raw mobile pull has no upper date bound and carried a **partial seam day**
(2026-10-05, Focus apps only); training correctly ends 2026-10-04 and the partial day is not in the forecast parquet.

### Adjustment-effects record (desktop built 2026-10-06; mobile pending)

`adjustment_combinatorics/` holds all 32 desktop subsets at the canonical seam/config (D-014). **`e` realized +713,619 at
Dec-15 (Shapley; 0.97 pass-through of its 736,935 nominal)**; raw model + `h` alone is 49,571,434 (+238,991 vs September);
the four carried overlays net −95,000, with `o` at −117,465 (stale 2026-08-31 curve) and `l`/`i`/`j` passing through at
5–20%. Report: `adjustment_combinatorics/index.html`. The mobile `p` on/off pair is not built; see that directory's index.

### Open decisions (October)

- Re-export `o`, `j`, `i`, `e` (and possibly `l`) at the October data edge. (Paid curve re-pulled and wired 2026-10-06 — done; note it is the feed's point estimate where September shipped the Med scenario, a leadership choice that may need revisiting.)
- `t` (+299,000) carried a third time; `h` anchor is September's c-suite calibration. Decide whether either is re-derived.
- Isolate `e`'s realized Dec-15 effect (an `e`-off desktop run, or the combinatorics record — quick update deferred it).
- Mobile near-horizon minimum is now the seam day itself (17,890,445 on 2026-10-05); the late-October dip of the first build was the Med curve's paid seam step and is gone.
- The `p` tail-overlap step at 2026-07-02 (+0.41pp organic share) grows every cycle; a per-bucket ratio splice is the candidate fix.

## Roll-forward record (2026-10-05, superseded by the build above)

### Carried forward provisionally (2026-10-05) — seams set 2026-10-06 (see above)

Every registered code whose September spec resolved inside `../2026-09/` was copied here: spec, curve
parquet, `model_meta`, `_index.md` (still September's text) and `source_data/`. Build directories, raw
pulls, notebooks, CSVs, plots and logs were not copied. Each spec's `notes` now opens with a PROVISIONAL
warning. **Two gate conventions, deliberately different:**

- **Per-tile overlays and `p`** carry `applies_to_forecast_start = "PROVISIONAL-SET-OCTOBER-SEAM"`, a
  placeholder, **not** September's `2026-09-09`. `resolve_overlays` does an exact string match and
  raises when two specs claim one date, so a verbatim copy would have broken every September
  reproduction run (ladder, combinatorics currency check). A run at the October seam before these are
  set applies **no** overlays and writes `.raw.`, which the notebook's `require_state` then rejects — a
  loud failure, not a silent wrong number. (This is a deliberate deviation from the skill text, which
  says to leave the prior seam in place; recorded in the skill history.)
- **Display-layer ramps `h` / `t` / `u`** keep `start_date = 2026-09-09` (they have no date gate and no
  collision mechanism; they are only read when a notebook points at this `adjustments/` directory). Move
  the start to the October seam before trusting any October number.

| code | directory | value that matters | inherited from September |
|---|---|---|---|
| `h` | `adjustments/headwind.json` (+ `headwinds/`) | desktop **−1,017,277** at Dec-15, `clamp_at_anchor`, ramp start 2026-09-09 | the 2026-09-15 c-suite anchor (eased +150,000 from −1,167,277) |
| `t` | `adjustments/tailwind.json` (+ `tailwind/`) | mobile **+299,000** at Dec-15, ramp start 2026-09-09 | August's calibration tailwind, carried twice now; revisit vs the rebuilt `p` |
| `u` | `adjustments/tou_mobile_headwind.json` (+ `tou_mobile_headwind/`) | mobile **−27,162** at Dec-15, ramp start 2026-09-09 | unchanged since it was split out of `h` |
| `l` | `launch_at_login_new_users/` | 200K ceiling curve `lol_tailwind.2026-07-29.cap200k.parquet` | carried unchanged since August |
| `o` | `mozillaonline/` | `mozillaonline_migration.2026-08-31.parquet`, Dec-15 28d-MA 668,839 | the 2026-09-02 official export; a fresh export is expected each cycle |
| `j` | `japan_bot/` | `japan_bot.2026-09-07.parquet`, PEAK plateau 43,813 | data edge 2026-09-07; re-export expected |
| `i` | `india_excess/` | `india_excess.2026-09-06.parquet`, PROPORTIONAL (50,994 at Dec-15); hold/linger/settle/fade alternates beside it | data edge 2026-09-06; re-export expected |
| `e` | `launch_at_login_existing_users/` | `launch_at_login_existing_users.2026-09-08.parquet`, **WITHHELD** (`withheld: true`) | ingested 2026-09-10, never applied; the decision to apply it is still open |
| `p` | `organic/` + `marketing/` | split `fenix_paid_organic.2026-09-09.parquet` (**September's window — always rebuilt**, `scripts/build_fenix_organic_split.py --production-raw`); paid level `marketing/marketing_lift_model.gmio_uac_meta_total_med.2026-09-09.pull2026-09-15.parquet` (workbook Med, Dec-15 1,833,753) until re-pulled via `/pull-marketing-curve` | the 2026-09-15 c-suite scenario choice |

`m` is retired and has no September spec, so nothing was copied for it.

## What is already here

- `october_canonical.ipynb` — the producer-notebook **template**, copied from
  `../2026-09/september_canonical_v2026-09-04.ipynb` with outputs cleared. `[setup]` points
  `PREV_*` at September's canonical parquets and specs, pins `PRIOR_DELIVERED_{DESKTOP,MOBILE}_DEC15`
  to September's published values, leaves `DESKTOP_FORECAST_PATH` / `MOBILE_FORECAST_PATH` /
  `FORECAST_START` as `None` (it raises until set), and turns the DRAFT watermark back on. The two
  Dec-15 cells use the generic `PRIOR_*` names; other cells' plot titles and printed labels still say
  "September" / "August" and need renaming when the build lands. Not executed.
- `STALE_REFERENCES_from_september_button_down.md` — the repointing to-do list from the September
  button-down: cycle-scoped script constants (which of them were edited at the roll-forward and which
  are flagged), references to archived pickles, and the standing `2026-06` dependencies. **Read before
  running any cycle-scoped script.**
- `csv/`, `plots/` — empty; the notebook and the export scripts write here.
- `adjustment_combinatorics/` does not exist yet. Once October's record is built (skill Phase 1 step 0, or
  the monthly update), compare it with
  `python scripts/compare_adjustment_effects.py --prior 2026-09 --current 2026-10`; the baseline is
  `../2026-09/adjustment_combinatorics/adjustment_effects.csv`.

## Repointed at the roll-forward (unambiguous constants; everything else is flagged in the stale report)

| script | constant | now |
|---|---|---|
| `scripts/mobile_scoring.py` | `DEFAULT_HEADWIND` | `2026-10/adjustments/tou_mobile_headwind.json` |
| `scripts/score_near_horizon.py` | `DEFAULT_HEADWIND` | `2026-10/adjustments/headwind.json` |
| `scripts/export_desktop_no_headwind_csv.py` | `CSV_DIR`, `CURRENT_ADJUSTMENTS_DIR`, `PRIOR_ADJUSTMENTS_DIR`, `PREV_FORECAST_START` | `2026-10/csv`, `2026-10/adjustments`, `2026-09/adjustments`, `2026-09-09` |
| `scripts/export_desktop_ex_ir_cn_csv.py` | `CSV_DIR`, `CURRENT_ADJUSTMENTS_DIR`, `PRIOR_ADJUSTMENTS_DIR` | `2026-10/csv`, `2026-10/adjustments`, `2026-09/adjustments` (its two seam dates were still August's and are flagged, not edited) |

Not edited (flagged): every `FORECAST_START` / `TARGET_DEC15` / `DEFAULT_TARGET_DATE` (the October seam
does not exist yet), `scripts/export_desktop_daily_csv.py` entirely (its test reads the published
September CSV through `CSV_DIR`), `scripts/mobile_app_breakdown.py` (`DEFAULT_FORECAST`).

## Inherited open decisions (from `../2026-09/_index.md`)

- `t` (+299,000) was sized in August before `p` was measured; September carried it unchanged. Decide
  whether the rebuilt `p` covers part of it.
- `e` (existing-users Launch at Login) is withheld with no recorded reason; decide.
- The paid scenario (`Med`) and the `h` anchor were leadership choices on 2026-09-15; October's baseline
  should state whether they are carried or re-derived.
- Summer-trough scoring convention (inside the `display_ma` splice zone) — still undecided since August.
- `../2026-06/` is retained past the window for the third time; `../2026-07/` leaves at the November
  roll-forward and two test fixtures pin it. See `data-official/_index.md`.

## Expected layout (populate as the cycle progresses)

```
2026-10/
  october_canonical.ipynb            # present — template; rename nothing, set [setup] when the build exists
  STALE_REFERENCES_from_september_button_down.md   # present
  {desktop,mobile}_rawpull_<seam>/   # scripts/fetch_raw_pull.py, then the p split producer
  desktop_<config>_<seam>/           # canonical desktop build: .adj-<codes>. parquet + sidecar + parameters.json + pkl (gitignored)
  mobile_<config>_<seam>/            # canonical mobile build: .adj-p. parquet + sidecar + parameters.json + pkl
  adjustment_ladder/, adjustment_combinatorics/   # built with explicit approval only
  adjustments/ headwinds/ tailwind/ tou_mobile_headwind/   # present — PROVISIONAL copies (see above)
  launch_at_login_new_users/ mozillaonline/ japan_bot/ india_excess/ launch_at_login_existing_users/ organic/ marketing/   # present — PROVISIONAL copies
  csv/ plots/ kpi_sheet/             # csv/, plots/ present and empty
```

Every forecast artifact carries a `.raw.` / `.adj-<codes>.` state marker and a sidecar `.meta.json`;
load only through `mozaic_daily.adjustments.load_forecast()`. Per-cycle inputs the pipeline consumes get
their own subdirectory with a spec gated by `applies_to_forecast_start`.

## Where new files go

Month-scoped artifacts (this cycle's producer/diagnostic notebooks, adjustment specs, parquets, canonical
CSVs) live here. Cross-month or topic-anchored work goes to `research/{topic}/`. Each new subdirectory
gets an `_index.md`. At the end of the cycle run `/cycle-button-down`.
