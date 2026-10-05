# `data-official/2026-10/` — October 2026 forecast cycle

Active cycle (branch `october-forecast`, off `clean-slate` @ `60836f3`, which carries every September
tooling change). Opened 2026-10-05 by the button-down skill.

## Status: EMPTY CYCLE — `../2026-09/` remains authoritative until this branch produces output

No raw pull, no build, no CSV, no plot exists yet. Everything below was **copied forward from September
unmodified** so the October monthly update starts from the inherited assumptions rather than an empty
directory (the September 2026 roll-forward left `2026-09/` nearly empty and every adjustment had to be
rebuilt by hand; the skill's Phase 5 steps 3–5 exist because of that).

Published September numbers (Dec-15 28d-MA, `h` + `t` + `u` applied, published 2026-09-15): desktop
**49,332,443** · mobile **18,214,594** · ALL **67,547,037**, at the 2026-09-09 seam. Those are the N-1
comparison series for October; they come from `../2026-09/csv/september_canonical_curves.csv`, and the
canonical parquets are `../2026-09/desktop_g01_2026-09-09/` (`.adj-ijlo.`) and
`../2026-09/mobile_cpr0725_paid0915med_2026-09-09/` (`.adj-p.`).

## Carried forward provisionally (2026-10-05) — ⚠️ seams NOT set

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
