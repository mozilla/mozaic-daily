# `data-official/2026-09/` — September 2026 forecast cycle

Active cycle (branch `september-forecast`, off `clean-slate` @ `a59d04f`, which carries every August
tooling change). Opened 2026-09-04 by the button-down skill.

## Status: EMPTY CYCLE — `../2026-08/` remains authoritative until this branch produces output

Published August numbers (Dec-15 28d-MA, `h` + `t` applied): desktop **48,703,443** · mobile
**17,924,562** · ALL **66,628,005**, at the 2026-08-02 seam. Those are the N-1 comparison series for
September; they come from `../2026-08/csv/august_canonical_curves.csv`.

## What is already here (pre-work, produced before August was locked)

Two **desktop overlays**, both `desktop_overlay`-style components on `legacy_desktop`, both produced by a
different agent in `product-data-science-core/scratch/brwells/regional-story/`, and **both wired on 2026-09-04**
through the registry (rerun pending). Each directory has a `HANDOFF.md` — read it first — an
`_index.md`, a spec JSON, and scenario curves as `.parquet` (what the pipeline would load; tracked) with
`.csv` twins (read-only, gitignored) and `.meta.json` sidecars.

| dir | code | what | spec points at | data edge |
|---|---|---|---|---|
| `japan_bot/` | `j` | Japan's non-organic automated desktop traffic since late June 2026, to subtract before training and add back after. A masking effect, not growth. **Wired 2026-09-04** via `/ingest-adjustment` (registered, spec rebuilt, MIDDLE kept, LOW/HIGH archived); **rerun pending** | MIDDLE | 2026-08-30 |
| `india_excess/` | `i` | India desktop DAU running above the 2022–25 typical curve since late May 2026, carried forward as real. **Already net of `l`** — do not net again. **Wired 2026-09-04** (registered; spec switched SETTLE → PROPORTIONAL and rebuilt via `/ingest-adjustment`; four alternates kept on disk); **rerun pending** | PROPORTIONAL | 2026-08-29 |

Wiring either is now registry-only (since 2026-09-04, `src/mozaic_daily/overlays.py`): add the code to
`../adjustment_codes.yaml` with `applier: per_tile_overlay` and a `spec_glob`, then a model re-run — a
spec-only change moves nothing because the curve is subtracted from training rows. No `main.py` edits.
Change one overlay per run so the Dec-15 delta stays interpretable. The `/ingest-adjustment` skill does the
registration and bookkeeping.

- **`STALE_REFERENCES_from_august_button_down.md`** — every script/notebook that still hardcodes an
  August path whose blobs were archived, plus the six cycle-scoped scripts whose constants
  (`FORECAST_START`, `DEFAULT_HEADWIND`, `TARGET_DEC15`, CSV dirs, raw-cache dirs) still say August or
  July. **Repoint before running any of them.**

## Inherited from August (`../2026-08/_index.md` § Next up)

- ~~Re-measure and swap the **`o` MozillaOnline curve**~~ **Done 2026-09-04**: rebuilt in `mozillaonline/` from the 2026-09-02 official export (Dec-15 28d-MA 668,839 vs the stale 567,549). Rerun pending.
- ~~The **Win10 headwind `h` anchor** sits at −1,315,000~~ **Replaced 2026-09-04** by the Dec-15 value of Brad's model curve: −726,000, applied as a linear ramp from the September seam, flat after Dec-15 (`headwinds/`). A DRAFT; the producer may revise.
- Decide how the **summer trough is scored** now that it fell inside the `display_ma` splice zone.
- Re-check the **data-refresh sign** (−64,769 then +100,840 on consecutive refreshes).
- **Summer-trough overlay** go/no-go (`research/param-scans/aug22-retune/`, target shape
  `research/summer-slump/`). Not implemented; nothing tuned toward the trough.
- Mobile: the **`t` tailwind** (+299,000, the August calibration tailwind — not terms-of-use, which is `u`) **carried forward unchanged 2026-09-04** (`tailwind/`); revisit once the rebuilt `p` (paid level +331,525 at Dec-15) has been run, since it may cover part of what `t` was sized for. The paid curve `p` reads was re-pulled the same day (`marketing/`).
- Start `TODO_factors.md` as a diff against `../2026-07/TODO_factors.md`.
- **`../2026-06/` is retained under the 3-month rule until the October roll-forward.** `_archive/` and
  `research/ma-seam-turbulence/` import its frozen `export_canonical_curves.py`, and July code reads its
  marketing parquet and `june_delivered_mo_tailwind.json`. Before June leaves the window, the current
  cycle must own copies of whatever it still needs.

## Attribution ledger (desktop, Dec-15 28d-MA) — pending rerun

Starts from August's delivered desktop figure. Each row differences two builds that differ in exactly one
input; **no realised step exists until the September build runs**. Expected add-backs are the curve's own
Dec-15 28d-MA and are what the rerun is checked against (pass-through in a 0.5–1.5× band via
`scripts/verify_overlay.py`).

| step | expected | realised | running |
|---|--:|--:|--:|
| August delivered (`h` + `l` + `o`) | | | 48,703,443 |
| September data refresh + re-gated `l`/`o`/`h` | — | pending rerun | |
| `l` launch at login (new users) carried forward unchanged (200K ceiling) | 0 vs August | pending rerun (config-dependent) | |
| `j` japan_bot MIDDLE add-back | +67,094 | pending rerun | |
| `i` india_excess PROPORTIONAL add-back | +41,945 | pending rerun | |
| `o` MozillaOnline curve refreshed (Dec-15 28d-MA 567,549 → 668,839) | curve +101,290; realised effect empirical | pending rerun | |
| `h` Win10 headwind: August ramp −1,315,000 → Brad's Dec-15 value −726,000, ramped from the seam, flat after (display layer, exact) | +589,000 | +589,000 (no rerun needed) | |

Mobile (Dec-15 28d-MA), from August's delivered 17,924,562:

| step | expected | realised | running |
|---|--:|--:|--:|
| August delivered (`h` mobile −27,162 + `t` +299,000 + `p`) | | | 17,924,562 |
| `u` tou_mobile_headwind: the −27,162 mobile leg moved out of `headwind.json`, anchor unchanged | 0 | 0 (exact) | |
| `t` mobile calibration tailwind carried forward unchanged (+299,000) | 0 vs August | 0 (exact) | |
| `p` paid level: August curve 1,559,477 → GMIO curve 1,891,002 at Dec-15 (anchor 800,831); split rebuilt + wired | +331,525 | pending rerun | |

## Read this before quoting the headline

| lever | change | Dec-15 effect | basis |
|---|---|---|---|
| `h` Win10 headwind | anchor −1,315,000 → −726,000, ramp re-anchored at the September seam, flat after Dec-15 | +589,000 on desktop vs August | producer's draft model curve, Dec-15 value only; the shape is our convention |

## Expected layout (populate as the cycle progresses)

```
2026-09/
  september_canonical_v2026-09-04.ipynb  # present — plots + numeric tables only (targets restored, ex-Iran kept, ladder cell added;
                                         #   caveats/benchmark prose dropped). [setup] raises until DESKTOP_FORECAST_PATH / MOBILE_FORECAST_PATH are set
  adjustment_ladder/                 # ladder_manifest.json + cached per-rung desktop runs from scripts/build_adjustment_ladder.py (parquet/pkl gitignored)
  desktop_<config>_<seam>/           # canonical desktop build: .adj-lo(.j.i.)? parquet + sidecar + parameters.json + pkl (gitignored)
  mobile_<config>_<seam>/            # canonical mobile build: .adj-p. parquet + sidecar + parameters.json + pkl
  mobile_rawpull_2026-09-02/         # present — raw BQ mobile pull (fetch_raw_pull.py, 2026-09-04); reuse via --raw-cache-dir
  desktop_rawpull_2026-09-02/        # present — raw BQ legacy_desktop pull (2026-09-04); the ladder and scans reuse it
  adjustments/headwind.json          # present — h re-anchored 2026-09-04: linear ramp 0 at seam → −726,000 (Brad's Dec-15) flat after; desktop only; DRAFT
  headwinds/                         # present — h delivered file + value-read meta + plot + rationale
  adjustments/tou_mobile_headwind.json  # present — u (display layer): the mobile -27,162 leg split out of headwind.json 2026-09-04
  tou_mobile_headwind/               # present — u rationale
  adjustments/tailwind.json          # present — t carried forward unchanged 2026-09-04 (+299,000 mobile at Dec-15, ramp from the seam)
  tailwind/                          # present — t September rationale record
  marketing/                         # present — paid-DAU curve for `p` from the GMIO feed, BUILT + WIRED 2026-09-04
  organic/                           # present — p REBUILT 2026-09-04 (split through 2026-09-01, four checks PASS) and pointed at marketing/ (anchor 800,831)
  launch_at_login_new_users/         # present — `l` re-gated 2026-09-04, renamed 'Launch at Login for new users' (dir, spec, registry name); 200K curve carried unchanged
  mozillaonline/                     # present — `o` REBUILT 2026-09-04 from the 2026-09-02 official export via /ingest-adjustment; rerun pending
  japan_bot/                         # present — `j` WIRED 2026-09-04 (registry + spec + curve + source_data/); rerun pending
  japan_bot_REVERT_2026-09-04/       # present — the handoff's original spec/parquet; revert target, keep while cycle is live
  india_excess/                      # present — `i` WIRED 2026-09-04 (PROPORTIONAL; hold/linger/settle/fade alternates kept); rerun pending
  india_excess_REVERT_2026-09-04/    # present — pre-ingest spec/parquet; revert target, keep while cycle is live
  csv/september_canonical_curves.csv # + september_dec15_summary.csv (add .gitignore exceptions)
  plots/  kpi_sheet/  handoff/
  TODO_factors.md
```

Every forecast artifact carries a `.raw.` / `.adj-<codes>.` state marker and a sidecar `.meta.json`;
load only through `mozaic_daily.adjustments.load_forecast()`. Per-cycle inputs the pipeline consumes get
their own subdirectory with a spec gated by `applies_to_forecast_start`.

## Where new files go

Month-scoped artifacts (this cycle's producer/diagnostic notebooks, adjustment specs, parquets, canonical
CSVs) live here. Cross-month or topic-anchored work goes to `research/{topic}/`. Each new subdirectory
gets an `_index.md`. At the end of the cycle run `/cycle-button-down`.
