# `data-official/2026-09/` — September 2026 forecast cycle

Active cycle (branch `september-forecast`, off `clean-slate` @ `a59d04f`, which carries every August
tooling change). Opened 2026-09-04 by the button-down skill.

## Status: EMPTY CYCLE — `../2026-08/` remains authoritative until this branch produces output

Published August numbers (Dec-15 28d-MA, `h` + `t` applied): desktop **48,703,443** · mobile
**17,924,562** · ALL **66,628,005**, at the 2026-08-02 seam. Those are the N-1 comparison series for
September; they come from `../2026-08/csv/august_canonical_curves.csv`.

## What is already here (pre-work, produced before August was locked)

Two candidate **desktop overlays**, both `desktop_overlay`-style bidirectional components on
`legacy_desktop`, both produced by a different agent in `product-data-science-core/scratch/brwells/regional-story/`,
and **neither wired into the pipeline**. Each directory has a `HANDOFF.md` — read it first — an
`_index.md`, a spec JSON, and scenario curves as `.parquet` (what the pipeline would load; tracked) with
`.csv` twins (read-only, gitignored) and `.meta.json` sidecars.

| dir | code | what | spec points at | data edge |
|---|---|---|---|---|
| `japan_bot/` | `j` | Japan's non-organic automated desktop traffic since late June 2026, to subtract before training and add back after. Three scenarios (low / **middle** / high plateau). A masking effect, not growth | MIDDLE | 2026-08-30 |
| `india_excess/` | `i` | India desktop DAU running above the 2022–25 typical curve since late May 2026, carried forward as real. **Already net of `l`** — do not net again. Five scenarios | SETTLE | 2026-08-29 |

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

- Re-measure and swap the **`o` MozillaOnline curve** — a stale carry-forward since July, the last stale
  overlay, and the main reason August desktop was flagged "not the number to publish".
- The **Win10 headwind `h` anchor** sits at −1,315,000 after nine values across three cycles;
  `research/headwinds/WIN10_ANCHOR_FINDINGS.md` retired the old attenuation rationale. Do not move it
  on that rationale.
- Decide how the **summer trough is scored** now that it fell inside the `display_ma` splice zone.
- Re-check the **data-refresh sign** (−64,769 then +100,840 on consecutive refreshes).
- **Summer-trough overlay** go/no-go (`research/param-scans/aug22-retune/`, target shape
  `research/summer-slump/`). Not implemented; nothing tuned toward the trough.
- Mobile: the **`t` tailwind** (+299,000) is a discretionary, partly calibrated number carried in its
  own spec so it cannot hide in the headwind line; the paid curve `p` reads is worth re-measuring
  (`../2026-08/organic/_index.md`).
- Start `TODO_factors.md` as a diff against `../2026-07/TODO_factors.md`.
- **`../2026-06/` is retained under the 3-month rule until the October roll-forward.** `_archive/` and
  `research/ma-seam-turbulence/` import its frozen `export_canonical_curves.py`, and July code reads its
  marketing parquet and `june_delivered_mo_tailwind.json`. Before June leaves the window, the current
  cycle must own copies of whatever it still needs.

## Expected layout (populate as the cycle progresses)

```
2026-09/
  september_canonical_v<date>.ipynb  # producer/review notebook
  desktop_<config>_<seam>/           # canonical desktop build: .adj-lo(.j.i.)? parquet + sidecar + parameters.json + pkl (gitignored)
  mobile_<config>_<seam>/            # canonical mobile build: .adj-p. parquet + sidecar + parameters.json + pkl
  mobile_rawpull_<seam>/             # raw BQ mobile pull for the `p` split's shredder-drift check (fetch_raw_pull.py)
  adjustments/headwind.json          # h (display layer) — copy from ../2026-08 and re-gate applies_to_forecast_start
  adjustments/tailwind.json          # t (display layer) — decide whether it carries forward
  organic/                           # p spec + measured split rebuilt for the new training window
  launch_on_login/lol.json           # l (200K ceiling; re-gate)
  mozillaonline/mozillaonline.json   # o — REBUILD, do not carry forward again
  japan_bot/ india_excess/           # present — candidates, unwired
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
