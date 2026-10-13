# `data-official/2027-glean-draft/` — 2027 desktop forecast on Glean-equivalent history

**Not a monthly cycle.** A separate PRE-DRAFT forecast of desktop **MAU** (headline: raw daily MAU on 2027-12-15) and
desktop **DAU** (secondary, 28-day MA) through 2027-12-31, trained on the legacy-to-glean project's *Glean-equivalent*
history as if it were real Glean history. Branch `glean-2027-forecast` (off `october-forecast`). Opened 2026-10-12;
**locked 2026-10-12 at build 5** (`LOCK.md`). Nothing here feeds the KPI sheet, the BigQuery output table, or `main`.

Start with `LOCK.md` (what is locked and why), then `PROCEDURE.html` (every decision and build, the full record).
`REPORT.html` is the external copy; `handoff/` holds the per-metric packages for coworkers.

## Current state (2026-10-13)

| | |
|---|---|
| locked build | `desktop_glean_history_cpr087_o_2026-10-10/` — g01 lock with changepoint range 0.87, Iran fill, 2025 outage calendar, China correction on DAU + MAU (`.adj-o.`) |
| headline | world MAU 2027-12-15 **157,222,571** (2026-12-15 160,221,373); DAU 28d MA 52,731,308 (53,891,970) |
| uncertainty | backtest of the locked config at 8 seams (≥ 4 y history): MAU 431-day \|error\| p50 6.7% / p90 9.9%, forecasts ran low ~3%; translation band ±~1.3% |
| validation | same model on Legacy telemetry = this forecast ÷ the translation factor (MAU 1.030, DAU 1.040), Dec→Dec equal |
| comparisons | `COMPARISONS.html` (2026-10-13): Legacy = the same forecast ÷ the translation factor; real Glean with no simulated history = unusable at 17 months (world MAU 169M → 229M), already +15% at one month so a shorter horizon does not help; the run also masked Aug–Dec 2025 with the Legacy-outage calendar, which real Glean does not need |
| next | Brendan's decision on the real-Glean options in `COMPARISONS.html` §3 (recommended: rerun without the outage mask + a real-Glean Iran fill, for the record; show the real-units restatement beside the headline with its caveat) |

## What's here

| path | what |
|---|---|
| `LOCK.md`, `PROCEDURE.html`, `REPORT.html` | lock record; full record (draft 15); external PRE-DRAFT copy (`scripts/render_glean_history_report.py`) |
| `COMPARISONS.html` | the two formal comparisons as one PRE-DRAFT page (`scripts/render_glean_history_comparisons.py`; plots `plots/comparisons/` from `scripts/plot_glean_history_comparisons.py`) |
| `handoff/` | per-metric packages for coworkers: `glean2027_{mau,dau}_handoff/` + `.zip` (page, forecast CSVs, plots, README); `scripts/render_glean_history_handoff.py` |
| `inputs/legacy_to_glean_export/` | the delivered export, **tracked**: fc15 per-cell parquets (DAU, MAU) + D-118 continuation to 2026-10-09, roll-ups, cell summary, gap/seam CSVs, the export project's notes. Source: `~/work/experiments/legacy-to-glean/.claude/worktrees/export-rollups/exports/` @ 22e01e2 |
| `raw_glean_history_2026-09-08/`, `raw_glean_history_2026-10-09/` | adapter output (`mozaic_daily.glean_history`), regenerable; parquets untracked, `glean_history_inputs.meta.json` tracked |
| `raw_legacy_osxcountry_2026-10-09/`, `raw_glean_real_osxcountry_2026-10-09/` | comparison inputs (Legacy; real Glean from 2025-05-22), country × OS, from `scripts/sql/glean_history_comparison_*.sql` via `build_comparison_raw_frames.py` |
| `iran_fill/`, `iran_fill_legacy/` | regenerated Iran counterfactual for this layout (Glean-equivalent units; Legacy units for the comparison), scale 1.0 / 1.0 |
| `mozillaonline/` | the `o` curve as a **data correction**: spec `mozillaonline.glean_history.json` (flag `windows`, windows/mac split, DAU + derived MAU curve), `MAU_OFFSET_APPROACH.md`, measurements |
| `adjustments/` | combinatorics **candidates, not applied**: `d` duplicate-id inflation (MAU), `x` Linux Glean-only temporary (DAU + MAU); `candidate_only: true` |
| `desktop_glean_history_cpr087_o_2026-10-10/` | **build 5, LOCKED** |
| `desktop_glean_history_g01_*`, `build*_seam*/` | earlier builds and their exported outputs (build 1 seam 09-09; build 2 plain; build 3 o on DAU only; build 4 o on both, g01) |
| `probes_mau_cpr/`, `probes_cps/` | parameter probes (changepoint range; changepoint prior scale); `probes_mau_cpr/cpr*/` ran without the China correction (gate bug, fixed) — use `fixed_cpr*/` |
| `backtest/`, `backtest_round1/` | backtest of the locked config: round 2 (≥ 4 y seams, inputs cut at seam) is the stated one; round 1 kept for the record |
| `comparison_legacy_cpr087_2026-10-10/`, `comparison_glean_real_cpr087_2026-10-10/` | the same model on Legacy telemetry (validates the translation factor) and on real Glean from 2025-05-22 (unusable, 17 months) |
| `csv/`, `plots/`, `plots/canonical/`, `plots/comparisons/` | the locked build's curves, Dec-15 summary, build comparison, Legacy comparison table, real-Glean comparison by horizon, real-units restatement, Easter alignment; canonical-format plots (purple PRE-DRAFT); comparison plots |
| `intervals/` | build-1 DAU Prophet bands (kept; the Prophet band is not usable at this horizon) |
| `_internal_plots/`, `_shakedown_nofill_*`, `_superseded_float_inputs_*` | internal diagnostics and records; not for circulation |

## Rules for this directory

- Headline column is `glean_equivalent`; tiles are OS × country (`windows` / `mac` / `linux`), not the production
  Windows-version segments. Do not mix the two layouts in one artifact.
- Every forecast artifact follows the repo convention: `.raw.` / `.adj-*.` marker + sidecar `.meta.json`, loaded via
  `mozaic_daily.adjustments.load_forecast`.
- Inputs are never edited in place; a refreshed export goes in beside the old one with its source commit recorded.
- External outputs (REPORT, handoff, canonical plots) carry the purple **PRE-DRAFT** stamp and never show the raw model.
- Unlocking means a new build directory and a new row in `LOCK.md`.
