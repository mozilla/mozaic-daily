# `data-official/2027-glean-draft/` — 2027 desktop forecast on Glean-equivalent history

**Not a monthly cycle.** A separate draft forecast of desktop **MAU** (headline: raw daily MAU on 2027-12-15) and
desktop **DAU** (secondary) through 2027-12-31, trained on the legacy-to-glean project's *Glean-equivalent* history
as if it were real Glean history. Branch `glean-2027-forecast` (off `october-forecast`). Opened 2026-10-12.
Nothing here feeds the KPI sheet, the BigQuery output table, or `main`.

Start with `PROCEDURE.html` — goal, the 18 decisions of 2026-10-12, data facts, the proposed procedure, open items.

## What's here

| path | what |
|---|---|
| `PROCEDURE.html` | the procedure document (regenerated each revision; banner carries the timestamp) |
| `inputs/legacy_to_glean_export/` | the delivered export, **tracked in git**: per-cell fc15 parquets (DAU, MAU; 45 OS×country cells + 3 ROW + 5 roll-ups, 2018-01-01..2026-09-08), the roll-up parquet/CSV pair, `fc15_cells_summary.csv`, the export project's `README.md` and `FOR_AGENTS.md`. Source: `~/work/experiments/legacy-to-glean/.claude/worktrees/export-rollups/exports/` @ c13de2d |
| `plots/` | diagnostics; `legacy_outage_windows_2025.png` measures the 2025 Legacy outage dips the export leaves in |

## What's not here (yet)

Adapter frames, the raw build, pickles, CSV outputs, intervals, comparison runs — all per `PROCEDURE.html` §5, pending
approval of the open items in §6.

## Rules for this directory

- Headline column is `glean_equivalent`; tiles are OS × country (`windows` / `mac` / `linux`), not the production
  Windows-version segments. Do not mix the two layouts in one artifact.
- Every forecast artifact follows the repo convention: `.raw.` / `.adj-*.` marker + sidecar `.meta.json`, loaded via
  `mozaic_daily.adjustments.load_forecast`.
- Inputs are never edited in place; a refreshed export goes in beside the old one with its source commit recorded.
