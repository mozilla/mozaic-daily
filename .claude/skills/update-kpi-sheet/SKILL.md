---
name: update-kpi-sheet
description: Fold the current forecast cycle into the "2026 Firefox KPI Forecasts — Official Forecast Data" Google Sheet tab from its CSV export (downloaded to ~/Downloads). Use when the user says "update the KPI sheet", "update the remote forecast storage", "promote September in the sheet", or hands over a fresh "Official Forecast Data(N).csv". Demotes the outgoing CURRENT cycle to its month label, splices it onto the prior-forecasts line with the handoff blank, installs the new cycle as CURRENT (or appends it as a FUTURE draft), and writes the full replacement CSV + export copy + meta + check plot under data-official/{cycle}/kpi_sheet/. File only — the user uploads by hand; nothing here touches the sheet or BigQuery.
disable-model-invocation: false
---

# Update the KPI sheet

The "Official Forecast Data" tab is the KPI dashboard's source (it loads into
`mozdata.analysis.browser_kpi_forecasts_2026` via a `_staging` table, then `_backup`). Each month
the delivered curve is folded into it. The deliverable is **one full replacement CSV** that the
user pastes over the whole tab. Deterministic work is in `scripts/build_kpi_sheet_update.py`
(logic: `mozaic_daily.kpi_sheet` builds, `kpi_sheet_checks` verifies); this skill supplies the
conversation around it.

**The tab's shape.** One row per `submission_date × product × forecast_name`; `year=2026`,
`quarter=1` on every row; `created_on = updated_on` = the day the rows were generated;
`dau_28_ma` = published 28d-MA DAU as an integer. Each cycle is two lines per product:

- `<LABEL> forecast` — the cycle's published curve, seam → Dec 31.
- `<LABEL> prior forecasts` — Jan 1 → seam−1: every earlier cycle's **own as-published
  forecast** over the window it was official, spliced end to end, with **one blank day at each
  handoff** (the day before the next cycle's seam) so the dashboard draws the vintages as
  separate segments. **Not actuals** — the tab has none.

The official cycle is aliased `CURRENT`; superseded cycles carry the month of their `created_on`
(`JAN … JUN, JUL, AUG`). The prior line is always copied **from the sheet**, never regenerated
from a curve file (past forecasts are never modified, even where known to be wrong).

**Rules.**

1. **Ask with question blocks; a harness timeout is never an answer.** Every GATE below waits.
2. **Always ask about non-standard changes** before building — an extra variant line (April had
   `APR z forecast ex-Iran`), a label to fix, a block to drop. September 2026's export carried
   July's rows under `AUG *` from a hand-edit; the fix was `--rename AUG=JUL`. Assume nothing.
3. **Never overwrite, never upload.** The script refuses to clobber an existing output; the paste
   into Sheets and the BigQuery load are the user's, by hand.
4. **Lock Dec-15.** Read the two headline numbers from `csv/<month>_dec15_summary.csv`, confirm
   them with the user, pass them as `--expect-dec15`; the build fails if the curve file moved.
5. **Do not touch** the canonical CSVs, specs, registry or model. This is a reshaping step.

---

## Phase 0 — Inputs (read-only)

Establish and show as a table for confirmation:

| name | how |
|---|---|
| `EXPORT` | the newest `~/Downloads/2026 Firefox KPI Forecasts - Official Forecast Data*.csv` (Sheets appends `(N)`; check mtime, not N). Confirm it is today's download |
| `CYCLE` | newest `data-official/YYYY-MM/` with a `csv/<month>_canonical_curves.csv`. Infer, confirm |
| `CURVES` | `data-official/$CYCLE/csv/<month>_canonical_curves.csv`; columns `desktop_current_<month>` / `mobile_current_<month>`. Confirm this is the **delivered** curve (the cycle `_index.md` status line can lag; git log on the CSV is the better witness) |
| `SEAM` | first non-null date of the current column — the script derives it, you state it |
| `DEC15` | `csv/<month>_dec15_summary.csv` → desktop and mobile `current_<month>` |
| `PUBLISH_DATE` | today, unless the user says otherwise (precedent: the day the rows are generated) |
| outgoing cycle | from the export: `CURRENT forecast` min date (its seam) and `created_on`; the demote label defaults to that month, e.g. created 2026-08-10 → `AUG` |

Run the inventory and plan without writing:

```bash
source .venv/bin/activate
python scripts/build_kpi_sheet_update.py --sheet-export "$EXPORT" --cycle $CYCLE \
    --publish-date $PUBLISH_DATE --expect-dec15 desktop=$DEC15_DESKTOP --expect-dec15 mobile=$DEC15_MOBILE --dry-run
```

Read the inventory **block by block** against the convention: every superseded label is a
month, spans are contiguous, each prior line's blank days are exactly the earlier handoffs, and
`CURRENT` is the cycle you expect to demote. Anything off is a non-standard change (Rule 2).

**GATE (question block), always asked, four questions:**

1. **Scheme** — promote (rename `CURRENT *` → month label, install as `CURRENT`) or draft
   (`--draft`: append as `FUTURE *`, touch nothing else, as August 2026 did while the dashboard
   still read July).
2. **Demote label** — the derived month, or something else. If a block in the export is already
   mislabelled, add `--rename OLD=NEW` (repeatable) and say so.
3. **Publish date** and **Dec-15 lock values** — read back for confirmation.
4. **Any other change this time?** Extra variant line, dropped block, older rows to edit. The
   script handles label renames only; anything else is new code, not a per-run improvisation.

---

## Phase 1 — Build

```bash
python scripts/build_kpi_sheet_update.py --sheet-export "$EXPORT" --cycle $CYCLE \
    --publish-date $PUBLISH_DATE [--draft] [--demote-to LABEL] [--rename OLD=NEW ...] \
    --expect-dec15 desktop=… --expect-dec15 mobile=…
```

Writes to `data-official/$CYCLE/kpi_sheet/`:

- `official_forecast_data.$PUBLISH_DATE.csv` — the full replacement table (gitignored, archived
  with the cycle);
- `official_forecast_data.$PUBLISH_DATE.meta.json` — plan, sha1s of export / curves / output,
  Dec-15s, seam step and handoff-gap step per product;
- `source_data/sheet_export.$PUBLISH_DATE.csv` — the export it read, verbatim (tracked);
- `../plots/kpi_sheet_$PUBLISH_DATE.png` — render check.

The checks that ran (all raise): no duplicate keys; every input row carried field-for-field
under its (possibly renamed) label — in draft mode also first and in order; new forecast line
seam → Dec 31 with no blanks and Dec-15 equal to the lock; new prior line Jan 1 → seam−1 whose
blanks are exactly the inherited handoffs plus the new one, and whose tail equals the outgoing
forecast rows as published.

---

## Phase 2 — Look at it

Open the plot (`code <path>`) and read it against the numbers the script printed:

- prior line: one segment per vintage, a visible break at every handoff, the newest break at the
  outgoing seam − 1;
- forecast line starts at the seam; the outgoing forecast is dotted behind it;
- seam step and handoff-gap step are what the script printed, and the Dec-15 in each title is the
  locked value;
- adjacent y tick labels differ (two decimals of millions).

Then a spot check the user can repeat: `grep '^2026-12-15,.*CURRENT forecast'` on the output
gives the two Dec-15 rows.

---

## Phase 3 — Bookkeeping

- `data-official/$CYCLE/kpi_sheet/_index.md`: what was published, the label mapping, the two
  steps per product, what the dashboard will show, and how to rebuild. Copy the previous cycle's
  and edit; note anything non-standard (Rule 2) in its own paragraph.
- `data-official/$CYCLE/_index.md`: a dated entry under the cycle's ledger.
- `.claude/commands/monthly-forecast-update.md` Step 5 points here; nothing to change unless the
  process did.

---

## Phase 4 — Report and hand off

Report from the meta JSON, never from memory: rows in / out, label mapping, Dec-15 per product
with the delta to the outgoing cycle, seam step, handoff-gap step, the output path. Then the
hand-off, which is the user's:

1. Open the sheet, select the whole "Official Forecast Data" tab, paste the CSV over it
   (a promotion renames rows, so the whole tab must be replaced — appending is only right for a
   draft).
2. The BigQuery load follows the sheet (`_staging` → table → `_backup`); not ours.
3. If they re-export afterwards, the new export is the next cycle's input; keep it.

---

## Reference: what the script derives and what it will not do

| derived | from |
|---|---|
| seam | first non-null date of `desktop_current_<month>` / `mobile_current_<month>` |
| outgoing seam, vintage | `CURRENT forecast` rows' min date / `created_on` |
| demote label | month of the outgoing `created_on` (override `--demote-to`) |
| handoff blank | outgoing seam − 1, inserted into the new prior line |
| `year`, `quarter` | the single value each column carries in the export |
| block order | as they appear in the export; new label last; prior → forecast → variant within a label |

Not handled: a variant line (`z forecast …`) for the new cycle, dropping blocks, editing older
values, more than one `CURRENT` cycle in the export. Each is a deliberate extension of
`mozaic_daily.kpi_sheet` with a test, not a flag.

History: June 2026 (xlsx, promotion), July 2026 (CSV export, promotion, handoff blank added after a
Looker render fused two vintages), August 2026 (`FUTURE` draft), September 2026 (first run of this
skill; fixed the hand-applied `AUG` label to `JUL`). `tests/test_kpi_sheet.py` reproduces July's
and August's outputs byte-for-byte from the exports still in `~/Downloads`.
