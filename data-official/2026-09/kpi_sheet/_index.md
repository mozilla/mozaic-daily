# `data-official/2026-09/kpi_sheet/` — September 2026 promotion in the KPI workbook

Full replacement table for the "Official Forecast Data" tab of the "2026 Firefox KPI Forecasts"
Google Sheet (loads into `mozdata.analysis.browser_kpi_forecasts_2026`, powers the KPI dashboard).
First cycle produced by the generic `scripts/build_kpi_sheet_update.py` through the
`/update-kpi-sheet` skill instead of a per-cycle script copy; logic in `mozaic_daily.kpi_sheet` /
`kpi_sheet_checks`, tests in `tests/test_kpi_sheet.py` (which reproduce July's and August's
outputs byte-for-byte).

## What's here

| File | What it is |
|---|---|
| `official_forecast_data.2026-09-17.csv` | The full replacement table, **7,120 rows** (6,390 in + 730 new). Gitignored; archived with the cycle |
| `official_forecast_data.2026-09-17.meta.json` | Plan, sha1s of the export / curves / output, Dec-15s, seam and handoff-gap steps |
| `source_data/sheet_export.2026-09-17.csv` | The tab as exported on 2026-09-17, verbatim (tracked) |
| `../plots/kpi_sheet_2026-09-17.png` | Render check: both products' new `CURRENT` lines, the outgoing `AUG forecast` dotted |

## What changed (publish date 2026-09-17, seam 2026-09-09)

Scheme: **promotion**. Label mapping applied to existing rows, values and vintages untouched:

| was | now | why |
|---|---|---|
| `AUG prior forecasts` / `AUG forecast` (created 2026-07-06, seam 2026-07-06) | `JUL *` | These are the **July** cycle's rows. August's `FUTURE` draft was promoted by hand in Sheets and the outgoing July block was mislabelled `AUG`; fixed here (`--rename AUG=JUL`) |
| `CURRENT prior forecasts` / `CURRENT forecast` (created 2026-08-10, seam 2026-08-02) | `AUG *` | The August cycle, demoted to the month of its `created_on` |

New rows, `created_on = updated_on = 2026-09-17`, from `../csv/september_canonical_curves.csv`
(`desktop_current_september`, `mobile_current_september`; `h` −1,017,277, `l`, `o`, `i`, `j` on
desktop, `p` on the Med paid scenario + `t` + `u` on mobile — all baked into the published columns):

| Line | Span | Rows/product | Dec-15 | vs August |
|---|---|--:|--:|--:|
| `CURRENT forecast` desktop | 2026-09-09 → 12-31 | 114 | **49,332,443** | +629,000 |
| `CURRENT forecast` mobile | 2026-09-09 → 12-31 | 114 | **18,214,594** | +290,032 |
| `CURRENT prior forecasts` (both) | 2026-01-01 → 09-08 | 251 | | |

The prior line is August's prior line (Jan 1 → Aug 1, blanks 01-31, 02-28, 03-31, 05-25, 07-05)
with **2026-08-01 blanked** (the new handoff) and August's own forecast spliced in for
Aug 2 → Sep 8, copied from the tab as published. Seven segments, six breaks.

| product | seam step (Sep 8 prior → Sep 9 forecast) | handoff-gap step (Jul 31 → Aug 2) |
|---|--:|--:|
| desktop | +1,367,549 | +1,862,809 |
| mobile | +153,636 | +99,106 |

Both are level disagreements between vintages, not data: the seam step is August's forecast for
Sep 8 against September's Sep 9; the gap step is July's forecast for Jul 31 against August's
Aug 2. The blank keeps the dashboard from drawing the latter as a spike.

## What isn't here

- The curves themselves and their data card → `../csv/`.
- Any variant line (ex-IR/CN, headwind-removed) — the tab carries only the published world totals.
- The upload. Brendan pastes the CSV over the whole tab by hand; the BigQuery load follows the sheet.

## Rebuilding

```bash
source .venv/bin/activate
python scripts/build_kpi_sheet_update.py \
    --sheet-export "source_data/sheet_export.2026-09-17.csv" --cycle 2026-09 --publish-date 2026-09-17 \
    --rename AUG=JUL --expect-dec15 desktop=49332443 --expect-dec15 mobile=18214594
```

Refuses to overwrite; move the existing output aside first.
