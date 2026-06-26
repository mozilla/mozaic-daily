# `data-official/2026-06/kpi_sheet/` — June update for the KPI forecast workbook

Produces the **full replacement table** for the "Official Forecast Data" tab of
`~/Downloads/2026 Firefox KPI Forecasts.xlsx`, which loads into BigQuery
(`mozdata.analysis.browser_kpi_forecasts_2026` via `_staging` → table → `_backup`)
and powers the KPI dashboard.

## What's here

| File | What it is |
|---|---|
| `build_kpi_sheet_update.py` | Reads the workbook's "Official Forecast Data" tab + `../update_scenarios/augmented_curves.csv`, folds in the June cycle, writes the replacement CSV. No BigQuery needed. |
| `official_forecast_data.2026-06-09.csv` | The full replacement table (8-column long format), ready to paste over the tab or load to the `_staging` table. |

## The update scheme (June cycle)

The workbook is a tidy long table: one row per (`submission_date` × `product` ×
`forecast_name`), with `year=2026`, `quarter=1`, a publish-date pair
(`created_on`/`updated_on`), and `dau_28_ma` (headwind-adjusted 28-day MA DAU).

Convention: past cycles are named by month (`JAN/FEB/MAR forecast`); the newest is
aliased `CURRENT`. Each cycle is two stitched full-year lines — `<CYC> forecast`
(forecast-start → Dec 31) and `<CYC> prior forecasts` (the spliced *as-published
history* before forecast-start — each prior month's then-official forecast, NOT
actuals). The `z forecast ex-Iran` variant is the no-Iran curve (`z` sorts it last).

June becomes the new **`CURRENT`** cycle, with a single forecast line per product (no
ex-Iran / MozillaOnline variants): **desktop** = the +Iran+MozillaOnline curve,
**mobile** = the +Iran curve, both labeled plainly `CURRENT forecast`. The previous
April cycle is backed up to `APR`.

This script:

1. **Backs up** the April `CURRENT *` rows → `APR *` (all three lines: forecast,
   prior forecasts, z forecast ex-Iran), keeping their 2026-04-13 vintage.
2. **Inserts** the June cycle as `CURRENT` (`created_on=updated_on=2026-06-09`),
   forecast start **2026-05-26**:
   - desktop `CURRENT forecast` = +Iran + MozillaOnline (+500k step on Jun 2)
   - mobile `CURRENT forecast` = +Iran
   - `CURRENT prior forecasts` (both products) = April's prior (Jan 1→Mar 31) + the
     April +Iran forecast for Apr 1→May 25, giving a continuous full-year line. History
     predates the Jun 2 MozillaOnline step, so desktop's prior uses plain +Iran.
3. Leaves `JAN/FEB/MAR` rows and the `NO forecast` placeholder untouched.

Source mapping (verified Dec-15 against the README): `augmented_curves.csv` columns
`desktop_mozillaonline_plus_iran` (desktop) and `mobile_current_june_plus_iran`
(mobile); the April backup values are byte-identical to the workbook's April rows.

## What isn't here

- The June curves themselves and their data card → `../csv/` and
  `../update_scenarios/`. This dir only reshapes them into the workbook's format.
- The other workbook tabs ("raw forecasts + headwinds", "INFO", "queries") are not
  reproduced — only "Official Forecast Data" is regenerated.

## Where new code goes

Future monthly updates to this workbook belong in the corresponding
`data-official/{YYYY-MM}/kpi_sheet/`. Cross-month or mechanism work belongs under
`research/`.

## Rebuilding

```bash
source .venv/bin/activate
python3 data-official/2026-06/kpi_sheet/build_kpi_sheet_update.py
```
