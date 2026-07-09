# `data-official/2026-07/kpi_sheet/` — July update for the KPI forecast workbook

Produces the **full replacement table** for the "Official Forecast Data" tab of the
"2026 Firefox KPI Forecasts" Google Sheet, which loads into BigQuery
(`mozdata.analysis.browser_kpi_forecasts_2026`) and powers the KPI dashboard.

July sibling of `../../2026-06/kpi_sheet/`.

## What's here

| File | What it is |
|---|---|
| `build_kpi_sheet_update.py` | Reads the current tab export + `../csv/july_canonical_curves.csv`, folds in the July cycle, writes the replacement CSV. No BigQuery needed. |
| `official_forecast_data.2026-07-06.csv` | The full replacement table (8-column long format, 5,660 rows), ready to paste over the tab / load to the `_staging` table. |

## The update scheme (July cycle)

The tab is a tidy long table: one row per (`submission_date` × `product` ×
`forecast_name`), with `year=2026`, `quarter=1`, a publish-date pair
(`created_on`/`updated_on`), and `dau_28_ma` (headwind-adjusted 28-day MA DAU).

Convention: past cycles are named by month (`JAN/FEB/MAR/APR/JUN forecast`); the newest
is aliased `CURRENT`. Each cycle is two stitched full-year lines — `<CYC> forecast`
(forecast-start → Dec 31) and `<CYC> prior forecasts` (the spliced *as-published
history* before forecast-start — the previous cycle's then-official forecast, NOT
actuals).

July becomes the new **`CURRENT`** cycle (forecast start **2026-07-06**,
`created_on=updated_on=2026-07-06`), a single forecast line per product with Iran
included natively (no ex-Iran variant): **desktop** carries the launch-on-login (`l`)
and MozillaOnline-migration (`o`) overlays, **mobile** carries the marketing lift (`m`)
— all already baked into the `july_canonical_curves.csv` columns. The previous June
cycle is backed up to `JUN`.

This script:

1. **Backs up** the June `CURRENT *` rows → `JUN *` (both lines: forecast, prior
   forecasts), keeping their 2026-06-09 vintage.
2. **Inserts** the July cycle as `CURRENT` (`created_on=updated_on=2026-07-06`),
   forecast start **2026-07-06**:
   - desktop `CURRENT forecast` = `desktop_current_july` (headwind + `l` + `o`)
   - mobile `CURRENT forecast` = `mobile_current_july` (headwind + `m`)
   - `CURRENT prior forecasts` (both products) = June's prior (Jan 1 → May 25) + the
     June forecast for May 26 → Jul 5, spliced straight from the June rows in the source
     tab (before the `JUN` rename), giving a full-year line broken into per-cycle
     segments by **handoff gaps**.
3. Leaves `JAN/FEB/MAR/APR` rows and the `NO forecast` placeholder untouched (original
   vintages preserved).

### Prior-line handoff gaps (important)

The `prior forecasts` line renders in Looker as the light-purple "Prior Forecasts"
series, and each superseded cycle's forecast should show as a **separate** segment. The
segments are separated by a single **null day** on each cycle's handoff date — the day
*before* the next cycle's forecast start:

| Handoff | Null day | Why |
|---|---|---|
| JAN → FEB | 2026-01-31 | FEB forecast started Feb 1 |
| FEB → MAR | 2026-02-28 | MAR forecast started Mar 1 |
| MAR → APR | 2026-03-31 | APR forecast started Apr 1 |
| APR → JUN | **2026-05-25** | June forecast started **May 26** |

The Jan/Feb/Mar cycles all started on the 1st, so their handoff nulls landed on
month-ends that were already blank — no special handling. **June started mid-month
(May 26)**, so the APR→JUN gap at 2026-05-25 must be inserted explicitly
(`PRIOR_GAP_DATE` in the script; `build_july_prior_rows` nulls it). Without it, the
demoted April and June forecasts fuse into one continuous line — the bug found in the
first Looker render. The newest prior segment (JUN forecast) gets **no** trailing null
because the next thing is the July `CURRENT forecast`, a separate Looker series.

Source: `../csv/july_canonical_curves.csv` columns `desktop_current_july` and
`mobile_current_july` (July `CURRENT forecast`); the June prior line is byte-identical
to the source tab's June rows.

### Differences from the June sibling

- Reads the **current tab CSV export** (`~/Downloads/2026 Firefox KPI Forecasts -
  Official Forecast Data.csv`) instead of the live `.xlsx` (the tab moved to a Google
  Sheet).
- Curve source is `../csv/july_canonical_curves.csv` (not an `augmented_curves.csv`);
  the July curves are Iran-native, so there is no `*_plus_iran` column and no ex-Iran
  line.
- `dau_28_ma` is written as a nullable integer (no trailing `.0`), matching the sheet's
  presentation.

## What isn't here

- The July curves themselves and their data card → `../csv/`. This dir only reshapes
  them into the tab's long format.
- The other workbook tabs ("raw forecasts + headwinds", "INFO", "queries") are not
  reproduced — only "Official Forecast Data" is regenerated.

## Where new code goes

Future monthly updates to this workbook belong in the corresponding
`data-official/{YYYY-MM}/kpi_sheet/`. Cross-month or mechanism work belongs under
`research/`.

## Rebuilding

```bash
source .venv/bin/activate
python3 data-official/2026-07/kpi_sheet/build_kpi_sheet_update.py
```
