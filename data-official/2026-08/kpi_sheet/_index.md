# `data-official/2026-08/kpi_sheet/` — August DRAFT (`FUTURE`) update for the KPI workbook

Produces the **full replacement table** for the "Official Forecast Data" tab of the
"2026 Firefox KPI Forecasts" Google Sheet, which loads into BigQuery
(`mozdata.analysis.browser_kpi_forecasts_2026`) and powers the KPI dashboard.

August sibling of `../../2026-07/kpi_sheet/` — but a **different update scheme**. See
"Draft, not promotion" below before reusing this as the template for the next cycle.

## What's here

| File | What it is |
|---|---|
| `build_kpi_sheet_update.py` | Reads the current tab export + `../csv/august_canonical_curves.csv`, appends the August cycle as `FUTURE`, writes both CSVs below. No BigQuery needed. |
| `official_forecast_data.2026-08-10.csv` | The full replacement table (8-column long format, **6,390 rows** = the 5,660 existing rows, unchanged and in place, + 730 new `FUTURE` rows). |
| `official_forecast_data.FUTURE_ONLY.csv` | Just the **730** new rows, for pasting onto the bottom of the live tab. |
| `plot_future_lines.ipynb` | Render check: draws both `FUTURE` lines per product from the generated rows → `../plots/kpi_sheet_future_{desktop,mobile}.png`. Asserts the gap days and prints both junction steps. |

## Draft, not promotion

The tab is a tidy long table: one row per (`submission_date` × `product` ×
`forecast_name`), with `year=2026`, `quarter=1`, a publish-date pair
(`created_on`/`updated_on`), and `dau_28_ma` (adjusted 28-day MA DAU). Each cycle is two
stitched full-year lines — `<CYC> forecast` (forecast-start → Dec 31) and
`<CYC> prior forecasts` (the spliced *as-published history* before forecast-start — the
older cycles' then-official forecasts, **NOT actuals**; the tab holds no actuals at all).
Past cycles are named by month (`JAN/FEB/MAR/APR/JUN forecast`); the official one is
aliased `CURRENT`.

July's script **promoted**: it demoted the outgoing `CURRENT` to `JUN` and installed July
as the new `CURRENT`. This script does **not**. August is appended under a third prefix,
`FUTURE`, so a draft dashboard can be built off `FUTURE *` while the official dashboard
keeps reading `CURRENT *` (July). Nothing is renamed, nothing is demoted, and the script
asserts the first 5,660 output rows are field-for-field the input, in the same order.
(Field-for-field, not byte-for-byte: the Sheets export is CRLF with no trailing newline
and pandas writes LF. That is the only difference, and it matches July's output file.)

August cycle, added as `FUTURE` (forecast start **2026-08-02**, trained through
2026-08-01, `created_on=updated_on=2026-08-10`):

| Line | Product | Span | Rows | Source |
|---|---|---|--:|---|
| `FUTURE forecast` | desktop | 2026-08-02 → 12-31 | 152 | `../csv/august_canonical_curves.csv` → `desktop_current_august` |
| `FUTURE forecast` | mobile | 2026-08-02 → 12-31 | 152 | `../csv/august_canonical_curves.csv` → `mobile_current_august` |
| `FUTURE prior forecasts` | desktop | 2026-01-01 → 08-01 | 213 | the tab's `CURRENT prior forecasts` (Jan 1 → Jul 5) + `CURRENT forecast` (Jul 6 → Aug 1) |
| `FUTURE prior forecasts` | mobile | 2026-01-01 → 08-01 | 213 | same |

Both forecast curves are the published canonical ones, with every overlay already baked
into the curve columns: desktop carries the Win10 headwind (`h`, −1,315,000),
launch-on-login (`l`, 200K ceiling) and MozillaOnline (`o`); mobile carries the
paid/organic split (`p`), the headwind and the mobile tailwind (`t`, +299,000). Dec-15
headline: **desktop 48,703,443 · mobile 17,924,562**, locked as literals in
`EXPECTED_DEC15` so a curve refresh that moves the published number fails the build
instead of silently reshaping the draft dashboard.

### Handoff gaps

The workbook's convention is to null the day before each superseded cycle's forecast
start, so every vintage renders as its own segment of the light-purple "Prior Forecasts"
line. July's script had to add that explicitly for the APR→JUN junction
(`PRIOR_GAP_DATE`, 2026-05-25) after the first Looker render fused the two segments. This
cycle adds the JUN→JUL one the same way:

| Handoff | Null day | Source |
|---|---|---|
| JAN → FEB | 2026-01-31 | inherited from the tab |
| FEB → MAR | 2026-02-28 | inherited from the tab |
| MAR → APR | 2026-03-31 | inherited from the tab |
| APR → JUN | 2026-05-25 | inherited from the tab |
| **JUN → JUL** | **2026-07-05** | **added by this build** (`INSERT_JUL_HANDOFF_GAP`) |

Six segments, five breaks. The gap is what keeps the JUN and JUL vintages from connecting
across their level disagreement at that date — **+895,841** desktop / **+414,383** mobile
between the days either side of it. `plot_future_lines.ipynb` renders both states; the
draft was first cut with `INSERT_JUL_HANDOFF_GAP = False` (2026-08-10) and the fused
version drew that disagreement as a near-vertical spike in the history line. Set the flag
back to `False` to reproduce it.

Contrast the 2026-08-02 seam, where the desktop step is more than twice as large
(**+1,955,351**) and needs no gap: the two sides are *separate series*, so Looker draws no
connector across them.

### The prior line's July segment is the AS-PUBLISHED July curve, not the regenerated one

Two different July curves exist for 2026-07-06 → 2026-08-01, and they disagree:

- the tab's own `CURRENT forecast` rows — what July actually published, and what this
  script splices in;
- `../csv/august_canonical_curves.csv` → `desktop_prior_july` / `mobile_prior_july` — July's
  build re-exported under **current** package code, which includes the 2026-07-29 `Fix A`
  seam fix that July's delivered curve predates.

They differ on **exactly 27 days** — 2026-07-06 → 2026-08-01, July's `display_ma` splice
window — by up to **418,345** desktop / **17,016** mobile, and are identical from seam+27
(2026-08-02) onward, including at Dec-15 where both read 48,585,483 / 17,923,869. That is
the documented `display_ma` guarantee, not a discrepancy.

The as-published values are the right ones here: the line means "what we were telling
people at the time," and per `CLAUDE.md` past forecasts are never modified even where
they are known to be wrong. Note this is the same 27-day stretch the fused prior line now
draws as one continuous segment, so it is the visible part of the chart.

## What isn't here

- The August curves themselves and their data card → `../csv/`. This dir only reshapes
  them into the tab's long format.
- The promotion of August to `CURRENT` (with the `CURRENT → JUL` rename). When that
  happens it is a new script, or this one with the FUTURE scheme swapped for July's.
- The other workbook tabs ("raw forecasts + headwinds", "INFO", "queries") — only
  "Official Forecast Data" is regenerated.
- Any scoped or counterfactual variant of the August curve (ex-IR/CN, headwind-removed).
  The tab carries only the published world totals; the APR cycle's
  `APR z forecast ex-Iran` line has no August analogue.

## Where new code goes

Future monthly updates to this workbook belong in the corresponding
`data-official/{YYYY-MM}/kpi_sheet/`. Cross-month or mechanism work belongs under
`research/`.

## Rebuilding

Requires the current tab export at
`~/Downloads/2026 Firefox KPI Forecasts - Official Forecast Data(1).csv` (verified
field-for-field identical to `../../2026-07/kpi_sheet/official_forecast_data.2026-07-06.csv`,
i.e. the tab has not drifted since July's update). The build refuses to run if that export
already contains `FUTURE *` rows.

```bash
source .venv/bin/activate
python3 data-official/2026-08/kpi_sheet/build_kpi_sheet_update.py
```
