# `research/csv-vs-actuals/` — validate exported forecast CSVs against actuals

Sanity check that runs before each release: compares the exported forecast CSV (the leadership-facing deliverable) against actual DAU pulled directly from BigQuery, to catch any error introduced by the export step.

## Files

| File | Purpose |
|---|---|
| `compare_csvs_to_actuals.ipynb` | Validates exported forecast CSVs against actuals; mobile + desktop, with and without Iran |
| `data/` | Cached actuals parquets (gitignored): YTD paid DAU, mobile-no-Iran, etc. |
| `tmp/` | Notebook-cell drafts (gitignored) |

## When to run

Before declaring a monthly forecast cycle complete. The notebook is purely diagnostic — it doesn't mutate the data-official artifacts; it just plots `csv_export.csv` against the actuals from `mozdata.fenix.active_users` and equivalents.
