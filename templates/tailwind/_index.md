# `templates/tailwind/` — hand-off template for a daily tailwind curve

Give this directory to anyone (person or agent) who is producing a new tailwind or headwind curve
for the forecast. It tells them exactly what file to send us and shows a real one.

| file | role |
|---|---|
| `TAILWIND_CSV_FORMAT.md` | The contract: three-column CSV (`submission_date,type,dau`), split into need-to-have and nice-to-have, plus notes for agents. Format only; nothing about our side of the process. |
| `example_daily_tailwind.mozillaonline_2026-07.csv` | A real hand-off (Brad Ochocki Szasz's MozillaOnline migration model, July 2026 cycle) trimmed to the columns we consumed, with the pre-onset first row relabelled `actuals` so the measured block is contiguous. Derived from `data-official/2026-07/mozillaonline/source_data/mozilla_online_forecast_jul.csv`. |

## What isn't here

The wiring. Turning a delivered CSV into a forecast adjustment — the horizon-spanning parquet, the
spec JSON, the model meta, the code letter in `data-official/adjustment_codes.yaml`, the model
re-run — is our side of the job and is documented in `CLAUDE.md` ("Forecast Artifact Naming
Convention") and in each cycle's overlay directory (`data-official/{YYYY-MM}/mozillaonline/`,
`.../launch_on_login/`). The July MozillaOnline ingestion,
`data-official/2026-07/mozillaonline/build_official_series.py`, is the reference for that step.

## Where new files go

- A new *kind* of hand-off contract (e.g. a per-country curve) → a sibling directory under
  `templates/` with its own `_index.md`.
- A second worked example → this directory, named `example_daily_tailwind.<topic>_<YYYY-MM>.csv`,
  and add a row to the table above. Keep examples real; never fabricate one.
- The delivered CSVs themselves → the cycle's overlay directory under `data-official/{YYYY-MM}/`,
  not here.
