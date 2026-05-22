# `forecast-parameters/` — monthly forecast provenance records

One markdown file per forecast cycle, capturing the exact parameters used and a hash trail to the data-official artifacts. Leadership-facing — when someone asks "what changed between April and May," these files are the answer.

## Files

| File | Cycle |
|---|---|
| `2026-04-01.md` | April 2026 forecast (forecast_start 2026-04-01) |
| `2026-05-13.md` | May 13 refresh |
| `2026-05-17.md` | May 17 refresh (current pinned) |

## Format

Each file lists:
- The forecast files shipped to leadership
- The data-official directories they were produced from
- Full parameter set (DesktopModelConfig / MobileModelConfig values)
- Adjustment specs applied
- MD5 / SHA-1 hashes for traceability

## When to add a new file

When a new official forecast is produced (typically monthly, sometimes mid-month for refreshes). The file name is `YYYY-MM-DD.md` matching the forecast_start_date. Follow the format of the most recent file in the dir.
