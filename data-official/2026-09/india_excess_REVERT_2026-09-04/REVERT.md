# Revert target for `i` (india_excess), stashed 2026-09-04

The build that was live before the 2026-09-04 re-ingest. Not an archive: delete only after the cycle closes.

To restore, move these files back and re-run the model:

- `india_excess.json` → `data-official/2026-09/india_excess/`
- `india_excess.proportional.2026-08-29.parquet` → `data-official/2026-09/india_excess/`
- `india_excess.proportional.2026-08-29.meta.json` → `data-official/2026-09/india_excess/`

The registry entry and `.gitignore` exception were not changed by the re-ingest and need no revert.
