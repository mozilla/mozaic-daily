# Revert target for `j` (japan_bot), stashed 2026-09-09

The build that was live before the 2026-09-09 re-ingest. Not an archive: delete only after the cycle closes.

To restore, move these files back and re-run the model:

- `japan_bot.json` → `data-official/2026-09/japan_bot/`
- `japan_bot.2026-08-30.parquet` → `data-official/2026-09/japan_bot/`
- `japan_bot.2026-08-30.meta.json` → `data-official/2026-09/japan_bot/`

The registry entry and `.gitignore` exception were not changed by the re-ingest and need no revert.
