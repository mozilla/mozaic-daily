# `desktop_rawpull_2026-09-09/` — raw BigQuery pull, legacy_desktop DAU, training through 2026-09-08

`mozaic_parts.raw.legacy.desktop.DAU.parquet` is the checkpointed query result from `scripts/fetch_raw_pull.py` (no forecasting), `fetch.log` its stdout. Model-config independent: build directories symlink to it and scans reuse it via `--raw-cache-dir`. Read by the canonical desktop build, `desktop_raw_ci_2026-09-09/` and the 2026-09-09-seam ladder runs.

**Present vs Archived.** Stays on disk through the retention window (small, and every build's symlink resolves here). A full copy is in `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/september-2026/data-official/2026-09/``desktop_rawpull_2026-09-09/` and, because `gcloud storage` resolves symlinks, inside every archived build directory that linked to it.
