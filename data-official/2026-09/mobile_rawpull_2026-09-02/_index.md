# `mobile_rawpull_2026-09-02/` — raw BigQuery pull, glean_mobile DAU, training through 2026-09-01

`mozaic_parts.raw.glean.mobile.DAU.parquet` is the checkpointed query result from `scripts/fetch_raw_pull.py` (no forecasting), `fetch.log` its stdout. Model-config independent: build directories symlink to it and scans reuse it via `--raw-cache-dir`. Read by every 2026-09-02-seam mobile build and the `p` split producer.

**Present vs Archived.** Stays on disk through the retention window (small, and every build's symlink resolves here). A full copy is in `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/september-2026/data-official/2026-09/``mobile_rawpull_2026-09-02/` and, because `gcloud storage` resolves symlinks, inside every archived build directory that linked to it.
