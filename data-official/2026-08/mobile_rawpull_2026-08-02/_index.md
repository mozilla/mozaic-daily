# `mobile_rawpull_2026-08-02/` — raw BQ mobile pull at the 08-02 seam

`mozaic_parts.raw.glean.mobile.DAU.parquet` (629 KB, gitignored): the `glean_mobile` DAU query result
trained through 2026-08-01, fetched with `scripts/fetch_raw_pull.py` and **no forecasting**. Exists to
break the roll-forward circularity: `scripts/build_fenix_organic_split.py` needs this pull for its
shredder-drift check before any `p`-gated mobile scan can run. Model-config independent, so the
canonical build consumed it via `--raw-cache-dir`; several scan probes symlink to it.

**Present vs Archived.** Archived to `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/august-2026/data-official/2026-08/mobile_rawpull_2026-08-02/` at the September button-down and
removed from disk. Regenerable in minutes by re-running `fetch_raw_pull.py` for the same seam.
