# `mobile_organic_2026-07-28/` — first `p` build, superseded (cpr 0.75)

The 2026-07-31 build that introduced the `p` paid/organic split at July's full parameter lock
(cpr 0.75), forecast_start 2026-07-28. Dec-15 17,601,155 — the "pre-tailwind, pre-re-lock, pre-refresh"
value quoted in `../_index.md`. Superseded by the cpr 0.725 re-lock (`../mobile_cpr0725_2026-07-28/`)
and then by the 08-02 refresh; kept as the **revert target for the re-lock**.

Tracked: `.adj-p.` parquet sidecar + `parameters.json`. Gitignored: the parquet and
`mozaic_objects.glean_mobile.2026-07-28.pkl` (838 MB).

**Present vs Archived.** Archived in full to `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/august-2026/data-official/2026-08/mobile_organic_2026-07-28/` at the September
button-down; blobs removed from disk once September is the live cycle (the revert window closed with
August). Sidecars remain.
