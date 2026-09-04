# `mobile_uac_meta_2026-07-28/` — last `m`-era mobile build (UAC + Meta lift), superseded

The `.adj-m.` build at forecast_start 2026-07-28 using the UAC+Meta marketing-lift curve, Dec-15
17,864,732. The final mobile build produced under the retired `m` overlay before the 2026-07-31 swap to
`p`; retained during August as a **revert target** together with `../mobile_adjm_REVERT_2026-07-31/`.

Tracked: `.adj-m.` parquet sidecar + `parameters.json`. Gitignored: the parquet, the raw pull, and
`mozaic_objects.glean_mobile.2026-07-28.pkl` (838 MB).

**Present vs Archived.** Archived in full to `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/august-2026/data-official/2026-08/mobile_uac_meta_2026-07-28/` at the September
button-down; blobs removed from disk once September is live. Sidecars remain. `m` stays registered in
`adjustment_codes.yaml` so this sidecar keeps loading.
