# `desktop_g01_2026-08-02/` — CANONICAL August desktop build

The published August 2026 desktop forecast: config **g01** (`cps 0.1649, cpr 0.814, ncp 40, recent 17,
regime multiplicative`), `l` (200K launch-on-login) + `o` overlays baked in, forecast_start **2026-08-02**
(trained through 2026-08-01). `h` is display-layer and is applied by the canonical notebook, not here.

One config subdirectory holds `mozaic_daily_forecast.2026-08-02.ld-D.adj-lo.parquet` + sidecar,
`parameters.json` (both tracked), and the gitignored `mozaic_objects.legacy_desktop.2026-08-02.pkl`
(628 MB) and raw BQ pull.

**Present vs Archived.** The parquet + sidecars stay on disk through the retention window (the notebook
reads them). The pkl and raw pull are archived to `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/august-2026/data-official/2026-08/desktop_g01_2026-08-02/` at the September
button-down and removed from disk. Its predecessor at the 07-28 seam is `../desktop_locked/`; the
revert target is `../desktop_s01_REVERT_2026-07-29/`.
