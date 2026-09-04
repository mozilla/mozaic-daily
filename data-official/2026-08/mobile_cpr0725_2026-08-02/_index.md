# `mobile_cpr0725_2026-08-02/` — CANONICAL August mobile build

The published August 2026 mobile forecast: July's parameter lock with **cpr re-locked to 0.725**, the
`p` paid/organic split applied (`.adj-p.`), forecast_start **2026-08-02**. `h` and `t` are display-layer
and are applied by the canonical notebook. Mobile Dec-15 model value 17,625,562 (+299,000 `t` = published
17,924,562).

One config subdirectory: `mozaic_daily_forecast.2026-08-02.gm-D.adj-p.parquet` + sidecar and
`parameters.json` (tracked); gitignored `mozaic_objects.glean_mobile.2026-08-02.pkl` (830 MB). The raw
pull it consumed is `../mobile_rawpull_2026-08-02/`.

**Present vs Archived.** Parquet + sidecars stay on disk; the pkl is archived to
`gs://moz-data-science-brwells-bucket/mozaic-daily-archive/august-2026/data-official/2026-08/mobile_cpr0725_2026-08-02/` at the September button-down and removed from disk. The 07-28-seam
predecessor is `../mobile_cpr0725_2026-07-28/`.
