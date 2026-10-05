# `mobile_cpr0725_paid0909_2026-09-02/` — September mobile on the 2026-09-09 point-estimate paid pull, 2026-09-02 seam

cpr 0.725, `p` on the re-pulled GMIO cross-channel point estimate (`…pull2026-09-09`), forecast_start **2026-09-02**. Built 2026-09-09; raw-model Dec-15 28d-MA −7,072 vs the 09-04 build, training rows identical. Published mobile was 18,250,938 on it.

One config subdirectory (`cps0.035_thresh055_recent13_cpr0.725_ncp25_clip0.6_sps0.1/`) holds `mozaic_daily_forecast.2026-09-02.gm-D.adj-p.parquet` + `.meta.json`, `parameters.json` (all tracked), and the gitignored `mozaic_objects.glean_mobile.2026-09-02.pkl` (782 MB). `run.log` is the run's stdout.

**Present vs Archived.** The parquet, sidecar, `parameters.json` and `run.log` stay on disk through the retention window (the notebooks and scripts read them). The gitignored `mozaic_objects.*.pkl` was archived to `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/september-2026/data-official/2026-09/``mobile_cpr0725_paid0909_2026-09-02/` (verified 2026-09-17 and again at the 2026-10-05 button-down) and removed from disk at the October roll-forward. The `mozaic_parts.raw.*.parquet` here is a symlink to the shared raw pull in `../mobile_rawpull_2026-09-02/`.
