# `mobile_cpr0725_paid0910low_2026-09-09/` — September mobile on the workbook's Low paid scenario — first revert target (window closed)

Same config, seam and raw pull as the canonical build; `p` on the workbook's **`Low Forecast`** scenario (point estimate minus the 3.3% backtest error). Built 2026-09-10; published mobile was 18,194,860 on it until the 2026-09-15 switch to Med.

One config subdirectory (`cps0.035_thresh055_recent13_cpr0.725_ncp25_clip0.6_sps0.1/`) holds `mozaic_daily_forecast.2026-09-09.gm-D.adj-p.parquet` + `.meta.json`, `parameters.json` (all tracked), and the gitignored `mozaic_objects.glean_mobile.2026-09-09.pkl` (771 MB). `run.log` is the run's stdout.

**Present vs Archived.** The parquet, sidecar, `parameters.json` and `run.log` stay on disk through the retention window (the notebooks and scripts read them). The gitignored `mozaic_objects.*.pkl` was archived to `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/september-2026/data-official/2026-09/``mobile_cpr0725_paid0910low_2026-09-09/` (verified 2026-09-17 and again at the 2026-10-05 button-down) and removed from disk at the October roll-forward. The `mozaic_parts.raw.*.parquet` here is a symlink to the shared raw pull in `../mobile_rawpull_2026-09-09/`.
