# `desktop_g01_2026-09-09/` — CANONICAL September desktop build

The published September 2026 desktop forecast: config **g01** (`cps 0.1649, cpr 0.814, ncp 40, recent 17, regime multiplicative`), overlays **`i j l o`** baked in (`.adj-ijlo.`), forecast_start **2026-09-09** (trained through 2026-09-08). `h` is display-layer and is applied by the canonical notebook, not here. Dec-15 28d-MA with `h` −1,017,277: **49,332,443**. Built 2026-09-10 at the seam refresh.

One config subdirectory (`cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/`) holds `mozaic_daily_forecast.2026-09-09.ld-D.adj-ijlo.parquet` + `.meta.json`, `parameters.json` (all tracked), and the gitignored `mozaic_objects.legacy_desktop.2026-09-09.pkl` (583 MB). `run.log` is the run's stdout.

**Present vs Archived.** The parquet, sidecar, `parameters.json` and `run.log` stay on disk through the retention window (the notebooks and scripts read them). The gitignored `mozaic_objects.*.pkl` was archived to `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/september-2026/data-official/2026-09/``desktop_g01_2026-09-09/` (verified 2026-09-17 and again at the 2026-10-05 button-down) and removed from disk at the October roll-forward. The `mozaic_parts.raw.*.parquet` here is a symlink to the shared raw pull in `../desktop_rawpull_2026-09-09/`.
