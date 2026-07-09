# `data-official/2026-07/mobile_refresh_2026-07-06/` — locked mobile forecast

The **canonical July mobile forecast** (`glean_mobile` DAU), retrained through the latest landed day
(2026-07-05) at forecast_start 2026-07-06. Dec-15 28d-MA = **17,923,869**. The canonical notebook
reads mobile from here (desktop comes from `../desktop_locked/`).

Single config subdir: `cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6/` (`grad_moderate` grid
params from `research/param-scans/mobile-july/`).

## ✅ Present & usable

- `.../mozaic_daily_forecast.2026-07-06.gm-D.adj-m.parquet` (+ `.meta.json`) — the forecast, `adj-m`.
- `.../parameters.json` — locked model config.

## Archived at button-down (GCS)

- `.../mozaic_objects.glean_mobile.2026-07-06.pkl` (~872M fitted Mozaic dict) and
  `.../mozaic_parts.raw.glean.mobile.DAU.parquet` — regenerable intermediates; archived to
  `gs://…/july-2026/` and removed from the pruned working branch (`clean-slate`). Still present in
  the `july-forecast` branch. Pull back only to re-fit from the exact pickled state.

**Where new files go:** nothing new — this is a locked artifact dir. A refit means a new dated
`mobile_refresh_<date>/`.
