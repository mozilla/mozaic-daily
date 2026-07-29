# `research/summer-slump/` — desktop DAU summer-slump seasonal shape

Cross-month, topic-anchored visualization (per the repo hybrid rule): the recurring
May→September dip in desktop DAU, one curve per year, each indexed to its own May 1 value
so the *shape* of the slump is comparable across years regardless of absolute level.

## What's here

- **`summer_slump.ipynb`** — builds the plot. Reads the global top-line desktop series
  (`country=ALL`, `app_name=desktop`, `segment={"os":"ALL"}`) from the locked July desktop
  forecast parquet. Years 2020–2025 are pure actuals; 2026 is actuals through the training
  seam (2026-07-05) then the latest July forecast (`desktop_locked` `adj-lo`, forecast_start
  2026-07-06) projected forward.
- **`plots/summer_slump_desktop_dau.png`** — the output.

## Data source

`data-official/2026-07/desktop_locked/mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet`
(loaded via `mozaic_daily.adjustments.load_forecast`). This is legacy_desktop DAU; its
training rows carry actuals back to 2020-01-01.

## What isn't here

Not a production artifact and not wired into the pipeline — purely a diagnostic/communication
plot. If the underlying forecast is re-locked, re-point `FORECAST_PATH` in the notebook.
