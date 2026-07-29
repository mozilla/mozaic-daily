# Mobile baseline — August 2026 (forecast_start 2026-07-28)

`glean_mobile` DAU baseline. **Not a delivered forecast** — see `../_index.md`.

Single config subdir: `cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/`.

## Files

- `mozaic_daily_forecast.2026-07-28.gm-D.adj-m.parquet` — the forecast, with the marketing lift (`m`)
  applied bidirectionally in-pipeline (subtracted from Fenix Android training rows before mozaic, added
  back to the per-tile forecast). **Pre-headwind** — the mobile headwind is applied at the display layer
  in the canonical notebook.
- `…meta.json` — sidecar provenance (model config, adjustments + spec sha1s, commit).
- `parameters.json` — the exact `MobileModelConfig` used.
- `mozaic_objects.glean_mobile.2026-07-28.pkl`, `mozaic_parts.raw.glean.mobile.DAU.parquet` — fitted
  state and the pre-forecast BQ aggregate. Gitignored; archive to GCS at button-down.

## Configuration — July's lock, unchanged

`cps=0.035`, `changepoint_range=0.75`, `n_changepoints=25`, `recent_weeks=13`,
`holiday_threshold=−0.055`, `max_radius=5`, `min_radius=3`, `effect_floor=−0.6`.

These are the `grad_moderate` grid-search params from `research/param-scans/mobile-july/`, identical to
`../../2026-07/mobile_refresh_2026-07-06/…/parameters.json`. The `_sps0.1` in the slug is the
newly-exposed `seasonality_prior_scale` at its default, which reproduces the value July hardcoded.

Trained through **2026-07-27**. Iran queried natively (all-level fill auto-applied by the package).

## Result (Dec-15 2026, 28d-MA, headwind applied)

**17,924,607** — **+738 (+0.00%)** vs July's delivered 17,923,869. Essentially unchanged.

Ex-Iran Dec-15: 17,303,955 (vs July 17,302,425, +1,530). Aug-22 trough: 17,046,467 (post-headwind).

## Reproduce

See the command block in `../_index.md`. `scripts/run_mobile_param_scan.py` is the producer; it threads
the marketing spec through a patched `process_data_source` so the lift is baked in-pipeline rather than
added at the display layer.
