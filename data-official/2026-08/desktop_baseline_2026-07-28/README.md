# Desktop baseline — August 2026 (forecast_start 2026-07-28)

`legacy_desktop` DAU baseline. **Not a delivered forecast** — see `../_index.md`.

Single config subdir: `cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/`.

## Files

- `mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` — the forecast. Overlays `l`
  (launch-on-login) + `o` (MozillaOnline migration) applied bidirectionally on `modern_windows`.
  **Pre-headwind** — the Win10 headwind is a display-layer adjustment applied in the canonical notebook.
- `…meta.json` — sidecar provenance (model config, adjustments + spec sha1s, commit).
- `parameters.json` — the exact `DesktopModelConfig` used.
- `mozaic_objects.legacy_desktop.2026-07-28.pkl`, `mozaic_parts.raw.legacy.desktop.DAU.parquet` —
  fitted state and the pre-forecast BQ aggregate. Gitignored; archive to GCS at button-down.

## Configuration — July's lock, unchanged

`cps=0.08983`, `changepoint_range=0.65`, `n_changepoints=25`, `recent_weeks=13`,
`holiday_threshold=−0.032`, `max_radius=5`, `min_radius=3`, `effect_floor=−0.6`.

Identical to `../../2026-07/desktop_locked/parameters.json`. The `_sps0.00825` in the slug is the
newly-exposed `seasonality_prior_scale` at its default, which reproduces the value July hardcoded — so
the slug differs from July's while the behaviour does not.

Trained through **2026-07-27**. Iran queried natively; shutdown gap covered by the mozaic package's
built-in counterfactual fill.

## Result (Dec-15 2026, 28d-MA, headwind applied)

**48,520,714** — **−64,769 (−0.13%)** vs July's delivered 48,585,483.

Aug-22 summer trough: 43,349,248 (post-headwind).

## Reproduce

See the command block in `../_index.md`. `scripts/run_param_scan.py` is the producer —
`run_main.py` cannot reproduce this config (no parameter flags).
