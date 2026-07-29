# Desktop — August 2026 (forecast_start 2026-07-28, LOL 165K)

`legacy_desktop` DAU. **Not a delivered forecast** — see `../_index.md`.

⚠️ **The directory name is a misnomer.** It originally held the 125K-LOL *baseline*, which the
2026-07-29 LOL-165K rebuild **overwrote in place** (deliberately — 125K was July's number). The name is
kept because the canonical notebook and the committed sidecar reference this path. The superseded
baseline's Dec-15 was **48,520,714**, preserved here and in git commit `140412e`.

Single config subdir: `cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/`.

## Files

- `mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` — the forecast. Overlays `l`
  (launch-on-login, **165K ceiling**) + `o` (MozillaOnline migration, stale July carry-forward) applied
  bidirectionally on `modern_windows`.
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

**48,611,795** — **+26,312 (+0.05%)** vs July's delivered 48,585,483.

Aug-22 summer trough: 43,415,259 (post-headwind).

Attribution ledger (the notebook asserts this closes against the measured value; residual −0):

| | Dec-15 28d-MA | step |
|---|--:|--:|
| July delivered (125K LOL, hw −1,345,000, 07-06 anchor) | 48,585,483 | — |
| Aug baseline (125K LOL, hw −1,345,000, 07-28 anchor) — superseded | 48,520,714 | −64,769 data refresh |
| Aug, LOL 165K (hw −1,345,000) — superseded | 48,561,795 | +41,081 LOL cap |
| **Aug current (165K LOL, hw −1,295,000)** | **48,611,795** | +50,000 headwind |

**Only the first two steps involved this parquet.** The LOL cap change required rebuilding it; the
headwind step did not — `h` is applied to the 28-day MA in the canonical notebook, never to the training
frame, so its Dec-15 effect is exactly the anchor delta with no Prophet interaction.

The LOL curve is +40,000/day higher at every forecast date and the realised effect was +41,081 —
essentially pass-through, a +3% amplification rather than an offset, because the extra training
subtraction covers only 39 recent days and barely shifts the fitted trend five months out.

## Reproduce

See the command block in `../_index.md`. `scripts/run_param_scan.py` is the producer —
`run_main.py` cannot reproduce this config (no parameter flags).
