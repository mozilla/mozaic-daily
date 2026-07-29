# `data-official/2026-08/launch_on_login/` — launch-on-login desktop tailwind (`l`)

Bidirectional overlay on `legacy_desktop` DAU, `modern_windows` segment: the measured historical rise
is subtracted from training rows before mozaic (so Prophet learns the no-LOL dynamic), then the capped
curve is added back to the per-country forecast.

| file | role |
|---|---|
| `lol.json` | the spec — gated on `applies_to_forecast_start: 2026-07-28` |
| `lol_tailwind.2026-06-29.parquet` | the curve (`lol_lift_daily`), 2026-01-01 → 2027-12-31 |
| `lol_tailwind.2026-06-29.model_meta.json` | model provenance for the curve |

## Carried forward from July — STALE

The two curve files are **byte-identical copies** of `../../2026-07/launch_on_login/`. Only
`applies_to_forecast_start` moved (2026-07-06 → 2026-07-28). The `.2026-06-29.` in the filenames is
honest: that is when the curve was built, and it has not been rebuilt.

The curve is clamped **flat at 125,000 DAU/day from 2026-06-19**, which was deliberately conservative —
the measured effect had **not** plateaued (still rising ~19K/wk at the last clean date, 2026-06-23;
contamination begins 2026-06-24). Training now runs to 2026-07-27, so ~5 weeks of training rows have
the flat modelled 125K subtracted rather than a measured value. If the real effect kept rising, the
subtraction is too small and Prophet absorbs the residual into trend.

**Re-measure and swap before this cycle ships.** A swap is a drop-in: build a new
`lol_tailwind.<date>.parquet`, point `data_file` at it, leave everything else alone.

**Where new files go:** refreshed curve builds for this cycle (new dated parquet + model meta), and
diagnostic plots under `plots/`. The producer lives outside this repo at
`~/work/launch-on-login/build_lol_tailwind.py`.
