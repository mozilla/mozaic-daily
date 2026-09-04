# `j` — japan_bot, cycle 2026-09

Japan's non-organic automated traffic, as a `desktop_overlay` for the daily forecast.
Data edge **2026-08-30**. Produced by
`scratch/brwells/regional-story/forecast/deliver.py`; evidence page `site/forecast.html`.

## Which file to use

* **`japan_bot.json`** — the spec. Points at the **MIDDLE** scenario.
* **`japan_bot.{scenario}.2026-08-30.parquet`** — what the pipeline loads.
  `load_lift_series` reads `japan_bot_dau_daily` off a DatetimeIndex, so the date must stay the
  index and never become a column.
* **`.csv`** — the same numbers for reading. **Not** loadable by the pipeline.
* **`.meta.json`** — provenance, fit statistics and the caveats.

## Scenarios

| scenario | plateau DAU/day | % of Japan | reaches | Dec-15 DAU (28ma) | Dec-15 MAU (28ma) |
|---|--:|--:|---|--:|--:|
| Low · freeze at today's level | 33,551 | 3.09% | 2026-08-30 | 33,551 | 139,094 |
| Middle · the ramp runs as long again | 67,101 | 6.19% | 2026-11-28 | 67,094 | 277,108 |
| High · a tenth of Japan | 108,423 | 10.00% | 2027-02-17 | 82,617 | 326,265 |

Switching scenario is a one-line edit to `data_file` in the spec **plus a model re-run** —
the curve is subtracted from training rows, so a spec-only change moves nothing downstream.

## Three things to know before using it

1. **History is the raw measured excess, deliberately unsmoothed.** The daily swing is real
   contamination, not measurement error, so a smoothed subtraction would leave a synthetic
   oscillation in the training frame after `subtract_lift_from_training` runs.
2. **The curve runs to 2027-12-31, held flat.** `add_lift_to_forecast` zero-fills
   absent dates; a curve ending at end-2026 would drop the whole component on 1 January.
3. **The plateau is a planning assumption, not an estimate.** No inflection has been
   observed, so the ceiling is not identifiable from the data.

## Not wired

Registering `j` in `data-official/adjustment_codes.yaml` with `applier: per_tile_overlay` and
`spec_glob: "data-official/*/japan_bot/japan_bot.json"` is the whole wiring — `src/mozaic_daily/overlays.py`
dispatches from the registry (2026-09-04), so the finder/applier pair the original handoff describes is no
longer needed. Single-letter codes only — `parse_state_from_path` splits the filename marker into
characters. Then a model re-run.
