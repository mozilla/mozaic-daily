# `j` — japan_bot, cycle 2026-09

**What it is:** Japan's reported desktop DAU has carried a population of automated clients since 2026-06-24, defined by behaviour (sub-minute sessions, 0 to 10 URIs, not the default browser, profile age under 28 days) with no channel term; 95.1% of them turn out to be ESR. This curve is that population's daily DAU, measured as the excess over a flat 1,876 ± 265 DAU/day baseline through 2026-08-30 and projected after, so the model can learn Japan without it and have it stacked back on at face value. It is a **masking effect, not growth**: underneath it, ordinary Japanese users on ordinary Firefox are down. It does not double count `o`, whose migration population is entirely non-ESR. Produced by `scratch/brwells/regional-story/forecast/deliver.py`; evidence at that project's `site/forecast.html`, reasoning in its `DECISIONS.md` D76–D90. `HANDOFF.md` here is the producer's original hand-over, kept for the caveats.

**Family:** per-tile overlay: subtracted from training rows before mozaic and added back after; **needs a model re-run**. **Platform:** desktop (`legacy_desktop`). **Sign:** tailwind (+).

## Files

| file | role |
|---|---|
| `japan_bot/japan_bot.json` | the spec, gated on `applies_to_forecast_start: 2026-09-02` |
| `japan_bot.2026-08-30.parquet` | what the pipeline loads: `japan_bot_dau_daily` on a `target_date` DatetimeIndex, `japan_bot_dau_ma`, `source` |
| `japan_bot.2026-08-30.meta.json` | provenance: source sha1, column mapping, coverage, hold-flat rule, checks |
| `source_data/japan_bot.middle.2026-08-30.csv` | the delivered file, byte for byte |
| `plots/japan_bot.2026-08-30.curve.png` | the curve's shape: daily + 28d mean, measured / projected, seam and Dec-15 marked |

## Coverage

| | |
|---|---|
| delivered | 2026-04-01 → 2027-12-31 |
| actuals through | 2026-08-30 |
| held flat from | not needed — the delivered file already reaches the horizon |
| horizon | 2026-01-01 → 2027-12-31 |
| Dec-15 28d MA | 67,094 |

## Allocation

Localized: fixed country shares {"JP": 1.0}; excluded: none.

## What is measured and what is assumed

| quantity | status |
|---|---|
| daily excess, 2026-06-24 → 2026-08-30 | **measured** — this is what history ships |
| arrivals through 2026-08-03; per-cohort activity kernel | **measured** (counted cohorts) |
| sample scale (6.0) | calibration, one constant |
| arrivals 2026-08-04 → edge | inverted from measured DAU |
| tail kernel scale (4.94 active days) | solved from MAU |
| tail kernel shape | borrowed from the last measurable cohort |
| the plateau, 67,101 DAU/day (6.19% of Japan), reached 2026-11-28 | **an assumption** — MIDDLE of three, chosen 2026-09-04 |

Fit against raw daily values: DAU WAPE 3.3%, r 0.9972, 10 of 10 turning points at zero lag. History is
**deliberately unsmoothed**: the daily swing is real contamination, so a smoothed subtraction would leave a
synthetic oscillation in the training frame.

## Alternates (decided 2026-09-04)

LOW (plateau 33,551; Dec-15 28d-MA 33,551) and HIGH (108,423; Dec-15 82,617) were delivered alongside MIDDLE.
They are no longer on disk. Parquets and metas are in git at `0a6e751`
(`japan_bot.{low,high}.2026-08-30.{parquet,meta.json}`); the CSV twins and `japan_bot.all_scenarios.2026-08-30.csv`
are archived at
`gs://moz-data-science-brwells-bucket/mozaic-daily-archive/september-2026/data-official/2026-09/japan_bot/alternates/`.
Switching scenario is a re-ingest of the corresponding CSV with `--replace`, plus a model rerun. The handoff's
original MIDDLE spec and parquet are in `../japan_bot_REVERT_2026-09-04/`.

## Where new files go

A refreshed curve for this cycle: re-run the ingest with `--replace`; the previous build moves to `japan_bot_REVERT_<date>/`. Cross-cycle analysis of this effect goes to `research/`.
