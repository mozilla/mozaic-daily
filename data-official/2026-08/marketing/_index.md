# `data-official/2026-08/marketing/` — Fenix Android marketing lift (`m`)

Bidirectional overlay on `glean_mobile` DAU: the daily paid-acquisition lift is subtracted from Fenix
Android training rows before mozaic (so Prophet learns the no-marketing dynamic), then added back to the
per-tile forecast. Allocated by each country's share of Fenix Android DAU over a trailing 28-day window
ending at `training_end_date`, frozen for the horizon. IR excluded (no paid spend there).

| file | role |
|---|---|
| `marketing.json` | the spec — gated on `applies_to_forecast_start: 2026-07-28` |
| `marketing_lift_model.total.2026-06-29.parquet` | the curve (`marketing_lift_daily`) |
| `marketing_lift_model.total.2026-06-29.meta.json` | model provenance |

## Carried forward from July — STALE

Byte-identical copies of `../../2026-07/marketing/`; only `applies_to_forecast_start` moved. Built
June-anchored: `L_july(d) = L_june(d) + (July CSV Total Paid DAU − June CSV Total Paid DAU)`.

Two caveats:

1. **~4 weeks of modelled subtraction.** Training now runs to 2026-07-27 while the curve was built from
   data through late June, so those extra rows subtract a modelled rather than measured lift. Whether
   that over- or under-states depends on how July's actual paid delivery tracked the marketing team's
   outlook — unknown until re-measured.
2. **The curve ends 2026-12-31** and `add_lift_to_forecast` fills missing dates with `0.0`, so the lift
   drops abruptly to zero across the 2027 horizon. Pre-existing in July, harmless for the Dec-15 2026
   KPI, and wrong for anyone reading 2027 out of the mobile parquet.

**Re-measure and swap before this cycle ships.** The data source for a real-data rebuild is STMO query
118452 (`mozdata.fenix.active_users` + modifications) — **not** the `*_marketing_geo_testing_v1` tables,
which are the wrong source for lift measurement.

**Where new files go:** refreshed lift-model builds for this cycle (dated parquet + meta) and the
walkthrough notebook if one is produced. July's candidate variants stay in `../../2026-07/marketing/`.
