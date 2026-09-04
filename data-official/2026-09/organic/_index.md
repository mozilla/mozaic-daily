# `data-official/2026-09/organic/` — measured Fenix paid/organic split (`p`), September 2026

The input to adjustment **`p`** (`paid_organic_split`): mozaic forecasts **organic** Fenix DAU, and the paid
**level** from `../marketing/` is stacked back on from the seam. Rebuilt 2026-09-04 for the September training
window (through 2026-09-01).

| file | role |
|---|---|
| `organic.json` | the spec — gated to `applies_to_forecast_start: 2026-09-02`; `paid_forecast` → `../marketing/marketing_lift_model.gmio_uac_meta_total.2026-09-02.parquet`, **`anchor_paid_dau` 800,831.00** (copied from that curve's meta — must match) |
| `fenix_paid_organic.2026-09-02.parquet` | measured split, `date × country` (823 days × 16 countries = 13,168 rows) |
| `fenix_paid_organic.2026-09-02.parquet.meta.json` | sidecar: definition, sources, coverage, the four build checks |
| `build.log` | the producer's run log (checks: tail overlap, partition identity, split coverage, shredder drift — all PASS) |

Produced by `scripts/build_fenix_organic_split.py --forecast-start-date 2026-09-02 --production-raw
../mobile_rawpull_2026-09-02/mozaic_parts.raw.glean.mobile.DAU.parquet`. Scan was **268 GB (~$1.34)** this cycle — the
mirror snapshot ends 2026-07-01, so the tail extension covered 2026-06-25 → 2026-09-01; the "~141 GB" figure in older docs
assumed a fresher snapshot.

## What changed vs August

| | August | September |
|---|--:|--:|
| training end | 2026-08-01 | 2026-09-01 |
| paid curve | two single-channel feeds, `uac_meta_total.2026-07-28` | GMIO cross-channel feed, `gmio_uac_meta_total.2026-09-02` |
| `anchor_paid_dau` | 922,250.47 | 800,831.00 |
| paid level at Dec-15 | 1,559,477 | 1,891,002 (+331,525) |

The seam step between measured paid (training rows) and marketing's level (forecast rows) is seam-dependent and
**must be re-measured after the rerun** (`paid_seam_step`); do not carry August's +1,903 forward.

## Where new files go

A refreshed split for this cycle: re-run the producer with the new `--forecast-start-date` and repoint `data_file`.
A refreshed paid curve: rebuild in `../marketing/` and copy its `key_values.anchor_paid_dau` here — the anchor
changes with every re-pull.
