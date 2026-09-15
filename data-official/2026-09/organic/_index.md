# `data-official/2026-09/organic/` — measured Fenix paid/organic split (`p`), September 2026

The input to adjustment **`p`** (`paid_organic_split`): mozaic forecasts **organic** Fenix DAU, and the paid
**level** from `../marketing/` is stacked back on from the seam. Rebuilt 2026-09-04 for the September training
window (through 2026-09-01), and **again 2026-09-10 for the refreshed seam 2026-09-09** (training through 2026-09-08; scan
294 GB, ~$1.47; all four checks PASS, 830 days × 16 countries, shredder drift −2.9% at the oldest day, 0.0 at the edge).
The 2026-09-02 split stays on disk beside it.

| file | role |
|---|---|
| `organic.json` | the spec — gated to `applies_to_forecast_start: 2026-09-09` (refreshed 2026-09-10 from 2026-09-02); `data_file` → `fenix_paid_organic.2026-09-09.parquet`; `paid_forecast` → `../marketing/marketing_lift_model.gmio_uac_meta_total_med.2026-09-09.pull2026-09-15.parquet` (the delivered workbook's **Med** scenario = the sheet's 90% CI lower end, the c-suite's "marketing midpoint"; repointed 2026-09-15 at leadership's request from the **Low** scenario wired 2026-09-10 = point estimate − 3.3%; Low, ci90lo, the 09-09 point estimate and the 09-04 pull all stay beside it), `value_column: paid_dau_level_daily` — the level **as delivered, no anchor** (framing changed 2026-09-09; August's lift-plus-anchor specs still load through the legacy path) |
| `fenix_paid_organic.2026-09-09.parquet` | **live** measured split, `date × country` (830 days × 16 countries = 13,280 rows), training through 2026-09-08 |
| `fenix_paid_organic.2026-09-02.parquet` | the 2026-09-02-seam split (823 days), kept beside it for the revert-target builds |
| `fenix_paid_organic.2026-09-02.parquet.meta.json` | sidecar: definition, sources, coverage, the four build checks |
| `build.log`, `build.2026-09-09.log` | the producer's run logs for the two splits (checks: tail overlap, partition identity, split coverage, shredder drift — all PASS) |

Produced by `scripts/build_fenix_organic_split.py --forecast-start-date 2026-09-02 --production-raw
../mobile_rawpull_2026-09-02/mozaic_parts.raw.glean.mobile.DAU.parquet`. Scan was **268 GB (~$1.34)** this cycle — the
mirror snapshot ends 2026-07-01, so the tail extension covered 2026-06-25 → 2026-09-01; the "~141 GB" figure in older docs
assumed a fresher snapshot.

## What changed vs August

| | August | September |
|---|--:|--:|
| training end | 2026-08-01 | 2026-09-08 (seam 2026-09-09; was 2026-09-01 until the 2026-09-10 refresh) |
| paid curve | two single-channel feeds, `uac_meta_total.2026-07-28` | marketing's delivered workbook, Med scenario `gmio_uac_meta_total_med.2026-09-09.pull2026-09-15` (c-suite, 2026-09-15; replaced the Low scenario 1,814,609 wired 2026-09-10, which had replaced the GMIO query pulls: ci90lo 1,826,168, point estimate 1,883,182) |
| framing | lift + `anchor_paid_dau` 922,250.47 | level column, no anchor |
| paid level at Dec-15 | 1,559,477 | 1,833,753 Med scenario (+274,276; Low was 1,814,609, the ci90lo pull 1,826,168, the 09-09 point estimate 1,883,182, the 09-04 pull 1,891,002) |

The seam step between measured paid (training rows) and marketing's level (forecast rows) is seam-dependent and
**must be re-measured after the rerun** (`paid_seam_step`); do not carry August's +1,903 forward.

## Where new files go

A refreshed split for this cycle: re-run the producer with the new `--forecast-start-date` and repoint `data_file`.
A refreshed paid curve: pull it into `../marketing/` (`/pull-marketing-curve`; a delivered workbook goes through the same
script's `--from-xlsx` path) and repoint `paid_forecast.data_file` here. Nothing else changes — there is no anchor to copy.
