# `data-official/2026-07/iran_fill/` — Iran counterfactual-fill artifact

The **counterfactual fill** for Iran's internet-shutdown gap (2026-03-01 → 2026-05-25, fully
recovered 2026-05-26), for the July 2026 forecast cycle. Iran returned to native queries this
cycle; the 86-day near-zero hole would corrupt mozaic's Prophet fit, so we substitute synthetic
"what Iran would have been with no shutdown" values over the gap.

The values come from **propagating the mozaic model forward** (train on clean pre-shutdown IR,
forecast the gap), harvested at mozaic's per-tile granularity, then **re-seasonalized** to restore
the real weekday→weekend amplitude that Prophet damps (see `FILL_FORMAT_SPEC.md` §9). The mozaic
package will be taught to ingest this file (replace IR gap rows before `populate_tiles()`); **this
directory only produces the artifact + its contract.**

## What's here
- `FILL_FORMAT_SPEC.md` — **the contract** (schema, fill windows, invariants, ingestion recipe).
  Source of truth for the package-side ingestion code.
- `iran_fill.<data_source>.parquet` — the fill, one file per data source (`glean_desktop`,
  `legacy_desktop`, `glean_mobile`). Long, segment-boolean format. *(written by `--finalize`)*
- `iran_fill.<data_source>.meta.json` — provenance sidecar (params, fill windows, seam scaling).
- `seam_diagnostics.csv` + `seam_plots/` — forward-forecast-vs-real-recovery seam evidence for the
  human go/no-go on scaling.
- `seam_scale_spec.json` — the approved per-(source, metric) seam scale factors (1.0 = no scaling).
- `_draft/` — unscaled per-source fills from Phase 1 (inputs to `--finalize`); not the deliverable.

## What's not here
- The mozaic-side ingestion code (lives in the mozaic-forecasting package, written separately).
- The recovery-curve modeling for the *post-shutdown smaller-population* regime — that premise is
  obsolete (Iran fully recovered). See `research/iran/` for the retired recovery model.

## Producer & where new code goes
- Producer: `scripts/generate_iran_fill.py` (forward-forecast → harvest → seam diagnostics →
  finalize). Adapted from the retired `scripts/generate_iran_synthetic.py`.
- Tests: `tests/test_iran_fill.py`.
- New month's fill → a sibling `data-official/{YYYY-MM}/iran_fill/` dir (this is shutdown-specific
  and should not recur, but the pattern is reusable for any future contiguous-gap fill).
