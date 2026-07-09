# `data-official/2026-07/marketing/` — mobile marketing-lift (`m`)

The Fenix Android paid-acquisition DAU lift applied to `glean_mobile` DAU as the bidirectional `m`
adjustment. Several candidate lift models were built this cycle; **only one is wired.**

## ✅ Current / wired

- **`marketing.json`** — the spec the pipeline consumes (`applies_to_forecast_start: 2026-07-06`).
  Its `model_meta_file` points at the wired model:
- **`marketing_lift_model.total.2026-06-29.parquet`** (+ `.meta.json`) — the wired daily lift series.
- **`build_lift.py`** — builder for the wired model. `source_data/` — the marketing-team CSV inputs
  (STMO 118452 lineage; see memory `feedback_marketing_lift_data_source`).
- **`july_marketing_walkthrough.ipynb`** — the walkthrough/derivation notebook.

## ⚠️ Superseded / experimental (not wired — archived at button-down)

These were parallel candidates for the same anchor; kept only for lineage, not consumed by the pipeline:

- `marketing_lift_model.real_data_v2.hybrid.2026-06-29.*` (June's v2-hybrid shape)
- `marketing_lift_model.cohort2026.2026-06-29.*`
- `marketing_lift_model.june_anchored.2026-06-29.*`
- `marketing_lift_model.total_14dMA.EXPERIMENT.2026-06-29.parquet`,
  `marketing_lift_model.total_spline2.EXPERIMENT.2026-06-29.parquet`
- `build_lift_variants.py` — the sweep builder that produced the above candidates.

**Where new files go:** refreshed lift models for this cycle; point `marketing.json` at the new one
and note the previous as superseded here.
