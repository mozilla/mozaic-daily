# `v2-real-data/` — current marketing-lift approach (CSV-driven hybrid)

Drops the convolution-from-installs assumption. Uses Paid-DAU directly from the marketing analyst's CSV (delivered weekly) and stitches it with the empirical Fenix gap to produce a hybrid lift series.

## Notebook flow

| Notebook | Purpose |
|---|---|
| `01_signal_extraction.ipynb` | Loads marketing CSV; documents the 3 Paid-DAU definitions |
| `02_forecast_projection.ipynb` | CSV-only lift series, anchor=2026-03-30 (Option B) |
| `03_exploration.ipynb` | Gap composition (Fenix vs. ALL MOBILE) + anchor-date sensitivity |
| `04_hybrid_lift.ipynb` | Empirical Fenix gap historical + additively-stitched CSV future |
| `05_trial_forecast_diagnostic.ipynb` | June-composite-style plot with April N-1, hybrid, no-marketing-lift baseline; w/ and w/o headwinds |

The `*_v2_*.ipynb` variants are post-validation refinements of 02–05 against the latest marketing-team CSV (2026-05-22 delivery). Open in the v2 file when current; keep the non-v2 as the lineage anchor.

## Operational status

- Hybrid model output drops in unchanged via the existing `m`-adjustment plumbing (no pipeline changes)
- Dec-15 mobile MA28: hybrid is **+563k** above the no-marketing baseline (convolution model would have been +1.43M)
- Open questions for the marketing analyst tracked in `questions_for_marketing_analyst.md`

## Source

Marketing-team CSV is delivered via STMO 118452 (`mozdata.fenix.active_users` + modifications), NOT the `*_marketing_geo_testing_v1` tables.
