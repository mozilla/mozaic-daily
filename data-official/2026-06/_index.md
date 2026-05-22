# `data-official/2026-06/` — June 2026 forecast cycle

Forecast_start_date: **2026-05-13** initial; refreshed to **2026-05-17** with cap-426 fix. Adds the `m` (marketing-lift) adjustment on top of `h` (headwinds), producing `.adj-hm.` composites.

## Producer notebooks

| File | Output |
|---|---|
| `june_composite_forecast.ipynb` | Headwind-only composite: `june_composite_forecast_28ma.adj-h.csv` |
| `june_composite_forecast_no_headwinds.ipynb` | Counterfactual for sensitivity discussions |

The threshold-aligned variant `june_thresh_aligned_composite_forecast_28ma.adj-hm.csv` is produced by the threshold-matched build (threshold=-0.05, headwind anchor -1,408,000) to bring June desktop within ~157k of April at Dec-15. See memory `project_june_thresh_aligned_build`.

The marketing-lift composite `june_with_marketing_composite_forecast_28ma.adj-hm.csv` is the consensus deliverable: headwinds + marketing-lift applied.

## Diagnostics (june-anchored)

| File | What it does |
|---|---|
| `june_vs_april_desktop_diagnostic.ipynb` | Why June desktop forecast levels lower than April — trend + changepoint analysis |
| `june_mobile_dau_forecast_comparison.ipynb` | Compares June mobile DAU across region/OS splits |

## Per-config subdirs

| Dir | Config |
|---|---|
| `desktop_cps0.15983_thresh50_recent13_clip0.6/` | Initial June config (threshold=-0.50, pre-cap-426 fix) |
| `desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/` | Threshold-aligned build (threshold=-0.05, cap-426 fix) |
| `desktop_cps0.15983_thresh032_recent13_clip0.6_cap426/` | Default-threshold cap-426 variant for comparison |
| `mobile_cps0.02_thresh32_recent13_clip0.6/` | Initial June mobile |
| `mobile_cps0.02_thresh32_recent13_clip0.6_cap426/` | With cap-426 fix; carries `.adj-m.` parquets (marketing-lift applied) |

## Adjustments

- `adjustments/headwind.json` — June's headwind ramp spec (note anchor difference from April)
- `marketing/` — Marketing-lift `m` adjustment artifacts:
  - `marketing.json` — spec consumed by the pipeline
  - `marketing_lift_model.real_data_v2.hybrid.<date>.parquet` — daily lift series
  - `*.meta.json` sidecars for each parquet

## Other artifacts

- `comparisons/` — scratch param-scan runs (all raw, not held to adjustment-state convention)
- `stakeholder_scenarios/` + `.zip` — pre-packaged scenario CSVs for leadership review
