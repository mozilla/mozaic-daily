# `data-official/2026-04/` — April 2026 forecast cycle

Forecast_start_date: **2026-04-01**. The April cycle is the reference against which all subsequent refreshes are compared.

## Producer notebook

| File | What it does |
|---|---|
| `april_composite_forecast.ipynb` | Builds the headline composite forecast from desktop + mobile parquets, applies headwinds, writes `april_composite_forecast_28ma.adj-h.csv` |

## Diagnostics (april-anchored)

| File | What it does |
|---|---|
| `april_sanity_checks.ipynb` | Sanity checks on April forecast parquets and regional splits |
| `april_forecast_regional_baseline.ipynb` | Regional baseline analysis (UK/US/Germany/ROW) |
| `april_h_vs_june_noh_vs_actuals.ipynb` | April-with-headwinds vs. June-without-headwinds vs. actuals comparison |

## Per-config subdirs

| Dir | Config |
|---|---|
| `desktop_cps0.15983_thresh050_recent13_clip0.6/` | Production desktop config (threshold=-0.05) |
| `mobile_cps0.02_thresh32_recent13_clip0.6/` | Production mobile config |

Each subdir contains the raw parquet, `.plus_iran.parquet`, sidecar `.meta.json` files, fitted Mozaic pkls, and `parameters.json`.

## Composite output (load-bearing)

`april_composite_forecast_28ma.adj-h.csv` — leadership-facing CSV with 28-day MA, headwind-adjusted. Sidecar `.meta.json` carries provenance. Loaded via `mozaic_daily.adjustments.load_forecast()`.

## Adjustments + comparisons

- `adjustments/headwind.json` — April's headwind ramp spec
- `comparisons/` — scratch param-scan runs (not held to the adjustment-state convention; all raw model output)
