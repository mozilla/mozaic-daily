# `research/april-vs-june-mechanism/` — why June forecasts level lower than April

Cross-month diagnostic thread investigating why each forecast refresh systematically lands below the previous month's forecast for the same target date. The conclusion (per `project_june_gap_resolution` memory) is that ~75-82k of the Dec-15 MA28 gap is a real Win10 headwind Prophet has begun absorbing; pinning and tuning cannot close it, and the `adj-h` headwind ramp should attenuate as the headwind shows up in actual data.

## Notebooks

| File | Purpose |
|---|---|
| `prophet_april_vs_june.ipynb` | Compares Prophet components (trend, seasonality, regressors) between April and June fits |
| `prophet_global_components_april_vs_june.ipynb` | Deeper dive into global seasonality + trend changepoint differences |
| `actuals_vs_april_diagnostic.ipynb` | Reconciles contradictory April-vs-May actuals deltas (+105k vs. -700k) across no-Iran / plus-Iran / adj-h configs |
| `mozaic_changepoints.ipynb` | Earlier (Apr 8) exploration of Prophet `changepoint_prior_scale` sensitivity |

## Related artifacts

- April-anchored sanity work lives under `data-official/2026-04/` (composite notebook, regional baseline, april-h-vs-june-noh diagnostic)
- Param-scan sensitivity is its own cluster: `../param-scans/`
- Headwind-ramp exploration: `../headwinds/`
