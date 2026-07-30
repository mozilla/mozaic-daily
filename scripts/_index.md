# scripts/ — operational and development helper scripts

One-off runners, backfill tools, and utilities for local development. Not part of the `mozaic_daily` package.

## Scripts

| Script | What it does |
|---|---|
| `run_main.py` | Run the full forecasting pipeline locally with checkpoints (bypasses Metaflow) |
| `run_flow.py` | Unified Metaflow runner: `local`, `remote`, `deploy`, `backfill` |
| `run_validation.py` | Validate a checkpointed forecast parquet file |
| `check_logs.py` | Parse backfill logs to find successes, failures, and ambiguous runs |
| `export_forecast_csv.py` | Export a forecast parquet checkpoint to CSV |
| `generate_iran_synthetic.py` | Run Mozaic for Iran alone and save ALL-level totals (historical + forecast) |
| `add_iran_to_forecast.py` | Add synthetic Iran DAU values to a no-Iran forecast via summation |
| `run_comparison_forecasts.py` | Run multiple forecast variants for side-by-side comparison |
| `test_local_docker.sh` | Build and smoke-test the Docker image locally |
| `generate_iran_fill.py` | Produces the Iran counterfactual-fill artifact for the shutdown gap (current approach; supersedes the synthetic pair above) |
| `verify_forecast_states.py` | Audit on-disk forecast artifacts, verify raw/adjusted state, write `tmp/inventory.csv` |
| `migrate_forecast_names.py` | Rename artifacts to the `.raw.` / `.adj-{codes}.` convention and write sidecar metas |
| `regenerate_composites.py` | Reproduce composite CSVs from raw parquets via `mozaic_daily.adjustments`; diffs against on-disk |
| `plot_forecast_set.py` | Generate the canonical plot set (`global_<platform>.png` etc.) from a forecast checkpoint, mirroring the prior cycle's `csv/plots` |
| `verify_lol_overlay.py` | End-to-end check of the `l` overlay: produces the three ALL-desktop DAU curves from the cached raw legacy pull |
| `verify_mozillaonline_overlay.py` | The `o` equivalent of the above — three ALL-desktop 28d-MA curves |
| `score_near_horizon.py` | Score a build at the near-horizon trough and Dec-15, global and ex-CN/IR, pre/post headwind. Uses the canonical `display_ma`; its ramp clamps at the anchor date. **Scores are not comparable across the 2026-07-29 `Fix A` boundary** |
| `seam_bridge.py` | Seam kink diagnostic (`kink_score`) + daily-level bridge helpers; platform-agnostic |
| `run_param_scan.py` | **One desktop forecast with a fully configurable `DesktopModelConfig`.** The only way to reproduce the s01 lock — `run_main.py` has no parameter flags |
| `run_pinned_scan.py` | Desktop forecast with per-tile Prophet **changepoints pinned to April's locations** — built to test whether changepoint placement explained the April↔June trend gap |
| `run_desktop_gradient.py` | Drives desktop July-2026 parameter-search rounds (gradient + combinations); each probe is one `run_param_scan.py` invocation |
| `run_aug_trough_gradient.py` | Aug-2026 **near-horizon** finite-difference gradient, targeting the 28d-MA of world-headline `legacy_desktop` DAU (the `aug22-retune` search) |
| `run_s01_gradient.py` | Sensitivity gradient around the s01 centre point |
| `run_summer_trough_grid.py` | The summer-trough grid driver (`summer-trough-v2`) |
| `run_trend_only_grid.py` | Trend-only grid. **Refuses holiday overrides by design** — holiday knobs are excluded from tuning by policy |
| `run_mobile_param_scan.py` | One `glean_mobile` DAU forecast with a configurable `MobileModelConfig` — the mobile analog of `run_param_scan.py` |
| `mobile_grid_search.py` | Round-1 one-at-a-time (OAT) sensitivity probe for the July mobile forecast, from the `MobileModelConfig` defaults + central-difference slopes |
| `tile_corr_distribution.py` | Reports the per-tile level/volatility correlation behind desktop's `seasonality_regime="auto"` per-tile regime switch; used to place grid points |

## Where new code goes

- **New operational task**: add a script here, not inside `src/mozaic_daily/`
- **Reusable logic**: if a script grows shared helper functions, extract them into `src/mozaic_daily/` (or a new module there) and import
- **Throwaway one-offs**: `tmp/` is for intermediate data and scratch files, not scripts. Scripts here are versioned and re-runnable
