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

## Where new code goes

- **New operational task**: add a script here, not inside `src/mozaic_daily/`
- **Reusable logic**: if a script grows shared helper functions, extract them into `src/mozaic_daily/` (or a new module there) and import
- **Throwaway one-offs**: `tmp/` is for intermediate data and scratch files, not scripts. Scripts here are versioned and re-runnable
