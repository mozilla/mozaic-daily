# tests/ — test suite

Unit and integration tests for `mozaic_daily`. Mirrors the package structure: one test file per source module.

## Files

| File | Tests |
|---|---|
| `test_adjustments.py` | Filename markers, sidecar meta round-trip, composite adjustment math (`linear_ramp`, `step`, `daily_series`), per-tile marketing-lift applier (spec validation, country shares, subtract from training, add-back to forecast) |
| `test_validation.py` | All validation rules: schema, string formats, row counts, nulls, duplicates |
| `test_forecasting.py` | `get_forecast_dfs`, `get_desktop_forecast_dfs`, `get_mobile_forecast_dfs` |
| `test_config.py` | `get_runtime_config`, date logic, `DateConstraints` |
| `test_queries.py` | `QuerySpec.build_query()` SQL generation |
| `test_data_fetching.py` | Data fetching helpers, checkpoint read/write, `query_to_dataframe` heartbeat watchdog (format, hint-on-first-tick, `Next ≤Ns` liveness bound) |
| `test_table_manipulation.py` | `format_output_table`, ALL-row generation, column renaming |
| `test_export_canonical_curves.py` | `display_ma` seam fix in `data-official/2026-06/export_canonical_curves.py`: bridge smooths the actuals→forecast seam, far-horizon (Dec-15) byte-identical, anti-tautology guard that the naive blend wobbles. See `research/ma-seam-turbulence/` |
| `test_run_flow.py` | `run_flow.py` CLI argument parsing and backfill state logic |
| `test_smoke.py` | Import smoke test — verifies the package and key symbols load |
| `conftest.py` | Shared fixtures (sample DataFrames, runtime config stubs) |

## Where new tests go

- **New module**: add `test_<module_name>.py` here
- **New validation rule**: add to `test_validation.py` in the appropriate section
- **Shared fixtures**: add to `conftest.py` if used by more than one test file
- **Integration tests that hit BigQuery**: mark with `@pytest.mark.integration` and gate on CI credentials
