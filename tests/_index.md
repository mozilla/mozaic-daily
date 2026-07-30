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
| `test_seam_ma.py` | `src/mozaic_daily/seam_ma.py` — the variance-matched actuals→forecast seam MA. Smoothness, far-horizon (Dec-15) byte-identity, anti-tautology guard that the naive blend wobbles, **weekday-invariance of the trend estimator**, the **identity invariant** (all-actuals input ⇒ the transition is a no-op), edge unbiasedness, and a **canary** asserting the suite still rejects the known-bad estimator. Replaces the archived `test_export_canonical_curves.py`. See `research/ma-seam-turbulence/` § Fix A |
| `test_run_flow.py` | `run_flow.py` CLI argument parsing and backfill state logic |
| `test_smoke.py` | Import smoke test — verifies the package and key symbols load |
| `test_model_config_knobs.py` | The mozaic `ModelConfig` knobs this repo drives, focused on `seasonality_corr_threshold` — the per-tile cutoff on desktop's level/volatility correlation that decides the additive/multiplicative regime |
| `test_iran_fill.py` | The fill **producer** (`scripts/generate_iran_fill.py`): unit tests on the pure transforms (population↔segment mapping, day-of-week profile, re-seasonalization) plus artifact-level checks |
| `test_iran_fill_integration.py` | The mozaic-daily **wiring**: that `data_source` is forwarded from the platform wrappers into `mozaic.populate_tiles`, so the package selects the matching built-in fill |
| `test_mozillaonline_model.py` | **Contract** tests on the active MozillaOnline migration *artifact* — the output contract the `o` applier depends on — run against whatever `mozillaonline.json` currently points at |
| `test_seam_bridge.py` | `scripts/seam_bridge.py` — the seam kink diagnostic (`kink_score`) and the daily-level bridge that corrects an actuals→forecast **level** mismatch by decaying an offset over the first window |
| `test_score_near_horizon.py` | `scripts/score_near_horizon.py` — near-horizon scorer, incl. that its MA is the canonical `display_ma` and that its headwind ramp clamps at the anchor date |
| `__init__.py` | Marks the package so `pytest` import mode resolves sibling modules |
| `conftest.py` | Shared fixtures (sample DataFrames, runtime config stubs) |

**Not collected by the live suite:** `_archive/tests/` is in `norecursedirs` (`pyproject.toml`). Those two
modules exercise the frozen 2026-06 seam MA, which cannot change, so reporting on them would be noise.
Run them explicitly with `pytest _archive/tests/ -q` to confirm frozen behaviour still holds.

## Where new tests go

- **New module**: add `test_<module_name>.py` here
- **New validation rule**: add to `test_validation.py` in the appropriate section
- **Shared fixtures**: add to `conftest.py` if used by more than one test file
- **Integration tests that hit BigQuery**: mark with `@pytest.mark.integration` and gate on CI credentials
