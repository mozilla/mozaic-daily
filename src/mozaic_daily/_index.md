# mozaic_daily — package index

Core forecasting package for Mozilla Firefox metrics. Each module has a single responsibility; see below for where to find and add code.

## Modules

| Module | What's in it | What isn't |
|---|---|---|
| `config.py` | `STATIC_CONFIG`, `FORECAST_CONFIG`, `get_runtime_config()`, `DateConstraints`, country lists, date index generation, git hash retrieval | SQL query logic, BigQuery I/O |
| `queries.py` | `QuerySpec` dataclass with `DateConstraints`; `QUERY_SPECS` dict; `build_query()` SQL generation | BigQuery execution, data caching |
| `data.py` | BigQuery data fetching, pre-flight availability checks, checkpoint read/write, `get_aggregate_data()` | Forecasting, table formatting |
| `forecast.py` | `get_forecast_dfs()`, `get_desktop_forecast_dfs()`, `get_mobile_forecast_dfs()`; `ForecastResult` dataclass; `ModelConfig`/`DesktopModelConfig`/`MobileModelConfig` usage | Data fetching, output formatting |
| `tables.py` | `format_output_table()` — combines Desktop/Mobile, creates ALL rows, renames columns, sets data_source values | BigQuery upload, validation |
| `validation.py` | `validate_output_dataframe()` — schema, format, row counts, nulls, duplicates | Data fetching, formatting |
| `adjustments.py` | Adjustment-state filename markers (`.raw.` / `.adj-{codes}.`), sidecar `.meta.json` write/read, `load_forecast()` state-validating loader, applier functions (`apply_net_adjustment_to_series`, `render_adjustment`, `load_adjustments_from_dir`) | Forecast generation, BigQuery I/O |
| `main.py` | Pipeline entry point; ties together fetch → forecast → format → validate; `save_mozaic_objects()` | Individual step logic |
| `__init__.py` | Public surface: `main`, `validate_output_dataframe`, `get_git_commit_hash` | |

## Where new code goes

- **New metric or data source**: add a `QuerySpec` to `queries.py` and wire it into `data.py`
- **New forecast configuration knob**: add a field to `ModelConfig` (or a subclass) in `mozaic.models`, thread through `get_forecast_dfs()` kwargs
- **New output column**: `tables.py` (`format_output_table`) and `validation.py` (schema check)
- **New validation rule**: `validation.py` alongside existing checks; add a test to `tests/test_validation.py`
- **New pipeline step**: `main.py`, with heavy logic in its own module
- **New adjustment type** (e.g., tailwinds, regulatory shifts): register a one-letter code in `data-official/adjustment_codes.yaml`, add the applier to `adjustments.py`, extend `tests/test_adjustments.py`. The filename marker is derived automatically via `state_marker()`.

## Key data flow

```
config.py           → runtime config (dates, countries)
queries.py          → SQL for each metric
data.py             → BigQuery results as DataFrames
forecast.py         → ForecastResult (dfs + mozaic objects + config)
tables.py           → combined, formatted output DataFrame
validation.py       → validated DataFrame ready for BQ upload
main.py             → orchestrates all of the above
```

## Configurable forecast parameters

`forecast.py` accepts a `config` argument (`DesktopModelConfig` or `MobileModelConfig`) that controls:
- `prophet_changepoint_prior_scale` — Prophet trend flexibility
- `prophet_changepoint_range` — fraction of history where changepoints are placed (desktop default 0.7, mobile 0.82)
- `prophet_n_changepoints` — number of potential changepoints (default 25)
- `prophet_recent_weeks` — window size for conditional weekly seasonality
- `holiday_threshold` — holiday impact detection cutoff (forwarded to `populate_tiles`)
- `holiday_max_radius` / `holiday_min_radius` — holiday smoothing window (forwarded to `populate_tiles`)
- `holiday_effect_floor` — lower bound on proportional holiday effects (forwarded to `curate_mozaics`)

Default `None` uses hardcoded defaults from the mozaic package.

## Loading forecast artifacts

**Always load forecast parquets/CSVs through `adjustments.load_forecast(path)`** rather than `pd.read_parquet()` directly. The loader:
- Refuses files without a `.raw.` or `.adj-{codes}.` state marker
- Refuses files without a sidecar `.meta.json`
- Refuses files whose filename marker disagrees with `meta["adjustments_applied"]`
- Returns `(df, meta)` so callers always know the adjustment state

```python
from mozaic_daily.adjustments import load_forecast
df, meta = load_forecast("data-official/2026-06/.../forecast.2026-05-13.ld-D.raw.parquet")
df, meta = load_forecast(path, require_state=["h"])  # raises ValueError if not adj-h
```

See `data-official/_index.md` for the full naming convention and `CLAUDE.md` for the LLM-facing summary.
