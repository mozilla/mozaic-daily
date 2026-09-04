# mozaic_daily — package index

Core forecasting package for Mozilla Firefox metrics. Each module has a single responsibility; see below for where to find and add code.

## Modules

| Module | What's in it | What isn't |
|---|---|---|
| `config.py` | `STATIC_CONFIG`, `FORECAST_CONFIG`, `get_runtime_config()`, `DateConstraints`, country lists, date index generation, git hash retrieval | SQL query logic, BigQuery I/O |
| `queries.py` | `QuerySpec` dataclass with `DateConstraints`; `QUERY_SPECS` dict; `build_query()` SQL generation | BigQuery execution, data caching |
| `data.py` | BigQuery data fetching, pre-flight availability checks, checkpoint read/write, `get_aggregate_data()`, `query_to_dataframe()` (heartbeat-instrumented single-query wrapper — see CLAUDE.md "BQ download appears to hang") | Forecasting, table formatting |
| `forecast.py` | `get_forecast_dfs()`, `get_desktop_forecast_dfs()`, `get_mobile_forecast_dfs()`; `ForecastResult` dataclass; `ModelConfig`/`DesktopModelConfig`/`MobileModelConfig` usage | Data fetching, output formatting |
| `tables.py` | `format_output_table()` — combines Desktop/Mobile, creates ALL rows, renames columns, sets data_source values | BigQuery upload, validation |
| `validation.py` | `validate_output_dataframe()` — schema, format, row counts, nulls, duplicates | Data fetching, formatting |
| `overlays.py` | **Registry-driven dispatch of per-tile overlays.** `registered_overlay_codes()` (codes with `applier: per_tile_overlay`), `find_spec_for_forecast()` (the `applies_to_forecast_start` gate), `resolve_overlays()` → `ResolvedOverlay` (code, name, spec, data source, derived `sentinel_attr`), `overlay_country_shares()` (dispatch on `allocation.key`), `subtract_overlays_pre_mozaic()` / `add_overlays_post_mozaic()`. `main.py` calls these instead of hand-wiring each code | The appliers themselves (`adjustments.py`), the `m`/`p` paths |
| `adjustments.py` | Adjustment-state filename markers (`.raw.` / `.adj-{codes}.`), sidecar `.meta.json` write/read, `load_forecast()` state-validating loader, **composite appliers** (`apply_net_adjustment_to_series`, `render_adjustment` for `linear_ramp` / `step` / `daily_series` / `daily_file`, `load_adjustments_from_dir(..., require_specs=)` — e.g. `h` headwinds, `t` mobile tailwind), **per-tile bidirectional appliers** (`load_overlay_spec`, `load_lift_series`, `compute_country_shares`, `fixed_country_shares_from_spec`, `subtract_lift_from_training`, `add_lift_to_forecast` — e.g. `l`, `o`) | Forecast generation, BigQuery I/O, the mobile paid/organic split (`organic*.py`) |
| `organic.py` | **CONSUMER** side of the mobile paid/organic split `p`: `split_training_to_organic()` (pre-mozaic, scales Fenix training rows by the measured organic share), `marketing_paid_level()` (lift + anchor, held flat past the curve's end), `add_paid_to_forecast()` (post-mozaic level add-back), `paid_seam_step()` (diagnostic) | BigQuery I/O, producing the split itself |
| `organic_source.py` | **PRODUCER** side of `p`: pure transforms turning raw growth-source rows into the per-cycle measured split, plus four checks that raise (partition identity, tail overlap, split coverage, shredder drift). Called only by `scripts/build_fenix_organic_split.py` | BigQuery I/O (the script owns it), consuming the split |
| `seam_ma.py` | Display-layer moving averages: `display_ma()` (variance-matched actuals→forecast seam transition), `reconstruct_matched_daily()`, `daily_to_28ma()`. **The home for seam-MA logic going forward** | Forecast generation, plotting, BigQuery I/O, any cycle-specific paths |
| `main.py` | Pipeline entry point; ties together fetch → forecast → format → validate; `save_mozaic_objects()` | Individual step logic |
| `__init__.py` | Public surface: `main`, `validate_output_dataframe`, `get_git_commit_hash`, `display_ma`, `reconstruct_matched_daily`, `daily_to_28ma` | |

## Where new code goes

- **Display/plot-layer MA or seam handling**: `seam_ma.py`, with a test in `tests/test_seam_ma.py`. Do **not** copy it into a cycle directory — cycles through 2026-07 import a frozen copy from `data-official/2026-06/export_canonical_curves.py` so their delivered curves cannot move, and that file stays untouched. Everything new imports from here. See `_archive/_index.md`.
- **New metric or data source**: add a `QuerySpec` to `queries.py` and wire it into `data.py`
- **New forecast configuration knob**: add a field to `ModelConfig` (or a subclass) in `mozaic.models`, thread through `get_forecast_dfs()` kwargs
- **New output column**: `tables.py` (`format_output_table`) and `validation.py` (schema check)
- **New validation rule**: `validation.py` alongside existing checks; add a test to `tests/test_validation.py`
- **New pipeline step**: `main.py`, with heavy logic in its own module
- **New adjustment type** (e.g., tailwinds, regulatory shifts): register a one-letter code in `data-official/adjustment_codes.yaml` **with an `applier` field**; the filename marker is derived automatically via `state_marker()`. For the two common styles no Python is needed:
  - **Display layer** (`applier: display_layer`) — a spec in the cycle's `adjustments/` dir, summed onto the 28d-MA after mozaic by `load_adjustments_from_dir()`. Live by presence, no date gate. Use for effects well-described at the world rollup level (`h`, `t`) or for a curve that must not enter the training frame. Types: `linear_ramp`, `step`, `daily_series`, `daily_file` (parquet curve beside the spec, applied as its trailing 28d mean to one platform). Dec-15 effect is exactly the spec value, no model re-run.
  - **Per-tile overlay** (`applier: per_tile_overlay`) — a `desktop_overlay` spec + curve parquet under the code's own cycle dir; `overlays.py` discovers, gates and applies it, deriving the idempotency sentinel from the registry `name` so several stack. Use when the adjustment should shift the *model's view of recent history* so it does not extrapolate the effect implicitly — sound only with a hard start date. `l` and `o` are the references. Requires a model re-run. Add a case to `tests/test_overlays.py::TestCommittedRegistryAndSpecs` if the new code should gate on a live seam.
  - **Measured split** (`applier: paid_organic_split`) — scales training rows by a *measured* share pre-mozaic, then adds a separately-forecast level back post-mozaic. Use when the thing being removed can be measured rather than modelled and has no meaningful start date. Reference: `p`, in its own module pair (`organic_source.py` / `organic.py`). This is why the retired `m` overlay was replaced: paid acquisition has no start date, so an overlay anchor was an accounting choice and absorbed 58% of any curve change. A new mechanism of this kind is the only case that still needs code in this package.

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

| knob | desktop default | mobile default | notes |
|---|--:|--:|---|
| `prophet_changepoint_prior_scale` | 0.15983 | 0.02 | Prophet trend flexibility |
| `prophet_changepoint_range` | 0.7 | 0.82 | fraction of history where changepoints may be placed |
| `prophet_n_changepoints` | 25 | 25 | number of potential changepoints |
| `prophet_recent_weeks` | 13 | 13 | window for conditional weekly seasonality |
| `prophet_seasonality_prior_scale` | 0.00825 | 0.1 | exposed 2026-07-31; the defaults reproduce the values previously hardcoded |
| `seasonality_regime` | `'auto'` | `'auto'` | exposed 2026-07-31. **Platform-asymmetric**: on desktop it also flips growth linear↔logistic; on mobile it sets `seasonality_mode` only, and `auto` resolves to *additive* for tiles above 2e6 DAU |
| `seasonality_corr_threshold` | 0.0 | **unavailable** | **desktop-only** — `MobileModelConfig` raises on any non-zero value, because mobile's regime switch is volume-driven, not correlation-driven |
| `holiday_threshold` | −0.032 | −0.032 | holiday impact detection cutoff (forwarded to `populate_tiles`) |
| `holiday_max_radius` / `holiday_min_radius` | 5 / 3 | 5 / 3 | holiday smoothing window (forwarded to `populate_tiles`) |
| `holiday_effect_floor` | −0.6 | −0.6 | lower bound on proportional holiday effects (forwarded to `curate_mozaics`) |

Default `None` uses hardcoded defaults from the mozaic package. `config.to_slug()` renders a compact
label used for scan result directories.

**The four holiday knobs are excluded from parameter searches by standing policy** — strictly local
effects must not be used to move a whole-season quantity. See `research/param-scans/_index.md`.

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
