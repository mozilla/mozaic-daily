# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This repository implements automated daily forecasting for Mozilla Firefox metrics using the Mozaic package. The system runs as a Metaflow pipeline on Outerbounds infrastructure, querying BigQuery for telemetry data and producing forecasts for Desktop and Mobile platforms.

## Code Quality Standards

**All code written in this repository must be human-readable and maintainable.** Code is written for humans to review, understand, and edit, not just for machines to execute.

### Expectations

- **Clean variable and function names**: Use descriptive, meaningful names that communicate purpose (e.g., `forecast_start_date`, not `fsd` or `x1`)
- **Human-like decomposition**: Break complex logic into well-named functions with clear responsibilities
- **Self-documenting code**: Code should be understandable without excessive comments through good naming and structure
- **Consistent style**: Follow existing patterns in the codebase for formatting, naming conventions, and organization
- **Logical organization**: Group related functionality together; keep functions focused on a single responsibility
- **Readable flow**: Structure code in the order it will be read and understood, not just executed

### What to Avoid

- Cryptic abbreviations or single-letter variables (except standard loop counters like `i`, `j`)
- Overly complex one-liners that sacrifice readability for brevity
- Deep nesting that makes control flow hard to follow
- Functions that do too many unrelated things
- Magic numbers or strings without clear names

Code reviews are a core part of development. Write code that will make sense to your future self and your colleagues.

## Planning and Implementation

**All implementation plans must include documentation and test updates.** When creating a plan for any code change:

- **Documentation**: Plans must identify all documentation that needs updating (CLAUDE.md, docstrings, code comments, README files)
- **Tests**: Plans must specify what tests need to be added, updated, or verified
- **Verification**: Plans must include a verification step to ensure tests pass and documentation is accurate

This is not optional. Documentation and tests are first-class artifacts, not afterthoughts. A plan that omits these is incomplete.

## Development Environment

### Virtual Environment

This project uses a Python virtual environment (`.venv/`) to isolate dependencies. **Always activate the virtual environment before installing packages or running module files.**

```bash
# Activate the virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r docker/requirements.outerbounds.txt

# The Mozaic package is installed from a specific branch
# (already in .venv if cloned, but to reinstall):
pip install -e 'git+https://github.com/mozilla/mozaic-forecasting@april-metaparameter-updates#egg=mozaic'

# Deactivate when done
deactivate
```

**Important:** All Python commands below assume the virtual environment is activated.

## Code Structure

The codebase is organized as a Python package:

```
src/mozaic_daily/
├── __init__.py       # Package exports
├── config.py         # Constants and date logic
├── queries.py        # SQL query specifications (QuerySpec.build_query() generates SQL)
├── data.py           # BigQuery data fetching and query execution
├── forecast.py       # Mozaic forecasting logic
├── tables.py         # Table formatting and manipulation
├── validation.py     # Output validation
├── seam_ma.py        # Display-layer 28d MAs; variance-matched actuals→forecast seam transition
└── main.py           # Main entry point
```

### Display-layer moving averages (`seam_ma.py`)

Stakeholder-facing forecast curves are plotted as 28-day trailing MAs. A trailing window straddling
the actuals→forecast seam mixes the two, and because the forecast's weekly amplitude is damped
relative to recent actuals it fails to cancel the weekly cycle — so the MA wobbles for ~a month.
`display_ma()` replaces those transition points with a variance-matched transition, and is
**byte-identical to a plain `rolling(28).mean()` from seam+27 onward**, so Dec-15 and every headline
number are untouched by anything it does.

**Always import it from the package**, never from a cycle directory:

```python
from mozaic_daily.seam_ma import display_ma, daily_to_28ma
```

Cycles through 2026-07 import a **frozen** copy from `data-official/2026-06/export_canonical_curves.py`.
That file must not be edited — past forecasts are never modified, even where they are known to be
wrong — and code still bound to it lives in `_archive/` (see `_archive/_index.md`). On 2026-07-29 a
trend-estimator defect was fixed here (the deseasonalizing window at the seam was day-of-week
unbalanced, stepping the published August desktop curve +102,595); the fix went into the package and
the frozen copy was left alone, so June's and July's delivered curves cannot move. Full record:
`research/ma-seam-turbulence/LOG.md` § Fix A.

### Where new files go: the hybrid rule

Analysis and supporting work splits along two axes:

- **Month-scoped artifact** (the composite producer notebook, a sanity check tied to one month's data, that month's adjusted CSV) → `data-official/{YYYY-MM}/`. The directory already holds the production parquets, sidecar `.meta.json` files, adjustment specs, and parameters.json — keep the producer + diagnostic notebooks alongside them.
- **Cross-month or topic-anchored work** (mechanism diagnostics, model exploration, validation against actuals over time, version-spanning approaches) → `research/{topic}/`. Topics so far: `iran/`, `marketing-lift/`, `april-vs-june-mechanism/`, `param-scans/`, `headwinds/`, `csv-vs-actuals/`.

Each directory has an `_index.md` describing what's in it, what isn't, and where new code goes. Follow that convention when creating a new dir.

### Importing Modules

```python
# In Docker container or with src/ in PYTHONPATH:
from mozaic_daily import main
from mozaic_daily.config import get_runtime_config, STATIC_CONFIG, FORECAST_CONFIG
from mozaic_daily.data import get_queries, get_aggregate_data
from mozaic_daily.forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs, ForecastResult
from mozaic_daily.tables import format_output_table
from mozaic_daily.validation import validate_output_dataframe

# Configurable model parameters (from mozaic package):
from mozaic.models import ModelConfig, DesktopModelConfig, MobileModelConfig, make_desktop_model, make_mobile_model
```

### Scripts

The `scripts/` directory contains helper scripts for common tasks:
- `run_flow.py` - Unified runner for Metaflow operations (local, deploy, backfill)
- `run_main.py` - Run the main forecasting pipeline with checkpoints (local development)
- `run_validation.py` - Validate the checkpoint forecast file
- `check_logs.py` - Check backfill log files for successes, failures, and ambiguous results
- `export_forecast_csv.py` - Export a forecast parquet checkpoint to CSV
- `run_comparison_forecasts.py` - Run multiple forecast variants side-by-side for comparison
- `test_local_docker.sh` - Test Docker image builds locally
- `generate_iran_fill.py` - Produce the Iran counterfactual gap-fill artifact (**current** approach)
- `generate_iran_synthetic.py` - **RETIRED** (superseded by the fill): ALL-level synthetic Iran data from BigQuery
- `add_iran_to_forecast.py` - **RETIRED** (superseded by the fill): add synthetic Iran DAU via summation
- `run_param_scan.py` - **One desktop forecast with a fully configurable `DesktopModelConfig`.** The only way to
  reproduce a locked parameter config — `run_main.py` has no parameter flags. Scan drivers
  (`run_s01_gradient.py`, `run_summer_trough_grid.py`, `run_trend_only_grid.py`, `run_desktop_gradient.py`,
  `run_aug_trough_gradient.py`, `run_mobile_param_scan.py`, `mobile_grid_search.py`) each wrap it
- `score_near_horizon.py` - Score a build at the near-horizon trough and Dec-15. **Scores are not comparable
  across the 2026-07-29 `Fix A` boundary** — its window overlaps the seam transition
- `verify_lol_overlay.py` / `verify_mozillaonline_overlay.py` - End-to-end checks of the `l` and `o` overlays
- `verify_forecast_states.py` - Audit on-disk forecast artifacts, verify raw/adj-h state, write `tmp/inventory.csv`
- `migrate_forecast_names.py` - Rename forecast artifacts to the `.raw.` / `.adj-h.` marker convention and write sidecar metas
- `regenerate_composites.py` - Reproduce composite CSVs from raw parquets via `mozaic_daily.adjustments`; diffs against on-disk

The `docker/` directory contains Docker management scripts:
- `build_and_push.sh` - Build and push Docker images for local (arm64) or remote (amd64)
- `run_mozaic_docker.sh` - Run Docker containers interactively with proper Google Cloud credentials

## Key Commands

### Running Locally
```bash
# Activate virtual environment first
source .venv/bin/activate

# Run the main forecasting pipeline locally with checkpoints
python scripts/run_main.py

# Testing mode (desktop glean/DAU only, quick iteration)
python scripts/run_main.py --testing

# Filter to specific data sources and/or metrics
python scripts/run_main.py --data-sources glean_mobile
python scripts/run_main.py --data-sources glean_desktop --data-sources legacy_desktop
python scripts/run_main.py --metrics DAU
python scripts/run_main.py --data-sources glean_mobile --metrics DAU

# Clean run: ignore existing checkpoints, re-query and re-forecast (still saves new checkpoints)
python scripts/run_main.py --clean

# Write checkpoint files to a specific directory (avoids conflicts between parallel runs)
python scripts/run_main.py --output-dir /tmp/my-run

# Disable the marketing-lift `m` adjustment (default: applied when a matching spec exists)
python scripts/run_main.py --data-sources glean_mobile --metrics DAU --no-marketing-lift

# Run validation on checkpointed forecast data (defaults to yesterday's date)
python scripts/run_validation.py

# Validate checkpoint for a specific forecast date
python scripts/run_validation.py --forecast-start-date 2026-02-24

# Validate checkpoint files in a specific directory
python scripts/run_validation.py --output-dir /tmp/my-run
```

### Docker Build & Push
```bash
# All docker commands run from the docker/ directory
cd docker

# Build locally for arm64 (development/testing)
./build_and_push.sh --local -v 1.2.3

# Build for amd64 and push to Docker Hub (production)
./build_and_push.sh --remote -v 1.2.3

# Build without cache
./build_and_push.sh --remote -v 1.2.3 --no-cache
```

### Docker Run
```bash
# Run from the docker/ directory
cd docker

# Run remote (amd64) image interactively
./run_mozaic_docker.sh --remote -v 0.0.9

# Run local (arm64) image interactively
./run_mozaic_docker.sh --local -v 0.0.9

# Run forecast inside container
./run_mozaic_docker.sh --local -v 0.0.9 -- /run_forecast.sh

# Or manually inside container:
./run_mozaic_docker.sh --local -v 0.0.9
# Inside container:
# /run_forecast.sh
# OR
# python -c "from mozaic_daily import main; main(checkpoints=True)"

# Notes:
# - Automatically mounts Google Cloud credentials from ~/.config/gcloud
# - Sets CLOUDSDK_CONFIG environment variable for BigQuery access
# - Use --local for arm64 (Mac M1/M2), --remote for amd64 (production platform)
# - Version must be specified with -v flag (required)
# - PYTHONPATH is set to /src inside the container for package imports
```

### Metaflow Operations
```bash
# Activate virtual environment first
source .venv/bin/activate

# Run flow locally (uses today's date)
python scripts/run_flow.py local

# Run flow with Kubernetes (test production path)
python scripts/run_flow.py remote

# Deploy/update scheduled job
python scripts/run_flow.py deploy

# Backfill single date
python scripts/run_flow.py backfill 2024-06-15

# Backfill date range (inclusive, sequential)
python scripts/run_flow.py backfill 2024-06-01 2024-06-30

# Backfill with parallel workers (faster for large date ranges)
python scripts/run_flow.py backfill 2024-06-01 2024-06-30 --parallel 4

# Backfill only Mondays (useful for day-of-week patterns)
python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --parallel 2

# Backfill multiple weekdays
python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --weekday friday

# Preview backfill plan without executing (dry run)
python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --dry-run

# Resume a previous backfill (skips completed dates)
python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --resume --parallel 2

# Run backfill in local mode (no Kubernetes)
python scripts/run_flow.py backfill 2024-06-01 2024-06-30 --local

# Backfill from a file of dates (one YYYY-MM-DD per line)
python scripts/run_flow.py backfill --dates-file failures.txt

# Preview dates from file without executing
python scripts/run_flow.py backfill --dates-file failures.txt --dry-run

# Backfill from file with parallel workers
python scripts/run_flow.py backfill --dates-file failures.txt --parallel 4

# Backfill filtered to a single data source and metric (validation is skipped)
python scripts/run_flow.py backfill 2024-06-15 --data-source legacy_desktop --metric DAU

# Remote run with filter
python scripts/run_flow.py remote --data-source glean_mobile --metric DAU

# Multiple filters (repeatable flags, same as run_main.py)
python scripts/run_flow.py backfill 2024-06-15 --data-source glean_desktop --data-source legacy_desktop
```

#### Backfill Configuration

The backfill mode supports several advanced features for large-scale historical forecasting:

**Flags:**
- `--dates-file FILE` / `-f FILE` — Backfill specific dates from a file (one YYYY-MM-DD per line, e.g. output of `check_logs.py -o`). Cannot be combined with positional date arguments, `--weekday`, or `--resume`
- `--parallel N` — Run N backfills concurrently using ProcessPoolExecutor
- `--weekday DAY` — Filter to specific weekday(s). Can be specified multiple times (e.g., `--weekday monday --weekday friday`). Valid values: monday, tuesday, wednesday, thursday, friday, saturday, sunday. Only for date-range mode
- `--dry-run` — Print execution plan (dates, weekdays, mode) without running backfill. Works with both date-range and `--dates-file` modes
- `--resume` — Skip dates from previous runs based on state file. Only for date-range mode
- `--local` — Run in local mode without Kubernetes (default: remote with Kubernetes)
- `--data-source SOURCE` — Filter to specific data source(s). Can be specified multiple times. Valid: glean_desktop, legacy_desktop, glean_mobile. Also available on `local` and `remote` modes. Validation is skipped for filtered runs
- `--metric METRIC` — Filter to specific metric(s). Can be specified multiple times. Valid: DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU. Also available on `local` and `remote` modes. Validation is skipped for filtered runs

**State Files:**
Backfill runs create state files in `logs/backfill_state_{start}_{end}[_{weekday}].json` that track:
- Completed dates (for `--resume`)
- Failed dates
- Configuration (date range, weekdays, local/remote mode)
- Timestamps (created_at, updated_at)

The state file path is deterministic based on start date, end date, and weekdays, so `--resume` automatically finds the correct state file for the same backfill configuration.

**State File Format:**
```json
{
    "start_date": "2025-07-01",
    "end_date": "2026-02-01",
    "weekdays": ["monday"],
    "local_mode": false,
    "created_at": "2026-02-17T10:30:00",
    "updated_at": "2026-02-17T14:45:00",
    "completed_dates": ["2025-07-07", "2025-07-14"],
    "failed_dates": ["2025-07-28"]
}
```

**General Notes:**
- Date ranges are inclusive (both start and end dates are processed)
- Each run creates a log file in `logs/backfill_YYYY-MM-DD.log` for debugging. Reruns for the same date create `backfill_YYYY-MM-DD.run2.log`, `.run3.log`, etc. to preserve history
- Parallel execution uses ProcessPoolExecutor for true parallelism
- Failed runs continue processing remaining dates - check summary for failures
- Historical forecasts validate that the date is not in the future
- All forecasts write to `moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v2`

## Architecture

### Pipeline Flow

1. **Data Collection** (`mozaic_daily.data:get_aggregate_data`)
   - Queries BigQuery for Desktop and Mobile metrics: DAU, New Profiles, Existing Engagement DAU/MAU
   - Desktop segmentation: country, Windows version (modern_windows/winX)
   - Mobile segmentation: country, app (fenix_android, firefox_ios, focus_android, focus_ios)
   - Supports checkpointing to parquet files for faster iteration

2. **Forecasting** (`mozaic_daily.forecast:get_forecast_dfs`)
   - Uses the Mozaic package (`mozaic.TileSet`, `mozaic.Mozaic`)
   - Creates tiles via `mozaic.populate_tiles()` for each metric/country/population segment
   - Curates mozaics via `mozaic.utils.curate_mozaics()` to aggregate tiles
   - Applies platform-specific models via `make_desktop_model(config)` / `make_mobile_model(config)` factory functions
   - Accepts an optional `config` argument (`DesktopModelConfig` or `MobileModelConfig`) to override Prophet and holiday-detrending parameters
   - Returns a `ForecastResult(dfs, mozaics, config)` dataclass

3. **Table Formatting** (`mozaic_daily.tables:format_output_table`)
   - Combines Desktop and Mobile forecasts
   - Creates aggregate "ALL" rows for Desktop+Mobile combined
   - Formats columns: renames metrics to lowercase, adds metadata (forecast_start_date, mozaic_hash)
   - Converts "actual" source to "training" for historical data
   - Renames "source" column to "data_type"
   - Sets data_source values to lowercase (glean_desktop, legacy_desktop, glean_mobile)

4. **Validation** (`mozaic_daily.validation:validate_output_dataframe`)
   - Validates against BigQuery schema (column presence, types)
   - Checks string formats (timestamps, dates, git hashes, JSON segments)
   - Validates row counts: ensures all required countries, dates, and segments present
   - Checks for null values in expected metric/date combinations
   - Detects duplicate rows

5. **Upload** (`mozaic_daily_flow.py:load`)
   - Appends validated forecast to `moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v2`

### Configuration System (`mozaic_daily.config`)

The configuration system is split into static and runtime components:

**Static Configuration (`STATIC_CONFIG`):**
- Project names, table names, file paths
- Testing mode constants
- True constants that never change at runtime

**Forecast Configuration (`FORECAST_CONFIG`):**
- Default quantile for forecasting (0.5)
- Other forecast-related parameters

**Runtime Configuration (`get_runtime_config()`):**
The `get_runtime_config()` function dynamically calculates dates and markets based on current time:
- `forecast_start_date`: yesterday (T-1)
- `forecast_end_date`: December 31 of next year
- `training_end_date`: T-2
- Countries: union of top DAU markets, top Google markets, and non-monetized Google markets

Per-metric training data parameters are defined using the `DateConstraints` dataclass in `mozaic_daily.queries`:
- Start dates vary by metric (e.g., Desktop DAU from 2023-04-17, Mobile DAU from 2020-12-31)
- Some metrics have excluded date ranges (e.g., New Profiles excludes 2023-07-18 to 2023-07-19)
- Different date fields: `submission_date` vs `first_seen_date`
- Each `QuerySpec` in the `QUERY_SPECS` dictionary contains a `DateConstraints` object that generates SQL WHERE clauses

### Metaflow Integration

The `MozaicDailyFlow` class in `mozaic_daily_flow.py`:
- Runs on schedule: `@schedule(cron='0 7 * * ? *')` (7 AM daily)
- Uses Kubernetes decorator with custom Docker image (16GB memory, 1 CPU)
- Tracks Mozaic version via `/mozaic_commit.txt` file in container
- Uses `@card` decorators for Metaflow UI visualization

## Forecast Artifact Naming Convention

Every forecast artifact (parquet, CSV) under `data-official/` and at the repo root carries an **explicit state marker** in its filename so adjustment state is never ambiguous:

- `.raw.` — direct model output, no adjustments applied
- `.adj-{codes}.` — one or more adjustments applied; codes are sorted alphabetically and concatenated

Adjustment codes are registered in `data-official/adjustment_codes.yaml`. Current codes:

| Code | Name | Description |
|------|------|-------------|
| `h`  | headwinds | Linear ramp anchored at a target date; spec lives in `data-official/{YYYY-MM}/adjustments/headwind.json`. Composite-style applier (post-forecast Series mutation). |
| `m`  | marketing_lift | Daily DAU lift from the Fenix Android paid campaign launched 2026-04-06; spec + parquet live in `data-official/{YYYY-MM}/marketing/`. Per-tile bidirectional applier: subtracts lift from Fenix training rows before mozaic so Prophet learns the no-marketing dynamic, then adds the lift back to the per-tile forecast. Only applies to `glean_mobile` DAU. |
| `l`  | launch_on_login | Launch-on-login desktop DAU tailwind (feature launched 2026-05-08); spec + curve live in `data-official/{YYYY-MM}/launch_on_login/`. Same per-tile bidirectional applier as `m` but on `legacy_desktop` DAU, `modern_windows` segment: subtracts the measured historical rise from modern_windows training rows before mozaic, then adds the capped/flat curve back. Spec type is the generic `desktop_overlay`. **The ceiling is per-cycle, not a constant, and it moved three times inside the August cycle alone** — July 2026 used 125K; August 2026 built 165K, 180K and 200K and shipped **200K**. Never assume a value: read `data-official/{YYYY-MM}/launch_on_login/lol.json` for which curve is active, then `cap_dau_daily` from that curve's `model_meta.json`. All variants share one anchor and slope and differ only in the ceiling, so switching is a two-line spec edit **plus a model re-run** (the curve is baked into the parquet). The choice is unfalsifiable by construction — see below. The measurement window closed permanently on 2026-06-23 (the holdback control received the feature), so every cycle's curve is measured to that date and extrapolated after it. |
| `o`  | mozillaonline_migration | MozillaOnline → canonical Firefox desktop migration tailwind (China distribution partner migrating users onto mainline Firefox; migrating users flip `app_name` and newly count as `Firefox Desktop`); spec + curve live in `data-official/{YYYY-MM}/mozillaonline/`. Same per-tile bidirectional `desktop_overlay` applier as `l`, on `legacy_desktop` DAU `modern_windows` segment, but with **fixed geo shares** (CN ~93%, IR excluded, renormalized over training-present countries) instead of trailing-DAU shares. modern_windows-only by measurement (older-Windows users are pinned on Firefox too old to receive the migrating build). Distinct sentinel `mozillaonline_subtracted` so it stacks with `l`. |

Combined with existing markers, filenames look like:

```
mozaic_daily_forecast.2026-05-13.ld-D.raw.parquet               # raw model output
mozaic_daily_forecast.2026-05-13.ld-D.raw.plus_iran.parquet     # raw + synthetic Iran composition
june_composite_forecast_28ma.adj-h.csv                          # headwinds applied
mozaic_daily_forecast.2026-05-17.gm-D.adj-m.parquet             # marketing-lift applied (mobile)
mozaic_daily_forecast.2026-06-29.ld-D.adj-l.parquet             # launch-on-login applied (desktop)
mozaic_daily_forecast.2026-06-29.ld-D.adj-lo.parquet            # launch-on-login + MozillaOnline (desktop)
mozaic_daily_forecast.2026-06-29.gm+ld-D+NP.adj-lmo.parquet     # desktop l+o + mobile marketing (combined)
june_composite_forecast_28ma.adj-hm.csv                         # headwinds + marketing-lift
```

**Every artifact has a sidecar `<name>.meta.json`** with full provenance: model config, list of `adjustments_applied` (with code + spec_file + spec_sha1), parent files, mozaic-daily commit hash. The sidecar is the source of truth for adjustment state; the filename marker is required to match it.

**Always load through `mozaic_daily.adjustments.load_forecast(path)`** — it validates filename ↔ meta consistency and refuses to load anything missing a sidecar or state marker. Direct `pd.read_parquet()` bypasses this safety net.

```python
from mozaic_daily.adjustments import load_forecast
df, meta = load_forecast("data-official/2026-06/.../mozaic_daily_forecast.2026-05-13.ld-D.raw.parquet")
df, meta = load_forecast(path, require_state=["h"])  # raises if filename codes != ["h"]
```

**Producing a new artifact**: whenever you write a forecast parquet or CSV, you must (1) insert the state marker into the filename via `insert_state_marker(path, codes)` and (2) write a sidecar via `write_meta(path, ..., adjustments_applied=build_adjustments_applied_list(...))`. The composite-CSV regenerator (`scripts/regenerate_composites.py`) is the canonical example.

**Auditing on-disk files**: `scripts/verify_forecast_states.py` reproduces composite CSVs from raw parquets and writes `tmp/inventory.csv` with verified state per file. Run this whenever you suspect adjustment state drift.

**Adding a new adjustment type**: add the one-letter code to `data-official/adjustment_codes.yaml`, register its applier in `src/mozaic_daily/adjustments.py`, and extend `tests/test_adjustments.py`. Two applier styles exist:
- *Composite post-forecast* (mutates a 28d-MA Series after mozaic — cheap, low-impact). Reference: `h` (headwinds).
- *Per-tile bidirectional* (subtracts from training before mozaic, adds back after — required when the adjustment should shift the *model's view of recent history* so it doesn't extrapolate the adjustment forward implicitly). Reference: `m` (marketing-lift), `l` (launch-on-login).

**Bidirectional overlay machinery (generalized).** The per-tile bidirectional appliers are model-agnostic: `compute_country_shares(training_df, flag_column=...)`, `subtract_lift_from_training(df, flag_column=..., sentinel_attr=...)`, and `add_lift_to_forecast(df, population_value=...)`. They key off a boolean segment column (`fenix_android` for mobile `m`; `modern_windows` for desktop `l` and `o`) and allocate a single daily aggregate series across country tiles by either trailing DAU share (`compute_country_shares`, e.g. `m`/`l`) or fixed spec shares (`fixed_country_shares_from_spec`, e.g. `o` — drops `scope.exclude_countries` and renormalizes over training-present countries). `m`, `l`, and `o` are thin wrappers over these. **The desktop `desktop_overlay` spec type (`load_overlay_spec`) is the reuse path for desktop tailwinds** — `l` (launch-on-login) and `o` (MozillaOnline migration) both use it. Each overlay must pass a distinct `sentinel_attr` (`launch_on_login_subtracted`, `mozillaonline_subtracted`) so they stack on the same desktop training frame.

**Disabling an overlay for a one-off run**: `python scripts/run_main.py --no-marketing-lift ...` (mobile `m`), `--no-launch-on-login ...` (desktop `l`), or `--no-mozillaonline ...` (desktop `o`). Default behavior is to apply an overlay whenever its spec (`.../marketing/marketing.json`, `.../launch_on_login/lol.json`, `.../mozillaonline/mozillaonline.json`) has `applies_to_forecast_start == forecast_start_date`.

## Iran Internet Shutdown — counterfactual gap fill (current approach)

Iran's internet shutdown drove native Firefox telemetry to ~zero from **2026-03-01 → 2026-05-25**
(fully recovered 2026-05-26). Iran (IR) is a top DAU market, so the 86-day crater would corrupt
Prophet (spurious changepoints/trend, broken reconciliation) if fed raw.

**Current treatment (July 2026+):** IR is queried **natively** and the gap is corrected by a
**counterfactual fill** that mozaic applies automatically — the model trains on the synthetic
"what Iran would have been with no shutdown" series while `actuals` stay real telemetry.

- IR is back in the market list (`config.py` `top_DAU_markets`) so `build_query` surfaces it as its
  own `'IR'` country (not folded into `ROW`); the `country != 'IR'` SQL exclusion is removed.
- The fill ships **inside the mozaic package** (`mozaic/fills/iran_2026/<data_source>.parquet`) and
  is auto-applied by `populate_tiles` when `data_source` is passed. `process_data_source` forwards
  `data_source=data_source.value` through the platform wrappers (`forecast.py`) into `populate_tiles`.
- The producer for the fill is `scripts/generate_iran_fill.py` (build output under
  `data-official/2026-07/iran_fill/`); on regeneration, copy the per-source parquets into the mozaic
  package. See `data-official/2026-07/iran_fill/FILL_FORMAT_SPEC.md`.

**Retired:** the prior `no-Iran-queries + synthetic-Iran-add-back-by-summation` workflow
(`scripts/generate_iran_synthetic.py`, `add_iran_to_forecast.py`) and the never-adopted NaN-mask
"gap holiday." The standalone 150k-cap recovery model is obsolete (Iran fully recovered).

### Configurable model parameters
- `ModelConfig` params are supported (see "Configurable Model Parameters" above); `make_desktop_model(config)`
  / `make_mobile_model(config)` are used when a config is provided.
- `run_comparison_forecasts.py` runs multiple configs side-by-side for parameter sensitivity.

## Important Notes

### Configurable Model Parameters

Forecast behavior is controlled via `ModelConfig` dataclasses in `mozaic.models`. Pass a config to `get_desktop_forecast_dfs` or `get_mobile_forecast_dfs`; omitting it uses hardcoded defaults.

```python
from mozaic.models import DesktopModelConfig, MobileModelConfig

# Override specific parameters (all have sensible defaults)
config = DesktopModelConfig(
    prophet_changepoint_prior_scale=0.10,  # default: 0.15983 — lower = smoother trend
    prophet_recent_weeks=8,                # default: 13 — window for conditional seasonality
    holiday_threshold=-0.025,              # default: -0.032 — holiday detection sensitivity
    holiday_max_radius=4,                  # default: 5 — days around holiday to adjust
    holiday_min_radius=2,                  # default: 3 — min meaningful day difference
    holiday_effect_floor=-0.5,             # default: -0.6 — max allowed holiday reduction
)

result = get_desktop_forecast_dfs(metric_data, start, end, config=config)
result.config  # config is stored on the ForecastResult
```

**Parameter destinations:**
- `prophet_changepoint_prior_scale`, `prophet_recent_weeks` → Prophet model (via factory function closure)
- `holiday_threshold`, `holiday_max_radius`, `holiday_min_radius` → forwarded to `mozaic.populate_tiles()`
- `holiday_effect_floor` → forwarded to `mozaic.utils.curate_mozaics()`

**Serialization:** `config.to_dict()` returns a plain dict; `config.to_slug()` returns a compact label like `cps0.10_thresh025_recent8_clip0.5` suitable for file naming in comparison runs.

### Mozaic Package
- Installed from the canonical repo: `github.com/mozilla/mozaic-forecasting`
- Git commit hash is captured during Docker build and stored in `/mozaic_commit.txt`
- Hash is retrieved via `get_git_commit_hash()` and added to forecast output as `mozaic_hash` column

### Docker Image Management
- Docker files are located in the `docker/` directory
- Production images must be built for `linux/amd64` (Outerbounds infrastructure)
- Build script must be run from the `docker/` directory: `cd docker && ./build_and_push.sh`
- Image reference is hardcoded in `mozaic_daily_flow.py` (line 20) - update after building new version
- Format: `registry.hub.docker.com/brwells78094/mozaic-daily:v<version>_amd64`

### Checkpointing
- Set `checkpoints=True` in `main()` to enable file-based checkpointing
- Raw query results saved as `mozaic_parts.raw.{source}.{platform}.{metric}.parquet`
- Final forecast saved as `mozaic_daily_forecast.{forecast_start_date}.parquet` (e.g., `mozaic_daily_forecast.2026-02-24.parquet`)
- Testing mode forecast saved as `mozaic_parts.forecast.TESTING.parquet`
- Useful for development to avoid re-querying BigQuery and re-running forecasts

### BigQuery Projects
- Default project: `moz-fx-data-bq-data-science`
- Production project (in flow): `moz-fx-mfouterbounds-prod-f98d`
- Output table: `moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v2`

### GCS archive

Large per-cycle artifacts (`.pkl` mozaic objects, `.parquet` forecasts and raw caches, `.zip` cycle bundles) are gitignored. The canonical archive lives at:

```
gs://moz-data-science-brwells-bucket/mozaic-daily-archive/
├── april-2026/data-official/    # April 2026 forecast cycle
└── june-2026/data-official/     # June 2026 forecast cycle
```

Project: `moz-fx-data-bq-data-science`. Each cycle prefix mirrors `data-official/{YYYY-MM}/` under it, with a `README.md` at the cycle root explaining what's archived.

**When to push:** at the end of each forecast cycle ("button down for storage"), upload everything under that month's `data-official/{YYYY-MM}/` directory. For mid-cycle additions, do an incremental upload.

**How to push** (single-process — do NOT use `gsutil -m`, see "gsutil on macOS" in `~/.claude/CLAUDE.md`):

```bash
# Whole cycle:
gsutil cp -r data-official/2026-06 \
  gs://moz-data-science-brwells-bucket/mozaic-daily-archive/june-2026/data-official/

# Individual subdir (incremental):
gsutil cp -r data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6 \
  gs://moz-data-science-brwells-bucket/mozaic-daily-archive/april-2026/data-official/

# Resume / sync (skip already-uploaded files). MUST set parallel_process_count=1
# even though there's no `-m` flag — `gsutil rsync` defaults to multi-process,
# which hits the macOS Python crash bug. See memory feedback-gsutil-rsync-multiprocessing.
gsutil -o "GSUtil:parallel_process_count=1" rsync -r data-official/2026-06 \
  gs://moz-data-science-brwells-bucket/mozaic-daily-archive/june-2026/data-official/2026-06
```

**What's tracked vs archived:** `.json` (sidecar metas, adjustment specs, parameters), `.md`, `.py`, `.ipynb` all live in git. The small public-facing canonical CSVs (`{month}_canonical_curves.csv`, `april_composite_forecast_28ma.adj-h.csv`) have explicit `!` gitignore exceptions and are also tracked. Everything else under `data-official/{YYYY-MM}/` — pkl, parquet, larger CSVs, zip bundles — goes to GCS only.

### Validation Requirements
- All string columns have strict format requirements (ISO timestamps, SHA1 hashes, JSON segments)
- Segment JSON must contain an `"os"` key with values from: modern_windows, winX, other, ALL, or null
- Training data must span from metric-specific start dates through `training_end_date`
- Forecast data must span from `forecast_start_date` through `forecast_end_date`
- No duplicate rows allowed (on non-metric columns)

### Troubleshooting

**Pre-flight Data Availability Check**

The pipeline runs a fast pre-flight check (~30 seconds) before querying BigQuery to verify that training data has landed for all tables through `training_end_date`. If any table is behind, the pipeline fails immediately with an actionable error rather than running for ~90 minutes before validation catches the problem.

Common cause: running at 5 PM PST (= 1 AM UTC next day). The Kubernetes pod computes `training_end_date = yesterday` in UTC, which may reference a date whose BigQuery data hasn't landed yet.

Error example:
```
Pre-flight check failed: training data not yet available.
  Table: moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates
  Required through: 2026-02-16
  Available through: 2026-02-15

Suggested fix: --forecast_start_date 2026-02-16
```

To fix: re-run with the suggested `--forecast_start_date`, which shifts the pipeline to use data that has actually landed. The check is skipped automatically when a forecast checkpoint file already exists.

**Prophet/Stan Optimization Errors**

If you see errors like `RuntimeError: Error during optimization!` when forecasting:

1. **Architecture issues**: Prophet's Stan binaries work best on amd64. If testing locally on arm64 (Mac M1/M2), try the remote image instead:
   ```bash
   cd docker
   ./run_mozaic_docker.sh --remote -- /run_forecast.sh
   ```

2. **Data quality**: Stan optimization can fail when:
   - Too few data points for a segment
   - All zeros or flat lines (no variation)
   - Missing or invalid values (NaN, infinite)

   Check the specific segment mentioned in the error (e.g., "AR: other") by examining the raw data.

3. **Prophet configuration**: The models in `src/mozaic_daily/forecast.py` configure Prophet parameters. Segments with sparse data may need special handling.

4. **Error context**: The error handling in `src/mozaic_daily/forecast.py` provides context about which metrics and date ranges were being processed when Mozaic fails. Look for error messages like:
   ```
   ERROR: Mozaic populate_tiles failed
   Processing metrics: ['DAU', 'New Profiles', ...]
   Forecast period: 2024-02-01 to 2025-12-31
   ```

**BQ download appears to hang**

Every BQ download routes through `query_to_dataframe()` in `src/mozaic_daily/data.py`, which runs the download in a worker thread and prints a tagged heartbeat to stdout every 30s while the result is pending. Three line forms:

- First heartbeat (carries the diagnostic hint, fires only after 30s):
  `[BQ-WATCHDOG] '<label>' 30s. Next ≤35s. If stalled: check bigquerystorage host allowlist, ADC creds, or set MOZAIC_DAILY_DISABLE_BQSTORAGE=1.`
- Subsequent heartbeats (terse): `[BQ-WATCHDOG] '<label>' 60s. Next ≤35s.`
- Completion: `[BQ-WATCHDOG] '<label>' done 62s.`

How to read the log:
- *No heartbeat line at all → done line*: healthy fast query (under 30s).
- *Heartbeats at cadence → done line*: healthy slow query.
- *Heartbeats keep appearing, no `done` line*: BQ download is stalled. Causes named in the first heartbeat — most often `bigquerystorage.googleapis.com` blocked by the Bash sandbox, or expired ADC creds. Workaround: `MOZAIC_DAILY_DISABLE_BQSTORAGE=1` forces REST instead of gRPC streaming.
- *Heartbeat then silence past the predicted `Next ≤Ns` bound*: the Python process itself is stuck, not the BQ download. Inspect/kill the process.

The watchdog deliberately does NOT enforce a timeout — query budgets are unknown — so a stall doesn't auto-kill the run; it just becomes visible.

**Data Quality Errors**

The pipeline includes automated checks to catch data quality issues early:

1. **Empty query results**: If you see `BigQuery returned 0 rows for...`:
   - Check the date range in runtime config
   - Verify the country list includes expected countries
   - Check if the metric is available for the time period
   - Review the SQL query output in the logs

2. **Data health warnings**: If you see warnings like `Zero variance in metric...`:
   - The segment may have flat/constant data
   - Stan optimization may fail downstream
   - Check if this segment/country combination is expected to have data
   - These warnings appear in the `--- Populate tiles` section of logs

3. **Progress tracking**: The pipeline logs progress for:
   - BigQuery queries: `[X/Y] Querying Desktop Glean DAU`
   - Data source forecasting: `[X/Y] Forecasting Desktop Glean`
   - Metric extraction: `[X/Y] DAU` during forecast generation

### Testing

**Validation Tests**

The validation module has comprehensive test coverage in `tests/test_validation.py`:
- Column presence and type validation
- String format validation (timestamps, git hashes, JSON, countries, etc.)
- Row count validation (training/forecast dates, countries, segments)
- Null value validation
- Duplicate row detection
- Integration tests with full validation pipeline

Run validation tests:
```bash
pytest tests/test_validation.py -v
```
