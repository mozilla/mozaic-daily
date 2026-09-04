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
├── organic.py        # Mobile paid/organic split `p` — CONSUMER: split training, stack paid back
├── organic_source.py # Mobile paid/organic split `p` — PRODUCER: build the measured split + checks
├── seam_ma.py        # Display-layer 28d MAs; variance-matched actuals→forecast seam transition
├── overlays.py       # Registry-driven dispatch of per-tile overlays (`l`, `o`, any `per_tile_overlay` code)
├── ingest_inspect.py # Ingest step, read-only half: read a delivered curve file, guess columns, check the contract
├── ingest_build.py   # Ingest step, write half: horizon parquet + meta + spec + registry entry + gitignore
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
- **Cross-month or topic-anchored work** (mechanism diagnostics, model exploration, validation against actuals over time, version-spanning approaches) → `research/{topic}/`. Topics **currently on disk**: `param-scans/`, `mobile-organic/`, `marketing-lift/`, `headwinds/`, `ma-seam-turbulence/`, `csv-vs-actuals/`, `summer-slump/`, `autumn-decoupling/`, `collaboration-review/`, `forecast-vs-summer-actuals/`. Archived to GCS and removed from the tree at the July button-down (do **not** treat these paths as live): `iran/`, `april-vs-june-mechanism/`, `marketing-lift/v1-convolution/`, `desktop-gap-decomp/`, `country-overrides/`. See `research/_index.md`.

Per-cycle inputs that the pipeline *consumes* get their own subdirectory under `data-official/{YYYY-MM}/`, each with a spec JSON gated by `applies_to_forecast_start`, a data file, a sidecar `.meta.json`, and an `_index.md`: `adjustments/` (`h`), `marketing/` (`m`, retired), `launch_on_login/` (`l`), `mozillaonline/` (`o`), `organic/` (`p`).

Each directory has an `_index.md` describing what's in it, what isn't, and where new code goes. Follow that convention when creating a new dir.

**Ingesting a delivered curve is a skill: `.claude/skills/ingest-adjustment/SKILL.md`** (inventory existing
adjustments → new or update → family in plain words → inspect and confirm columns/sign → allocation → code letter →
draft prose → `scripts/ingest_adjustment.py build` → bookkeeping). Import only; the model rerun is a separate step.

**Hand-off templates for external producers** live in `templates/` — currently `templates/tailwind/`, the
three-column daily-DAU CSV contract (date, actuals/forecast flag, DAU) (plus a real example) to give anyone modelling a new tailwind curve.

### Revert targets: `*_REVERT_{date}/` directories

When a canonical build is swapped out and going back is a live possibility, the predecessor is kept
as `data-official/{YYYY-MM}/{platform}_{config}_REVERT_{date}/` — **a revert target, not an archive.**
Distinct from `*_superseded_*/`, which means "kept for provenance, not expected to return."

A revert directory holds the complete build (parquet + sidecar + `parameters.json` + `.pkl`), a copy
of any **spec that changed alongside the config**, the README it carried while canonical, and a
`REVERT.md` giving step-by-step restore instructions. **Never delete one while its cycle is live.**

**The critical rule: a swap is often more than one change, and a revert must undo all of them.**
Record every co-changed input in `REVERT.md` as a unit. Restoring a model config while leaving a
compensating headwind in place silently republishes a number neither build ever produced.

**Current instance — August 2026 desktop.** `data-official/2026-08/desktop_s01_REVERT_2026-07-29/`
holds the **s01** config, replaced by **g01** on 2026-07-30 to close ~9.5% of the Aug-25 gap to July.
Two things changed together and revert as one unit:

| | from | to |
|---|---|---|
| desktop model config | s01 (`cps 0.1849, cpr 0.734, ncp 35, recent 17`) | g01 (`cps 0.1649, cpr 0.814, ncp 40, recent 17`) |
| `h` Win10 desktop anchor | −1,245,000 | −1,220,000 → −1,295,000 (2026-08-03) → **−1,315,000** (2026-08-04) |

A revert is a real possibility because **g01 is an isolated optimum** — the deepest cell of a 243-cell
factorial, with all seven measured one-step neighbours 52,092–165,860 shallower at Aug-25. It is
deterministic and reproduces exactly, but any future data refresh or re-tune that shifts an effective
parameter will likely lose the gain while keeping its 1.73× seam-kink cost. Search evidence:
`research/param-scans/aug25-gap/`.

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
- `build_fenix_organic_split.py` - **Producer for the `p` adjustment's measured paid/organic split.** Queries the
  growth-source mirror + a tail extension to `training_end_date`, buckets country the same way the production
  query does, and writes `data-official/{YYYY-MM}/organic/fenix_paid_organic.<T-0>.parquet` + sidecar. Four
  checks raise on failure. **Always pass `--production-raw`** — the shredder-drift check against the real level
  source is the only thing that catches the mirror and production covering different Fenix populations.
  ~141 GB scan, ~$0.70. Run once per cycle
- `run_param_scan.py` - **One desktop forecast with a fully configurable `DesktopModelConfig`.** The only way to
  reproduce a locked parameter config — `run_main.py` has no parameter flags. Injects via
  `main(model_configs=...)`; it no longer monkeypatches `process_data_source`. Scan drivers
  (`run_s01_gradient.py`, `run_summer_trough_grid.py`, `run_trend_only_grid.py`, `run_desktop_gradient.py`,
  `run_aug_trough_gradient.py`) each wrap it
- `run_mobile_param_scan.py` - The mobile analog: one `glean_mobile` DAU forecast with a configurable
  `MobileModelConfig`. Exposes **all seven non-holiday knobs** — the three seasonality flags were added
  2026-07-31, and before that a mobile scan silently could not vary them, which is why no mobile build has
  ever left `seasonality_regime='auto'`. Two mobile-specific facts: `seasonality_corr_threshold` is
  **desktop-only** (`MobileModelConfig` raises on non-zero — the regime switch is volume-driven), and
  `seasonality_regime` sets `seasonality_mode` only, with `auto` resolving to **additive** for tiles above
  2e6 DAU. Wrapped by `run_mobile_gradient.py`
- `run_mobile_gradient.py` - August mobile round-1 central-difference gradient: center + 5 numeric axes × ±δ,
  first **and second** derivatives of Dec-15. `--regime` re-runs the whole set under a different
  `seasonality_regime`, giving both that regime's own effect and the local gradient at the new center;
  probes share one results dir because `to_slug()` appends `_regime<value>`. Holiday knobs excluded by policy
- `score_near_horizon.py` - Score a **desktop** build at the near-horizon trough and Dec-15. **Scores are not
  comparable across the 2026-07-29 `Fix A` boundary** — its window overlaps the seam transition
- `mobile_scoring.py` - The **mobile** scorer (August `adj-p` builds). Scored KPI is the ALL-MOBILE world
  Dec-15 28d-MA post-headwind; seam step, slope kink and YoY are **reported, never scored**. Not
  interchangeable with `score_near_horizon.py`: it reads `mobile_dau` from the headwind spec (~45× smaller
  than `desktop_dau`) and selects on mobile's `"{}"` segment rather than `'{"os": "ALL"}'`. Actuals come from
  the parquet's `training` rows, which `p` guarantees are byte-identical to raw actuals. **Cycle-scoped** —
  repoint `FORECAST_START`, `DEFAULT_HEADWIND`, `TARGET_DEC15` each roll-forward
- `verify_overlay.py` - End-to-end three-curve isolation check for **any** per-tile overlay code
  (`--code j --cycle 2026-09`): raw vs subtract-only vs subtract+add, Dec-15 deltas, pass-through ratio
  against a 0.5–1.5× band, plot + numbers JSON in the spec dir's `plots/`. Replaced the per-code
  `verify_lol_overlay.py` / `verify_mozillaonline_overlay.py` on 2026-09-04. Forecasts twice; the
  ingest skill prints the invocation and leaves running it to you
- `build_adjustment_ladder.py` - **Desktop adjustment ladder with cached isolation reruns.** Raw model, then one
  run per overlay to measure its Dec-15 impact, then the cumulative subsets the impact order needs; each run is
  a real desktop forecast cached under `data-official/{cycle}/adjustment_ladder/<codes>.<key>/`, keyed on seam +
  config + the fingerprints of **only the overlays in that run**, so editing India's curve re-runs only rungs
  containing `i`. Display-layer codes are exact and never rerun. Writes `ladder_manifest.json`; the canonical
  notebook's `[plot-desktop-ladder]` only reads it, so the chart refreshes on request, not on every notebook run.
  **Prompts before every model run; `--yes` only after the user approves that specific rebuild** — a broad
  refresh never implies a ladder rerun. Logic in `mozaic_daily.ladder`
- `ingest_adjustment.py` - **Turn a delivered headwind/tailwind file into a registered adjustment.** `inspect`
  reads CSV/parquet/Excel, guesses the date / value / actuals-forecast columns with evidence, checks the
  `templates/tailwind/` contract (daily rows, starts at or before the seam, reaches Dec 31 of the forecast
  year) and exits 2 on any error — weekly rows always halt. `build` takes the confirmed mapping and writes
  `data-official/{cycle}/{name}/`: `source_data/` copy (sha1 in meta), horizon parquet (zero before the
  file, verbatim inside, **held flat at the final 28d mean** to Dec 31 of the following year) + csv twin +
  meta, the spec (`desktop_overlay` in the curve dir, or `daily_file` in `adjustments/` for display
  layer), a shape plot in `plots/` (opened during the skill's validation step), an `_index.md` skeleton, the registry
  entry, and the `.gitignore` exception. `--values-are-28d-ma` for files that already carry the 28d series;
  `--rebase-to-seam` for curves anchored at an earlier seam (0 at this seam, delivered series kept). `--replace` stashes
  the live build in `{name}_REVERT_{date}/`. **Never runs the model.** Driven by the
  `/ingest-adjustment` skill; logic lives in `mozaic_daily.ingest_inspect` / `ingest_build`
- `verify_forecast_states.py` - Audit on-disk forecast artifacts, verify raw/adj-h state, write `tmp/inventory.csv`
- `verify_training_rows_are_actuals.py` - Confirm a forecast parquet's `training` rows equal raw BigQuery actuals over
  sampled date windows. Run before using training rows as a stand-in for an actuals query (e.g. the canonical
  notebooks' prior-year reference line), which saves ~1TB of scan per notebook run. Exit 0 = safe to substitute
- `migrate_forecast_names.py` - Rename forecast artifacts to the `.raw.` / `.adj-h.` marker convention and write sidecar metas
- `regenerate_composites.py` - Reproduce composite CSVs from raw parquets via `mozaic_daily.adjustments`; diffs against on-disk
- `mobile_app_breakdown.py` - Per-app DAU split of a mobile build (point-in-time + trailing window, actuals + forecast).
  Cross-checks `ALL MOBILE` against the sum of its parts. Defaults are **cycle-scoped** — repoint at each roll-forward
- `fetch_raw_pull.py` - Fetch + checkpoint one data source's raw BQ pull with **no forecasting**. Needed at
  roll-forward: the `p` split producer wants the new cycle's raw mobile pull, but the mobile scan won't run
  until a paid spec is gated to the new date, which needs the split. The raw pull is model-config
  independent, so a later scan reuses it via `--raw-cache-dir` with no re-query
- `export_desktop_no_headwind_csv.py` - Write the **desktop-only, `h`-removed** counterfactual twins of a
  cycle's canonical CSVs, beside the published pair. Works by pure arithmetic on the **published CSVs**
  plus the current and prior `headwind.json`: because `h` is display-layer, `published − ramp` is exact,
  needing no model re-run and no parquet. The prior-July column is stripped with **July's own frozen
  spec** (read-only). Every value column is suffixed `_NO_WIN10_HEADWIND`, so code written against the
  canonical schema `KeyError`s instead of silently reading the counterfactual. Only `h` is removed —
  `l`/`o` are baked into the parquet. **Cycle-scoped**
- `export_desktop_ex_ir_cn_csv.py` - Write the **desktop-only, ex-Iran/ex-China** twins of a cycle's
  canonical CSVs — four files, `h`-applied and `h`-removed. Unlike the no-headwind exporter this **reads
  the parquets**: the published CSVs hold only world totals, and subtracting per-country 28d MAs is
  invalid because `display_ma`'s splice is **non-linear** (~2,900 DAU in the splice window). Differences
  the daily series, then recomputes `display_ma`. Exactness rests on `ALL == sum(country tiles)`,
  asserted with a documented float tolerance. `h` applied at its **full unscaled anchor** (the Win10
  mechanism was measured ex-IR/CN to begin with). Note **excluding CN also removes ~93% of the `o`
  MozillaOnline tailwind** — the scope is not "world minus 5.8%". Actuals come from `training` rows, so
  they end one day earlier than the published files. **Cycle-scoped**
- `plot_forecast_set.py` - Generate the canonical plot set (`global_<platform>.png` etc.) from a forecast checkpoint
- `seam_bridge.py` - Seam kink diagnostic (`kink_score`) + daily-level bridge helpers; platform-agnostic
- `tile_corr_distribution.py` - Reports the per-tile level/volatility correlation behind desktop's
  `seasonality_regime="auto"` switch; used to place grid points. Desktop-only — mobile's regime switch is
  volume-driven, so `seasonality_corr_threshold` does not exist there
- `run_pinned_scan.py` - Desktop forecast with per-tile Prophet changepoints pinned to April's locations;
  built to test whether changepoint placement explained the April↔June trend gap

**`scripts/_index.md` is the complete, authoritative inventory** (including `scripts/sql/`); the list
above is the LLM-facing summary and may lag it. Add new scripts to both.

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

# Disable the mobile paid/organic split `p` (default: applied when a matching spec exists).
# NOTE this gives a TOTAL-DAU mobile forecast with no paid treatment, not an organic one.
python scripts/run_main.py --data-sources glean_mobile --metrics DAU --no-organic-split

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
   - **The mobile universe is exactly those four apps and there is no "other" bucket.** The mobile
     `QuerySpec` filters `app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS")`, so any
     other Glean mobile product (Klar, Firefox Lite, Reference Browser, ...) is absent from both the
     training rows and the forecast rather than folded into a residual. Composition as of 2026-07-27:
     Fenix 75.6%, Firefox iOS 21.5%, Focus Android 1.5%, Focus iOS 1.3%. Reproduce with
     `scripts/mobile_app_breakdown.py`
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
| `h`  | headwinds | Win10 desktop headwind, display layer; spec lives in `data-official/{YYYY-MM}/adjustments/headwind.json` as a two-number `linear_ramp` from 0 at the cycle's seam to an anchor at Dec-15. Through August 2026 the anchor was −1,315,000 (unclamped past Dec-15) and the file also carried mobile −27,162. **From September 2026 the anchor is the Dec-15 value of Brad's Win10 model curve, −726,000** (delivered file + sha1 in `data-official/2026-09/headwinds/`), `clamp_at_anchor: true` so it is flat after Dec-15 like his file, desktop only; the mobile leg is code `u`. Applying his curve's *shape* shifted to the seam was tried and reverted on 2026-09-04 because it lost 166,711 of headwind. A DRAFT. Composite-style applier; exact at Dec-15, no rerun. |
| `m`  | marketing_lift | **RETIRED for new cycles as of 2026-08 — superseded by `p`.** Still registered, and must stay registered: July's and August's pre-swap `.adj-m.` artifacts have to keep loading and reproducing. Daily DAU lift from the Fenix Android paid campaign launched 2026-04-06; spec + parquet live in `data-official/{YYYY-MM}/marketing/`. Per-tile bidirectional applier: subtracts lift from Fenix training rows before mozaic, then adds the lift back. Retired because the lift is an increment *since an anchor*, and mobile paid acquisition has no start date — so the anchor was an accounting choice, and the bidirectional overlay **absorbed 58%** of any change to the curve (a −141,653 Dec-15 lift change moved the KPI only −59,875). A lift-curve comparison was never a KPI estimate. The lift parquet is still consumed — `p` reads it as the *paid forecast* and adds the anchor back to turn it into a level. |
| `p`  | paid_organic_split | **The mobile paid treatment from 2026-08 on.** Paid DAU is *measured*, not modelled: Fenix training rows are multiplied by `organic_share(date, country)` from the client-level growth-source mirror, so mozaic forecasts **organic** DAU only. Paid is then stacked back on as a **level** — measured paid for training rows (which keeps them byte-identical to raw actuals), marketing's paid level (`lift + anchor`) allocated by trailing-28d **measured-paid** share for forecast rows. Because the add-back is additive and post-mozaic, paid contributes **exactly its own value** with no Prophet interaction, the way `h` does. Spec + measured split live in `data-official/{YYYY-MM}/organic/`; producer is `scripts/build_fenix_organic_split.py`. The paid level it stacks comes from `data-official/{YYYY-MM}/marketing/` (August: the two single-channel feeds; September: the GMIO cross-channel feed via `data-official/2026-09/marketing/build_paid_dau_curve.py`, composed as UAC+Meta where present else UAC, Dec-15 level 1,891,002 vs August's 1,559,477, anchor 800,831). **`anchor_paid_dau` in `organic.json` must match the curve's meta `key_values.anchor_paid_dau`** — it changes with every re-pull. Share comes from the mirror but the **level comes from the production query** — the mirror loses ~2.9% to shredder attrition over 26 months, so using it for the level reads as +3.3pp of fake growth. IR excluded (98.8% organic; marketing's curve is ex-IR). Non-Fenix mobile apps are 100% organic — Firefox iOS has no paid signal at all. **Mutually exclusive with `m`**: `main.process_data_source` raises if both specs claim the same `applies_to_forecast_start`. |
| `l`  | launch_on_login_new_users | Launch-at-login desktop DAU tailwind for **new users** (feature launched 2026-05-08; retitled 2026-09-04, a separate existing-users tailwind is expected as its own code; the directory/spec keep the `launch_on_login/lol.json` layout); spec + curve live in `data-official/{YYYY-MM}/launch_on_login/`. Same per-tile bidirectional applier as `m` but on `legacy_desktop` DAU, `modern_windows` segment: subtracts the measured historical rise from modern_windows training rows before mozaic, then adds the capped/flat curve back. Spec type is the generic `desktop_overlay`. **The ceiling is per-cycle, not a constant** — July 2026 shipped 125K, August 2026 shipped **200K**. Never assume a value: read `data-official/{YYYY-MM}/launch_on_login/lol.json` for which curve is active, then `cap_dau_daily` from that curve's `model_meta.json`. Changing a ceiling is a spec edit **plus a model re-run** (the curve is baked into the parquet), and the realised Dec-15 effect is config-dependent — never model it as a level shift. **Keep exactly one curve per cycle on disk.** August accumulated four (125K/165K/180K/200K) and they were deleted on 2026-07-30 because superseded alternates were being cited as if they were live options; build variants while deciding, then delete the losers and record the decision in the cycle's `_index.md`. The choice is unfalsifiable by construction — the measurement window closed permanently on 2026-06-23 when the holdback control received the feature, so every cycle's curve is measured to that date and extrapolated after it, and deleting alternates removes the menu but not the uncertainty. |
| `o`  | mozillaonline_migration | MozillaOnline → canonical Firefox desktop migration tailwind (China distribution partner migrating users onto mainline Firefox; migrating users flip `app_name` and newly count as `Firefox Desktop`); spec + curve live in `data-official/{YYYY-MM}/mozillaonline/`. Same per-tile bidirectional `desktop_overlay` applier as `l`, on `legacy_desktop` DAU `modern_windows` segment, but with **fixed geo shares** (CN ~93%, IR excluded, renormalized over training-present countries) instead of trailing-DAU shares. modern_windows-only by measurement (older-Windows users are pinned on Firefox too old to receive the migrating build). Sentinel is derived from the registry name (`mozillaonline_migration_subtracted`) so it stacks with `l`. **Refreshed for September 2026 on 2026-09-04** via `/ingest-adjustment` from the 2026-09-02 official export: Dec-15 28d-MA 668,839 against the stale July curve's 567,549 that August carried; held flat at 644,169 through 2027. The curve lives in `data-official/2026-09/mozillaonline/` (dir/spec names follow the registered `spec_glob`, not the registry name). |
| `t`  | mobile_tailwind | **The mobile calibration tailwind, adopted 2026-08-03.** Linear ramp, 0 at the seam (2026-08-02) to **+299,000** at the 2026-12-15 anchor, mobile only. Adopted at +276,000, raised to +299,000 the same day so published mobile lands within 1,000 DAU of July's delivered figure — **that last +23,000 is calibration to a prior published number, not a measurement.** **Sign is POSITIVE** — unlike `h`, whose `mobile_dau` is −27,162. Registered as its own code rather than an edit to `headwind.json` precisely so a discretionary upward judgement cannot hide inside the headwind line. Composite-style display-layer applier like `h`: applied to the 28d MA after mozaic, so no model re-run and no Prophet interaction — its Dec-15 effect is exactly its anchor. Exists because the `m`→`p` swap cost 322,714 at Dec-15 and a 33-probe search across three seasonality regimes showed no exposed non-holiday parameter combination recovers it (whole envelope 63,539) — mozaic reconciles **top-down**, so the mobile headline is effectively one Prophet fit on the aggregate and per-tile knobs cancel. ~47% of it is the measured excess of an independent implementation; ~45% is a planning decision; ~8% is calibration to July's published number. Spec `data-official/{YYYY-MM}/adjustments/tailwind.json`; rationale `data-official/2026-08/tailwind/_index.md`. |
| `j`  | japan_bot | **Japan automated desktop traffic, wired 2026-09-04 for the September cycle.** A population of non-organic clients arriving in Japan since 2026-06-24 (behavioural definition, no channel term; 95.1% ESR as an outcome). Per-tile overlay on `legacy_desktop` DAU, `modern_windows` segment, **fixed shares 100% JP**, no exclusions: the measured daily excess (through the 2026-08-30 edge) plus an arrival-rate projection is subtracted from training rows before mozaic and added back after, so Prophet does not extrapolate it. **The plateau (MIDDLE, 67,101 DAU/day = 6.19% of Japan, reached 2026-11-28) is a planning choice, not an estimate** — no inflection has been observed; LOW/HIGH alternates are archived, not on disk. History is **deliberately unsmoothed** (the daily swing is real contamination). A masking effect, not growth. No double count with `o` (that population is entirely non-ESR). Spec + curve + `source_data/` in `data-official/{YYYY-MM}/japan_bot/`; first code ingested through `/ingest-adjustment` and dispatched purely from the registry. |
| `i`  | india_excess | **India desktop DAU above a typical year, wired 2026-09-04 for the September cycle.** The gap between India's 2026 rebased 28-day DAU curve and the 2022–2025 mean by calendar day, delivered as `actual × (2026 − typical) / 2026` so weekday structure survives; onset 2026-05-22; holidays and India-only dips bridged out of the ratio only. Per-tile overlay on `legacy_desktop` DAU, `modern_windows` segment, **fixed shares 100% IN**, no exclusions. **Already net of `l`** (India's 5.72% share of the launch-on-login lift is subtracted in the curve) — never net it again. **The persisting path is a planning choice**: shipped PROPORTIONAL (1.58% of India's typical level, 41,945 at Dec-15), with hold / linger / settle / fade alternates kept on disk beside it. **The cause is a hypothesis** (university calendar); report as "India above typical". Spec + curve + `source_data/` in `data-official/{YYYY-MM}/india_excess/`. |
| `u`  | tou_mobile_headwind | **Mobile terms-of-use headwind, split out of `headwind.json` on 2026-09-04.** Linear ramp 0 at the seam to **−27,162** at Dec-15, mobile only — the same number the `h` file carried as its `mobile_dau` leg through August, now its own code because it is a different source (risk from the 2025 terms-of-use acceptance requirement) from the Win10 desktop curve and must be sized independently. Display layer like `h` and `t`: exact at Dec-15, no rerun. Spec `data-official/{YYYY-MM}/adjustments/tou_mobile_headwind.json`; rationale `data-official/2026-09/tou_mobile_headwind/_index.md`. |

Combined with existing markers, filenames look like:

```
mozaic_daily_forecast.2026-05-13.ld-D.raw.parquet               # raw model output
mozaic_daily_forecast.2026-05-13.ld-D.raw.plus_iran.parquet     # raw + synthetic Iran composition
june_composite_forecast_28ma.adj-h.csv                          # headwinds applied
mozaic_daily_forecast.2026-05-17.gm-D.adj-m.parquet             # marketing-lift applied (mobile, retired)
mozaic_daily_forecast.2026-07-28.gm-D.adj-p.parquet             # paid/organic split applied (mobile)
mozaic_daily_forecast.2026-06-29.ld-D.adj-l.parquet             # launch-on-login applied (desktop)
mozaic_daily_forecast.2026-06-29.ld-D.adj-lo.parquet            # launch-on-login + MozillaOnline (desktop)
mozaic_daily_forecast.2026-06-29.gm+ld-D+NP.adj-lmo.parquet     # desktop l+o + mobile marketing (combined)
june_composite_forecast_28ma.adj-hm.csv                         # headwinds + marketing-lift
august canonical curves (display layer)                         # h + t: net mobile +271,838 at Dec-15
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

**Adding a new adjustment type**: add the one-letter code to `data-official/adjustment_codes.yaml` with an `applier` field, and extend the tests. The `applier` value is load-bearing — it selects the machinery, and for the two common styles **no Python is needed**:
- *Display layer* (`applier: display_layer`; `h`, `t`) — a spec in the cycle's `adjustments/` dir, summed onto the 28d MA after mozaic. Live by presence, no date gate. Spec types: `linear_ramp`, `step`, `daily_series`, and `daily_file` (a parquet curve next to the spec, applied as its trailing 28d mean to one platform). Dec-15 effect is exactly the spec's value, no model re-run.
- *Per-tile overlay* (`applier: per_tile_overlay`; `l`, `o`) — a `desktop_overlay` spec + curve parquet. `src/mozaic_daily/overlays.py` discovers every such code from the registry, finds its spec via `spec_glob`, gates on `applies_to_forecast_start`, applies it to the data source named in `applies_to_data_source`, subtracts the curve from training rows before mozaic and adds it back after. Sentinel is derived from the registry `name`, so overlays stack without collisions. Use when the effect has a hard start date so "incremental since launch" is well defined; requires a model re-run.
- *Measured split* (`applier: paid_organic_split`; `p`) — scales training rows by a measured share pre-mozaic, adds a separately-forecast level back post-mozaic, for effects that can be *measured* and have no start date. Its own module pair (`organic_source.py` / `organic.py`). A new mechanism of this kind is the only case that still needs code.
- `applier: marketing_lift` is the retired `m` path, kept so pre-swap artifacts reproduce.

**Overlay machinery (generalized).** The per-tile appliers are model-agnostic: `compute_country_shares(training_df, flag_column=..., exclude_countries=...)`, `fixed_country_shares_from_spec(spec, present)`, `subtract_lift_from_training(df, flag_column=..., sentinel_attr=...)`, and `add_lift_to_forecast(df, population_value=...)`. They key off a boolean segment column (`modern_windows` for desktop `l` and `o`; `fenix_android` for the retired mobile `m`) and split one world-total daily series across country tiles by `allocation.key`: `trailing_dau_share` (**proportional to population** — each country's recent DAU in the segment, e.g. `l`) or `fixed_country_shares` (**localized** — an explicit per-country dict in the spec, e.g. `o` at ~93% CN). Both drop `scope.exclude_countries` and renormalize over training-present countries, so none of the curve lands in an excluded country and the world total is preserved. `overlays.py` dispatches on the key; `main.py` never inspects it.

**Desktop keeps this pattern; mobile no longer uses it.** It is sound for desktop precisely because `l` and `o` have hard start dates, so "incremental since launch" is a well-defined quantity. It was never sound for mobile, where paid acquisition has run continuously and there is a client-level attribution flag we can measure instead — see `p` below.

**Mobile paid/organic machinery (`p`).** Two modules, deliberately split by side:
- `src/mozaic_daily/organic_source.py` — **producer**: pure transforms turning raw growth-source rows into the per-cycle measured split, plus four checks that raise (partition identity, tail overlap, split coverage, shredder drift). Called only by `scripts/build_fenix_organic_split.py`, the one place that touches BigQuery.
- `src/mozaic_daily/organic.py` — **consumer**: `split_training_to_organic` (pre-mozaic), `marketing_paid_level` (lift + anchor, held flat past the curve's end), `add_paid_to_forecast` (post-mozaic), `paid_seam_step` (diagnostic).

Three things about `p` that are easy to get wrong:
1. **The add-back is two-piece.** Training rows get the **measured** paid back so they return to raw actuals exactly (`scripts/verify_training_rows_are_actuals.py` enforces this); forecast rows get **marketing's** level. They disagree at the seam by an amount that is **seam-dependent and must be re-measured after every refresh** — at the 2026-07-28 seam it was ~+0.24% of total, at the refreshed 2026-08-02 seam it is **+1,903 (+0.01%)**. That step is reported, not smoothed. **Derive the last-measured day from the seam, never hardcode it**: a hardcoded date survived the 2026-08-03 refresh and turned the one-day step into a six-day-offset comparison, reporting −41,798 instead of +1,903.
2. **Allocation is by measured-paid share, not total-DAU share.** Paid intensity ranges from 0.2% of Fenix DAU in RU to 27.6% in ID, so a total-DAU key (what `m` used) pushes paid into markets that have none.
3. **The share is only measured from 2024-06-01** (client-level retention limit) while mobile DAU trains from 2020-12-31, so the earliest per-country share is **held flat backwards** over ~3.5 years. Bounded at ~1.1pp — paid was only 1.10% of Fenix DAU ex-IR at the oldest measured month. Masking instead is not available: mozaic requires one common date grid across tiles, so NaN-ing Fenix pre-2024-06 would corrupt the published `ALL MOBILE` training rows.

**Disabling an adjustment for a one-off run**: `python scripts/run_main.py --disable-adjustment CODE` (repeatable) forces any registered code off even when its spec gates on this cycle; `run_param_scan.py` takes the same flag and leaves the code off the output marker. The older `--no-organic-split` (`p`), `--no-launch-on-login` (`l`), `--no-mozillaonline` (`o`) and `--no-marketing-lift` (`m`) flags remain as aliases. Default behavior is to apply every adjustment whose spec has `applies_to_forecast_start == forecast_start_date`; `main()` prints one resolution line per registered code. Note `--no-organic-split` yields a **total**-DAU mobile forecast with no paid treatment at all, not an organic one.

**Running a tuned build**: pass `main(model_configs={DataSource.LEGACY_DESKTOP: cfg})`. This is how `run_param_scan.py` / `run_mobile_param_scan.py` inject their config; they used to monkeypatch `process_data_source` with a hand-copied platform branch, which had to be kept in sync with `main.py` by hand. **Do not reintroduce that pattern.** `run_main.py` still exposes no parameter flags, so a plain `run_main.py` run uses package defaults and cannot reproduce a tuned build.

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
├── june-2026/data-official/     # June 2026 forecast cycle
├── july-2026/{data-official,param-scans,root_intermediates_2026-06-29}/
├── august-2026/{data-official,param-scans,research,root_intermediates_2026-08}/  # + README.md
├── research-superseded/         # retired research clusters
└── {april,august}-2026-model-handoff/   # out-of-tree colleague handoff bundles
```

Project: `moz-fx-data-bq-data-science`. Each cycle prefix mirrors `data-official/{YYYY-MM}/` under it, with a `README.md` at the cycle root explaining what's archived.

**When to push:** at the end of each forecast cycle ("button down for storage"), upload everything under that month's `data-official/{YYYY-MM}/` directory. For mid-cycle additions, do an incremental upload.

**The end-of-cycle procedure is a skill: `.claude/skills/cycle-button-down/SKILL.md`** (lock the cycle branch → archive + verify → flag stale references → prune on `clean-slate` → roll forward). Retention is the last 3 months on disk; fitted pickles, including per-probe scan pickles, are first-class and are always archived before deletion. Prefer `gcloud storage cp -r` over `gsutil` for transfers.

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
