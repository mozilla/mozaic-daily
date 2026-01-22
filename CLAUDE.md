# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This repository implements automated daily forecasting for Mozilla Firefox metrics using the Mozaic package. The system runs as a Metaflow pipeline on Outerbounds infrastructure, querying BigQuery for telemetry data and producing forecasts for Desktop and Mobile platforms.

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
pip install -e 'git+https://github.com/brendanwells-moz/mozaic-forecasting@getters_branch#egg=mozaic'

# Deactivate when done
deactivate
```

**Important:** All Python commands below assume the virtual environment is activated.

## Code Structure

The codebase is organized as a Python package:

```
src/mozaic_daily/
├── __init__.py       # Package exports
├── config.py         # Constants and date logic (from constants.py)
├── data.py           # BigQuery data fetching + SQL query builders
├── forecast.py       # Mozaic forecasting logic
├── tables.py         # Table formatting and manipulation
├── validation.py     # Output validation (from mozaic_daily_validation.py)
└── main.py           # Main entry point
```

### Importing Modules

```python
# In Docker container or with src/ in PYTHONPATH:
from mozaic_daily import main
from mozaic_daily.config import get_runtime_config, STATIC_CONFIG, FORECAST_CONFIG
from mozaic_daily.data import get_queries, get_aggregate_data
from mozaic_daily.forecast import get_desktop_forecast_dfs
from mozaic_daily.tables import format_output_table
from mozaic_daily.validation import validate_output_dataframe
```

### Scripts

The `scripts/` directory contains helper scripts for common tasks:
- `run_main.py` - Run the main forecasting pipeline with checkpoints
- `run_validation.py` - Validate the checkpoint forecast file
- `test_local_docker.sh` - Test Docker image builds locally

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

# Run validation on checkpointed forecast data
python scripts/run_validation.py

# Run the Metaflow flow locally
python mozaic_daily_flow.py run
```

### Docker Build & Push
```bash
# All docker commands run from the docker/ directory
cd docker

# Build locally for arm64 (development/testing)
./build_and_push.sh local v1.2.3

# Build for amd64 and push to Docker Hub (production)
./build_and_push.sh remote v1.2.3

# Build without cache
./build_and_push.sh remote v1.2.3 --no-cache
```

### Docker Run
```bash
# Run from the docker/ directory
cd docker

# Run remote (amd64) image interactively
./run_mozaic_docker.sh --remote

# Run local (arm64) image interactively
./run_mozaic_docker.sh --local

# Run forecast inside container
./run_mozaic_docker.sh --local -- /run_forecast.sh

# Or manually inside container:
./run_mozaic_docker.sh --local
# Inside container:
# /run_forecast.sh
# OR
# python -c "from mozaic_daily import main; main(checkpoints=True)"

# Notes:
# - Automatically mounts Google Cloud credentials from ~/.config/gcloud
# - Sets CLOUDSDK_CONFIG environment variable for BigQuery access
# - Use --local for arm64 (Mac M1/M2), --remote for amd64 (production platform)
# - Default version is 0.0.7, override with -v flag
# - PYTHONPATH is set to /src inside the container for package imports
```

### Metaflow Operations
```bash
# Activate virtual environment first
source .venv/bin/activate

# Run the flow locally
python mozaic_daily_flow.py run

# Deploy to production with schedule (cron: 7 AM daily)
python mozaic_daily_flow.py argo-workflows create

# Test specific step
python mozaic_daily_flow.py run --with kubernetes:image=<image>
```

## Architecture

### Pipeline Flow

1. **Data Collection** (`mozaic_daily.data:get_aggregate_data`)
   - Queries BigQuery for Desktop and Mobile metrics: DAU, New Profiles, Existing Engagement DAU/MAU
   - Desktop segmentation: country, Windows version (win10/win11/winX)
   - Mobile segmentation: country, app (fenix_android, firefox_ios, focus_android, focus_ios)
   - Supports checkpointing to parquet files for faster iteration

2. **Forecasting** (`mozaic_daily.forecast:get_forecast_dfs`)
   - Uses the Mozaic package (`mozaic.TileSet`, `mozaic.Mozaic`)
   - Creates tiles via `populate_tiles()` for each metric/country/population segment
   - Curates mozaics via `curate_mozaics()` to aggregate tiles
   - Applies platform-specific models: `desktop_forecast_model`, `mobile_forecast_model`

3. **Table Formatting** (`mozaic_daily.tables:format_output_table`)
   - Combines Desktop and Mobile forecasts
   - Creates aggregate "ALL" rows for Desktop+Mobile combined
   - Formats columns: renames metrics to lowercase, adds metadata (forecast_start_date, mozaic_hash)
   - Converts "actual" source to "training" for historical data

4. **Validation** (`mozaic_daily.validation:validate_output_dataframe`)
   - Validates against BigQuery schema (column presence, types)
   - Checks string formats (timestamps, dates, git hashes, JSON segments)
   - Validates row counts: ensures all required countries, dates, and segments present
   - Checks for null values in expected metric/date combinations
   - Detects duplicate rows

5. **Upload** (`mozaic_daily_flow.py:load`)
   - Appends validated forecast to `moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v1`

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

The `get_date_constraints()` function defines per-metric training data parameters:
- Start dates vary by metric (e.g., Desktop DAU from 2023-04-17, Mobile DAU from 2020-12-31)
- Some metrics have excluded date ranges (e.g., New Profiles excludes 2023-07-18 to 2023-07-19)
- Different date fields: `submission_date` vs `first_seen_date`

### Metaflow Integration

The `MozaicDailyFlow` class in `mozaic_daily_flow.py`:
- Runs on schedule: `@schedule(cron='0 7 * * ? *')` (7 AM daily)
- Uses Kubernetes decorator with custom Docker image (16GB memory, 1 CPU)
- Tracks Mozaic version via `/mozaic_commit.txt` file in container
- Uses `@card` decorators for Metaflow UI visualization

## Important Notes

### Mozaic Package
- Installed from a fork: `github.com/brendanwells-moz/mozaic-forecasting@getters_branch`
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
- Raw query results saved as `mozaic_parts.raw.{platform}.{metric}.parquet`
- Final forecast saved as `mozaic_parts.forecast.parquet`
- Useful for development to avoid re-querying BigQuery and re-running forecasts

### BigQuery Projects
- Default project: `moz-fx-data-bq-data-science`
- Production project (in flow): `moz-fx-mfouterbounds-prod-f98d`
- Output table: `moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v1`

### Validation Requirements
- All string columns have strict format requirements (ISO timestamps, SHA1 hashes, JSON segments)
- Segment JSON must contain an `"os"` key with values from: win10, win11, winX, other, ALL, or null
- Training data must span from metric-specific start dates through `training_end_date`
- Forecast data must span from `forecast_start_date` through `forecast_end_date`
- No duplicate rows allowed (on non-metric columns)

### Troubleshooting

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
