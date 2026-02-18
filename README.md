# mozaic-daily

Automated daily forecasting for Mozilla Firefox metrics using the Mozaic package.

## Overview

This repository implements automated daily forecasting for Mozilla Firefox metrics. The system runs as a Metaflow pipeline on Outerbounds infrastructure, querying BigQuery for telemetry data and producing forecasts for Desktop and Mobile platforms.

Forecasts are written to:
`moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v2`

## Project Structure

```
mozaic-daily/
├── src/
│   └── mozaic_daily/          # Main package
│       ├── __init__.py        # Package exports
│       ├── config.py          # Constants and date logic
│       ├── queries.py         # SQL query specifications
│       ├── data.py            # BigQuery data fetching
│       ├── forecast.py        # Mozaic forecasting logic
│       ├── tables.py          # Table formatting/manipulation
│       ├── validation.py      # Output validation
│       └── main.py            # Main entry point
├── scripts/                   # Helper scripts
│   ├── run_flow.py            # Unified Metaflow runner (local, remote, deploy, backfill)
│   ├── run_main.py            # Run forecasting pipeline locally with checkpoints
│   └── run_validation.py      # Validate checkpointed forecast output
├── docker/                    # Docker build files
│   ├── Dockerfile             # Docker image definition
│   ├── build_and_push.sh      # Docker build and push script
│   ├── run_mozaic_docker.sh   # Run Docker container interactively
│   ├── requirements.outerbounds.txt  # Python dependencies for Docker
│   └── test_docker.py         # Docker image smoke test
├── tests/                     # Test suite
├── logs/                      # Backfill logs and state files (gitignored)
├── mozaic_daily_flow.py       # Metaflow pipeline definition
├── pyproject.toml             # Package metadata
├── REQUIREMENTS.md            # System and dependency requirements
├── CLAUDE.md                  # Development guide for AI-assisted work
└── README.md                  # This file
```

## Getting Started on a New Machine

### Quick Setup (copy-paste)

```bash
# After cloning the repo and cd-ing into it:
pip install --upgrade pip
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r docker/requirements.outerbounds.txt
pip install -e 'git+https://github.com/brendanwells-moz/mozaic-forecasting#egg=mozaic'
pip install metaflow
pip install cmdstanpy prophet
python -c "import cmdstanpy; cmdstanpy.install_cmdstan()"
pip install numpy pandas scipy pyarrow plotly holidays python-dateutil
pip install pytest
pip install -e .
python -c "from mozaic_daily import main; print('Setup OK')"
python -m pip install -U 'outerbounds[gcp]'
<outerbounds setup command>
```

### 1. Prerequisites

Before setting up the project, ensure the following are installed and configured on your machine. See [REQUIREMENTS.md](REQUIREMENTS.md) for a full breakdown.

- **Python 3.10** (exact version required)
- **Google Cloud SDK** (`gcloud`) with Application Default Credentials:
  ```bash
  gcloud auth application-default login
  ```
- **Git** (for cloning the Mozaic package)
- **Docker** (for building and testing container images — optional for basic local runs)
- **Outerbounds CLI** configured (for remote pipeline execution and deployment)

### 2. Clone the Repository

```bash
git clone <repo-url>
cd mozaic-daily
```

### 3. Create and Activate the Virtual Environment

This project uses a Python virtual environment to isolate its dependencies from your system Python.

```bash
# Create the virtual environment using Python 3.10
python3.10 -m venv .venv

# Activate it (run this every time you open a new terminal for this project)
source .venv/bin/activate

# Confirm the right Python is active
python --version   # should show Python 3.10.x
which python       # should show /path/to/mozaic-daily/.venv/bin/python
```

> **Note:** The `.venv/` directory is gitignored. You need to create it once on each machine.

### 4. Install Dependencies

With the virtual environment active:

```bash
# Install the BigQuery and Outerbounds dependencies
pip install -r docker/requirements.outerbounds.txt

# Install the Mozaic forecasting package from the Mozilla fork
pip install -e 'git+https://github.com/brendanwells-moz/mozaic-forecasting#egg=mozaic'

# Install Metaflow for pipeline orchestration
pip install metaflow

# Install Prophet and its Stan backend for time-series forecasting
pip install cmdstanpy prophet
python -c "import cmdstanpy; cmdstanpy.install_cmdstan()"

# Install remaining scientific and data dependencies
pip install numpy pandas scipy pyarrow plotly holidays python-dateutil

# Install test dependencies
pip install pytest
```

> **Note:** Installing Prophet may take several minutes as it compiles Stan from source.

### 5. Install the mozaic-daily Package

The `mozaic_daily` package in `src/` must be on your Python path. The easiest way is to install it in editable mode:

```bash
pip install -e .
```

Alternatively, `scripts/run_main.py` and `scripts/run_flow.py` add `src/` to the path automatically, so you can also run those scripts directly without installing the package.

### 6. Verify the Setup

```bash
# Run a quick import check
python -c "from mozaic_daily import main; print('Setup OK')"

# Run the test suite
pytest tests/ -v
```

### 7. Deactivate When Done

```bash
deactivate
```

Remember to `source .venv/bin/activate` again the next time you work in this project.

---

## Running Locally

With the virtual environment active:

```bash
# Run the full forecasting pipeline locally (with BigQuery access and checkpoints)
python scripts/run_main.py

# Run in testing mode (Desktop DAU only — faster for development)
python scripts/run_main.py --testing

# Run a historical forecast for a specific date
python scripts/run_main.py --forecast-start-date 2024-06-15

# Validate the checkpointed forecast output
python scripts/run_validation.py
```

## Metaflow Pipeline

```bash
# Run pipeline locally (no Kubernetes)
python scripts/run_flow.py local

# Run pipeline with Kubernetes (test production path)
python scripts/run_flow.py remote

# Deploy/update the scheduled job on Argo Workflows
python scripts/run_flow.py deploy

# Backfill a single date
python scripts/run_flow.py backfill 2024-06-15

# Backfill a date range
python scripts/run_flow.py backfill 2024-06-01 2024-06-30

# Backfill with parallel workers
python scripts/run_flow.py backfill 2024-06-01 2024-06-30 --parallel 4

# Backfill only Mondays with 2 parallel workers
python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --parallel 2

# Preview a backfill plan without running it
python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --dry-run

# Resume an interrupted backfill (skips already-completed dates)
python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --resume --parallel 2
```

## Docker

```bash
# Build from the docker/ directory
cd docker

# Build locally for arm64 (development on Mac M1/M2)
./build_and_push.sh --local -v 1.2.3

# Build for amd64 and push to Docker Hub (production)
./build_and_push.sh --remote -v 1.2.3

# Run the container interactively (with Google Cloud credentials mounted)
./run_mozaic_docker.sh --local -v 1.2.3

# Run a forecast directly inside the container
./run_mozaic_docker.sh --local -v 1.2.3 -- /run_forecast.sh
```

After building a new image, update the `IMAGE` constant in `mozaic_daily_flow.py` to reference the new version.

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run only validation tests
pytest tests/test_validation.py -v
```

## Further Reading

- [REQUIREMENTS.md](REQUIREMENTS.md) — System and dependency requirements for new machines
- [CLAUDE.md](CLAUDE.md) — Architecture details, configuration system, troubleshooting guide, and development conventions
