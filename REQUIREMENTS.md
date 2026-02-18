# System Requirements

This document lists the system-level dependencies required to set up and run `mozaic-daily` on a new machine.

## Operating System

- Linux (amd64) for production (Outerbounds/Kubernetes)
- macOS (arm64, M1/M2) supported for local development

## Python

- Python **3.10** (exact version required for compatibility with Prophet/Stan and the Docker image)

## Google Cloud

- **Google Cloud SDK** (`gcloud`) must be installed and authenticated
- Application Default Credentials (ADC) must be configured:
  ```bash
  gcloud auth application-default login
  ```
- The authenticated account must have BigQuery read access to:
  - `moz-fx-data-shared-prod` (source telemetry tables)
  - `moz-fx-data-bq-data-science` (default query project)
- The authenticated account must have BigQuery write access to:
  - `moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v2` (output table)

## Metaflow / Outerbounds

- **Outerbounds** account and CLI credentials configured for pipeline deployment and remote execution
- Access to the Outerbounds Kubernetes cluster used by `MozaicDailyFlow`

## Docker

- **Docker** installed and running (required for building and testing container images)
- Docker Hub account with push access to `brwells78094/mozaic-daily` (for image publishing)
- `docker buildx` with `linux/amd64` build support (required for cross-platform production builds on Mac)

## Git

- **Git** installed (required for cloning the Mozaic package from GitHub during Docker builds)

## Python Package Dependencies

Python dependencies are split by context:

### Local Development / Virtual Environment

The following packages are installed via the virtual environment. The exact versions are pinned in `docker/requirements.outerbounds.txt` for the packages shared with Docker:

| Package | Version | Purpose |
|---|---|---|
| mozmlops | 0.1.4 | Outerbounds ML ops utilities |
| google-cloud-bigquery | 3.38.0 | BigQuery client |
| db-dtypes | 1.4.4 | BigQuery data types for pandas |
| mozaic | (from git) | Core forecasting framework |
| metaflow | (latest) | Pipeline orchestration |
| prophet | (latest) | Time-series forecasting |
| cmdstanpy | (latest) | Stan interface for Prophet |
| numpy | (latest) | Numerical computing |
| pandas | (latest) | Data manipulation |
| scipy | (latest) | Scientific computing |
| pyarrow | (latest) | Parquet file support (checkpointing) |
| pytest | (latest) | Test runner |

### Mozaic Package (Internal Fork)

The Mozaic package is installed from a Mozilla-internal fork on GitHub:

```
git+https://github.com/brendanwells-moz/mozaic-forecasting@getters_branch
```

This requires read access to the `brendanwells-moz` GitHub organization.

### Docker Image (Production)

The production Docker image (`brwells78094/mozaic-daily`) is built for `linux/amd64` and bundles all dependencies including compiled Stan/CmdStan binaries. It is defined in `docker/Dockerfile` and does not need to be built on the deployment machine — it is pulled from Docker Hub at runtime.

## Network / Firewall

- Outbound HTTPS access to:
  - `bigquery.googleapis.com` (BigQuery API)
  - `github.com` (Mozaic package install)
  - `registry.hub.docker.com` (Docker image pull/push)
  - Outerbounds infrastructure endpoints (for Metaflow remote execution)

## Credentials Summary

| Credential | Where stored | Purpose |
|---|---|---|
| Google Cloud ADC | `~/.config/gcloud/` | BigQuery access |
| Outerbounds config | `~/.metaflowconfig` | Remote pipeline execution |
| Docker Hub credentials | `~/.docker/config.json` | Image push (publishing only) |
