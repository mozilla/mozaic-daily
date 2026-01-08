# mozaic-daily
Daily automated forecasting using the Mozaic package

## Overview

This repository implements automated daily forecasting for Mozilla Firefox metrics using the Mozaic package. The system runs as a Metaflow pipeline on Outerbounds infrastructure, querying BigQuery for telemetry data and producing forecasts for Desktop and Mobile platforms.

## Project Structure

```
mozaic-daily/
├── src/
│   └── mozaic_daily/          # Main package
│       ├── __init__.py        # Package exports
│       ├── config.py          # Constants and date logic
│       ├── data.py            # BigQuery data fetching + SQL queries
│       ├── forecast.py        # Mozaic forecasting logic
│       ├── tables.py          # Table formatting/manipulation
│       ├── validation.py      # Output validation
│       └── main.py            # Main entry point
├── tests/                     # Test suite
├── mozaic_daily.py            # Compatibility shim (temporary)
├── mozaic_daily_validation.py # Compatibility shim (temporary)
├── constants.py               # Compatibility shim (temporary)
├── mozaic_daily_flow.py       # Metaflow pipeline definition
├── Dockerfile-mozaic-daily    # Docker build configuration
├── build_and_push.sh          # Docker build script
├── pyproject.toml             # Package metadata
└── CLAUDE.md                  # Development guide

Note: Compatibility shims will be removed in a future update once the
transition to the package structure is complete.
```

## Setup

See [CLAUDE.md](CLAUDE.md) for detailed development environment setup and usage instructions.
