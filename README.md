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
├── scripts/                   # Helper scripts
│   ├── run_main.py            # Run forecasting pipeline
│   ├── run_validation.py      # Run validation on checkpoint
│   └── test_local_docker.sh   # Test Docker builds
├── docker/                    # Docker build files
│   ├── Dockerfile             # Docker image definition
│   ├── build_and_push.sh      # Docker build script
│   └── requirements.outerbounds.txt  # Python dependencies for Docker
├── tests/                     # Test suite
├── mozaic_daily_flow.py       # Metaflow pipeline definition
├── pyproject.toml             # Package metadata
├── CLAUDE.md                  # Development guide
└── README.md                  # This file
```

## Setup

See [CLAUDE.md](CLAUDE.md) for detailed development environment setup and usage instructions.
