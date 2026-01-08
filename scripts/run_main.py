#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run the mozaic-daily main pipeline with checkpoints enabled.

This script mimics the old behavior of running `python mozaic_daily.py`.
It can be run from anywhere in the project.

Usage:
    python scripts/run_main.py
"""

import sys
from pathlib import Path

# Add src directory to path so we can import the package
repo_root = Path(__file__).parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from mozaic_daily import main

if __name__ == '__main__':
    main(checkpoints=True)
