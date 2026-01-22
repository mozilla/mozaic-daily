"""Mozaic Daily Forecasting Package.

Automated daily forecasting for Mozilla Firefox metrics using the Mozaic package.
Runs as a Metaflow pipeline on Outerbounds infrastructure.
"""
from .main import main
from .validation import validate_output_dataframe
from .tables import get_git_commit_hash

__version__ = "0.1.0"

__all__ = ["main", "validate_output_dataframe"]
