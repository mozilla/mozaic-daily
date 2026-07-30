"""Mozaic Daily Forecasting Package.

Automated daily forecasting for Mozilla Firefox metrics using the Mozaic package.
Runs as a Metaflow pipeline on Outerbounds infrastructure.
"""
from .main import main
from .validation import validate_output_dataframe
from .config import get_git_commit_hash
from .seam_ma import daily_to_28ma, display_ma, reconstruct_matched_daily

__version__ = "0.1.0"

__all__ = [
    "main",
    "validate_output_dataframe",
    "get_git_commit_hash",
    "daily_to_28ma",
    "display_ma",
    "reconstruct_matched_daily",
]
