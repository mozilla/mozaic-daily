#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Combine multiple historical DAU forecast files into a single parquet file.

This script scans a directory for dau_forecast_*.parquet files and combines
them into a single output file. Useful for batch processing historical forecasts.

Usage:
    # Combine all forecasts in a directory
    python scripts/combine_forecasts.py --input-dir ./forecasts --output combined.parquet

    # Default output name is combined_forecasts.parquet
    python scripts/combine_forecasts.py --input-dir ./forecasts
"""

import argparse
import pandas as pd
from pathlib import Path
from typing import List


def find_forecast_files(input_dir: Path) -> List[Path]:
    """Find all dau_forecast_*.parquet files in the input directory.

    Args:
        input_dir: Directory to scan for forecast files

    Returns:
        List of Path objects for matching files, sorted by name
    """
    pattern = "dau_forecast_*.parquet"
    files = sorted(input_dir.glob(pattern))
    return files


def load_and_combine_forecasts(files: List[Path]) -> pd.DataFrame:
    """Load and combine multiple forecast parquet files.

    Args:
        files: List of Path objects to parquet files

    Returns:
        Combined DataFrame with all forecasts
    """
    dfs = []
    for file_path in files:
        print(f"Loading {file_path.name}")
        df = pd.read_parquet(file_path)
        dfs.append(df)

    print(f"\nCombining {len(dfs)} files...")
    combined = pd.concat(dfs, ignore_index=True)
    return combined


def main():
    parser = argparse.ArgumentParser(
        description="Combine historical DAU forecast files into a single parquet file",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Directory containing dau_forecast_*.parquet files"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="combined_forecasts.parquet",
        help="Output filename (default: combined_forecasts.parquet)"
    )
    args = parser.parse_args()

    # Validate input directory
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")
    if not input_dir.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {input_dir}")

    # Find forecast files
    files = find_forecast_files(input_dir)
    if not files:
        print(f"No dau_forecast_*.parquet files found in {input_dir}")
        return

    print(f"Found {len(files)} forecast files:")
    for f in files:
        print(f"  - {f.name}")
    print()

    # Load and combine
    combined = load_and_combine_forecasts(files)

    # Save output
    output_path = Path(args.output)
    print(f"\nSaving combined output to {output_path}")
    combined.to_parquet(output_path)
    print(f"Done! Combined {len(files)} files into {len(combined)} rows")


if __name__ == "__main__":
    main()
