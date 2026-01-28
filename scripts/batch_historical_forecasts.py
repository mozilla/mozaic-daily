#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Batch process historical DAU forecasts for a date range.

This script automates running forecasts for multiple historical dates.
All flags are hardcoded except start_date and end_date.

Usage:
    python scripts/batch_historical_forecasts.py 2024-06-01 2024-06-30
"""

import sys
import subprocess
import argparse
import time
import statistics
from datetime import datetime, timedelta
from pathlib import Path


def parse_date(date_str: str) -> datetime:
    """Parse YYYY-MM-DD string to datetime."""
    return datetime.strptime(date_str, "%Y-%m-%d")


def format_date(dt: datetime) -> str:
    """Format datetime to YYYY-MM-DD string."""
    return dt.strftime("%Y-%m-%d")


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.2f}h"


def format_time_estimate(
    completed: int,
    total: int,
    median_duration: float
) -> str:
    """Format estimated time remaining."""
    remaining_count = total - completed
    estimated_remaining = remaining_count * median_duration

    return (
        f"Median duration: {format_duration(median_duration)} | "
        f"Estimated remaining: {format_duration(estimated_remaining)}"
    )


def generate_date_range(start_date: str, end_date: str):
    """Generate list of dates between start and end (inclusive)."""
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)

    current = start_dt
    dates = []
    while current <= end_dt:
        dates.append(format_date(current))
        current += timedelta(days=1)

    return dates


def run_forecast_for_date(date: str, output_dir: str, repo_root: Path):
    """Run forecast for a single date. Returns (success, duration_seconds)."""
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_main.py"),
        "--forecast-start-date", date,
        "--dau-only",
        "--forecast-only",
        "--output-dir", output_dir,
        "--no-checkpoints"  # Required for batch processing different dates
    ]

    print(f"Running command: {' '.join(cmd)}")
    start_time = time.time()
    result = subprocess.run(cmd, cwd=repo_root)
    duration = time.time() - start_time

    if result.returncode != 0:
        print(f"  ERROR: Forecast failed for {date}")
        return False, duration

    return True, duration


def combine_forecasts(output_dir: str, repo_root: Path):
    """Combine all forecast files into a single parquet."""
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "combine_forecasts.py"),
        "--input-dir", output_dir,
        "--output", "combined_forecasts.parquet"
    ]

    print(f"\nCombining forecasts...")
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=repo_root)

    if result.returncode != 0:
        print("  ERROR: Failed to combine forecasts")
        return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Batch process historical DAU forecasts for a date range",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "start_date",
        type=str,
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "end_date",
        type=str,
        help="End date (YYYY-MM-DD, inclusive)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./forecasts",
        help="Output directory for forecast files (default: ./forecasts)"
    )
    parser.add_argument(
        "--skip-combine",
        action="store_true",
        help="Skip combining forecasts at the end"
    )
    args = parser.parse_args()

    # Get repo root
    repo_root = Path(__file__).parent.parent

    # Generate date range
    dates = generate_date_range(args.start_date, args.end_date)
    total_dates = len(dates)

    print("=" * 80)
    print(f"BATCH HISTORICAL FORECASTS")
    print(f"Date range: {args.start_date} to {args.end_date}")
    print(f"Total dates: {total_dates}")
    print(f"Output directory: {args.output_dir}")
    print(f"Flags: --dau-only --forecast-only --no-checkpoints")
    print("=" * 80)
    print()

    # Process each date
    success_count = 0
    failed_dates = []
    durations = []
    batch_start_time = time.time()

    for i, date in enumerate(dates, 1):
        print(f"[{i}/{total_dates}] Processing date: {date}")
        print("-" * 80)

        success, duration = run_forecast_for_date(date, args.output_dir, repo_root)
        durations.append(duration)

        if success:
            success_count += 1
            print(f"  SUCCESS: Forecast completed for {date} (took {format_duration(duration)})")
        else:
            failed_dates.append(date)
            print(f"  Duration: {format_duration(duration)}")

        # Calculate and display time estimates
        if len(durations) >= 1:
            median_duration = statistics.median(durations)
            print(f"  {format_time_estimate(i, total_dates, median_duration)}")

        print()

    # Summary
    total_elapsed = time.time() - batch_start_time
    print("=" * 80)
    print("BATCH PROCESSING COMPLETE")
    print(f"Successful: {success_count}/{total_dates}")
    print(f"Total elapsed time: {format_duration(total_elapsed)}")

    if durations:
        median_duration = statistics.median(durations)
        print(f"Median duration per forecast: {format_duration(median_duration)}")
        print(f"Min duration: {format_duration(min(durations))}")
        print(f"Max duration: {format_duration(max(durations))}")

    if failed_dates:
        print(f"Failed dates: {', '.join(failed_dates)}")

    print("=" * 80)
    print()

    # Combine forecasts
    if not args.skip_combine and success_count > 0:
        print("=" * 80)
        combine_success = combine_forecasts(args.output_dir, repo_root)
        if combine_success:
            print(f"  SUCCESS: Combined forecasts saved to combined_forecasts.parquet")
        print("=" * 80)

    # Exit with error code if any forecasts failed
    if failed_dates:
        sys.exit(1)


if __name__ == "__main__":
    main()
