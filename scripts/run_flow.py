#!/usr/bin/env python3
"""Unified runner script for Metaflow operations.

This script provides three execution modes:
1. local - Run flow locally with today's date
2. deploy - Create/update scheduled job on Argo Workflows
3. backfill - Run historical forecasts with date range and optional parallelism

Usage:
    python scripts/run_flow.py local
    python scripts/run_flow.py deploy
    python scripts/run_flow.py backfill 2024-06-15
    python scripts/run_flow.py backfill 2024-06-01 2024-06-30
    python scripts/run_flow.py backfill 2024-06-01 2024-06-30 --parallel 4
"""

import argparse
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple


def run_local() -> int:
    """Run the flow locally with today's date.

    Returns:
        Exit code from subprocess
    """
    print("Running flow locally...")
    cmd = ["python", "mozaic_daily_flow.py", "run"]
    result = subprocess.run(cmd)
    return result.returncode


def run_deploy() -> int:
    """Deploy/update scheduled job on Argo Workflows.

    Returns:
        Exit code from subprocess
    """
    print("Deploying to Argo Workflows...")
    cmd = ["python", "mozaic_daily_flow.py", "--with", "retry", "argo-workflows", "create"]
    result = subprocess.run(cmd)
    return result.returncode


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate list of dates between start and end (inclusive).

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        List of date strings in YYYY-MM-DD format
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    if start > end:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates


def run_single_backfill(date: str, log_dir: Path) -> Tuple[str, bool, str]:
    """Run a single backfill for a specific date.

    Args:
        date: Date string in YYYY-MM-DD format
        log_dir: Directory to write log files

    Returns:
        Tuple of (date, success, output)
    """
    log_file = log_dir / f"backfill_{date}.log"
    cmd = ["python", "mozaic_daily_flow.py", "run", "--forecast_start_date", date]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=7200  # 2 hour timeout per run
        )

        # Write output to log file
        with open(log_file, 'w') as f:
            f.write(f"Command: {' '.join(cmd)}\n")
            f.write(f"Exit code: {result.returncode}\n\n")
            f.write("=== STDOUT ===\n")
            f.write(result.stdout)
            f.write("\n\n=== STDERR ===\n")
            f.write(result.stderr)

        success = result.returncode == 0
        return (date, success, str(log_file))

    except subprocess.TimeoutExpired:
        error_msg = f"Timeout after 2 hours"
        with open(log_file, 'w') as f:
            f.write(f"Command: {' '.join(cmd)}\n")
            f.write(f"Error: {error_msg}\n")
        return (date, False, str(log_file))
    except Exception as e:
        error_msg = f"Exception: {str(e)}"
        with open(log_file, 'w') as f:
            f.write(f"Command: {' '.join(cmd)}\n")
            f.write(f"Error: {error_msg}\n")
        return (date, False, str(log_file))


def run_backfill(start_date: str, end_date: str, parallel: int = 1) -> int:
    """Run backfill for a date range with optional parallelism.

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive)
        end_date: End date in YYYY-MM-DD format (inclusive)
        parallel: Number of concurrent workers (default: 1 for sequential)

    Returns:
        Exit code (0 if all succeeded, 1 if any failed)
    """
    # Generate date list
    dates = generate_date_range(start_date, end_date)
    total_dates = len(dates)

    print(f"Backfilling {total_dates} dates from {start_date} to {end_date}")
    print(f"Parallel workers: {parallel}")

    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Track results
    succeeded = []
    failed = []

    # Run with parallelism
    if parallel > 1:
        with ProcessPoolExecutor(max_workers=parallel) as executor:
            # Submit all jobs
            future_to_date = {
                executor.submit(run_single_backfill, date, log_dir): date
                for date in dates
            }

            # Process as they complete
            for future in as_completed(future_to_date):
                date, success, log_file = future.result()

                if success:
                    succeeded.append(date)
                else:
                    failed.append(date)

                # Show progress
                completed = len(succeeded) + len(failed)
                status = "✓" if success else "✗"
                print(f"[{completed}/{total_dates}] {status} {date} (log: {log_file})")
    else:
        # Sequential execution
        for i, date in enumerate(dates, 1):
            print(f"[{i}/{total_dates}] Processing {date}...")
            date_result, success, log_file = run_single_backfill(date, log_dir)

            if success:
                succeeded.append(date_result)
                print(f"  ✓ Success (log: {log_file})")
            else:
                failed.append(date_result)
                print(f"  ✗ Failed (log: {log_file})")

    # Print summary
    print("\n" + "=" * 60)
    print("BACKFILL SUMMARY")
    print("=" * 60)
    print(f"Total: {total_dates}")
    print(f"Succeeded: {len(succeeded)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print("\nFailed dates:")
        for date in sorted(failed):
            print(f"  - {date}")
        return 1

    print("\nAll backfills completed successfully!")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Unified runner for Metaflow operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run locally with today's date
  python scripts/run_flow.py local

  # Deploy scheduled job
  python scripts/run_flow.py deploy

  # Backfill single date
  python scripts/run_flow.py backfill 2024-06-15

  # Backfill date range (inclusive, sequential)
  python scripts/run_flow.py backfill 2024-06-01 2024-06-30

  # Backfill with 4 parallel workers
  python scripts/run_flow.py backfill 2024-06-01 2024-06-30 --parallel 4
        """
    )

    subparsers = parser.add_subparsers(dest="mode", required=True, help="Execution mode")

    # Local mode
    subparsers.add_parser("local", help="Run flow locally")

    # Deploy mode
    subparsers.add_parser("deploy", help="Deploy/update scheduled job")

    # Backfill mode
    backfill_parser = subparsers.add_parser("backfill", help="Run historical backfill")
    backfill_parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    backfill_parser.add_argument(
        "end_date",
        nargs="?",
        help="End date (YYYY-MM-DD, inclusive). Defaults to start_date for single-date backfill."
    )
    backfill_parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel workers (default: 1)"
    )

    args = parser.parse_args()

    # Execute based on mode
    if args.mode == "local":
        exit_code = run_local()
    elif args.mode == "deploy":
        exit_code = run_deploy()
    elif args.mode == "backfill":
        # Default end_date to start_date if not provided
        end_date = args.end_date if args.end_date else args.start_date
        exit_code = run_backfill(args.start_date, end_date, args.parallel)
    else:
        print(f"Unknown mode: {args.mode}")
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
