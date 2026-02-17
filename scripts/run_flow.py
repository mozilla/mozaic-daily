#!/usr/bin/env python3
"""Unified runner script for Metaflow operations.

This script provides four execution modes:
1. local - Run flow locally without Kubernetes (for development)
2. remote - Run flow with Kubernetes enabled (test production path)
3. deploy - Create/update scheduled job on Argo Workflows
4. backfill - Run historical forecasts with date range and optional parallelism

Usage:
    python scripts/run_flow.py local
    python scripts/run_flow.py remote
    python scripts/run_flow.py deploy
    python scripts/run_flow.py backfill 2024-06-15
    python scripts/run_flow.py backfill 2024-06-01 2024-06-30
    python scripts/run_flow.py backfill 2024-06-01 2024-06-30 --parallel 4
    python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --parallel 2
    python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --dry-run
    python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --resume
    python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --local
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple, Optional, Dict, Any

# Weekday name mapping (Monday=0, Sunday=6)
WEEKDAY_MAP = {
    'monday': 0,
    'tuesday': 1,
    'wednesday': 2,
    'thursday': 3,
    'friday': 4,
    'saturday': 5,
    'sunday': 6,
}


def run_flow_subprocess(
    extra_args: List[str],
    local_mode: bool = False,
    capture_output: bool = False,
    timeout: Optional[int] = None
) -> subprocess.CompletedProcess:
    """Run mozaic_daily_flow.py with specified arguments.

    Args:
        extra_args: Additional arguments to pass to the flow
        local_mode: If True, set METAFLOW_LOCAL_MODE=true
        capture_output: If True, capture stdout/stderr
        timeout: Optional timeout in seconds

    Returns:
        CompletedProcess instance
    """
    cmd = ["python", "mozaic_daily_flow.py", "run"] + extra_args
    env = os.environ.copy()
    if local_mode:
        env["METAFLOW_LOCAL_MODE"] = "true"

    return subprocess.run(
        cmd,
        env=env,
        capture_output=capture_output,
        text=capture_output,
        timeout=timeout
    )


def print_backfill_summary(total: int, succeeded: List[str], failed: List[str]) -> None:
    """Print backfill summary statistics.

    Args:
        total: Total number of dates processed
        succeeded: List of successfully completed dates
        failed: List of failed dates
    """
    print("\n" + "=" * 60)
    print("BACKFILL SUMMARY")
    print("=" * 60)
    print(f"Total: {total}")
    print(f"Succeeded: {len(succeeded)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print("\nFailed dates:")
        for date in sorted(failed):
            print(f"  - {date}")
    else:
        print("\nAll backfills completed successfully!")


def run_local() -> int:
    """Run the flow locally with today's date.

    Sets METAFLOW_LOCAL_MODE=true to skip Kubernetes decorator.

    Returns:
        Exit code from subprocess
    """
    print("Running flow locally (without Kubernetes)...")
    result = run_flow_subprocess([], local_mode=True)
    return result.returncode


def run_remote() -> int:
    """Run the flow with Kubernetes enabled (non-local mode).

    Uses Kubernetes decorator for execution. Useful for testing
    the production execution path without deploying.

    Returns:
        Exit code from subprocess
    """
    print("Running flow with Kubernetes enabled...")
    result = run_flow_subprocess([])
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


def filter_dates_by_weekday(dates: List[str], weekdays: List[str]) -> List[str]:
    """Filter date list to only include specified weekdays.

    Args:
        dates: List of date strings in YYYY-MM-DD format
        weekdays: List of weekday names (e.g., ['monday', 'friday'])

    Returns:
        Filtered list of date strings
    """
    if not weekdays:
        return dates

    # Convert weekday names to numbers
    weekday_nums = [WEEKDAY_MAP[day.lower()] for day in weekdays]

    filtered = []
    for date_str in dates:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        if date_obj.weekday() in weekday_nums:
            filtered.append(date_str)

    return filtered


def get_state_file_path(
    log_dir: Path,
    start_date: str,
    end_date: str,
    weekdays: Optional[List[str]] = None
) -> Path:
    """Get deterministic state file path from backfill parameters.

    Args:
        log_dir: Directory to store state files
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        weekdays: Optional list of weekday names

    Returns:
        Path to state file
    """
    if weekdays:
        weekday_suffix = "_" + "_".join(sorted(weekdays))
    else:
        weekday_suffix = ""

    filename = f"backfill_state_{start_date}_{end_date}{weekday_suffix}.json"
    return log_dir / filename


def load_backfill_state(state_file: Path) -> Dict[str, Any]:
    """Load backfill state from JSON file.

    Args:
        state_file: Path to state file

    Returns:
        State dictionary, or empty state if file doesn't exist
    """
    if not state_file.exists():
        return {
            "completed_dates": [],
            "failed_dates": [],
        }

    with open(state_file, 'r') as f:
        return json.load(f)


def save_backfill_state(state_file: Path, state: Dict[str, Any]) -> None:
    """Save backfill state to JSON file atomically.

    Args:
        state_file: Path to state file
        state: State dictionary to save
    """
    # Update timestamp
    state["updated_at"] = datetime.now().isoformat()

    # Write to temp file, then atomic replace
    state_file.parent.mkdir(exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode='w',
        dir=state_file.parent,
        delete=False,
        suffix='.json'
    ) as tmp:
        json.dump(state, tmp, indent=2)
        tmp_path = tmp.name

    os.replace(tmp_path, state_file)


def run_single_backfill(
    date: str,
    log_dir: Path,
    local_mode: bool = False
) -> Tuple[str, bool, str]:
    """Run a single backfill for a specific date.

    Args:
        date: Date string in YYYY-MM-DD format
        log_dir: Directory to write log files
        local_mode: If True, run in local mode (no Kubernetes)

    Returns:
        Tuple of (date, success, log_file_path)
    """
    log_file = log_dir / f"backfill_{date}.log"
    extra_args = ["--forecast_start_date", date]

    try:
        result = run_flow_subprocess(
            extra_args,
            local_mode=local_mode,
            capture_output=True,
            timeout=14400  # 4 hour timeout per run
        )

        # Write output to log file
        mode_str = "local" if local_mode else "remote"
        with open(log_file, 'w') as f:
            f.write(f"Mode: {mode_str}\n")
            f.write(f"Date: {date}\n")
            f.write(f"Exit code: {result.returncode}\n\n")
            f.write("=== STDOUT ===\n")
            f.write(result.stdout)
            f.write("\n\n=== STDERR ===\n")
            f.write(result.stderr)

        success = result.returncode == 0
        return (date, success, str(log_file))

    except subprocess.TimeoutExpired:
        error_msg = f"Timeout after 4 hours"
        with open(log_file, 'w') as f:
            f.write(f"Date: {date}\n")
            f.write(f"Error: {error_msg}\n")
        return (date, False, str(log_file))
    except Exception as e:
        error_msg = f"Exception: {str(e)}"
        with open(log_file, 'w') as f:
            f.write(f"Date: {date}\n")
            f.write(f"Error: {error_msg}\n")
        return (date, False, str(log_file))


def run_backfill(
    start_date: str,
    end_date: str,
    parallel: int = 1,
    weekdays: Optional[List[str]] = None,
    dry_run: bool = False,
    resume: bool = False,
    local_mode: bool = False
) -> int:
    """Run backfill for a date range with optional parallelism.

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive)
        end_date: End date in YYYY-MM-DD format (inclusive)
        parallel: Number of concurrent workers (default: 1 for sequential)
        weekdays: Optional list of weekday names to filter (e.g., ['monday', 'friday'])
        dry_run: If True, print plan and exit without running
        resume: If True, skip dates from previous runs
        local_mode: If True, run in local mode (no Kubernetes)

    Returns:
        Exit code (0 if all succeeded, 1 if any failed)
    """
    # Generate and filter date list
    dates = generate_date_range(start_date, end_date)

    if weekdays:
        dates = filter_dates_by_weekday(dates, weekdays)
        weekday_str = ", ".join(weekdays)
        print(f"Filtered to weekdays: {weekday_str}")

    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Handle resume mode
    state_file = get_state_file_path(log_dir, start_date, end_date, weekdays)
    state = None

    if resume:
        state = load_backfill_state(state_file)
        completed = state.get("completed_dates", [])
        if completed:
            print(f"Resuming from previous run, skipping {len(completed)} completed dates")
            dates = [d for d in dates if d not in completed]

    # Initialize or update state
    if state is None:
        state = {
            "start_date": start_date,
            "end_date": end_date,
            "weekdays": weekdays,
            "local_mode": local_mode,
            "created_at": datetime.now().isoformat(),
            "completed_dates": [],
            "failed_dates": [],
        }
        save_backfill_state(state_file, state)

    total_dates = len(dates)

    # Dry run mode - print plan and exit
    if dry_run:
        print("\n" + "=" * 60)
        print("DRY RUN - Backfill Plan")
        print("=" * 60)
        print(f"Date range: {start_date} to {end_date}")
        if weekdays:
            print(f"Weekdays: {', '.join(weekdays)}")
        print(f"Execution mode: {'local' if local_mode else 'remote'}")
        print(f"Parallel workers: {parallel}")
        print(f"Total dates to process: {total_dates}")
        print("\nDates to process:")
        for date_str in dates:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            weekday_name = date_obj.strftime("%A")
            print(f"  - {date_str} ({weekday_name})")
        return 0

    # Normal execution
    print(f"Backfilling {total_dates} dates from {start_date} to {end_date}")
    print(f"Execution mode: {'local' if local_mode else 'remote'}")
    print(f"Parallel workers: {parallel}")
    print(f"State file: {state_file}")

    # Track results
    succeeded = []
    failed = []

    # Run with parallelism
    if parallel > 1:
        with ProcessPoolExecutor(max_workers=parallel) as executor:
            # Submit all jobs
            future_to_date = {
                executor.submit(run_single_backfill, date, log_dir, local_mode): date
                for date in dates
            }

            # Process as they complete
            for future in as_completed(future_to_date):
                date, success, log_file = future.result()

                if success:
                    succeeded.append(date)
                    state["completed_dates"].append(date)
                else:
                    failed.append(date)
                    state["failed_dates"].append(date)

                # Update state file
                save_backfill_state(state_file, state)

                # Show progress
                completed = len(succeeded) + len(failed)
                status = "✓" if success else "✗"
                print(f"[{completed}/{total_dates}] {status} {date} (log: {log_file})")
    else:
        # Sequential execution
        for i, date in enumerate(dates, 1):
            print(f"[{i}/{total_dates}] Processing {date}...")
            date_result, success, log_file = run_single_backfill(date, log_dir, local_mode)

            if success:
                succeeded.append(date_result)
                state["completed_dates"].append(date_result)
                print(f"  ✓ Success (log: {log_file})")
            else:
                failed.append(date_result)
                state["failed_dates"].append(date_result)
                print(f"  ✗ Failed (log: {log_file})")

            # Update state file
            save_backfill_state(state_file, state)

    # Print summary
    print_backfill_summary(total_dates, succeeded, failed)

    return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(
        description="Unified runner for Metaflow operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run locally without Kubernetes (development)
  python scripts/run_flow.py local

  # Run with Kubernetes enabled (test production path)
  python scripts/run_flow.py remote

  # Deploy scheduled job
  python scripts/run_flow.py deploy

  # Backfill single date
  python scripts/run_flow.py backfill 2024-06-15

  # Backfill date range (inclusive, sequential)
  python scripts/run_flow.py backfill 2024-06-01 2024-06-30

  # Backfill with 4 parallel workers
  python scripts/run_flow.py backfill 2024-06-01 2024-06-30 --parallel 4

  # Backfill only Mondays with parallel execution
  python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --parallel 2

  # Dry run to see plan without executing
  python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --dry-run

  # Resume a previous backfill run
  python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --weekday monday --resume

  # Run backfill in local mode (no Kubernetes)
  python scripts/run_flow.py backfill 2025-07-01 2026-02-01 --local
        """
    )

    subparsers = parser.add_subparsers(dest="mode", required=True, help="Execution mode")

    # Local mode
    subparsers.add_parser("local", help="Run flow locally without Kubernetes")

    # Remote mode
    subparsers.add_parser("remote", help="Run flow with Kubernetes enabled")

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
    backfill_parser.add_argument(
        "--weekday",
        action="append",
        choices=list(WEEKDAY_MAP.keys()),
        help="Filter to specific weekday(s). Can be specified multiple times."
    )
    backfill_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print execution plan without running backfill"
    )
    backfill_parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous run, skipping completed dates"
    )
    backfill_parser.add_argument(
        "--local",
        action="store_true",
        help="Run in local mode without Kubernetes (default: remote)"
    )

    args = parser.parse_args()

    # Execute based on mode
    if args.mode == "local":
        exit_code = run_local()
    elif args.mode == "remote":
        exit_code = run_remote()
    elif args.mode == "deploy":
        exit_code = run_deploy()
    elif args.mode == "backfill":
        # Default end_date to start_date if not provided
        end_date = args.end_date if args.end_date else args.start_date
        exit_code = run_backfill(
            args.start_date,
            end_date,
            parallel=args.parallel,
            weekdays=args.weekday,
            dry_run=args.dry_run,
            resume=args.resume,
            local_mode=args.local
        )
    else:
        print(f"Unknown mode: {args.mode}")
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
