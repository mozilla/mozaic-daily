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
    python scripts/run_flow.py backfill --dates-file failures.txt
    python scripts/run_flow.py backfill --dates-file failures.txt --parallel 4
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

# Add src directory to path so we can import enum values for choices
repo_root = Path(__file__).parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from mozaic_daily.queries import DataSource, Metric

VALID_DATA_SOURCES = [ds.value for ds in DataSource]
VALID_METRICS = [m.value for m in Metric]

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


def add_filter_args(parser: argparse.ArgumentParser) -> None:
    """Add --data-source and --metric filter arguments to a subparser."""
    parser.add_argument(
        '--data-source',
        action='append',
        choices=VALID_DATA_SOURCES,
        metavar='SOURCE',
        help=f'Filter to specific data source(s). Valid: {", ".join(VALID_DATA_SOURCES)}'
    )
    parser.add_argument(
        '--metric',
        action='append',
        choices=VALID_METRICS,
        metavar='METRIC',
        help=f'Filter to specific metric(s). Valid: {", ".join(VALID_METRICS)}'
    )


def build_filter_args(args: argparse.Namespace) -> List[str]:
    """Build Metaflow CLI args from parsed filter values."""
    extra = []
    if args.data_source:
        extra += ["--data_sources", ",".join(args.data_source)]
    if args.metric:
        extra += ["--metrics", ",".join(args.metric)]
    return extra


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


def run_local(extra_flow_args: Optional[List[str]] = None) -> int:
    """Run the flow locally with today's date.

    Sets METAFLOW_LOCAL_MODE=true to skip Kubernetes decorator.

    Args:
        extra_flow_args: Additional arguments to pass to the flow (e.g., filters)

    Returns:
        Exit code from subprocess
    """
    print("Running flow locally (without Kubernetes)...")
    result = run_flow_subprocess(extra_flow_args or [], local_mode=True)
    return result.returncode


def run_remote(extra_flow_args: Optional[List[str]] = None) -> int:
    """Run the flow with Kubernetes enabled (non-local mode).

    Uses Kubernetes decorator for execution. Useful for testing
    the production execution path without deploying.

    Args:
        extra_flow_args: Additional arguments to pass to the flow (e.g., filters)

    Returns:
        Exit code from subprocess
    """
    print("Running flow with Kubernetes enabled...")
    result = run_flow_subprocess(extra_flow_args or [])
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


def get_log_file_path(log_dir: Path, date: str) -> Path:
    """Get next available log file path for a date, preserving history across reruns.

    First run:  backfill_YYYY-MM-DD.log
    Second run: backfill_YYYY-MM-DD.run2.log
    Third run:  backfill_YYYY-MM-DD.run3.log

    Args:
        log_dir: Directory containing log files
        date: Date string in YYYY-MM-DD format

    Returns:
        Path to the next available log file
    """
    base_path = log_dir / f"backfill_{date}.log"
    if not base_path.exists():
        return base_path

    # Find existing run files for this date
    run_number = 2
    while True:
        run_path = log_dir / f"backfill_{date}.run{run_number}.log"
        if not run_path.exists():
            return run_path
        run_number += 1


def load_dates_file(file_path: str) -> List[str]:
    """Load and validate a file of dates (one YYYY-MM-DD per line).

    Args:
        file_path: Path to the dates file

    Returns:
        Sorted list of validated date strings

    Raises:
        SystemExit: If file is invalid or contains bad dates
    """
    path = Path(file_path)
    if not path.is_file():
        print(f"Error: '{file_path}' is not a file or does not exist.", file=sys.stderr)
        sys.exit(1)

    today = datetime.now().strftime("%Y-%m-%d")
    dates = []
    seen = set()

    for line_number, raw_line in enumerate(path.read_text().splitlines(), 1):
        line = raw_line.strip()
        if not line:
            continue

        # Validate date format
        try:
            datetime.strptime(line, "%Y-%m-%d")
        except ValueError:
            print(
                f"Error: Invalid date '{line}' on line {line_number} of {file_path}. "
                f"Expected YYYY-MM-DD format.",
                file=sys.stderr,
            )
            sys.exit(1)

        # Check for future dates
        if line >= today:
            print(
                f"Error: Date '{line}' on line {line_number} of {file_path} "
                f"is today or in the future.",
                file=sys.stderr,
            )
            sys.exit(1)

        # Check for duplicates
        if line in seen:
            print(
                f"Error: Duplicate date '{line}' on line {line_number} of {file_path}.",
                file=sys.stderr,
            )
            sys.exit(1)

        seen.add(line)
        dates.append(line)

    if not dates:
        print(f"Error: No dates found in {file_path}.", file=sys.stderr)
        sys.exit(1)

    dates.sort()
    print(f"Loaded {len(dates)} dates from {file_path} ({dates[0]} to {dates[-1]})")
    return dates


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
    local_mode: bool = False,
    tee_output: bool = False,
    extra_flow_args: Optional[List[str]] = None,
) -> Tuple[str, bool, str]:
    """Run a single backfill for a specific date.

    Args:
        date: Date string in YYYY-MM-DD format
        log_dir: Directory to write log files
        local_mode: If True, run in local mode (no Kubernetes)
        tee_output: If True, stream output to terminal and log file simultaneously
        extra_flow_args: Additional arguments to pass to the flow (e.g., filters)

    Returns:
        Tuple of (date, success, log_file_path)
    """
    log_file = get_log_file_path(log_dir, date)
    extra_args = ["--forecast_start_date", date] + (extra_flow_args or [])

    try:
        if tee_output:
            cmd = ["python", "mozaic_daily_flow.py", "run"] + extra_args
            env = os.environ.copy()
            if local_mode:
                env["METAFLOW_LOCAL_MODE"] = "true"

            log_dir.mkdir(exist_ok=True)
            with open(log_file, 'w') as f:
                mode_str = "local" if local_mode else "remote"
                f.write(f"Mode: {mode_str}\n")
                f.write(f"Date: {date}\n\n")

                process = subprocess.Popen(
                    cmd,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True
                )
                for line in process.stdout:
                    print(line, end="", flush=True)
                    f.write(line)

                process.wait(timeout=14400)
                returncode = process.returncode
                f.write(f"\nExit code: {returncode}\n")

            success = returncode == 0
        else:
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
        with open(log_file, 'a') as f:
            f.write(f"\nError: {error_msg}\n")
        return (date, False, str(log_file))
    except Exception as e:
        error_msg = f"Exception: {str(e)}"
        with open(log_file, 'a') as f:
            f.write(f"\nError: {error_msg}\n")
        return (date, False, str(log_file))


def run_backfill(
    dates: List[str],
    parallel: int = 1,
    dry_run: bool = False,
    local_mode: bool = False,
    description: str = "",
    extra_flow_args: Optional[List[str]] = None,
) -> int:
    """Run backfill for a list of dates with optional parallelism.

    Args:
        dates: List of date strings in YYYY-MM-DD format
        parallel: Number of concurrent workers (default: 1 for sequential)
        dry_run: If True, print plan and exit without running
        local_mode: If True, run in local mode (no Kubernetes)
        description: Source description for dry-run header (e.g., "from failures.txt")
        extra_flow_args: Additional arguments to pass to the flow (e.g., filters)

    Returns:
        Exit code (0 if all succeeded, 1 if any failed)
    """
    total_dates = len(dates)

    # Dry run mode - print plan and exit
    if dry_run:
        print("\n" + "=" * 60)
        print("DRY RUN - Backfill Plan")
        print("=" * 60)
        if description:
            print(f"Source: {description}")
        if extra_flow_args:
            print(f"Extra flow args: {' '.join(extra_flow_args)}")
        print(f"Execution mode: {'local' if local_mode else 'remote'}")
        print(f"Parallel workers: {parallel}")
        print(f"Total dates to process: {total_dates}")
        print("\nDates to process:")
        for date_str in dates:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            weekday_name = date_obj.strftime("%A")
            print(f"  - {date_str} ({weekday_name})")
        return 0

    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Normal execution
    print(f"Backfilling {total_dates} dates{' ' + description if description else ''}")
    print(f"Execution mode: {'local' if local_mode else 'remote'}")
    print(f"Parallel workers: {parallel}")

    # Track results
    succeeded = []
    failed = []

    # Run with parallelism
    if parallel > 1:
        with ProcessPoolExecutor(max_workers=parallel) as executor:
            # Submit all jobs
            future_to_date = {
                executor.submit(run_single_backfill, date, log_dir, local_mode, False, extra_flow_args): date
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
                completed_count = len(succeeded) + len(failed)
                status = "✓" if success else "✗"
                print(f"[{completed_count}/{total_dates}] {status} {date} (log: {log_file})")
    else:
        # Sequential execution
        is_single_date = len(dates) == 1
        for i, date in enumerate(dates, 1):
            print(f"[{i}/{total_dates}] Processing {date}...")
            date_result, success, log_file = run_single_backfill(
                date, log_dir, local_mode, tee_output=is_single_date,
                extra_flow_args=extra_flow_args,
            )

            if success:
                succeeded.append(date_result)
                print(f"  ✓ Success (log: {log_file})")
            else:
                failed.append(date_result)
                print(f"  ✗ Failed (log: {log_file})")

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

  # Backfill from a file of dates (one YYYY-MM-DD per line)
  python scripts/run_flow.py backfill --dates-file failures.txt

  # Backfill from file with parallel workers
  python scripts/run_flow.py backfill --dates-file failures.txt --parallel 4
        """
    )

    subparsers = parser.add_subparsers(dest="mode", required=True, help="Execution mode")

    # Local mode
    local_parser = subparsers.add_parser("local", help="Run flow locally without Kubernetes")
    add_filter_args(local_parser)

    # Remote mode
    remote_parser = subparsers.add_parser("remote", help="Run flow with Kubernetes enabled")
    add_filter_args(remote_parser)

    # Deploy mode
    subparsers.add_parser("deploy", help="Deploy/update scheduled job")

    # Backfill mode
    backfill_parser = subparsers.add_parser("backfill", help="Run historical backfill")
    backfill_parser.add_argument(
        "start_date",
        nargs="?",
        help="Start date (YYYY-MM-DD). Required unless --dates-file is used."
    )
    backfill_parser.add_argument(
        "end_date",
        nargs="?",
        help="End date (YYYY-MM-DD, inclusive). Defaults to start_date for single-date backfill."
    )
    backfill_parser.add_argument(
        "--dates-file", "-f",
        help="File containing dates to backfill (one YYYY-MM-DD per line)"
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
    add_filter_args(backfill_parser)

    args = parser.parse_args()

    # Build filter args for modes that support them
    extra_flow_args = build_filter_args(args) if hasattr(args, 'data_source') else []

    # Execute based on mode
    if args.mode == "local":
        exit_code = run_local(extra_flow_args)
    elif args.mode == "remote":
        exit_code = run_remote(extra_flow_args)
    elif args.mode == "deploy":
        exit_code = run_deploy()
    elif args.mode == "backfill":
        # Validate argument combinations
        if args.dates_file and args.start_date:
            print(
                "Error: Cannot combine --dates-file with positional date arguments. "
                "Use one or the other.",
                file=sys.stderr,
            )
            sys.exit(1)

        if args.dates_file and args.weekday:
            print(
                "Error: Cannot use --weekday with --dates-file. "
                "The file already contains the exact dates to process.",
                file=sys.stderr,
            )
            sys.exit(1)

        if args.dates_file and args.resume:
            print(
                "Error: Cannot use --resume with --dates-file. "
                "To retry failures, generate a new dates file with check_logs.py.",
                file=sys.stderr,
            )
            sys.exit(1)

        if not args.dates_file and not args.start_date:
            print(
                "Error: Either start_date or --dates-file is required.",
                file=sys.stderr,
            )
            sys.exit(1)

        # Resolve dates from either source
        if args.dates_file:
            dates = load_dates_file(args.dates_file)
            description = f"from {args.dates_file}"
        else:
            start_date = args.start_date
            end_date = args.end_date if args.end_date else start_date
            dates = generate_date_range(start_date, end_date)

            if args.weekday:
                dates = filter_dates_by_weekday(dates, args.weekday)
                weekday_str = ", ".join(args.weekday)
                print(f"Filtered to weekdays: {weekday_str}")

            # Handle resume mode (only for date-range mode)
            if args.resume:
                log_dir = Path("logs")
                state_file = get_state_file_path(log_dir, start_date, end_date, args.weekday)
                state = load_backfill_state(state_file)
                completed = state.get("completed_dates", [])
                if completed:
                    print(f"Resuming from previous run, skipping {len(completed)} completed dates")
                    dates = [d for d in dates if d not in completed]

            description = f"{start_date} to {end_date}"

        exit_code = run_backfill(
            dates,
            parallel=args.parallel,
            dry_run=args.dry_run,
            local_mode=args.local,
            description=description,
            extra_flow_args=extra_flow_args,
        )
    else:
        print(f"Unknown mode: {args.mode}")
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
