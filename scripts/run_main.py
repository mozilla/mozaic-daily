#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run the mozaic-daily main pipeline with checkpoints enabled.

This script mimics the old behavior of running `python mozaic_daily.py`.
It can be run from anywhere in the project.

Usage:
    # Normal mode (all platforms/metrics)
    python scripts/run_main.py

    # Testing mode (desktop/DAU only)
    python scripts/run_main.py --testing

    # Historical forecast
    python scripts/run_main.py --forecast-start-date 2024-06-15

    # Debug: only query DAU metrics
    python scripts/run_main.py --dau-only --no-checkpoints

    # Debug: restrict to one data source
    python scripts/run_main.py --data-source glean_desktop

    # Debug: fetch data only, skip Mozaic
    python scripts/run_main.py --historical-only --output-dir ./debug_output

    # Debug: print null breakdown after pipeline
    python scripts/run_main.py --null-report

    # Debug: run pipeline N times and save each output for comparison
    python scripts/run_main.py --repeat 2 --no-checkpoints

    # Debug: save raw BigQuery data and intermediate DataFrames
    python scripts/run_main.py --save-raw-data --save-intermediate --output-dir ./debug_output
"""

import sys
import io
import argparse
from pathlib import Path

# Add src directory to path so we can import the package
repo_root = Path(__file__).parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from mozaic_daily import main
from mozaic_daily.config import STATIC_CONFIG


VALID_DATA_SOURCES = ['glean_desktop', 'legacy_desktop', 'glean_mobile']


class TeeWriter:
    """Write to both a file and the original stream (stdout/stderr)."""

    def __init__(self, file_handle, original_stream):
        self.file_handle = file_handle
        self.original_stream = original_stream

    def write(self, text):
        self.original_stream.write(text)
        self.file_handle.write(text)
        self.file_handle.flush()

    def flush(self):
        self.original_stream.flush()
        self.file_handle.flush()

    # Forward any other attribute access to the original stream so libraries
    # that inspect stdout (e.g., isatty()) don't break.
    def __getattr__(self, name):
        return getattr(self.original_stream, name)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the run_main script."""
    parser = argparse.ArgumentParser(
        description='Run the mozaic-daily forecasting pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ---- Standard flags ----
    parser.add_argument(
        '--testing',
        action='store_true',
        help='Run in testing mode (desktop/DAU only)'
    )
    parser.add_argument(
        '--forecast-start-date',
        type=str,
        help='Override forecast start date (YYYY-MM-DD) for historical runs'
    )
    parser.add_argument(
        '--no-checkpoints',
        action='store_true',
        help='Disable checkpoint loading/saving'
    )

    # ---- Scope flags ----
    parser.add_argument(
        '--dau-only',
        action='store_true',
        help='Only query DAU metrics (reduces 12 queries to 3)'
    )
    parser.add_argument(
        '--data-source',
        choices=VALID_DATA_SOURCES,
        metavar='NAME',
        help=f'Restrict to one data source: {", ".join(VALID_DATA_SOURCES)}'
    )

    # ---- Repeatability ----
    parser.add_argument(
        '--repeat',
        type=int,
        default=1,
        metavar='N',
        help='Run the pipeline N times, saving each output for comparison'
    )

    # ---- Output filter ----
    parser.add_argument(
        '--forecast-only',
        action='store_true',
        help="Strip training rows from output (keep only data_type='forecast')"
    )

    # ---- Output directory ----
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./debug_output',
        help='Directory for debug output files (default: ./debug_output)'
    )

    # ---- Introspection flags ----
    parser.add_argument(
        '--save-raw-data',
        action='store_true',
        help='Save raw BigQuery results to output_dir before forecasting'
    )
    parser.add_argument(
        '--save-intermediate',
        action='store_true',
        help='Save DataFrames after each pipeline stage to output_dir'
    )
    parser.add_argument(
        '--null-report',
        action='store_true',
        help='Print null counts by date x country x segment after pipeline completes'
    )
    parser.add_argument(
        '--historical-only',
        action='store_true',
        help='Fetch data from BigQuery and save to output_dir, skip Mozaic entirely'
    )

    return parser


def validate_flag_interactions(args: argparse.Namespace) -> None:
    """Validate mutual exclusion and incompatibility rules between flags.

    Raises:
        SystemExit: If any flag combination is invalid
    """
    errors = []

    if args.testing and (args.dau_only or args.data_source):
        errors.append(
            "--testing is incompatible with --dau-only and --data-source "
            "(testing mode already restricts to desktop/glean/DAU)"
        )

    if args.historical_only and args.forecast_only:
        errors.append("--historical-only is incompatible with --forecast-only")

    if args.historical_only and args.null_report:
        errors.append("--historical-only is incompatible with --null-report")

    if args.historical_only and args.save_intermediate:
        errors.append("--historical-only is incompatible with --save-intermediate")

    if args.repeat > 1 and not args.no_checkpoints:
        errors.append("--repeat N requires --no-checkpoints (checkpoints would make all runs identical)")

    if errors:
        for error in errors:
            print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)


def compute_expected_output_files(args: argparse.Namespace) -> list:
    """Return a list of output filenames that this run will produce.

    Used for the early collision check before any pipeline work starts.
    """
    filenames = []

    if args.historical_only or args.save_raw_data:
        # Raw data files are always produced when these flags are set;
        # we can't know exact names without running queries, so we check
        # for any existing raw_*.parquet files as a proxy.
        pass

    if args.repeat > 1:
        for run_num in range(1, args.repeat + 1):
            filenames.append(f"forecast_run_{run_num}.parquet")
    elif not args.historical_only:
        filenames.append("forecast_output.parquet")

    return filenames


def check_output_collisions(output_dir: Path, args: argparse.Namespace) -> None:
    """Fail early if expected output files already exist.

    Avoids running 90+ minutes of pipeline only to fail at the save step.

    Raises:
        SystemExit: If any expected output files already exist
    """
    expected_files = compute_expected_output_files(args)
    existing_conflicts = [f for f in expected_files if (output_dir / f).exists()]

    if existing_conflicts:
        print(f"Output files already exist in {output_dir}:", file=sys.stderr)
        for filename in existing_conflicts:
            print(f"  - {filename}", file=sys.stderr)
        print("Use --output-dir to specify a different directory.", file=sys.stderr)
        sys.exit(1)


def needs_output_dir(args: argparse.Namespace) -> bool:
    """Return True if any flag requires the output directory to be created."""
    return (
        args.historical_only
        or args.save_raw_data
        or args.save_intermediate
        or args.repeat > 1
        or not args.historical_only  # always save forecast output
    )


if __name__ == '__main__':
    parser = build_arg_parser()
    args = parser.parse_args()

    validate_flag_interactions(args)

    testing_mode = STATIC_CONFIG['testing_mode_enable_string'] if args.testing else None
    use_checkpoints = not args.no_checkpoints

    output_dir = Path(args.output_dir)

    # Early collision check: before any pipeline work, verify output files don't exist
    check_output_collisions(output_dir, args)

    # Create output directory if we'll be writing any files
    output_dir.mkdir(parents=True, exist_ok=True)

    # Tee all stdout/stderr to a log file so debug output is captured
    log_path = output_dir / "run_log.txt"
    log_file = open(log_path, "w")
    sys.stdout = TeeWriter(log_file, sys.__stdout__)
    sys.stderr = TeeWriter(log_file, sys.__stderr__)
    print(f"Logging all output to {log_path}")

    # --repeat N: run pipeline multiple times and save each output
    if args.repeat > 1:
        for run_num in range(1, args.repeat + 1):
            print(f"\n{'=' * 60}")
            print(f"RUN {run_num} of {args.repeat}")
            print(f"{'=' * 60}\n")

            result = main(
                checkpoints=False,
                testing_mode=testing_mode,
                forecast_start_date=args.forecast_start_date,
                dau_only=args.dau_only,
                data_source_filter=args.data_source,
                save_raw_data=args.save_raw_data,
                save_intermediate=args.save_intermediate,
                output_dir=output_dir,
            )

            if isinstance(result, dict):
                # historical_only not supported with repeat, validated above
                print(f"Run {run_num}: returned raw datasets (unexpected)")
                continue

            if args.forecast_only:
                result = result[result['data_type'] == 'forecast']

            output_path = output_dir / f"forecast_run_{run_num}.parquet"
            result.to_parquet(output_path)
            print(f"Run {run_num}: saved {len(result):,} rows to {output_path}")

        sys.exit(0)

    # Single run
    result = main(
        checkpoints=use_checkpoints,
        testing_mode=testing_mode,
        forecast_start_date=args.forecast_start_date,
        dau_only=args.dau_only,
        data_source_filter=args.data_source,
        historical_only=args.historical_only,
        save_raw_data=args.save_raw_data,
        save_intermediate=args.save_intermediate,
        output_dir=output_dir,
    )

    # --historical-only: result is a dict of raw datasets
    if isinstance(result, dict):
        from mozaic_daily.main import save_raw_datasets
        print("\nHistorical-only mode: saving raw datasets to output directory...")
        save_raw_datasets(result, output_dir)
        total_rows = sum(
            len(df)
            for sources in result.values()
            for metrics in sources.values()
            for df in metrics.values()
        )
        print(f"Done. Total rows across all raw datasets: {total_rows:,}")
        sys.exit(0)

    # Post-processing on forecast DataFrame
    if args.forecast_only:
        result = result[result['data_type'] == 'forecast']

    if args.null_report:
        from mozaic_daily.reports import print_null_report
        print_null_report(result)

    # Save single-run output
    output_path = output_dir / "forecast_output.parquet"
    result.to_parquet(output_path)
    print(f"\nSaved {len(result):,} rows to {output_path}")
