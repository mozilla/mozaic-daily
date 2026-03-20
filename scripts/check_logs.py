#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Check backfill log files for successes, failures, and ambiguous results.

Scans a directory of backfill log files and reports which dates completed
successfully, which failed, and which are ambiguous (exit code 0 but no
success marker).

Usage:
    # Check all logs in a directory
    python scripts/check_logs.py remote-logs/

    # Write failed dates to a file (one per line)
    python scripts/check_logs.py remote-logs/ -o /tmp/failed_dates.txt
"""

import argparse
import re
import sys
from pathlib import Path

FILENAME_PATTERN = re.compile(r"^backfill_(\d{4}-\d{2}-\d{2})(\.run(\d+))?\.log$")
EXIT_CODE_PATTERN = re.compile(r"^Exit code:\s*(-?\d+)")
SUCCESS_MARKER = "Done! See the run"


def parse_log_file(log_path):
    """Parse a backfill log file and return its status.

    Returns a dict with keys: date, exit_code, status, error_summary.
    Status is one of: "completed", "failed", "ambiguous".
    """
    filename_match = FILENAME_PATTERN.match(log_path.name)
    if not filename_match:
        return None

    date = filename_match.group(1)
    run_number = int(filename_match.group(3)) if filename_match.group(3) else 1
    lines = log_path.read_text().splitlines()

    # Parse exit code from line 3 (0-indexed: line index 2)
    exit_code = None
    for line in lines[:5]:
        code_match = EXIT_CODE_PATTERN.match(line)
        if code_match:
            exit_code = int(code_match.group(1))
            break

    if exit_code is None:
        return {
            "date": date,
            "run_number": run_number,
            "exit_code": None,
            "status": "ambiguous",
            "error_summary": "Could not parse exit code",
        }

    # Find last non-empty line for error summary
    last_nonempty = ""
    for line in reversed(lines):
        stripped = line.strip()
        if stripped:
            last_nonempty = stripped
            break

    if exit_code != 0:
        return {
            "date": date,
            "run_number": run_number,
            "exit_code": exit_code,
            "status": "failed",
            "error_summary": last_nonempty,
        }

    # Exit code 0 — check for success marker
    full_text = log_path.read_text()
    if SUCCESS_MARKER in full_text:
        return {
            "date": date,
            "run_number": run_number,
            "exit_code": 0,
            "status": "completed",
            "error_summary": None,
        }

    return {
        "date": date,
        "run_number": run_number,
        "exit_code": 0,
        "status": "ambiguous",
        "error_summary": "Exit code 0 but success marker not found",
    }


def main():
    parser = argparse.ArgumentParser(
        description="Check backfill log files for successes and failures."
    )
    parser.add_argument(
        "log_dir",
        type=Path,
        help="Directory containing backfill_YYYY-MM-DD.log files",
    )
    parser.add_argument(
        "--output-file", "-o",
        type=Path,
        default=None,
        help="Write failed dates to this file (one YYYY-MM-DD per line)",
    )
    args = parser.parse_args()

    if not args.log_dir.is_dir():
        print(f"Error: '{args.log_dir}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    log_files = sorted(args.log_dir.glob("backfill_*.log"))
    if not log_files:
        print(f"No backfill_*.log files found in '{args.log_dir}'.")
        sys.exit(0)

    # Parse all log files and keep only the latest run per date
    all_results = {}
    for log_path in log_files:
        result = parse_log_file(log_path)
        if result is None:
            continue
        date = result["date"]
        if date not in all_results or result["run_number"] > all_results[date]["run_number"]:
            all_results[date] = result

    # Group latest results by status
    completed = []
    failed = []
    ambiguous = []

    for result in sorted(all_results.values(), key=lambda r: r["date"]):
        if result["status"] == "completed":
            completed.append(result)
        elif result["status"] == "failed":
            failed.append(result)
        else:
            ambiguous.append(result)

    def format_run_annotation(run_number):
        """Return run annotation string, empty for first runs."""
        if run_number >= 2:
            return f" (run {run_number})"
        return ""

    # Print completed dates
    if completed:
        print(f"Completed ({len(completed)}):")
        for r in completed:
            annotation = format_run_annotation(r["run_number"])
            print(f"  {r['date']}{annotation}")

    # Print failed dates
    if failed:
        print(f"\nFailed ({len(failed)}):")
        for r in failed:
            annotation = format_run_annotation(r["run_number"])
            print(f"  {r['date']}{annotation}  exit={r['exit_code']}  {r['error_summary']}")

    # Print ambiguous dates
    if ambiguous:
        print(f"\nAmbiguous ({len(ambiguous)}):")
        for r in ambiguous:
            annotation = format_run_annotation(r["run_number"])
            print(f"  {r['date']}{annotation}  exit={r['exit_code']}  {r['error_summary']}")

    # Print totals
    total = len(completed) + len(failed) + len(ambiguous)
    print(f"\nTotal: {total} dates — {len(completed)} completed, {len(failed)} failed, {len(ambiguous)} ambiguous")

    # Write failed dates to file if requested
    if args.output_file and failed:
        args.output_file.write_text("\n".join(r["date"] for r in failed) + "\n")
        print(f"\nWrote {len(failed)} failed dates to {args.output_file}")
    elif args.output_file and not failed:
        print(f"\nNo failed dates to write.")


if __name__ == "__main__":
    main()
