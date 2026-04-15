#!/usr/bin/env python3
"""Run comparison forecasts across all three Iran-scenario branches.

Generates forecasts for each of the three comparison branches (april-forecasting,
world-with-fake-iran, world-without-iran) across a set of forecast dates, saving
results into a user-specified base directory that mirrors the structure of data/.

Steps performed:
  1. Regenerate iran_synthetic.parquet on the iran-synthetic-generation branch,
     saving it to <base_dir>/iran_synthetic/. The existing
     data/iran_synthetic/iran_synthetic.parquet is preserved.
  2. For each branch and each forecast date, run run_main.py and copy the
     checkpoint files into <base_dir>/comparisons/<folder>/<date>/.

The world-with-fake-iran runs use the newly generated iran_synthetic from
<base_dir>/iran_synthetic/ rather than the one in data/iran_synthetic/.

Usage:
    python scripts/run_comparison_forecasts.py data-48
    python scripts/run_comparison_forecasts.py data-48 --dates 2026-02-27 2026-03-28
    python scripts/run_comparison_forecasts.py data-48 --skip-iran-generation
    python scripts/run_comparison_forecasts.py data-48 --dry-run
    python scripts/run_comparison_forecasts.py data-48 --data-sources legacy_desktop glean_mobile --metrics DAU
"""

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent

# Branch name -> subfolder name under comparisons/
BRANCHES = [
    ("april-forecasting",    "april"),
    ("world-with-fake-iran", "fake-iran"),
    ("world-without-iran",   "no-iran"),
]

DEFAULT_DATES = ["2026-02-27", "2026-03-28"]

IRAN_SYNTHETIC_BRANCH = "iran-synthetic-generation"
IRAN_SYNTHETIC_REPO_PATH = REPO_ROOT / "data" / "iran_synthetic" / "iran_synthetic.parquet"


def run(cmd: list[str], dry_run: bool = False) -> None:
    """Run a shell command, printing it first. Exits on failure."""
    print(f"  $ {' '.join(str(c) for c in cmd)}")
    if dry_run:
        return
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        print(f"\nERROR: command failed with exit code {result.returncode}")
        sys.exit(result.returncode)


def git_checkout(branch: str, dry_run: bool = False) -> None:
    run(["git", "checkout", branch], dry_run=dry_run)


def create_directory_structure(base_dir: Path, folders: list[str], dates: list[str]) -> None:
    """Create all output directories under base_dir."""
    (base_dir / "iran_synthetic").mkdir(parents=True, exist_ok=True)
    for folder in folders:
        for date in dates:
            (base_dir / "comparisons" / folder / date).mkdir(parents=True, exist_ok=True)


def step_generate_iran_synthetic(base_dir: Path, dry_run: bool) -> None:
    """Run generate_iran_synthetic.py and save the result to base_dir/iran_synthetic/.

    Backs up the existing iran_synthetic.parquet before running, then restores
    it after, so the original file is not overwritten.
    """
    print("\n" + "=" * 60)
    print("Step 0: Regenerating iran_synthetic.parquet")
    print("=" * 60)

    destination = base_dir / "iran_synthetic" / "iran_synthetic.parquet"

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        backup_path = Path(tmp.name)

    print(f"  Backing up existing parquet to {backup_path}")
    if not dry_run:
        shutil.copy2(IRAN_SYNTHETIC_REPO_PATH, backup_path)

    try:
        git_checkout(IRAN_SYNTHETIC_BRANCH, dry_run=dry_run)
        run(
            [sys.executable, REPO_ROOT / "scripts" / "generate_iran_synthetic.py"],
            dry_run=dry_run,
        )
        print(f"  Copying new parquet to {destination}")
        if not dry_run:
            shutil.copy2(IRAN_SYNTHETIC_REPO_PATH, destination)
    finally:
        print(f"  Restoring original parquet from {backup_path}")
        if not dry_run:
            shutil.copy2(backup_path, IRAN_SYNTHETIC_REPO_PATH)
            backup_path.unlink()


def step_run_forecast(
    branch: str,
    folder: str,
    date: str,
    base_dir: Path,
    run_number: int,
    total_runs: int,
    data_sources: list[str],
    metrics: list[str],
    dry_run: bool,
) -> None:
    """Run a single forecast and copy output to base_dir/comparisons/folder/date/."""
    print(f"\n{'=' * 60}")
    print(f"Run {run_number}/{total_runs}: {branch}, {date}")
    print("=" * 60)

    destination = base_dir / "comparisons" / folder / date
    needs_iran_swap = branch == "world-with-fake-iran"
    iran_synthetic_source = base_dir / "iran_synthetic" / "iran_synthetic.parquet"

    with tempfile.TemporaryDirectory() as tmp_output_dir:
        output_dir = Path(tmp_output_dir)

        cmd = [
            sys.executable,
            REPO_ROOT / "scripts" / "run_main.py",
            "--forecast-start-date", date,
            "--output-dir", str(output_dir),
        ]
        if needs_iran_swap:
            print(f"  Using iran_synthetic from {iran_synthetic_source}")
            cmd += ["--iran-synthetic-path", str(iran_synthetic_source)]
        for source in data_sources:
            cmd += ["--data-sources", source]
        for metric in metrics:
            cmd += ["--metrics", metric]
        run(cmd, dry_run=dry_run)

        print(f"  Copying output files to {destination}")
        if not dry_run:
            for parquet_file in output_dir.glob("*.parquet"):
                shutil.copy2(parquet_file, destination / parquet_file.name)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run comparison forecasts across all Iran-scenario branches",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "base_dir",
        type=Path,
        help="Base output directory (e.g. data-48). Created if it does not exist.",
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        default=DEFAULT_DATES,
        metavar="YYYY-MM-DD",
        help=f"Forecast dates to run (default: {' '.join(DEFAULT_DATES)})",
    )
    parser.add_argument(
        "--skip-iran-generation",
        action="store_true",
        help=(
            "Skip the iran_synthetic generation step. "
            "Requires <base_dir>/iran_synthetic/iran_synthetic.parquet to already exist."
        ),
    )
    parser.add_argument(
        "--data-sources",
        nargs="+",
        default=[],
        metavar="SOURCE",
        help="Limit to specific data source(s): glean_desktop, legacy_desktop, glean_mobile. Defaults to all.",
    )
    parser.add_argument(
        "--metrics",
        nargs="+",
        default=[],
        metavar="METRIC",
        help='Limit to specific metric(s): DAU, "New Profiles", etc. Defaults to all.',
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print all commands without executing them.",
    )
    args = parser.parse_args()

    base_dir = args.base_dir
    if not base_dir.is_absolute():
        base_dir = REPO_ROOT / base_dir

    folders = [folder for _, folder in BRANCHES]
    dates = args.dates

    print(f"Base directory: {base_dir}")
    print(f"Forecast dates: {', '.join(dates)}")
    print(f"Branches: {', '.join(b for b, _ in BRANCHES)}")
    if args.data_sources:
        print(f"Data sources: {', '.join(args.data_sources)}")
    if args.metrics:
        print(f"Metrics: {', '.join(args.metrics)}")
    if args.dry_run:
        print("(DRY RUN — no commands will be executed)")

    create_directory_structure(base_dir, folders, dates)

    # Step 0: generate iran_synthetic
    if args.skip_iran_generation:
        iran_parquet = base_dir / "iran_synthetic" / "iran_synthetic.parquet"
        if not args.dry_run and not iran_parquet.exists():
            print(f"\nERROR: --skip-iran-generation specified but {iran_parquet} does not exist.")
            sys.exit(1)
        print(f"\nSkipping iran_synthetic generation (using existing {iran_parquet})")
    else:
        step_generate_iran_synthetic(base_dir, dry_run=args.dry_run)

    # Steps 1–N: one forecast per branch per date, sequentially
    total_runs = len(BRANCHES) * len(dates)
    run_number = 0

    for branch, folder in BRANCHES:
        git_checkout(branch, dry_run=args.dry_run)
        for date in dates:
            run_number += 1
            step_run_forecast(
                branch=branch,
                folder=folder,
                date=date,
                base_dir=base_dir,
                run_number=run_number,
                total_runs=total_runs,
                data_sources=args.data_sources,
                metrics=args.metrics,
                dry_run=args.dry_run,
            )

    print(f"\n{'=' * 60}")
    print(f"All {total_runs} runs complete. Output in {base_dir}/comparisons/")
    print("=" * 60)


if __name__ == "__main__":
    main()
