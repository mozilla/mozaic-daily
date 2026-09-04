#!/usr/bin/env python3
"""Ingest an external headwind/tailwind file into the forecast's adjustment system.

Two subcommands, run in order by the `/ingest-adjustment` skill (or by hand):

    inspect   read the delivered file, guess its columns, check the daily-DAU contract,
              and print the findings (JSON with --json). Writes nothing. Exit 2 when a
              finding is an error — a weekly file, a start after the seam, an end before
              31 December of the forecast year — so the halt is machine-readable.

    plot      re-render the shape plot (daily + 28d mean, measured/projected/held, seam and
              Dec-15) for an adjustment already on disk, into <name>/plots/.

    build     given the confirmed column mapping and decisions, write everything under
              data-official/<cycle>/<name>/: the source copy, the horizon curve parquet
              (+ csv twin + meta), the spec, an _index.md skeleton, the registry entry in
              adjustment_codes.yaml and the .gitignore exception. Never runs the model.

Examples
--------
    python scripts/ingest_adjustment.py inspect ~/Downloads/japan_bot.csv \\
        --forecast-start 2026-09-02 --platform desktop --json

    python scripts/ingest_adjustment.py build ~/Downloads/japan_bot.csv \\
        --name japan_bot --code j --family per_tile_overlay \\
        --platform desktop --data-source legacy_desktop \\
        --forecast-start 2026-09-02 --cycle 2026-09 \\
        --date-column submission_date --value-column dau --type-column type \\
        --allocation fixed_country_shares --shares '{"JP": 1.0}' --exclude IR \\
        --description "Japan automated desktop traffic since 2026-06-24, subtracted before training."

Families: `per_tile_overlay` (subtract from training, add back after; model re-run needed)
or `display_layer` (trailing 28d mean summed onto the published MA; no re-run).
Allocation: `trailing_dau_share` (proportional to each country's recent segment DAU) or
`fixed_country_shares` (localized; pass --shares).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from mozaic_daily.ingest_build import ALLOCATIONS, FAMILIES, IngestPlan, build, render_curve_plot  # noqa: E402
from mozaic_daily.ingest_inspect import inspect_file, read_source_table  # noqa: E402

EXIT_HALT = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    inspect = sub.add_parser("inspect", help="read + guess + check; writes nothing")
    inspect.add_argument("file", type=Path)
    inspect.add_argument("--forecast-start", required=True, help="the cycle's seam, YYYY-MM-DD")
    inspect.add_argument("--platform", choices=("desktop", "mobile"), default=None)
    inspect.add_argument("--sheet", default=None, help="Excel sheet name (default: first)")
    inspect.add_argument("--date-column", default=None, help="pin the date column instead of guessing")
    inspect.add_argument("--value-column", default=None)
    inspect.add_argument("--type-column", default=None)
    inspect.add_argument("--json", action="store_true", help="machine-readable output")

    b = sub.add_parser("build", help="write the curve, spec, meta, registry entry")
    b.add_argument("file", type=Path)
    b.add_argument("--name", required=True, help="snake_case adjustment name, e.g. japan_bot")
    b.add_argument("--code", required=True, help="single lowercase letter")
    b.add_argument("--family", required=True, choices=FAMILIES)
    b.add_argument("--platform", required=True, choices=("desktop", "mobile"))
    b.add_argument("--data-source", default="legacy_desktop", help="per_tile_overlay only")
    b.add_argument("--forecast-start", required=True)
    b.add_argument("--cycle", required=True, help="YYYY-MM")
    b.add_argument("--date-column", required=True)
    b.add_argument("--value-column", required=True)
    b.add_argument("--type-column", default=None)
    b.add_argument("--actuals-through", default=None, help="YYYY-MM-DD; required when there is no type column")
    b.add_argument("--ma-column", default=None, help="moving-average column, recorded as validated-not-used")
    b.add_argument("--sheet", default=None)
    b.add_argument("--sign", type=int, choices=(1, -1), default=1, help="-1 flips the delivered sign")
    b.add_argument("--allocation", choices=ALLOCATIONS, default="trailing_dau_share")
    b.add_argument("--shares", default=None, help='JSON dict for fixed_country_shares, e.g. \'{"JP": 1.0}\'')
    b.add_argument("--exclude", default="IR", help="comma-separated countries excluded from allocation (default IR; '' for none)")
    b.add_argument("--flag-column", default=None, help="segment flag column (default from data source)")
    b.add_argument("--description", default="", help="registry description")
    b.add_argument("--notes", default="", help="spec notes")
    b.add_argument("--replace", action="store_true", help="stash the live build for this code in a REVERT dir and overwrite")
    b.add_argument("--root", default=None, help=argparse.SUPPRESS)

    pl = sub.add_parser("plot", help="re-render the shape plot for an adjustment already on disk")
    pl.add_argument("--name", required=True)
    pl.add_argument("--code", required=True)
    pl.add_argument("--cycle", required=True)
    pl.add_argument("--family", required=True, choices=FAMILIES)
    pl.add_argument("--platform", required=True, choices=("desktop", "mobile"))
    pl.add_argument("--data-source", default="legacy_desktop")
    pl.add_argument("--forecast-start", required=True)
    return parser.parse_args()


def print_inspection(report) -> None:
    print(f"file: {report.source_path}" + (f"  sheet: {report.sheet}" if report.sheet else ""))
    print(f"rows: {report.n_rows}  columns: {report.columns}")
    for label, guess in (("date", report.date_column), ("value", report.value_column),
                         ("type", report.type_column), ("28d-MA", report.ma_column)):
        print(f"  {label:6s} -> {guess.column!s:20s} [{guess.confidence}] {guess.evidence}")
    print(f"cadence: {report.cadence}  range: {report.first_date} -> {report.last_date}  "
          f"actuals through: {report.actuals_through}  sign guess: {report.sign_guess}")
    for finding in report.findings:
        print(f"  {finding.level.upper():7s} {finding.code}: {finding.message}")
    print("HALT: fix the errors above before building" if report.halts else "ok: no blocking findings")


def run_inspect(args: argparse.Namespace) -> int:
    overrides = {k: v for k, v in (("date", args.date_column), ("value", args.value_column),
                                   ("type", args.type_column)) if v}
    report = inspect_file(args.file, forecast_start=args.forecast_start, sheet=args.sheet,
                          platform=args.platform, overrides=overrides)
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        print_inspection(report)
    return EXIT_HALT if report.halts else 0


def run_build(args: argparse.Namespace) -> int:
    plan = IngestPlan(
        source_path=str(args.file), name=args.name, code=args.code, family=args.family,
        platform=args.platform, data_source=args.data_source, forecast_start=args.forecast_start,
        cycle=args.cycle, date_column=args.date_column, value_column=args.value_column,
        type_column=args.type_column, actuals_through=args.actuals_through, ma_column=args.ma_column,
        sheet=args.sheet, sign=args.sign, allocation=args.allocation,
        shares=json.loads(args.shares) if args.shares else None,
        exclude_countries=[c.strip() for c in args.exclude.split(",") if c.strip()],
        flag_column=args.flag_column, description=args.description, notes=args.notes,
        replace=args.replace, root=args.root,
    )
    # Re-check the contract with the confirmed mapping; refuse to build past an error.
    report = inspect_file(args.file, forecast_start=args.forecast_start, sheet=args.sheet, platform=args.platform,
                          overrides={"date": args.date_column, "value": args.value_column,
                                     **({"type": args.type_column} if args.type_column else {})})
    if report.halts:
        print_inspection(report)
        return EXIT_HALT
    frame, _ = read_source_table(args.file, args.sheet)
    frame.columns = [str(c) for c in frame.columns]
    summary = build(plan, frame)
    print(json.dumps(summary, indent=2, default=str))
    return 0


def run_plot(args: argparse.Namespace) -> int:
    """Re-render the shape plot from the parquet the current spec points at."""
    import pandas as pd
    plan = IngestPlan(source_path="", name=args.name, code=args.code, family=args.family, platform=args.platform,
                      data_source=args.data_source, forecast_start=args.forecast_start, cycle=args.cycle,
                      date_column="", value_column="", actuals_through=args.forecast_start)
    spec = json.loads(plan.spec_path.read_text())
    parquet_path = (plan.spec_path.parent / spec["data_file"]).resolve()
    horizon = pd.read_parquet(parquet_path)
    horizon.index = pd.DatetimeIndex(horizon.index)
    dec15 = pd.Timestamp(year=pd.Timestamp(args.forecast_start).year, month=12, day=15)
    summary = {"dec15_ma28": float(horizon.loc[dec15, f"{plan.name}_dau_ma"]) if dec15 in horizon.index else float("nan")}
    stem = parquet_path.name.removesuffix(".parquet")
    out = render_curve_plot(horizon, plan, summary, plan.curve_dir / "plots" / f"{stem}.curve.png")
    print(out)
    return 0


def main() -> int:
    args = parse_args()
    if args.command == "inspect":
        return run_inspect(args)
    if args.command == "plot":
        return run_plot(args)
    return run_build(args)


if __name__ == "__main__":
    sys.exit(main())
