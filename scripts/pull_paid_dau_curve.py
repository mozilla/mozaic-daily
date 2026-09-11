#!/usr/bin/env python3
"""Run the marketing team's paid-DAU query and turn it into the daily curve `p` consumes. Import only.

    python scripts/pull_paid_dau_curve.py --sql ~/Downloads/gmio_widget.sql
    python scripts/pull_paid_dau_curve.py --sql ... --from-json <saved machine JSON>   # no BigQuery
    python scripts/pull_paid_dau_curve.py --sql ... --variant ci90lo   # a non-point-estimate twin, labelled in every file
    python scripts/pull_paid_dau_curve.py --from-xlsx ~/Downloads/scenarios.xlsx --sheet Scenarios \
        --date-column Weeks --actual-column "Actualized Total Paid DAU" --forecast-column "Low Forecast" \
        --fill-actual-from "result!uac_actual" --variant low --column-legend "point estimate minus 3.3% backtest error"

Steps (query): resolve the widget's `{{metric}}` / `{{country}}` params (defaults: Total Paid DAU / All) →
run the query read-only through bq_query.py → save the template SQL, the resolved SQL, the JSON
result and a CSV **verbatim** under `data-official/{cycle}/marketing/source_data/` → enforce the
four-column weekly contract → compose, interpolate to daily → write the level as delivered in the
pull-date-suffixed parquet + csv twin + meta + workbook + plot → leave `PENDING_WIRING.md`.

Steps (delivered workbook, `--from-xlsx`): copy the workbook byte for byte into `source_data/` → read one
scenario column of one sheet into the same weekly frame (`mozaic_daily.paid_curve_workbook`: a week's value
is the actual cell where present, else the chosen forecast cell; footer rows are dropped and reported) →
the same daily build and the same files. `--variant` is required on this path: a delivered scenario is
never the point estimate by default, and the slug is what keeps it from being mistaken for one.

Never touches organic.json, tests, the registry or the cycle `_index.md`: those belong to the wiring
step. Never overwrites an earlier pull. Logic lives in `mozaic_daily.paid_curve` (pure) and
`mozaic_daily.paid_curve_files` (writes).
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from mozaic_daily.paid_curve import (  # noqa: E402
    TEMPLATE_DEFAULTS, basis_slug, basis_with_variant, build_daily_table, check_contract, compose_weekly,
    feed_tables, interpolate_weekly_to_daily, key_values, resolve_template_params,
)
from mozaic_daily.paid_curve_files import (  # noqa: E402
    DeliveredFileProvenance, PullProvenance, write_curve_files, write_pending_note,
)
from mozaic_daily.paid_curve_workbook import ScenarioColumns, read_scenario_sheet  # noqa: E402

BQ_QUERY_TOOL = Path("/Users/brendanwells/work/product-data-science-core/scratch/brwells/tools/bq_query.py")
ROW_LIMIT = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--sql", type=Path, help="the widget query as delivered, with {{metric}} / {{country}} templates")
    source.add_argument("--from-xlsx", type=Path, help="a delivered workbook instead of a query; see the --sheet/--*-column flags")
    workbook = parser.add_argument_group("delivered workbook (--from-xlsx only)")
    workbook.add_argument("--sheet", default=None, help="sheet holding the weekly rows")
    workbook.add_argument("--date-column", default=None, help="column of ISO-Monday week dates")
    workbook.add_argument("--actual-column", default=None, help="column of actualized paid DAU")
    workbook.add_argument("--forecast-column", default=None, help="the ONE scenario column to import")
    workbook.add_argument("--fill-actual-from", default=None,
                          help="'sheet!column' used for weeks blank in both actual and forecast columns (default: such a week halts)")
    workbook.add_argument("--column-legend", default=None,
                          help="the producer's own words for the chosen column, copied into the meta (e.g. the sheet's legend)")
    parser.add_argument("--cycle", default=None, help="YYYY-MM (default: newest data-official/ directory)")
    parser.add_argument("--forecast-start", default=None, help="the cycle's seam (default: read from the cycle's organic.json)")
    parser.add_argument("--pull-date", default=date.today().isoformat(), help="YYYY-MM-DD used in every file name (default: today)")
    parser.add_argument("--metric", default=TEMPLATE_DEFAULTS["metric"], help="value for {{metric}}")
    parser.add_argument("--country", default=TEMPLATE_DEFAULTS["country"], help="value for {{country}}")
    parser.add_argument("--max-gb", type=float, default=10.0, help="bq_query.py billing guard")
    parser.add_argument("--from-json", type=Path, default=None, help="reuse a saved bq_query.py --machine result instead of querying")
    parser.add_argument("--out-dir", type=Path, default=None, help="default data-official/{cycle}/marketing")
    parser.add_argument("--variant", default=None,
                        help="slug ([a-z0-9]+) for a query that is not the point estimate, e.g. ci90lo for the lower end of "
                             "the 90%% credible interval; lands in every file name and the meta so the pull is never mistaken "
                             "for the point estimate")
    return parser.parse_args()


def newest_cycle(root: Path) -> str:
    cycles = sorted(p.name for p in (root / "data-official").iterdir() if re.fullmatch(r"\d{4}-\d{2}", p.name))
    if not cycles:
        raise SystemExit("no data-official/YYYY-MM directory found; pass --cycle")
    return cycles[-1]


def seam_from_organic_spec(cycle_dir: Path) -> str:
    spec = cycle_dir / "organic" / "organic.json"
    if not spec.exists():
        raise SystemExit(f"{spec} not found; pass --forecast-start explicitly")
    return json.loads(spec.read_text())["applies_to_forecast_start"]


def currently_wired_curve(cycle_dir: Path) -> str | None:
    spec = cycle_dir / "organic" / "organic.json"
    if not spec.exists():
        return None
    return json.loads(spec.read_text()).get("paid_forecast", {}).get("data_file")


def run_query(resolved_sql: Path, max_gb: float) -> dict:
    """bq_query.py is the only path to BigQuery: dry-run validated, SELECT-only, billing-capped."""
    command = [sys.executable, str(BQ_QUERY_TOOL), "--file", str(resolved_sql), "--machine",
               "--limit", str(ROW_LIMIT), "--max-gb", str(max_gb)]
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode != 0:
        raise SystemExit(f"bq_query.py failed (exit {completed.returncode}):\n{completed.stderr.strip()}")
    result = json.loads(completed.stdout)
    if result.get("status") != "ok":
        raise SystemExit(f"bq_query.py returned status {result.get('status')!r}: {result}")
    if result.get("truncated"):
        raise SystemExit(f"result truncated at {ROW_LIMIT} rows (total {result.get('total_rows')}); raise ROW_LIMIT")
    return result


def rows_to_frame(result: dict) -> pd.DataFrame:
    return pd.DataFrame(result["rows"], columns=result["columns"])


def refuse_existing(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            raise SystemExit(f"{path} exists; pulls are never overwritten. Pass another --pull-date.")


def weekly_from_query(args: argparse.Namespace, source_dir: Path, stamp: str) -> tuple[pd.DataFrame, pd.DataFrame, PullProvenance, str]:
    """Query path: save template + resolved SQL, JSON and CSV verbatim; return (weekly, raw rows, provenance, basis)."""
    params = {"metric": args.metric, "country": args.country}
    basis = basis_slug(args.metric)
    slug = f"gmio_paid_dau_{basis_with_variant(basis, args.variant)}_{args.country.lower()}"
    template_sql = source_dir / f"query_{slug}.template.{stamp}.sql"
    resolved_sql = source_dir / f"query_{slug}.{stamp}.sql"
    raw_json = source_dir / f"{slug}.{stamp}.json"
    raw_csv = source_dir / f"{slug}.{stamp}.csv"
    refuse_existing([template_sql, resolved_sql, raw_json, raw_csv])

    template_text = args.sql.read_text()
    shutil.copyfile(args.sql, template_sql)
    resolved_text = resolve_template_params(template_text, params)
    header = f"-- Resolved by scripts/pull_paid_dau_curve.py on {args.pull_date}: {json.dumps(params)}\n"
    resolved_sql.write_text(header + resolved_text)

    if args.from_json:
        result = json.loads(args.from_json.read_text())
        print(f"Reusing saved result {args.from_json} ({result.get('row_count')} rows); BigQuery not queried")
    else:
        print(f"Querying BigQuery via bq_query.py ({', '.join(feed_tables(resolved_text))}) ...")
        result = run_query(resolved_sql, args.max_gb)
    raw_json.write_text(json.dumps(result, indent=2, default=str) + "\n")
    raw = rows_to_frame(result)
    raw.to_csv(raw_csv, index=False)
    print(f"Saved {len(raw)} rows verbatim: {raw_csv.relative_to(REPO)} (+ .json, template + resolved SQL)")

    weekly = compose_weekly(check_contract(raw))
    provenance = PullProvenance(template_sql=template_sql, resolved_sql=resolved_sql, raw_json=raw_json, raw_csv=raw_csv,
                                feed_tables=feed_tables(resolved_text), template_params=params,
                                pull_date=args.pull_date, gb_processed=float(result.get("gb_processed", 0.0)),
                                variant=args.variant)
    return weekly, raw, provenance, basis


def weekly_from_workbook(args: argparse.Namespace, source_dir: Path, stamp: str) -> tuple[pd.DataFrame, pd.DataFrame, DeliveredFileProvenance, str]:
    """Delivered-workbook path: copy the file verbatim, read one scenario column; return (weekly, sheet, provenance, basis)."""
    required = {"--sheet": args.sheet, "--date-column": args.date_column, "--actual-column": args.actual_column,
                "--forecast-column": args.forecast_column, "--column-legend": args.column_legend, "--variant": args.variant}
    missing = [flag for flag, value in required.items() if value is None]
    if missing:
        raise SystemExit(f"--from-xlsx needs {', '.join(missing)}; a delivered scenario is never imported without naming "
                         "its column, what the producer calls it, and a variant slug")
    file_slug = re.sub(r"[^a-z0-9]+", "_", args.from_xlsx.stem.lower()).strip("_")
    delivered_copy = source_dir / f"delivered.{file_slug}.{stamp}{args.from_xlsx.suffix.lower()}"
    refuse_existing([delivered_copy])
    shutil.copyfile(args.from_xlsx, delivered_copy)
    print(f"Copied the workbook verbatim: {delivered_copy.relative_to(REPO)}")

    columns = ScenarioColumns(sheet=args.sheet, date=args.date_column, actual=args.actual_column,
                              forecast=args.forecast_column, fill_actual_from=args.fill_actual_from)
    read = read_scenario_sheet(delivered_copy, columns)
    if read.dropped_row_labels:
        print(f"Dropped {len(read.dropped_row_labels)} non-date row(s) from sheet {args.sheet!r}: {read.dropped_row_labels}")
    if read.filled_weeks:
        print(f"Filled {len(read.filled_weeks)} week(s) blank in both columns from {args.fill_actual_from}: {read.filled_weeks}")
    sheet = pd.read_excel(delivered_copy, sheet_name=args.sheet)
    provenance = DeliveredFileProvenance(
        delivered_copy=delivered_copy, original_path=str(args.from_xlsx), sheet=args.sheet,
        date_column=args.date_column, actual_column=args.actual_column, forecast_column=args.forecast_column,
        fill_actual_from=args.fill_actual_from, dropped_row_labels=read.dropped_row_labels, filled_weeks=read.filled_weeks,
        column_legend=args.column_legend, pull_date=args.pull_date, variant=args.variant)
    return read.weekly, sheet, provenance, basis_slug(args.metric)


def main() -> None:
    args = parse_args()
    cycle = args.cycle or newest_cycle(REPO)
    cycle_dir = REPO / "data-official" / cycle
    forecast_start = args.forecast_start or seam_from_organic_spec(cycle_dir)
    out_dir = args.out_dir or cycle_dir / "marketing"
    source_dir = out_dir / "source_data"
    source_dir.mkdir(parents=True, exist_ok=True)
    stamp = args.pull_date.replace("-", "")

    if args.from_xlsx:
        weekly, raw, provenance, basis = weekly_from_workbook(args, source_dir, stamp)
    else:
        weekly, raw, provenance, basis = weekly_from_query(args, source_dir, stamp)

    daily_end = pd.Timestamp(year=pd.Timestamp(forecast_start).year, month=12, day=31)
    level = interpolate_weekly_to_daily(weekly, daily_end)
    daily = build_daily_table(level)
    values = key_values(level, forecast_start)

    paths = write_curve_files(out_dir, basis, forecast_start, provenance, weekly, daily, values, REPO, raw_frame=raw)
    note = write_pending_note(out_dir, paths, values, provenance, forecast_start, currently_wired_curve(cycle_dir), REPO)

    last_actual = weekly.loc[weekly["is_actual"], "date"].max().date()
    print(f"Wrote {', '.join(p.name for p in paths.values())} and {note.name}")
    print(f"  weeks={len(weekly)} ({weekly['date'].min().date()} -> {weekly['date'].max().date()}), actuals through week of {last_actual}, "
          f"basis counts {weekly['basis'].value_counts().to_dict()}")
    for label in ("level_at_seam", "level_dec15", "level_year_end"):
        print(f"  {label:>16}: {values[label]:>12,.0f}" if values[label] is not None else f"  {label:>16}: n/a")
    print(f"  plot: {paths['plot'].relative_to(REPO)}")
    print("Not wired. See PENDING_WIRING.md for the hand-off; organic.json was not touched.")


if __name__ == "__main__":
    main()
