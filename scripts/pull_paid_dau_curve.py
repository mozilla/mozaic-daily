#!/usr/bin/env python3
"""Run the marketing team's paid-DAU query and turn it into the daily curve `p` consumes. Import only.

    python scripts/pull_paid_dau_curve.py --sql ~/Downloads/gmio_widget.sql
    python scripts/pull_paid_dau_curve.py --sql ... --from-json <saved machine JSON>   # no BigQuery
    python scripts/pull_paid_dau_curve.py --sql ... --variant ci90lo   # a non-point-estimate twin, labelled in every file

Steps: resolve the widget's `{{metric}}` / `{{country}}` params (defaults: Total Paid DAU / All) →
run the query read-only through bq_query.py → save the template SQL, the resolved SQL, the JSON
result and a CSV **verbatim** under `data-official/{cycle}/marketing/source_data/` → enforce the
four-column weekly contract → compose, interpolate to daily → write the level as delivered in the
pull-date-suffixed parquet + csv twin + meta + workbook + plot → leave `PENDING_WIRING.md`.

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
from mozaic_daily.paid_curve_files import PullProvenance, write_curve_files, write_pending_note  # noqa: E402

BQ_QUERY_TOOL = Path("/Users/brendanwells/work/product-data-science-core/scratch/brwells/tools/bq_query.py")
ROW_LIMIT = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sql", type=Path, required=True, help="the widget query as delivered, with {{metric}} / {{country}} templates")
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


def main() -> None:
    args = parse_args()
    cycle = args.cycle or newest_cycle(REPO)
    cycle_dir = REPO / "data-official" / cycle
    forecast_start = args.forecast_start or seam_from_organic_spec(cycle_dir)
    out_dir = args.out_dir or cycle_dir / "marketing"
    source_dir = out_dir / "source_data"
    source_dir.mkdir(parents=True, exist_ok=True)

    params = {"metric": args.metric, "country": args.country}
    basis = basis_slug(args.metric)
    slug = f"gmio_paid_dau_{basis_with_variant(basis, args.variant)}_{args.country.lower()}"
    stamp = args.pull_date.replace("-", "")
    template_sql = source_dir / f"query_{slug}.template.{stamp}.sql"
    resolved_sql = source_dir / f"query_{slug}.{stamp}.sql"
    raw_json = source_dir / f"{slug}.{stamp}.json"
    raw_csv = source_dir / f"{slug}.{stamp}.csv"
    for path in (template_sql, resolved_sql, raw_json, raw_csv):
        if path.exists():
            raise SystemExit(f"{path} exists; pulls are never overwritten. Pass another --pull-date.")

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

    frame = check_contract(raw)
    weekly = compose_weekly(frame)
    daily_end = pd.Timestamp(year=pd.Timestamp(forecast_start).year, month=12, day=31)
    level = interpolate_weekly_to_daily(weekly, daily_end)
    daily = build_daily_table(level)
    values = key_values(level, forecast_start)

    provenance = PullProvenance(template_sql=template_sql, resolved_sql=resolved_sql, raw_json=raw_json, raw_csv=raw_csv,
                                feed_tables=feed_tables(resolved_text), template_params=params,
                                pull_date=args.pull_date, gb_processed=float(result.get("gb_processed", 0.0)),
                                variant=args.variant)
    paths = write_curve_files(out_dir, basis, forecast_start, provenance, weekly, daily, values, REPO)
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
