#!/usr/bin/env python3
"""Fetch and checkpoint ONE data source's raw BigQuery pull, without forecasting.

Why this exists: the mobile paid/organic split producer
(``scripts/build_fenix_organic_split.py``) needs the cycle's
``mozaic_parts.raw.glean.mobile.DAU.parquet`` for its shredder-drift check, but
``run_mobile_param_scan.py`` refuses to run until a paid spec is gated to the cycle date — and
gating that spec requires the split to already exist. This breaks the cycle by fetching just the
raw pull.

**The raw pull is model-config independent** — it is the BigQuery query result, taken before any
model or overlay touches it. So the artifact this writes is byte-identical to the one a tuned scan
would have written at the same ``--forecast-start-date``, and a later scan can consume it via
``--raw-cache-dir`` with no re-query.

Usage:
    python scripts/fetch_raw_pull.py --forecast-start-date 2026-08-02 \
        --data-source glean_mobile --metric DAU \
        --output-dir data-official/2026-08/mobile_rawpull_2026-08-02
"""

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from mozaic_daily.config import STATIC_CONFIG, get_runtime_config  # noqa: E402
from mozaic_daily.data import (  # noqa: E402
    check_training_data_availability,
    get_aggregate_data,
    get_queries,
)

# get_queries() returns {platform: {telemetry_source: {metric: (sql, spec)}}}; the CLI's
# --data-source names are the pipeline's flat DataSource labels, so map between the two.
DATA_SOURCE_TO_KEYS = {
    "glean_desktop": ("desktop", "glean"),
    "legacy_desktop": ("desktop", "legacy"),
    "glean_mobile": ("mobile", "glean"),
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--forecast-start-date", required=True,
                        help="Cycle forecast start (T-0), e.g. 2026-08-02. training_end is T-1.")
    parser.add_argument("--data-source", required=True, choices=sorted(DATA_SOURCE_TO_KEYS),
                        help="Which data source to pull.")
    parser.add_argument("--metric", default="DAU",
                        help="Metric to pull (default: DAU).")
    parser.add_argument("--output-dir", required=True,
                        help="Directory to write the mozaic_parts.raw.* checkpoint into.")
    parser.add_argument("--project", default=None,
                        help="BigQuery project (default: STATIC_CONFIG['default_project']).")
    parser.add_argument("--skip-preflight", action="store_true",
                        help="Skip the training-data availability check.")
    return parser.parse_args()


def select_single_query(all_queries, data_source, metric):
    """Narrow get_queries() output to exactly one platform/source/metric, preserving its shape."""
    platform, telemetry_source = DATA_SOURCE_TO_KEYS[data_source]
    available = all_queries.get(platform, {}).get(telemetry_source, {})
    if metric not in available:
        raise SystemExit(
            f"metric {metric!r} not available for {data_source}. "
            f"Available: {sorted(available)}"
        )
    return {platform: {telemetry_source: {metric: available[metric]}}}


def main():
    args = parse_args()
    project = args.project or STATIC_CONFIG["default_project"]

    config = get_runtime_config(forecast_start_date_override=args.forecast_start_date)

    print(f"forecast_start : {config['forecast_start_date']}")
    print(f"training_end   : {config['training_end_date']}")
    print(f"data_source    : {args.data_source}  metric: {args.metric}")
    print(f"output_dir     : {args.output_dir}")

    if not args.skip_preflight:
        check_training_data_availability(project, config["training_end_date"])

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    all_queries = get_queries(config["country_string"])
    queries = select_single_query(all_queries, args.data_source, args.metric)

    get_aggregate_data(
        queries,
        project=project,
        checkpoints=True,
        clean=True,
        output_dir=args.output_dir,
    )

    written = sorted(Path(args.output_dir).glob("mozaic_parts.raw.*.parquet"))
    print("\nWrote:")
    for path in written:
        print(f"  {path}  ({path.stat().st_size:,} bytes)")
    if not written:
        raise SystemExit("no raw checkpoint was written — check the query output above")


if __name__ == "__main__":
    main()
