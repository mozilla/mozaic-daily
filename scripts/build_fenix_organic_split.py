#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Produce a cycle's measured Fenix paid/organic split artifact.

This is the producer for the pinned input that adjustment ``p`` (`paid_organic_split`) consumes:
``data-official/{YYYY-MM}/organic/fenix_paid_organic.{forecast_start}.parquet``.

Why a pinned artifact rather than a live query: the organic share comes from
``moz-fx-data-bq-data-science.brwells.fenix_dau_growth_source_v1``, a scratch mirror that
**expires 2027-04-01** and stops at its own build date (2026-07-01), so every cycle needs a tail
extension anyway. Pinning makes the daily production run independent of both facts, and gives the
cycle a SHA'd, reviewable input the way `marketing/` already does.

Run once per cycle, then commit the parquet's sidecar and the spec.

Usage:
    python scripts/build_fenix_organic_split.py \\
        --forecast-start-date 2026-07-28 \\
        --production-raw data-official/2026-08/mobile_uac_meta_2026-07-28/<slug>/mozaic_parts.raw.glean.mobile.DAU.parquet

    # See what it would scan and cost, without running anything:
    python scripts/build_fenix_organic_split.py --forecast-start-date 2026-07-28 --dry-run
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

repo_root = Path(__file__).parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from mozaic_daily.adjustments import write_meta
from mozaic_daily.config import STATIC_CONFIG, get_runtime_config
from mozaic_daily.organic_source import (
    build_split_frame,
    check_partition_identity,
    check_shredder_drift,
    check_split_coverage,
    check_tail_overlap,
    combine_snapshot_and_tail,
    production_fenix_daily,
)

SQL_DIR = Path(__file__).parent / "sql"

MIRROR_TABLE = "moz-fx-data-bq-data-science.brwells.fenix_dau_growth_source_v1"
FENIX_ACTIVE_USERS = "mozdata.fenix.active_users"
FENIX_NEW_PROFILE_CLIENTS = "mozdata.fenix.new_profile_clients"

#: Days the tail deliberately re-covers so `check_tail_overlap` has something to compare.
DEFAULT_OVERLAP_DAYS = 7

#: The tail joins client-level active_users against new_profile_clients, which is the expensive
#: half. ~26 days costs well under this; the cap exists so a mis-specified date range fails at
#: dry-run instead of billing for a full-history scan.
DEFAULT_MAX_GB = 500.0

#: BigQuery on-demand list price, for the cost line only.
_USD_PER_TB = 5.0


def _render(filename: str, **params) -> str:
    return (SQL_DIR / filename).read_text().format(**params)


def _dry_run_gb(client: bigquery.Client, sql: str) -> float:
    job = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    return job.total_bytes_processed / 1e9


def _run(client: bigquery.Client, sql: str, label: str, max_gb: float, dry_run: bool) -> pd.DataFrame:
    """Dry-run for cost, refuse above the cap, then execute."""
    gb = _dry_run_gb(client, sql)
    print(f"  [{label}] {gb:,.2f} GB (~${gb / 1000 * _USD_PER_TB:,.2f})")
    if gb > max_gb:
        raise SystemExit(
            f"{label}: estimated {gb:,.1f} GB exceeds --max-gb {max_gb:,.0f}. "
            f"Narrow the date range or raise the cap deliberately."
        )
    if dry_run:
        return pd.DataFrame()
    return client.query(sql).to_dataframe()


def _cycle_dir(forecast_start_date: str) -> Path:
    """``data-official/{YYYY-MM}/organic`` for the cycle that owns this forecast start.

    The cycle month is *not* the forecast start's month — the August 2026 cycle runs at
    forecast_start 2026-07-28 — so it cannot be derived from the date. Instead, find the cycle
    that already has a spec claiming this forecast start, which is the same
    ``applies_to_forecast_start`` exact-match rule ``main.py`` uses to gate the overlays.
    """
    import json

    claimants = set()
    for spec_path in (repo_root / "data-official").glob("*/*/*.json"):
        try:
            spec = json.loads(spec_path.read_text())
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if isinstance(spec, dict) and spec.get("applies_to_forecast_start") == forecast_start_date:
            claimants.add(spec_path.parents[1])

    if len(claimants) == 1:
        return claimants.pop() / "organic"
    raise SystemExit(
        f"cannot infer the cycle directory for forecast_start {forecast_start_date}: "
        f"{len(claimants)} cycle(s) have a spec claiming it ({sorted(str(c.name) for c in claimants)}). "
        f"Pass --out-dir explicitly."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--forecast-start-date", required=True,
                        help="Cycle's forecast start (T-0), e.g. 2026-07-28. training_end is T-1.")
    parser.add_argument("--production-raw", type=Path, default=None,
                        help="Path to the cycle's mozaic_parts.raw.glean.mobile.DAU.parquet. Used "
                             "for the shredder-drift check against the level source the split "
                             "will actually multiply against. Strongly recommended.")
    parser.add_argument("--out-dir", type=Path, default=None,
                        help="Default: data-official/{YYYY-MM}/organic/")
    parser.add_argument("--project", default=STATIC_CONFIG["default_project"])
    parser.add_argument("--overlap-days", type=int, default=DEFAULT_OVERLAP_DAYS)
    parser.add_argument("--max-gb", type=float, default=DEFAULT_MAX_GB)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print scan sizes and stop. Writes nothing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime = get_runtime_config(args.forecast_start_date)
    training_end = runtime["training_end_date"]
    countries = runtime["country_string"]
    out_dir = args.out_dir or _cycle_dir(args.forecast_start_date)

    print(f"Fenix paid/organic split — forecast_start {args.forecast_start_date}, "
          f"training_end {training_end}")
    print(f"  mirror:   {MIRROR_TABLE}")
    print(f"  out:      {out_dir}")

    client = bigquery.Client(project=args.project)

    print("\n--- Query")
    snapshot = _run(
        client,
        _render("fenix_growth_source_mirror.sql",
                mirror_table=MIRROR_TABLE, countries=countries, end=training_end),
        "mirror snapshot", args.max_gb, args.dry_run,
    )

    if args.dry_run:
        # The tail range depends on the snapshot's max date, which we do not have on a dry run.
        # Estimate against the widest plausible window so the number is an upper bound.
        tail_start = (pd.Timestamp(training_end) - pd.Timedelta(days=60)).strftime("%Y-%m-%d")
    else:
        mirror_max = pd.to_datetime(snapshot["submission_date"]).max()
        tail_start = (mirror_max - pd.Timedelta(days=args.overlap_days - 1)).strftime("%Y-%m-%d")
        print(f"  snapshot covers through {mirror_max.date()}; "
              f"tail {tail_start} → {training_end}")

    tail = _run(
        client,
        _render("fenix_growth_source_tail.sql",
                npc_table=FENIX_NEW_PROFILE_CLIENTS, au_table=FENIX_ACTIVE_USERS,
                countries=countries, start=tail_start, end=training_end),
        "mirror tail", args.max_gb, args.dry_run,
    )

    if args.dry_run:
        print("\nDry run — nothing written.")
        return 0

    print("\n--- Checks")
    checks = [
        check_tail_overlap(snapshot, tail),
        check_partition_identity(combine_snapshot_and_tail(snapshot, tail)),
    ]

    mirror = combine_snapshot_and_tail(snapshot, tail)
    split = build_split_frame(mirror)
    checks.append(check_split_coverage(
        split,
        expected_countries=runtime["countries"] | {"ROW"},
        training_end=pd.Timestamp(training_end),
    ))

    if args.production_raw is not None:
        import db_dtypes  # noqa: F401  — registers the dbdate extension type for read_parquet
        production_daily = production_fenix_daily(pd.read_parquet(args.production_raw))
        checks.append(check_shredder_drift(split, production_daily))
    else:
        print("  ! no --production-raw: skipping the shredder-drift check. That check is the "
              "only thing that catches the mirror and the production table covering different "
              "Fenix populations. Pass it.")

    for frame in checks:
        print(frame.to_string(index=False))

    print("\n--- Write")
    out_dir.mkdir(parents=True, exist_ok=True)
    artifact = out_dir / f"fenix_paid_organic.{args.forecast_start_date}.parquet"
    split.to_parquet(artifact, index=False)

    share_head = split.loc[split["submission_date"] == split["submission_date"].min()]
    share_tail = split.loc[split["submission_date"] == split["submission_date"].max()]
    write_meta(
        artifact,
        forecast_start_date=args.forecast_start_date,
        data_source="glean_mobile",
        produced_by="scripts/build_fenix_organic_split.py",
        model_config=None,
        adjustments_applied=[],
        extra={
            "artifact_type": "measured_paid_organic_split",
            "definition": (
                "Fenix client is paid iff mozdata.fenix.new_profile_clients has "
                "paid_vs_organic_gclid='Paid' AND normalized_channel='release' AND "
                "install_source='com.android.vending'. Organic is the residual. All channels."
            ),
            "sources": {
                "mirror_table": MIRROR_TABLE,
                "mirror_expires": "2027-04-01",
                "tail_active_users": FENIX_ACTIVE_USERS,
                "tail_new_profile_clients": FENIX_NEW_PROFILE_CLIENTS,
            },
            "coverage": {
                "measured_from": str(split["submission_date"].min().date()),
                "measured_to": str(split["submission_date"].max().date()),
                "training_end_date": training_end,
                "countries": int(split["country"].nunique()),
                "rows": int(len(split)),
            },
            "key_values": {
                "organic_share_first_day": float(
                    share_head["organic_dau"].sum() / share_head["total_dau"].sum()),
                "organic_share_last_day": float(
                    share_tail["organic_dau"].sum() / share_tail["total_dau"].sum()),
            },
            "checks": [frame.to_dict("records")[0] for frame in checks],
            "known_limitations": (
                "Measured coverage starts at the mirror's own start date (2024-06-01) because "
                "mozdata.fenix.active_users retains only a rolling ~25 months. Mobile DAU trains "
                "from 2020-12-31, so the applier holds the earliest measured per-country share "
                "flat backwards over the uncovered region. At the oldest measured month paid was "
                "1.10% of Fenix DAU ex-IR, so that assumption is bounded at ~1.1pp."
            ),
        },
    )
    print(f"  wrote {artifact}")
    print(f"  wrote {artifact}.meta.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
