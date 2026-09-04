#!/usr/bin/env python3
"""Verify a forecast parquet's `training` rows equal raw BigQuery actuals.

Motivation: the canonical notebooks plot a prior-year reference line. Reading that year from the
`training` rows already loaded from the forecast parquet is free; re-querying BigQuery costs ~$5 of
scan per notebook run (`telemetry.active_users_aggregates` is ~44 GB/month). That substitution is
only sound if the parquet's training rows for the reference year are untouched actuals.

They should be. Every training-row modification in the pipeline is confined to 2026:
  - Iran counterfactual fill  — 2026-03-01 .. 2026-05-25
  - launch at login for new users (`l`)     — feature launched 2026-05-08
  - MozillaOnline (`o`)       — 2026 migration
  - marketing lift (`m`)      — campaign launched 2026-04-06
This script asserts that rather than trusting it, by sampling date windows and diffing day-for-day
against the exact query definitions the notebooks' [bq-actuals] cell uses.

Sampling, not full-year, is deliberate: the failure modes this guards against (wrong table, wrong
app_name filter, incomplete country coverage, a year-wide fill) all show up in any window, and a
full year costs ~25x more to scan. Two windows in opposite halves of the year catch a modification
confined to one part of the year.

Exit code 0 = exact match on every probed day (safe to substitute); 1 = mismatch (do not).

Run:  source .venv/bin/activate && python scripts/verify_training_rows_are_actuals.py
      ... --year 2025 --windows 2025-03-01:2025-03-31 2025-09-01:2025-09-30
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

REPO = Path(__file__).resolve().parents[1]

BQ_PROJECT = "moz-fx-data-bq-data-science"

# Default targets: the August 2026 canonical notebook's two parquets. Each entry pins the
# World(ALL) row selector alongside the BQ query that must reproduce it.
DEFAULT_TARGETS = {
    "desktop": {
        "parquet": (
            "data-official/2026-08/desktop_locked/"
            "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet"
        ),
        "data_source": "legacy_desktop",
        "segment": '{"os": "ALL"}',
        "app_name": "desktop",
        # Mirrors the notebook [bq-actuals] desktop query: legacy telemetry, all countries.
        "table": "moz-fx-data-shared-prod.telemetry.active_users_aggregates",
        "app_filter": 'app_name = "Firefox Desktop"',
    },
    "mobile": {
        "parquet": (
            "data-official/2026-08/mobile_baseline_2026-07-28/"
            "cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/"
            "mozaic_daily_forecast.2026-07-28.gm-D.adj-m.parquet"
        ),
        "data_source": "glean_mobile",
        "segment": "{}",
        "app_name": "ALL MOBILE",
        # Mirrors the notebook [bq-actuals] mobile query: glean telemetry, four apps.
        "table": "moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates",
        "app_filter": (
            'app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS")'
        ),
    },
}

DEFAULT_WINDOWS = [("2025-03-01", "2025-03-31"), ("2025-09-01", "2025-09-30")]


def build_actuals_query(table: str, app_filter: str,
                        windows: list[tuple[str, str]]) -> str:
    """SQL summing daily DAU over the probe windows, matching the notebook's definition."""
    window_clauses = " OR ".join(
        f"submission_date BETWEEN '{start}' AND '{end}'" for start, end in windows
    )
    return f"""
        SELECT submission_date AS date, SUM(dau) AS dau
        FROM `{table}`
        WHERE {app_filter}
          AND ({window_clauses})
        GROUP BY submission_date
        ORDER BY 1
    """


def fetch_bq_actuals(client: bigquery.Client, table: str, app_filter: str,
                     windows: list[tuple[str, str]]) -> pd.Series:
    """Daily actual DAU from BigQuery, date-indexed."""
    sql = build_actuals_query(table, app_filter, windows)
    df = client.query(sql).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")["dau"].astype("float64").sort_index()


def load_training_rows(parquet_path: Path, data_source: str, segment: str,
                       app_name: str) -> pd.Series:
    """World(ALL) daily DAU `training` rows from a forecast parquet, date-indexed."""
    df = pd.read_parquet(parquet_path)
    mask = (
        (df["country"] == "ALL")
        & (df["segment"] == segment)
        & (df["data_source"] == data_source)
        & (df["app_name"] == app_name)
        & (df["data_type"] == "training")
    )
    rows = df.loc[mask, ["target_date", "dau"]].copy()
    if rows.empty:
        raise ValueError(
            f"No training rows matched in {parquet_path.name}: "
            f"data_source={data_source} segment={segment} app_name={app_name}"
        )
    rows["target_date"] = pd.to_datetime(rows["target_date"])
    return rows.set_index("target_date")["dau"].astype("float64").sort_index()


def compare(name: str, training: pd.Series, actuals: pd.Series) -> str | None:
    """Print a day-for-day comparison. Returns a failure summary, or None on exact match."""
    # An empty probe must NOT pass. Without this guard a window with no BQ rows (a future date, a
    # typo'd year) compares zero days, reports "0/0 exact" and exits 0 -- reading as verification
    # when nothing was verified.
    if actuals.empty:
        print(f"== {name} ==")
        print("  FAIL: BigQuery returned no rows for the probe windows — nothing was compared.\n")
        return f"{name}: probe returned 0 rows (window out of range for the source table?)"

    absent = actuals.index.difference(training.index)
    if len(absent):
        print(f"== {name} ==")
        print(f"  FAIL: {len(absent)} probed dates absent from training rows "
              f"(first: {absent.min().date()})\n")
        return f"{name}: {len(absent)} probed dates missing from training rows"

    aligned = training.reindex(actuals.index)
    diff = aligned - actuals
    max_abs = diff.abs().max()

    print(f"== {name} ==")
    print(f"  days compared      : {len(actuals)}")
    print(f"  exact matches      : {int((diff == 0).sum())}/{len(actuals)}")
    print(f"  max abs difference : {max_abs:,.0f} DAU")

    if max_abs == 0:
        print()
        return None

    print("  largest mismatches:")
    for date in diff.abs().sort_values(ascending=False).head(5).index:
        print(f"    {date.date()}  training {aligned[date]:,.0f}  "
              f"actual {actuals[date]:,.0f}  delta {diff[date]:+,.0f}")
    print()
    return f"{name}: max abs diff {max_abs:,.0f} DAU over {len(actuals)} days"


def parse_window(raw: str) -> tuple[str, str]:
    """Parse a 'START:END' window argument."""
    try:
        start, end = raw.split(":")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"window must be 'YYYY-MM-DD:YYYY-MM-DD', got {raw!r}"
        )
    return start, end


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--windows", nargs="+", type=parse_window, default=DEFAULT_WINDOWS,
                        metavar="START:END",
                        help="Date windows to probe (default: March + September 2025)")
    parser.add_argument("--platform", choices=sorted(DEFAULT_TARGETS), action="append",
                        help="Limit to one platform (repeatable; default: both)")
    args = parser.parse_args()

    platforms = args.platform or sorted(DEFAULT_TARGETS)
    windows = args.windows
    probe_days = sum(
        (pd.Timestamp(end) - pd.Timestamp(start)).days + 1 for start, end in windows
    )
    print(f"Probing {probe_days} days across {len(windows)} window(s): "
          + ", ".join(f"{s}..{e}" for s, e in windows))
    print(f"Platforms: {', '.join(platforms)}\n")

    client = bigquery.Client(project=BQ_PROJECT)

    failures = []
    for name in platforms:
        target = DEFAULT_TARGETS[name]
        training = load_training_rows(
            REPO / target["parquet"], target["data_source"],
            target["segment"], target["app_name"],
        )
        actuals = fetch_bq_actuals(
            client, target["table"], target["app_filter"], windows,
        )
        failure = compare(name, training, actuals)
        if failure:
            failures.append(failure)

    if failures:
        print("VERDICT: MISMATCH — training rows are NOT a safe substitute for BQ actuals.")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("VERDICT: EXACT MATCH on every probed day.")
    print("Training rows over the probed windows are raw actuals; safe to use as a "
          "prior-year reference line.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
