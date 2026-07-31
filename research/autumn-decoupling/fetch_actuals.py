"""Fetch and cache legacy-desktop DAU actuals for the autumn-decoupling exploration.

Same source and filter as the canonical notebook's `[bq-actuals]` cell (legacy telemetry,
`app_name = "Firefox Desktop"`, all countries), just over a multi-year window so the
summer-trough -> autumn-plateau relationship can be measured across seasons.

Idempotent: writes `actuals_desktop_dau.parquet` beside this file and reuses it unless
`--refresh` is passed.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

CACHE_PATH = Path(__file__).resolve().parent / "actuals_desktop_dau.parquet"

ACTUALS_SQL = """
    SELECT submission_date AS date, SUM(dau) AS dau
    FROM `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
    WHERE app_name = "Firefox Desktop"
      AND submission_date BETWEEN '2020-06-01' AND CURRENT_DATE("America/Los_Angeles") - 2
    GROUP BY submission_date
    ORDER BY 1
"""


def load_actuals(refresh: bool = False) -> pd.DataFrame:
    """Return date/dau desktop actuals, querying BigQuery only when the cache is absent."""
    if CACHE_PATH.exists() and not refresh:
        return pd.read_parquet(CACHE_PATH)

    from google.cloud import bigquery

    client = bigquery.Client(project="moz-fx-data-bq-data-science")
    df = client.query(ACTUALS_SQL).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df.to_parquet(CACHE_PATH, index=False)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true", help="re-query even if cached")
    args = parser.parse_args()

    actuals = load_actuals(refresh=args.refresh)
    print(f"{len(actuals)} rows, {actuals['date'].min().date()} to {actuals['date'].max().date()}")
    print(f"cached at {CACHE_PATH}")
