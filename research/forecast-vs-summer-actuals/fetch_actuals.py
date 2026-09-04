"""Cache per-country legacy-desktop DAU actuals for the forecast-vs-summer comparison.

Same source and filter as the canonical notebook's ``[bq-actuals]`` cell
(``telemetry.active_users_aggregates``, ``app_name = "Firefox Desktop"``, no channel filter),
but split by country so the ex-Iran/ex-China track can be built by subtraction.

The full 2019+ pull costs ~5.3 TB. A pull of that shape already exists in the regional-story
project, so by default this script seeds from it and tops up only the missing tail from
BigQuery (~28 GB per month of tail). Pass ``--full`` to pull the whole window standalone.

Run:
    python research/forecast-vs-summer-actuals/fetch_actuals.py
    python research/forecast-vs-summer-actuals/fetch_actuals.py --refresh
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
CACHE_PATH = HERE / "data" / "desktop_dau_by_country.parquet"
META_PATH = CACHE_PATH.with_suffix(".meta.json")

# B needs 2022-2025 seasonal shapes; the 28-day trailing MA needs 27 days of warmup before
# 2022-01-01. Pull from 2021-12-01 to leave margin.
WINDOW_START = "2021-12-01"

SEED_PARQUET = Path(
    "/Users/brendanwells/work/product-data-science-core/scratch/brwells/regional-story"
    "/data/dau_mau.parquet"
)

BASE_SQL = """
    SELECT submission_date AS date, country, SUM(dau) AS dau
    FROM `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
    WHERE app_name = "Firefox Desktop"
      AND country IS NOT NULL
      AND submission_date BETWEEN '{start}' AND {end}
    GROUP BY submission_date, country
    ORDER BY 1, 2
"""

# Actuals cutoff mirrors the canonical notebook: CURRENT_DATE in LA time minus two days, because
# the most-recent landed day can be partial and one partial day poisons the trailing MA endpoint.
END_EXPR = 'CURRENT_DATE("America/Los_Angeles") - 2'


def _query(start: str, end_expr: str) -> pd.DataFrame:
    from google.cloud import bigquery

    client = bigquery.Client(project="moz-fx-data-bq-data-science")
    df = client.query(BASE_SQL.format(start=start, end=end_expr)).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])
    return df


def _load_seed() -> pd.DataFrame | None:
    """Per-country actuals from the regional-story pull, if it is on disk."""
    if not SEED_PARQUET.exists():
        return None
    # The seed was written by a BigQuery client, so its date column carries the `dbdate`
    # extension dtype; pyarrow cannot materialise it unless db_dtypes is imported first.
    import db_dtypes  # noqa: F401

    seed = pd.read_parquet(SEED_PARQUET, columns=["submission_date", "country", "dau"])
    seed = seed.rename(columns={"submission_date": "date"})
    seed["date"] = pd.to_datetime(seed["date"].astype(str))
    seed["dau"] = seed["dau"].astype("float64")
    return seed[seed["date"] >= pd.Timestamp(WINDOW_START)].reset_index(drop=True)


def load_actuals(refresh: bool = False, full: bool = False) -> pd.DataFrame:
    """Return date/country/dau desktop actuals, querying BigQuery only for what is missing."""
    if CACHE_PATH.exists() and not refresh:
        return pd.read_parquet(CACHE_PATH)

    seed = None if full else _load_seed()
    if seed is None:
        frame = _query(WINDOW_START, END_EXPR)
        provenance = {"seed": None, "bq_window": [WINDOW_START, "CURRENT_DATE(LA)-2"]}
    else:
        tail_start = (seed["date"].max() + pd.Timedelta(days=1)).date().isoformat()
        tail = _query(tail_start, END_EXPR)
        frame = pd.concat([seed, tail], ignore_index=True) if len(tail) else seed
        provenance = {
            "seed": str(SEED_PARQUET),
            "seed_through": seed["date"].max().date().isoformat(),
            "bq_window": [tail_start, "CURRENT_DATE(LA)-2"],
        }

    frame = (
        frame.groupby(["date", "country"], as_index=False)["dau"]
        .sum()
        .sort_values(["date", "country"])
        .reset_index(drop=True)
    )
    frame["dau"] = frame["dau"].astype("float64")

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(CACHE_PATH, index=False)
    META_PATH.write_text(
        json.dumps(
            {
                "source_table": "moz-fx-data-shared-prod.telemetry.active_users_aggregates",
                "filter": 'app_name = "Firefox Desktop", country IS NOT NULL, no channel filter',
                "rows": int(len(frame)),
                "date_min": frame["date"].min().date().isoformat(),
                "date_max": frame["date"].max().date().isoformat(),
                "countries": int(frame["country"].nunique()),
                **provenance,
            },
            indent=2,
        )
        + "\n"
    )
    return frame


def world_total(frame: pd.DataFrame) -> pd.Series:
    """Date-indexed daily DAU summed over every country."""
    total = frame.groupby("date")["dau"].sum().sort_index()
    total.index = pd.DatetimeIndex(total.index)
    return total


def excluding(frame: pd.DataFrame, countries: tuple[str, ...]) -> pd.Series:
    """Date-indexed daily DAU with the named countries dropped."""
    kept = frame[~frame["country"].isin(countries)]
    return world_total(kept)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true", help="re-query even if cached")
    parser.add_argument("--full", action="store_true", help="ignore the seed; pull the whole window")
    args = parser.parse_args()

    actuals = load_actuals(refresh=args.refresh, full=args.full)
    print(f"rows={len(actuals):,}  countries={actuals['country'].nunique()}")
    print(f"dates {actuals['date'].min().date()} .. {actuals['date'].max().date()}")
    print(META_PATH.read_text())
