#!/usr/bin/env python3
"""Export forecast values for a data_source/metric combo from a parquet file as CSV.

Filters to the aggregate row (country=ALL, segment={"os": "ALL"}, and app_name=ALL MOBILE
for mobile data sources). Outputs two columns: date and the metric value.

Usage:
    python scripts/export_forecast_csv.py <parquet_file> <data_source> <metric> [output.csv]

Arguments:
    parquet_file   Path to the forecast parquet file
    data_source    One of: glean_desktop, legacy_desktop, glean_mobile
    metric         One of: dau, new_profiles, existing_engagement_dau, existing_engagement_mau
    output_csv     Optional output path (default: same directory/name as parquet with
                   .{data_source}.{metric}.csv suffix)

Examples:
    python scripts/export_forecast_csv.py mozaic_daily_forecast.2026-04-05.parquet glean_desktop dau
    python scripts/export_forecast_csv.py mozaic_daily_forecast.2026-04-05.parquet glean_mobile dau out.csv
"""

import argparse
from pathlib import Path
import sys

import pandas as pd


VALID_DATA_SOURCES = {"glean_desktop", "legacy_desktop", "glean_mobile"}
VALID_METRICS = {"dau", "new_profiles", "existing_engagement_dau", "existing_engagement_mau"}
MOBILE_APP_NAME = "ALL MOBILE"
DESKTOP_APP_NAME = "desktop"
AGGREGATE_COUNTRY = "ALL"
DESKTOP_AGGREGATE_SEGMENT = '{"os": "ALL"}'
MOBILE_AGGREGATE_SEGMENT = '{}'


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("parquet_file", help="Path to the forecast parquet file")
    parser.add_argument("data_source", choices=sorted(VALID_DATA_SOURCES), help="Data source to filter to")
    parser.add_argument("metric", choices=sorted(VALID_METRICS), help="Metric column to export")
    parser.add_argument("output_csv", nargs="?", default=None, help="Output CSV path (default: stdout)")
    return parser.parse_args()


def select_aggregate_rows(df, data_source):
    is_mobile = data_source == "glean_mobile"
    app_name = MOBILE_APP_NAME if is_mobile else DESKTOP_APP_NAME
    segment = MOBILE_AGGREGATE_SEGMENT if is_mobile else DESKTOP_AGGREGATE_SEGMENT
    mask = (
        (df["data_source"] == data_source)
        & (df["country"] == AGGREGATE_COUNTRY)
        & (df["app_name"] == app_name)
        & (df["segment"] == segment)
    )
    return df[mask]


MOVING_AVERAGE_WINDOW = 28


def main():
    args = parse_args()

    df = pd.read_parquet(args.parquet_file)

    filtered = select_aggregate_rows(df, args.data_source)

    if filtered.empty:
        print(
            f"Error: no rows found for data_source={args.data_source!r} "
            f"with country={AGGREGATE_COUNTRY!r}, segment={AGGREGATE_SEGMENT!r}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Sort full time series (training + forecast) so the rolling window is continuous
    all_dates = (
        filtered[["target_date", args.metric]]
        .rename(columns={"target_date": "date"})
        .sort_values("date")
        .reset_index(drop=True)
    )
    all_dates["28ma"] = all_dates[args.metric].rolling(window=MOVING_AVERAGE_WINDOW).mean()

    # Return only forecast rows
    forecast_dates = set(filtered.loc[filtered["data_type"] == "forecast", "target_date"])
    result = all_dates[all_dates["date"].isin(forecast_dates)].reset_index(drop=True)

    output_path = args.output_csv
    if output_path is None:
        parquet_path = Path(args.parquet_file)
        output_path = str(parquet_path.with_suffix(f".{args.data_source}.{args.metric}.csv"))

    result.to_csv(output_path, index=False)
    print(f"Wrote {len(result)} rows to {output_path}")


if __name__ == "__main__":
    main()
