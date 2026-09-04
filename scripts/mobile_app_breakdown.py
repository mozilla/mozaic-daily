#!/usr/bin/env python3
"""Break out mobile DAU by app (Fenix / Firefox iOS / Focus Android / Focus iOS).

The mobile headline is a sum over exactly **four** apps, fixed by the query's
``app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS")`` filter in
``mozaic_daily.queries``. There is **no "other" bucket**: any other Glean mobile
product (Klar, Firefox Lite, Reference Browser, ...) is absent from both the
training rows and the forecast, not folded into a residual. This script makes the
composition visible so that absence stays a known quantity rather than an
assumption.

Two inputs, either or both:

- ``--forecast`` a mobile forecast parquet. Loaded through
  ``mozaic_daily.adjustments.load_forecast`` so state markers and the sidecar are
  validated. Carries both ``training`` and ``forecast`` rows and an explicit
  ``ALL MOBILE`` row, which is cross-checked against the sum of the four apps.
- ``--raw`` the ``mozaic_parts.raw.glean.mobile.DAU.parquet`` pull that fed it.
  One row per (date, country, app) with four boolean flags; used to confirm the
  forecast's training rows really are the raw actuals.

Shares are reported at a single date and as a trailing-window mean, because a
single day's split carries day-of-week composition (iOS and Android weekday
cycles differ) while the window mean does not.

CLI
---
    source .venv/bin/activate
    python scripts/mobile_app_breakdown.py                    # current cycle defaults
    python scripts/mobile_app_breakdown.py --date 2026-12-15
    python scripts/mobile_app_breakdown.py --country US --csv tmp/us_split.csv

``DEFAULT_FORECAST`` / ``DEFAULT_RAW`` are **cycle-scoped** and must be repointed
each forecast cycle.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from mozaic_daily.adjustments import load_forecast  # noqa: E402

# The four apps the mobile forecast covers, as boolean flag columns in the raw
# pull and as app_name values in the forecast parquet. Source of truth for the
# filter itself: MOBILE_SEGMENT_SQL / the mobile QuerySpec in queries.py.
MOBILE_APPS = ("fenix_android", "firefox_ios", "focus_android", "focus_ios")
ALL_MOBILE_LABEL = "ALL MOBILE"

DEFAULT_TRAILING_WINDOW = 7

# Cycle-scoped: repoint at each roll-forward.
_AUGUST_MOBILE = (
    REPO_ROOT
    / "data-official/2026-09/mobile_rawpull_2026-09-02"  # repointed 2026-09-04
    / "cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1"
)
DEFAULT_FORECAST = _AUGUST_MOBILE / "mozaic_daily_forecast.2026-09-02.gm-D.adj-p.parquet"  # TODO: point at the September mobile build once it exists
DEFAULT_RAW = _AUGUST_MOBILE / "mozaic_parts.raw.glean.mobile.DAU.parquet"


# --------------------------------------------------------------------------
# Pure transforms
# --------------------------------------------------------------------------
def tidy_raw_parts(raw: pd.DataFrame) -> pd.DataFrame:
    """Convert the raw pull's four boolean flag columns into an ``app_name`` column.

    Every row must have exactly one flag set — the SQL ``WHERE`` clause restricts
    to the four apps and the flags are mutually exclusive ``LIKE`` matches, so a
    row with zero or two flags means the query or the app-name vocabulary changed
    upstream and the totals below would silently drop or double-count it.
    """
    missing = [col for col in MOBILE_APPS if col not in raw.columns]
    if missing:
        raise ValueError(
            f"Raw parts frame missing app flag columns {missing}; "
            f"has cols={list(raw.columns)}"
        )
    flags = raw[list(MOBILE_APPS)].fillna(False).astype(bool)
    flags_per_row = flags.sum(axis=1)
    if not (flags_per_row == 1).all():
        counts = flags_per_row.value_counts().to_dict()
        raise ValueError(
            f"Every raw row must set exactly one app flag; got flag-count "
            f"distribution {counts}. Upstream app_name vocabulary likely changed."
        )
    return pd.DataFrame(
        {
            "date": pd.to_datetime(raw["x"]),
            "country": raw["country"],
            "app_name": flags.idxmax(axis=1),
            "dau": raw["y"].astype("float64"),
        }
    )


def tidy_forecast(forecast: pd.DataFrame) -> pd.DataFrame:
    """Normalize a mobile forecast parquet to date/country/app_name/dau/data_type."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(forecast["target_date"]),
            "country": forecast["country"].astype(str),
            "app_name": forecast["app_name"].astype(str),
            "dau": forecast["dau"].astype("float64"),
            "data_type": forecast["data_type"].astype(str),
        }
    )


def app_shares(tidy: pd.DataFrame, dates: pd.Series | list) -> pd.DataFrame:
    """DAU and percent-of-total per app, averaged over ``dates``.

    Pass a single-element list for a point-in-time split, or a date range for a
    trailing mean. Rows for ``ALL MOBILE`` are excluded so the shares sum to 100.
    """
    wanted = pd.to_datetime(pd.Series(list(dates)))
    n_days = wanted.nunique()
    if n_days == 0:
        raise ValueError("app_shares called with no dates")

    window = tidy[tidy["date"].isin(wanted.values) & (tidy["app_name"] != ALL_MOBILE_LABEL)]
    if window.empty:
        raise ValueError(
            f"No app rows found for dates {wanted.min().date()}..{wanted.max().date()}"
        )

    daily_mean = window.groupby("app_name")["dau"].sum() / n_days
    total = daily_mean.sum()
    out = pd.DataFrame({"dau": daily_mean, "share_pct": 100.0 * daily_mean / total})
    return out.sort_values("dau", ascending=False)


def all_mobile_residual(tidy: pd.DataFrame, date: pd.Timestamp) -> float | None:
    """``ALL MOBILE`` row minus the sum of the four app rows at ``date``.

    Returns None when the frame carries no ``ALL MOBILE`` row (the raw pull
    doesn't). A non-zero residual means the aggregate and its parts disagree.
    """
    at_date = tidy[tidy["date"] == pd.to_datetime(date)]
    aggregate = at_date[at_date["app_name"] == ALL_MOBILE_LABEL]["dau"]
    if aggregate.empty:
        return None
    parts = at_date[at_date["app_name"] != ALL_MOBILE_LABEL]["dau"].sum()
    return float(aggregate.sum() - parts)


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------
def _print_shares(shares: pd.DataFrame, label: str) -> None:
    print(f"\n--- {label} (total {shares['dau'].sum():,.0f}) ---")
    print(
        shares.to_string(
            formatters={"dau": "{:,.0f}".format, "share_pct": "{:.3f}%".format}
        )
    )


def report_raw(raw_path: Path, window: int) -> pd.DataFrame:
    """Print the raw-actuals split at the last actual date and over a trailing window."""
    tidy = tidy_raw_parts(pd.read_parquet(raw_path))
    last = tidy["date"].max()
    print(f"\n=== RAW PULL {raw_path.name} ===")
    print(f"rows {len(tidy):,} | dates {tidy['date'].min().date()} -> {last.date()} "
          f"| countries {tidy['country'].nunique()}")

    point = app_shares(tidy, [last])
    _print_shares(point, f"raw actuals, all countries, {last.date()}")

    window_dates = pd.date_range(end=last, periods=window)
    _print_shares(
        app_shares(tidy, window_dates),
        f"raw actuals, all countries, trailing {window}d mean ending {last.date()}",
    )
    return point


def report_forecast(
    forecast_path: Path, country: str, window: int, extra_date: str | None
) -> pd.DataFrame:
    """Print the forecast parquet's split for training, forecast, and an extra date."""
    df, meta = load_forecast(forecast_path)
    tidy = tidy_forecast(df)
    tidy = tidy[tidy["country"] == country]
    if tidy.empty:
        raise ValueError(f"No rows for country={country!r} in {forecast_path.name}")

    codes = [a["code"] for a in meta.get("adjustments_applied", [])]
    print(f"\n=== FORECAST {forecast_path.name} ===")
    print(f"country={country} | adjustments={codes or ['none']} | "
          f"apps={sorted(set(tidy['app_name']) - {ALL_MOBILE_LABEL})}")

    last_training = None
    for data_type in ("training", "forecast"):
        subset = tidy[tidy["data_type"] == data_type]
        if subset.empty:
            continue
        last = subset["date"].max()
        if data_type == "training":
            last_training = app_shares(subset, [last])
        _print_shares(
            app_shares(subset, [last]), f"{data_type}, country={country}, {last.date()}"
        )
        residual = all_mobile_residual(subset, last)
        if residual is not None:
            flag = "OK" if abs(residual) < 1.0 else "MISMATCH"
            print(f"    {flag}: 'ALL MOBILE' minus sum of apps = {residual:,.2f}")

        window_dates = pd.date_range(end=last, periods=window)
        _print_shares(
            app_shares(subset, window_dates),
            f"{data_type}, country={country}, trailing {window}d mean ending {last.date()}",
        )

    if extra_date is not None:
        _print_shares(
            app_shares(tidy, [pd.Timestamp(extra_date)]),
            f"country={country}, {extra_date} (daily, not 28d-MA)",
        )

    return last_training


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--forecast", type=Path, default=DEFAULT_FORECAST,
                        help="mobile forecast parquet (default: current cycle)")
    parser.add_argument("--raw", type=Path, default=DEFAULT_RAW,
                        help="mozaic_parts raw mobile pull (default: current cycle)")
    parser.add_argument("--country", default="ALL",
                        help="country code to slice the forecast on (default: ALL)")
    parser.add_argument("--date", default=None,
                        help="extra date to report, e.g. 2026-12-15")
    parser.add_argument("--window", type=int, default=DEFAULT_TRAILING_WINDOW,
                        help=f"trailing-mean window in days (default: {DEFAULT_TRAILING_WINDOW})")
    parser.add_argument("--csv", type=Path, default=None,
                        help="write the last-actual-date split to this path")
    parser.add_argument("--skip-raw", action="store_true",
                        help="report the forecast parquet only")
    args = parser.parse_args()

    raw_point = None
    if not args.skip_raw:
        raw_point = report_raw(args.raw, args.window)

    forecast_point = report_forecast(args.forecast, args.country, args.window, args.date)

    # The forecast's training rows are supposed to BE the raw actuals. Only
    # comparable at country=ALL, where the parquet aggregates every country.
    if raw_point is not None and forecast_point is not None and args.country == "ALL":
        drift = (forecast_point["dau"] - raw_point["dau"]).abs().max()
        print(f"\nmax |training - raw| at the last actual date: {drift:,.2f}")

    if args.csv is not None:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        (forecast_point if raw_point is None else raw_point).to_csv(args.csv)
        print(f"\nwrote {args.csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
