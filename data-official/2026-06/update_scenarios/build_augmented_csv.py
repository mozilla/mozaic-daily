"""Build the augmented June-canonical curves CSV for the MozillaOnline scenario.

Reads the pristine canonical ALL-level curves (`../csv/june_canonical_curves.csv`,
13 columns of 28-day-MA DAU), then adds the two extra series the scenario plots need:

  * ``desktop_mozillaonline_plus_iran`` / ``mobile_mozillaonline_plus_iran`` —
    the June +Iran forecast with a MozillaOnline partner onboarding modeled as a
    +500k *daily-DAU step* on 2026-06-02. Because the canonical curves are 28-day
    moving averages, a daily step shows up as a 28-day linear ramp in MA space
    (from base to base+500k over the window), then parallels the base curve.
  * ``desktop_actuals_2025`` / ``mobile_actuals_2025`` — last-year (2025) 28dMA
    actuals, pulled once from BigQuery and aligned onto the 2026 calendar axis
    (the gray reference line in the plots). This is the only piece that needs BQ;
    once written into the augmented CSV the plot script is fully portable.

The canonical CSV is left untouched — this writes a derived copy in this folder.

Run:
    source .venv/bin/activate
    python3 data-official/2026-06/update_scenarios/build_augmented_csv.py
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
CANONICAL_CSV = os.path.join(HERE, "..", "csv", "june_canonical_curves.csv")
AUGMENTED_CSV = os.path.join(HERE, "augmented_curves.csv")

# MozillaOnline (China distribution partner) onboarding scenario.
MOZILLAONLINE_DAILY_DAU = 500_000
MOZILLAONLINE_START = pd.Timestamp("2026-06-02")
MA_WINDOW = 28

BQ_PROJECT = "moz-fx-data-bq-data-science"

# 2025 28dMA actuals by product. The 27-preceding window means we must read from
# 2024-12-01 so the first 2025 days have a full trailing window.
ACTUALS_2025_SQL = """
WITH daily AS (
  SELECT submission_date, product_category AS product, dau
  FROM `moz-fx-data-shared-prod.telemetry.daily_active_users_by_product_category`
  WHERE submission_date BETWEEN "2024-12-01" AND "2025-12-31"
    AND product_category IN ("desktop", "mobile")
),
ma AS (
  SELECT
    submission_date,
    product,
    AVG(dau) OVER (
      PARTITION BY product ORDER BY submission_date
      ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    ) AS dau_28ma
  FROM daily
)
SELECT submission_date, product, dau_28ma
FROM ma
WHERE submission_date >= "2025-01-01"
ORDER BY product, submission_date
"""


def fetch_2025_actuals_on_2026_axis(index: pd.DatetimeIndex) -> pd.DataFrame:
    """Pull 2025 28dMA actuals and align them onto the supplied 2026 date index.

    Each 2025 date maps to the same month/day in 2026 (shift +1 year), matching how
    the source notebook overlays last year's curve on the current-year axis.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=BQ_PROJECT)
    raw = client.query(ACTUALS_2025_SQL).to_dataframe(create_bqstorage_client=False)
    raw["submission_date"] = pd.to_datetime(raw["submission_date"])
    # Shift 2025 -> 2026 calendar axis.
    raw["axis_date"] = raw["submission_date"] + pd.DateOffset(years=1)

    out = pd.DataFrame(index=index)
    for product, column in [("desktop", "desktop_actuals_2025"),
                            ("mobile", "mobile_actuals_2025")]:
        series = (
            raw[raw["product"] == product]
            .set_index("axis_date")["dau_28ma"]
            .sort_index()
        )
        out[column] = series.reindex(index)
    return out


def mozillaonline_ramp(index: pd.DatetimeIndex) -> pd.Series:
    """28dMA contribution of a +500k daily-DAU step starting MOZILLAONLINE_START.

    On any date d the trailing 28-day window contains
    ``clip(d - start + 1, 0, 28)`` stepped-up days, so the MA contribution ramps
    linearly from 500k/28 on the start date to the full 500k at start+27 days.
    """
    days_since_start = (index - MOZILLAONLINE_START).days.to_numpy() + 1
    days_in_window = np.clip(days_since_start, 0, MA_WINDOW)
    return pd.Series(MOZILLAONLINE_DAILY_DAU * days_in_window / MA_WINDOW, index=index)


def build() -> pd.DataFrame:
    canon = pd.read_csv(CANONICAL_CSV, parse_dates=["date"]).set_index("date")

    ramp = mozillaonline_ramp(canon.index)
    # base + ramp; NaN base (pre-forecast-start) stays NaN since ramp is 0 there anyway.
    canon["desktop_mozillaonline_plus_iran"] = canon["desktop_current_june_plus_iran"] + ramp
    canon["mobile_mozillaonline_plus_iran"] = canon["mobile_current_june_plus_iran"] + ramp

    actuals_2025 = fetch_2025_actuals_on_2026_axis(canon.index)
    augmented = canon.join(actuals_2025)
    return augmented


def main() -> None:
    augmented = build()
    augmented.to_csv(AUGMENTED_CSV)
    dec15 = pd.Timestamp("2026-12-15")
    print(f"Wrote {AUGMENTED_CSV}: {len(augmented)} rows x {augmented.shape[1]} cols")
    print("New columns:", [c for c in augmented.columns if "mozillaonline" in c or "2025" in c])
    base = augmented.loc[dec15, "desktop_current_june_plus_iran"]
    moz = augmented.loc[dec15, "desktop_mozillaonline_plus_iran"]
    print(f"Desktop Dec-15 28dMA: +Iran {base:,.0f} -> +MozillaOnline {moz:,.0f} (Δ {moz - base:,.0f})")
    print(f"2025 actuals coverage: desktop {augmented['desktop_actuals_2025'].notna().sum()} pts, "
          f"mobile {augmented['mobile_actuals_2025'].notna().sum()} pts")


if __name__ == "__main__":
    main()
