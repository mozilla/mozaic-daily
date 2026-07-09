"""Fold the July 2026 forecast cycle into the KPI workbook's "Official Forecast Data".

Reads the current "Official Forecast Data" tab (exported as CSV) plus this repo's July
curves, demotes the old June `CURRENT` cycle to `JUN`, inserts the July cycle as the new
`CURRENT`, and writes a full replacement long-format CSV for the tab (which loads into
`mozdata.analysis.browser_kpi_forecasts_2026`).

No BigQuery: every value comes from the existing tab export + july_canonical_curves.csv.
The `prior forecasts` line is forecast-derived (the previous cycle's then-official
forecast spliced into a continuous full-year line), not actuals.

This is the July sibling of `../../2026-06/kpi_sheet/build_kpi_sheet_update.py`. It reads
the *current-tab CSV export* instead of the live `.xlsx` (the tab has since moved to a
Google Sheet), but the update scheme is identical.

Run:
    source .venv/bin/activate
    python3 data-official/2026-07/kpi_sheet/build_kpi_sheet_update.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

# --- Paths -----------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
# The current state of the "Official Forecast Data" tab, exported from the Google Sheet.
# (June is the CURRENT cycle in this file; created_on/updated_on = 2026-06-09.)
CURRENT_TAB_CSV = Path.home() / "Downloads" / "2026 Firefox KPI Forecasts - Official Forecast Data.csv"
JULY_CURVES = SCRIPT_DIR.parent / "csv" / "july_canonical_curves.csv"
OUTPUT_CSV = SCRIPT_DIR / "official_forecast_data.2026-07-06.csv"

# --- Cycle constants -------------------------------------------------------
PUBLISH_DATE = pd.Timestamp("2026-07-06")  # created_on / updated_on for July rows
FORECAST_START = pd.Timestamp("2026-07-06")  # first day of the July forecast portion
PRIOR_BOUNDARY = pd.Timestamp("2026-07-05")  # last day carried by the July prior line
PREV_FORECAST_START = pd.Timestamp("2026-05-26")  # June cycle's forecast start
# Handoff gap in the prior line: the day BEFORE the previous cycle's forecast start is
# nulled so the demoted April-forecast segment and June-forecast segment render as two
# separate "individual prior forecasts" (a gap), matching the Jan31/Feb28/Mar31 handoff
# nulls the earlier month cycles produced automatically (they started on the 1st, so
# their handoff day was a month-end that was already null). June started mid-month
# (May 26), so this gap must be inserted explicitly.
PRIOR_GAP_DATE = PREV_FORECAST_START - pd.Timedelta(days=1)  # 2026-05-25
YEAR = 2026
QUARTER = 1  # constant for every row in this workbook, regardless of submission month

# The old June cycle is aliased `CURRENT`; back it up by renaming to its month label
# `JUN` (both lines), freeing `CURRENT` for the new July cycle. June had no ex-Iran
# variant (Iran was already folded in), so only two lines carry over.
JUNE_RENAME = {
    "CURRENT forecast": "JUN forecast",
    "CURRENT prior forecasts": "JUN prior forecasts",
}

# The July cycle becomes the new `CURRENT`, one forecast line per product. Iran is
# included natively this cycle (no plus/no-iran or ex-Iran variants): desktop carries
# the launch-on-login + MozillaOnline overlays, mobile carries the marketing lift; both
# are already baked into the july_canonical_curves.csv columns and labeled plainly
# `CURRENT forecast`.
# july_canonical_curves.csv column -> (product, July forecast_name)
JULY_FORECAST_COLUMNS = {
    "desktop_current_july": ("desktop", "CURRENT forecast"),
    "mobile_current_july": ("mobile", "CURRENT forecast"),
}

OUTPUT_COLUMNS = [
    "submission_date",
    "product",
    "forecast_name",
    "year",
    "quarter",
    "created_on",
    "updated_on",
    "dau_28_ma",
]

# Output sort: forecast_name in this custom order, then product, then date. The order
# follows the workbook's existing cycle pattern (chronological cycles; within a cycle:
# prior -> forecast -> ex-Iran). June is backed up as `JUN`; July is the new `CURRENT`.
PRODUCT_ORDER = ["desktop", "mobile"]
FORECAST_NAME_ORDER = [
    "NO forecast",
    "JAN forecast",
    "FEB prior forecasts",
    "FEB forecast",
    "MAR prior forecasts",
    "MAR forecast",
    "APR prior forecasts",
    "APR forecast",
    "APR z forecast ex-Iran",
    "JUN prior forecasts",
    "JUN forecast",
    "CURRENT prior forecasts",
    "CURRENT forecast",
]


def _make_rows(
    dates: pd.Series,
    product: str,
    forecast_name: str,
    dau: pd.Series,
    created: pd.Timestamp,
) -> pd.DataFrame:
    """Assemble a block of output rows sharing product / forecast_name / vintage."""
    return pd.DataFrame(
        {
            "submission_date": pd.to_datetime(dates).values,
            "product": product,
            "forecast_name": forecast_name,
            "year": YEAR,
            "quarter": QUARTER,
            "created_on": created,
            "updated_on": created,
            "dau_28_ma": pd.Series(dau).values,
        }
    )


def build_july_forecast_rows(curves: pd.DataFrame) -> pd.DataFrame:
    """July `CURRENT forecast` lines: forecast-start through Dec 31."""
    horizon = curves[curves["date"] >= FORECAST_START]
    blocks = []
    for column, (product, forecast_name) in JULY_FORECAST_COLUMNS.items():
        series = horizon[["date", column]].dropna(subset=[column])
        blocks.append(
            _make_rows(series["date"], product, forecast_name, series[column], PUBLISH_DATE)
        )
    return pd.concat(blocks, ignore_index=True)


def build_july_prior_rows(sheet: pd.DataFrame) -> pd.DataFrame:
    """July `CURRENT prior forecasts`: the June prior line (Jan 1 -> May 25) extended
    with the June forecast for May 26 -> Jul 5 (the as-published history before the July
    forecast start), giving a full-year line broken into per-cycle segments by handoff
    gaps.

    Both segments are pulled from the original June rows (still labeled CURRENT here,
    before the JUN rename) by forecast_name, matching the workbook's splice convention.
    The day before the June forecast start (`PRIOR_GAP_DATE`, 2026-05-25) is nulled so
    the demoted April and June forecasts show as two separate prior forecasts rather than
    one fused line (see the constant's note).
    """
    june_prior = sheet[sheet["forecast_name"] == "CURRENT prior forecasts"]
    june_forecast = sheet[
        (sheet["forecast_name"] == "CURRENT forecast")
        & (sheet["submission_date"] <= PRIOR_BOUNDARY)
    ]

    blocks = []
    for product in ("desktop", "mobile"):
        prior_seg = june_prior[june_prior["product"] == product]
        forecast_seg = june_forecast[june_forecast["product"] == product]
        combined_dates = pd.concat([prior_seg["submission_date"], forecast_seg["submission_date"]])
        combined_dau = pd.concat([prior_seg["dau_28_ma"], forecast_seg["dau_28_ma"]])
        combined_dau = combined_dau.mask(combined_dates == PRIOR_GAP_DATE)  # insert the handoff gap
        blocks.append(
            _make_rows(combined_dates, product, "CURRENT prior forecasts", combined_dau, PUBLISH_DATE)
        )
    return pd.concat(blocks, ignore_index=True)


def main() -> None:
    sheet = pd.read_csv(CURRENT_TAB_CSV, parse_dates=["submission_date", "created_on", "updated_on"])
    curves = pd.read_csv(JULY_CURVES, parse_dates=["date"])

    # Rows that carry over untouched: everything that is NOT part of the old June
    # `CURRENT` cycle (JAN/FEB/MAR/APR forecasts, their priors, and the NO forecast stub).
    carried = sheet[~sheet["forecast_name"].isin(JUNE_RENAME)].copy()

    # June cycle, backed up to JUN (values and 2026-06-09 vintage preserved).
    demoted = sheet[sheet["forecast_name"].isin(JUNE_RENAME)].copy()
    demoted["forecast_name"] = demoted["forecast_name"].map(JUNE_RENAME)

    july_forecasts = build_july_forecast_rows(curves)
    july_prior = build_july_prior_rows(sheet)

    out = pd.concat([carried, demoted, july_forecasts, july_prior], ignore_index=True)

    unknown = set(out["forecast_name"]) - set(FORECAST_NAME_ORDER)
    if unknown:
        raise ValueError(f"forecast_name values missing from FORECAST_NAME_ORDER: {sorted(unknown)}")
    out["forecast_name"] = pd.Categorical(out["forecast_name"], FORECAST_NAME_ORDER, ordered=True)
    out["product"] = pd.Categorical(out["product"], PRODUCT_ORDER, ordered=True)
    out = out[OUTPUT_COLUMNS].sort_values(
        ["forecast_name", "product", "submission_date"]
    ).reset_index(drop=True)

    # Integer DAU (nullable, so the NO-forecast blanks stay blank), matching the sheet's
    # presentation (no trailing .0).
    out["dau_28_ma"] = out["dau_28_ma"].round().astype("Int64")

    # Dates as YYYY-MM-DD (no time component) to match the workbook's presentation.
    for date_col in ("submission_date", "created_on", "updated_on"):
        out[date_col] = pd.to_datetime(out[date_col]).dt.strftime("%Y-%m-%d")

    out.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {len(out):,} rows -> {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
