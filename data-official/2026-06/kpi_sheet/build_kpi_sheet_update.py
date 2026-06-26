"""Fold the June 2026 forecast cycle into the KPI workbook's "Official Forecast Data".

Reads the current workbook tab plus this repo's June curves, demotes the old April
`CURRENT` cycle to `APR`, inserts the June cycle as the new `CURRENT`, and writes a
full replacement long-format CSV for the tab (which loads into
`mozdata.analysis.browser_kpi_forecasts_2026`).

No BigQuery: every value comes from the existing workbook + augmented_curves.csv.
The `prior forecasts` line is forecast-derived (each prior month's then-official
forecast), not actuals, so the file's slightly-stale actuals are irrelevant here.

Run:
    source .venv/bin/activate
    python3 data-official/2026-06/kpi_sheet/build_kpi_sheet_update.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

# --- Paths -----------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
WORKBOOK_PATH = Path.home() / "Downloads" / "2026 Firefox KPI Forecasts.xlsx"
SHEET_NAME = "Official Forecast Data"
AUGMENTED_CSV = SCRIPT_DIR.parent / "update_scenarios" / "augmented_curves.csv"
OUTPUT_CSV = SCRIPT_DIR / "official_forecast_data.2026-06-09.csv"

# --- Cycle constants -------------------------------------------------------
PUBLISH_DATE = pd.Timestamp("2026-06-09")  # created_on / updated_on for June rows
FORECAST_START = pd.Timestamp("2026-05-26")  # first day of the June forecast portion
PRIOR_BOUNDARY = pd.Timestamp("2026-05-25")  # last day carried by the prior line
YEAR = 2026
QUARTER = 1  # constant for every row in this workbook, regardless of submission month

# The old April cycle is aliased `CURRENT`; back it up by renaming to its month label
# `APR` (all three lines), freeing `CURRENT` for the new June cycle.
APRIL_RENAME = {
    "CURRENT forecast": "APR forecast",
    "CURRENT prior forecasts": "APR prior forecasts",
    "CURRENT z forecast ex-Iran": "APR z forecast ex-Iran",
}

# The June cycle becomes the new `CURRENT`, with a single forecast line per product
# (no ex-Iran / MozillaOnline variants): desktop uses the +Iran+MozillaOnline curve,
# mobile uses the +Iran curve, both labeled plainly `CURRENT forecast`.
# augmented_curves.csv column -> (product, June forecast_name)
JUNE_FORECAST_COLUMNS = {
    "desktop_mozillaonline_plus_iran": ("desktop", "CURRENT forecast"),
    "mobile_current_june_plus_iran": ("mobile", "CURRENT forecast"),
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
# prior -> forecast -> ex-Iran). April is backed up as `APR`; June is the new `CURRENT`.
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


def build_june_forecast_rows(curves: pd.DataFrame) -> pd.DataFrame:
    """June `CURRENT [...] forecast` lines: forecast-start through Dec 31."""
    horizon = curves[curves["date"] >= FORECAST_START]
    blocks = []
    for column, (product, forecast_name) in JUNE_FORECAST_COLUMNS.items():
        series = horizon[["date", column]].dropna(subset=[column])
        blocks.append(
            _make_rows(series["date"], product, forecast_name, series[column], PUBLISH_DATE)
        )
    return pd.concat(blocks, ignore_index=True)


def build_june_prior_rows(sheet: pd.DataFrame) -> pd.DataFrame:
    """June `CURRENT prior forecasts`: the April prior line (Jan 1 -> Mar 31) extended
    with the April +Iran forecast for Apr 1 -> May 25 (the as-published history before
    the June forecast start), giving a continuous full-year line. History predates the
    Jun 2 MozillaOnline step, so desktop's prior uses plain +Iran.

    Both segments are pulled from the original April rows (still labeled CURRENT here,
    before the APR rename) by forecast_name, matching the workbook's splice convention.
    """
    # The April +Iran forecast spans Apr 1 -> Dec 31; take only the Apr 1 -> May 25
    # portion the June forecast now supersedes (upper bound suffices).
    april_prior = sheet[sheet["forecast_name"] == "CURRENT prior forecasts"]
    april_forecast = sheet[
        (sheet["forecast_name"] == "CURRENT forecast")
        & (sheet["submission_date"] <= PRIOR_BOUNDARY)
    ]

    blocks = []
    for product in ("desktop", "mobile"):
        prior_seg = april_prior[april_prior["product"] == product]
        forecast_seg = april_forecast[april_forecast["product"] == product]
        combined_dates = pd.concat([prior_seg["submission_date"], forecast_seg["submission_date"]])
        combined_dau = pd.concat([prior_seg["dau_28_ma"], forecast_seg["dau_28_ma"]])
        blocks.append(
            _make_rows(combined_dates, product, "CURRENT prior forecasts", combined_dau, PUBLISH_DATE)
        )
    return pd.concat(blocks, ignore_index=True)


def main() -> None:
    sheet = pd.read_excel(WORKBOOK_PATH, sheet_name=SHEET_NAME)
    curves = pd.read_csv(AUGMENTED_CSV, parse_dates=["date"])

    # Rows that carry over untouched: everything that is NOT part of the old April
    # `CURRENT` cycle (JAN/FEB/MAR forecasts, their priors, and the NO forecast stub).
    carried = sheet[~sheet["forecast_name"].isin(APRIL_RENAME)].copy()

    # April cycle, backed up to APR (values and 2026-04-13 vintage preserved).
    demoted = sheet[sheet["forecast_name"].isin(APRIL_RENAME)].copy()
    demoted["forecast_name"] = demoted["forecast_name"].map(APRIL_RENAME)

    june_forecasts = build_june_forecast_rows(curves)
    june_prior = build_june_prior_rows(sheet)

    out = pd.concat([carried, demoted, june_forecasts, june_prior], ignore_index=True)

    unknown = set(out["forecast_name"]) - set(FORECAST_NAME_ORDER)
    if unknown:
        raise ValueError(f"forecast_name values missing from FORECAST_NAME_ORDER: {sorted(unknown)}")
    out["forecast_name"] = pd.Categorical(out["forecast_name"], FORECAST_NAME_ORDER, ordered=True)
    out["product"] = pd.Categorical(out["product"], PRODUCT_ORDER, ordered=True)
    out = out[OUTPUT_COLUMNS].sort_values(
        ["forecast_name", "product", "submission_date"]
    ).reset_index(drop=True)

    # Dates as YYYY-MM-DD (no time component) to match the workbook's presentation.
    for date_col in ("submission_date", "created_on", "updated_on"):
        out[date_col] = pd.to_datetime(out[date_col]).dt.strftime("%Y-%m-%d")

    out.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {len(out):,} rows -> {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
