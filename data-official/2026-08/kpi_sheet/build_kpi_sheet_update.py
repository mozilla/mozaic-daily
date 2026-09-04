"""Add the August 2026 forecast cycle to the KPI workbook as a DRAFT (`FUTURE`) cycle.

Reads the current "Official Forecast Data" tab (exported as CSV) plus this repo's August
curves, and appends the August cycle under the `FUTURE` prefix — leaving the `CURRENT`
(July) cycle and every older vintage completely untouched. This is the publish-a-draft
path: the dashboard built off `FUTURE *` shows August without moving the official numbers.

Contrast with `../../2026-07/kpi_sheet/build_kpi_sheet_update.py`, the promote-to-official
path, which demotes the outgoing `CURRENT` to its month label (`JUN`) and installs the new
cycle as `CURRENT`. Nothing is renamed here. When August is promoted for real, that
script's scheme is the one to copy: rename `CURRENT * -> JUL *`, install August as
`CURRENT *`, and drop these `FUTURE *` rows.

No BigQuery: every value comes from the existing tab export + august_canonical_curves.csv.
The `prior forecasts` line is forecast-derived (each superseded cycle's then-official
forecast spliced into a continuous full-year line), not actuals — the tab holds no actuals.

Run:
    source .venv/bin/activate
    python3 data-official/2026-08/kpi_sheet/build_kpi_sheet_update.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

# --- Paths -----------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
# The current state of the "Official Forecast Data" tab, exported from the Google Sheet.
# (July is the CURRENT cycle in this file; created_on/updated_on = 2026-07-06. Verified
# field-for-field identical to ../../2026-07/kpi_sheet/official_forecast_data.2026-07-06.csv,
# i.e. the tab has not drifted since July's update. Not byte-identical: the Sheets export
# uses CRLF and no trailing newline, pandas writes LF — the only difference.)
CURRENT_TAB_CSV = Path.home() / "Downloads" / "2026 Firefox KPI Forecasts - Official Forecast Data(1).csv"
AUGUST_CURVES = SCRIPT_DIR.parent / "csv" / "august_canonical_curves.csv"
OUTPUT_CSV = SCRIPT_DIR / "official_forecast_data.2026-08-10.csv"
OUTPUT_FUTURE_ONLY_CSV = SCRIPT_DIR / "official_forecast_data.FUTURE_ONLY.csv"

# --- Cycle constants -------------------------------------------------------
PUBLISH_DATE = pd.Timestamp("2026-08-10")  # created_on / updated_on for the FUTURE rows
FORECAST_START = pd.Timestamp("2026-08-02")  # August seam (trained through 2026-08-01)
PRIOR_BOUNDARY = pd.Timestamp("2026-08-01")  # last day carried by the FUTURE prior line
PREV_FORECAST_START = pd.Timestamp("2026-07-06")  # July cycle's forecast start
YEAR = 2026
QUARTER = 1  # constant for every row in this workbook, regardless of submission month

# Handoff gap at the JUN -> JUL junction, nulling 2026-07-05.
#
# The workbook's convention: null the day before each superseded cycle's forecast start so
# each vintage renders as its own segment of the light-purple "Prior Forecasts" line — see
# ../../2026-07/kpi_sheet/ `PRIOR_GAP_DATE`, which had to be added after the first Looker
# render fused the April and June segments into one line. Without it here the June and July
# segments fuse and the line draws a near-vertical connector across the level disagreement
# between the two vintages (+999,721 desktop / +413,879 mobile), which reads as a spike in
# the history rather than as two forecasts that disagreed.
#
# This draft was first built with the gap suppressed (2026-08-10) and the fusion was visible
# in ../plots/kpi_sheet_future_*.png; the gap was restored the same day. Set False to
# reproduce that render.
INSERT_JUL_HANDOFF_GAP = True
PRIOR_GAP_DATE = PREV_FORECAST_START - pd.Timedelta(days=1)  # 2026-07-05

# Nulls the FUTURE prior line is expected to carry, all inherited from the source tab's
# `CURRENT prior forecasts` rows (the pre-July junctions).
INHERITED_PRIOR_GAPS = {"2026-01-31", "2026-02-28", "2026-03-31", "2026-05-25"}

# The August cycle is added as `FUTURE`, one forecast line per product. Both curves are
# the published canonical ones: desktop carries the Win10 headwind (`h`, -1,315,000),
# launch-on-login (`l`, 200K ceiling) and MozillaOnline (`o`) overlays; mobile carries the
# paid/organic split (`p`), the headwind and the mobile tailwind (`t`, +299,000) — all
# already baked into the august_canonical_curves.csv columns.
# august_canonical_curves.csv column -> (product, forecast_name)
AUGUST_FORECAST_COLUMNS = {
    "desktop_current_august": ("desktop", "FUTURE forecast"),
    "mobile_current_august": ("mobile", "FUTURE forecast"),
}

# Dec-15 headline per platform, locked as literals so a curve-file refresh that moves the
# published number fails here instead of silently reshaping the draft dashboard.
EXPECTED_DEC15 = {"desktop": 48_703_443, "mobile": 17_924_562}
DEC15 = pd.Timestamp("2026-12-15")

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
# prior -> forecast). `FUTURE` is appended last, which leaves the 5,660 existing rows
# byte-identical and in place (asserted in main()).
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
    "FUTURE prior forecasts",
    "FUTURE forecast",
]
FUTURE_NAMES = ["FUTURE prior forecasts", "FUTURE forecast"]


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


def build_august_forecast_rows(curves: pd.DataFrame) -> pd.DataFrame:
    """August `FUTURE forecast` lines: the 2026-08-02 seam through Dec 31."""
    horizon = curves[curves["date"] >= FORECAST_START]
    blocks = []
    for column, (product, forecast_name) in AUGUST_FORECAST_COLUMNS.items():
        series = horizon[["date", column]].dropna(subset=[column])
        blocks.append(
            _make_rows(series["date"], product, forecast_name, series[column], PUBLISH_DATE)
        )
    return pd.concat(blocks, ignore_index=True)


def build_august_prior_rows(sheet: pd.DataFrame) -> pd.DataFrame:
    """August `FUTURE prior forecasts`: July's prior line (Jan 1 -> Jul 5) extended with
    July's own forecast for Jul 6 -> Aug 1 — the as-published history before the August
    forecast start, as one full-year line.

    Both segments come from the source tab's July rows (`CURRENT *`), which stay in place
    under their own names; this line is an additional, independent copy. The JUL handoff
    day is left populated unless `INSERT_JUL_HANDOFF_GAP` is set (see that constant).
    """
    july_prior = sheet[sheet["forecast_name"] == "CURRENT prior forecasts"]
    july_forecast = sheet[
        (sheet["forecast_name"] == "CURRENT forecast")
        & (sheet["submission_date"] <= PRIOR_BOUNDARY)
    ]

    blocks = []
    for product in PRODUCT_ORDER:
        prior_seg = july_prior[july_prior["product"] == product]
        forecast_seg = july_forecast[july_forecast["product"] == product]
        combined_dates = pd.concat([prior_seg["submission_date"], forecast_seg["submission_date"]])
        combined_dau = pd.concat([prior_seg["dau_28_ma"], forecast_seg["dau_28_ma"]])
        if INSERT_JUL_HANDOFF_GAP:
            combined_dau = combined_dau.mask(combined_dates == PRIOR_GAP_DATE)
        blocks.append(
            _make_rows(
                combined_dates, product, "FUTURE prior forecasts", combined_dau, PUBLISH_DATE
            )
        )
    return pd.concat(blocks, ignore_index=True)


def _check_future_rows(future: pd.DataFrame, sheet: pd.DataFrame) -> None:
    """Fail loudly if the appended draft cycle is not shaped the way the tab expects."""
    forecast = future[future["forecast_name"] == "FUTURE forecast"]
    prior = future[future["forecast_name"] == "FUTURE prior forecasts"]

    for product, expected in EXPECTED_DEC15.items():
        got = forecast.loc[
            (forecast["product"] == product) & (forecast["submission_date"] == DEC15), "dau_28_ma"
        ]
        if len(got) != 1 or int(round(got.iloc[0])) != expected:
            raise ValueError(
                f"Dec-15 {product} mismatch: expected {expected:,}, got "
                f"{[int(round(v)) for v in got]} — august_canonical_curves.csv moved, or "
                f"the wrong column was read"
            )

    for product in PRODUCT_ORDER:
        # Forecast line: seam -> Dec 31, no holes.
        fp = forecast[forecast["product"] == product]
        expected_days = pd.date_range(FORECAST_START, "2026-12-31")
        if not fp["submission_date"].reset_index(drop=True).equals(pd.Series(expected_days)):
            raise ValueError(
                f"FUTURE forecast {product}: dates are not {FORECAST_START.date()}..2026-12-31 "
                f"(n={len(fp)}, expected {len(expected_days)})"
            )
        if fp["dau_28_ma"].isna().any():
            raise ValueError(f"FUTURE forecast {product}: {fp['dau_28_ma'].isna().sum()} null DAU")

        # Prior line: Jan 1 -> seam-1, nulls only at the inherited junctions.
        pp = prior[prior["product"] == product]
        expected_days = pd.date_range("2026-01-01", PRIOR_BOUNDARY)
        if not pp["submission_date"].reset_index(drop=True).equals(pd.Series(expected_days)):
            raise ValueError(
                f"FUTURE prior forecasts {product}: dates are not 2026-01-01.."
                f"{PRIOR_BOUNDARY.date()} (n={len(pp)}, expected {len(expected_days)})"
            )
        gaps = set(pp.loc[pp["dau_28_ma"].isna(), "submission_date"].dt.strftime("%Y-%m-%d"))
        expected_gaps = INHERITED_PRIOR_GAPS | (
            {PRIOR_GAP_DATE.strftime("%Y-%m-%d")} if INSERT_JUL_HANDOFF_GAP else set()
        )
        if gaps != expected_gaps:
            raise ValueError(
                f"FUTURE prior forecasts {product}: gap days {sorted(gaps)} != "
                f"expected {sorted(expected_gaps)}"
            )

        # The Jul 6 -> Aug 1 tail must reproduce July's published curve exactly.
        tail = pp[pp["submission_date"] >= PREV_FORECAST_START].set_index("submission_date")[
            "dau_28_ma"
        ]
        source = sheet[
            (sheet["forecast_name"] == "CURRENT forecast")
            & (sheet["product"] == product)
            & (sheet["submission_date"].between(PREV_FORECAST_START, PRIOR_BOUNDARY))
        ].set_index("submission_date")["dau_28_ma"]
        if not tail.equals(source):
            raise ValueError(
                f"FUTURE prior forecasts {product}: Jul 6 -> Aug 1 segment does not match "
                f"the tab's CURRENT forecast rows"
            )


def main() -> None:
    sheet = pd.read_csv(CURRENT_TAB_CSV, parse_dates=["submission_date", "created_on", "updated_on"])
    curves = pd.read_csv(AUGUST_CURVES, parse_dates=["date"])

    if set(sheet["forecast_name"]) & set(FUTURE_NAMES):
        raise ValueError(
            f"Source tab already contains {sorted(set(sheet['forecast_name']) & set(FUTURE_NAMES))} "
            f"— a previous draft was pasted in. Remove those rows before rebuilding."
        )

    august_forecasts = build_august_forecast_rows(curves)
    august_prior = build_august_prior_rows(sheet)
    future = pd.concat([august_prior, august_forecasts], ignore_index=True)
    _check_future_rows(future, sheet)

    # Every existing row carries over untouched — nothing is renamed or demoted.
    out = pd.concat([sheet, future], ignore_index=True)

    unknown = set(out["forecast_name"]) - set(FORECAST_NAME_ORDER)
    if unknown:
        raise ValueError(f"forecast_name values missing from FORECAST_NAME_ORDER: {sorted(unknown)}")
    out["forecast_name"] = pd.Categorical(out["forecast_name"], FORECAST_NAME_ORDER, ordered=True)
    out["product"] = pd.Categorical(out["product"], PRODUCT_ORDER, ordered=True)
    out = out[OUTPUT_COLUMNS].sort_values(
        ["forecast_name", "product", "submission_date"], kind="stable"
    ).reset_index(drop=True)

    # Integer DAU (nullable, so the NO-forecast blanks stay blank), matching the sheet's
    # presentation (no trailing .0).
    out["dau_28_ma"] = out["dau_28_ma"].round().astype("Int64")

    # Dates as YYYY-MM-DD (no time component) to match the workbook's presentation.
    for date_col in ("submission_date", "created_on", "updated_on"):
        out[date_col] = pd.to_datetime(out[date_col]).dt.strftime("%Y-%m-%d")

    # The whole point of a draft: the official rows must be field-for-field what came in,
    # in the same order, so the tab can be replaced wholesale without touching the
    # dashboard. (Compared as parsed text, not as bytes — the export is CRLF, we write LF.)
    original = pd.read_csv(CURRENT_TAB_CSV, dtype=str).fillna("")
    carried = out.head(len(original)).astype(str).replace("<NA>", "").reset_index(drop=True)
    if not carried.equals(original.reset_index(drop=True)):
        raise ValueError(
            "The first {n} output rows are not identical to the source tab — appending the "
            "FUTURE cycle disturbed existing rows".format(n=len(original))
        )

    out.to_csv(OUTPUT_CSV, index=False)
    out[out["forecast_name"].isin(FUTURE_NAMES)].to_csv(OUTPUT_FUTURE_ONLY_CSV, index=False)
    print(f"Wrote {len(out):,} rows -> {OUTPUT_CSV}")
    print(f"Wrote {len(future):,} FUTURE rows -> {OUTPUT_FUTURE_ONLY_CSV}")


if __name__ == "__main__":
    main()
