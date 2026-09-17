"""Fold a forecast cycle into the KPI workbook's "Official Forecast Data" tab — pure logic.

The tab (Google Sheet "2026 Firefox KPI Forecasts", loaded into
``mozdata.analysis.browser_kpi_forecasts_2026``) is a long table with one row per
``submission_date x product x forecast_name``. Each cycle is two lines per product:

* ``<LABEL> forecast``        — the cycle's published 28d-MA curve, seam .. Dec 31
* ``<LABEL> prior forecasts`` — Jan 1 .. seam-1, a splice of every earlier cycle's own
  as-published forecast over the window it was official (NOT actuals; the tab has none),
  with one blank day at each handoff (the day before the next cycle's seam) so the
  dashboard draws the vintages as separate segments.

The official cycle is aliased ``CURRENT``; superseded cycles carry a month label taken
from their ``created_on``. Two update schemes exist and both are here:

* **promote** (June, July 2026): rename ``CURRENT *`` to its month label, install the new
  cycle as ``CURRENT``.
* **draft** (August 2026): append the new cycle as ``FUTURE *``, touch nothing else.

Every prior-line value is copied from the sheet itself, never regenerated from a curve
file: the line means "what we were telling people at the time". I/O and the CLI live in
``scripts/build_kpi_sheet_update.py``; the checks in ``kpi_sheet_checks.py``.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

SHEET_COLUMNS = [
    "submission_date", "product", "forecast_name", "year", "quarter",
    "created_on", "updated_on", "dau_28_ma",
]
DATE_COLUMNS = ("submission_date", "created_on", "updated_on")
PRODUCTS = ("desktop", "mobile")
CURRENT = "CURRENT"
FUTURE = "FUTURE"
FORECAST_KIND = "forecast"
PRIOR_KIND = "prior forecasts"
# Within one label the sheet orders prior -> forecast -> any variant (whose kind starts
# with "z" so it sorts last in the workbook too).
KIND_ORDER = {PRIOR_KIND: 0, FORECAST_KIND: 1}


@dataclass(frozen=True)
class UpdatePlan:
    """The decisions one update needs; everything else is derived from the sheet and curves."""

    seam: pd.Timestamp                       # new cycle's forecast start
    publish_date: pd.Timestamp               # created_on / updated_on of the new rows
    curve_columns: dict[str, str]            # product -> column of the canonical curves CSV
    install_as: str = CURRENT                # CURRENT (promote) or FUTURE (draft)
    demote_to: str | None = None             # label the outgoing CURRENT takes; None in draft mode
    renames: dict[str, str] = field(default_factory=dict)  # other label fixes, e.g. {"AUG": "JUL"}
    expected_dec15: dict[str, int] = field(default_factory=dict)  # product -> locked Dec-15 value

    @property
    def is_draft(self) -> bool:
        return self.install_as != CURRENT

    @property
    def prior_boundary(self) -> pd.Timestamp:
        return self.seam - pd.Timedelta(days=1)

    def label_mapping(self) -> dict[str, str]:
        """Every label rename this update applies, outgoing CURRENT included."""
        mapping = dict(self.renames)
        if not self.is_draft:
            if not self.demote_to:
                raise ValueError("promotion needs demote_to (the label the outgoing CURRENT takes)")
            mapping[CURRENT] = self.demote_to
        return mapping


def read_sheet_export(path) -> pd.DataFrame:
    """Read the tab's CSV export with typed dates and a float DAU (blank -> NaN)."""
    sheet = pd.read_csv(path, dtype={"product": str, "forecast_name": str})
    missing = [c for c in SHEET_COLUMNS if c not in sheet.columns]
    if missing:
        raise ValueError(f"sheet export is missing columns {missing}; expected {SHEET_COLUMNS}")
    for column in DATE_COLUMNS:
        sheet[column] = pd.to_datetime(sheet[column])
    sheet["dau_28_ma"] = pd.to_numeric(sheet["dau_28_ma"])
    return sheet[SHEET_COLUMNS]


def split_name(forecast_name: str) -> tuple[str, str]:
    """'AUG prior forecasts' -> ('AUG', 'prior forecasts'); 'NO forecast' -> ('NO', 'forecast')."""
    label, _, kind = forecast_name.partition(" ")
    return label, kind


def month_label(created_on: pd.Timestamp) -> str:
    """The workbook's label for a superseded cycle: the month of its created_on, e.g. AUG."""
    return created_on.strftime("%b").upper()


def block_inventory(sheet: pd.DataFrame) -> pd.DataFrame:
    """One row per (forecast_name, product): span, row count, blank days, vintage."""
    rows = []
    for (name, product), block in sheet.groupby(["forecast_name", "product"], sort=False):
        blanks = block.loc[block["dau_28_ma"].isna(), "submission_date"]
        rows.append({
            "forecast_name": name, "product": product, "rows": len(block),
            "start": block["submission_date"].min().date(), "end": block["submission_date"].max().date(),
            "blank_days": ", ".join(d.strftime("%m-%d") for d in blanks) if len(blanks) <= 6 else f"{len(blanks)} (all)",
            "created_on": block["created_on"].iloc[0].date(),
        })
    return pd.DataFrame(rows)


def outgoing_forecast_start(sheet: pd.DataFrame, label: str = CURRENT) -> pd.Timestamp:
    """The seam of the cycle currently carrying `label` (first day of its forecast line)."""
    rows = sheet[sheet["forecast_name"] == f"{label} {FORECAST_KIND}"]
    if rows.empty:
        raise ValueError(f"sheet has no '{label} {FORECAST_KIND}' rows")
    return rows["submission_date"].min()


def outgoing_created_on(sheet: pd.DataFrame, label: str = CURRENT) -> pd.Timestamp:
    rows = sheet[sheet["forecast_name"] == f"{label} {FORECAST_KIND}"]
    return rows["created_on"].iloc[0]


def rename_labels(sheet: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """Swap the label prefix of every forecast_name whose label is in `mapping`; values untouched."""
    if not mapping:
        return sheet.copy()
    labels = sheet["forecast_name"].map(lambda n: split_name(n)[0])
    kinds = sheet["forecast_name"].map(lambda n: split_name(n)[1])
    absent = sorted(set(mapping) - set(labels))
    if absent:
        raise ValueError(f"rename source labels not in sheet: {absent}")
    collisions = sorted(set(mapping.values()) & (set(labels) - set(mapping)))
    if collisions:
        raise ValueError(f"rename targets already used by other blocks: {collisions}")
    out = sheet.copy()
    out["forecast_name"] = labels.map(lambda l: mapping.get(l, l)) + " " + kinds
    return out


def _make_rows(dates, product: str, forecast_name: str, dau, publish_date: pd.Timestamp,
               year: int, quarter: int) -> pd.DataFrame:
    return pd.DataFrame({
        "submission_date": pd.to_datetime(pd.Series(dates).values), "product": product,
        "forecast_name": forecast_name, "year": year, "quarter": quarter,
        "created_on": publish_date, "updated_on": publish_date,
        "dau_28_ma": pd.Series(dau).astype(float).values,
    })


def sheet_constants(sheet: pd.DataFrame) -> tuple[int, int]:
    """`year` and `quarter` are one value for the whole tab; read them rather than assume."""
    years, quarters = sheet["year"].unique(), sheet["quarter"].unique()
    if len(years) != 1 or len(quarters) != 1:
        raise ValueError(f"expected one year/quarter across the tab, got years={years} quarters={quarters}")
    return int(years[0]), int(quarters[0])


def build_forecast_rows(curves: pd.DataFrame, plan: UpdatePlan, year: int, quarter: int) -> pd.DataFrame:
    """The new `<install_as> forecast` line per product: seam .. end of the curve file."""
    horizon = curves[curves["date"] >= plan.seam]
    blocks = []
    for product, column in plan.curve_columns.items():
        if column not in curves.columns:
            raise ValueError(f"curves file has no column {column!r}; columns: {list(curves.columns)}")
        series = horizon[["date", column]].dropna(subset=[column])
        blocks.append(_make_rows(series["date"], product, f"{plan.install_as} {FORECAST_KIND}",
                                 series[column], plan.publish_date, year, quarter))
    return pd.concat(blocks, ignore_index=True)


def build_prior_rows(sheet: pd.DataFrame, plan: UpdatePlan, year: int, quarter: int,
                     source_label: str = CURRENT) -> pd.DataFrame:
    """The new `<install_as> prior forecasts` line: the outgoing cycle's prior line, then its
    own forecast up to seam-1, with the day before the outgoing seam blanked (the handoff gap).
    Both pieces are copied from the sheet as published."""
    prev_seam = outgoing_forecast_start(sheet, source_label)
    gap_date = prev_seam - pd.Timedelta(days=1)
    prior = sheet[sheet["forecast_name"] == f"{source_label} {PRIOR_KIND}"]
    forecast = sheet[(sheet["forecast_name"] == f"{source_label} {FORECAST_KIND}")
                     & (sheet["submission_date"] <= plan.prior_boundary)]
    blocks = []
    for product in PRODUCTS:
        prior_seg = prior[prior["product"] == product]
        forecast_seg = forecast[forecast["product"] == product]
        dates = pd.concat([prior_seg["submission_date"], forecast_seg["submission_date"]])
        dau = pd.concat([prior_seg["dau_28_ma"], forecast_seg["dau_28_ma"]]).mask(dates == gap_date)
        blocks.append(_make_rows(dates, product, f"{plan.install_as} {PRIOR_KIND}", dau,
                                 plan.publish_date, year, quarter))
    return pd.concat(blocks, ignore_index=True)


def order_rows(out: pd.DataFrame, label_order: list[str]) -> pd.DataFrame:
    """Sheet order: cycles chronologically (as they appear), prior -> forecast -> variants within a
    cycle, desktop before mobile, dates ascending."""
    labels = out["forecast_name"].map(lambda n: split_name(n)[0])
    kinds = out["forecast_name"].map(lambda n: split_name(n)[1])
    unknown = sorted(set(labels) - set(label_order))
    if unknown:
        raise ValueError(f"labels missing from the order: {unknown}")
    keyed = out.assign(
        _label=labels.map(label_order.index),
        _kind=kinds.map(lambda k: KIND_ORDER.get(k, 2)),
        _product=out["product"].map(list(PRODUCTS).index),
    )
    keyed = keyed.sort_values(["_label", "_kind", "_product", "submission_date"], kind="stable")
    return keyed[SHEET_COLUMNS].reset_index(drop=True)


def assemble_update(sheet: pd.DataFrame, curves: pd.DataFrame, plan: UpdatePlan) -> pd.DataFrame:
    """Sheet + plan + curves -> the full replacement table, ordered, DAU as float (NaN = blank)."""
    year, quarter = sheet_constants(sheet)
    new_rows = pd.concat([build_prior_rows(sheet, plan, year, quarter),
                          build_forecast_rows(curves, plan, year, quarter)], ignore_index=True)
    carried = rename_labels(sheet, plan.label_mapping())
    label_order = list(dict.fromkeys(carried["forecast_name"].map(lambda n: split_name(n)[0])))
    if plan.install_as in label_order:
        raise ValueError(f"sheet already carries '{plan.install_as} *' rows — a previous update was pasted in")
    label_order.append(plan.install_as)
    return order_rows(pd.concat([carried, new_rows], ignore_index=True), label_order)


def format_for_sheet(out: pd.DataFrame) -> pd.DataFrame:
    """Text presentation the tab uses: YYYY-MM-DD dates, integer DAU with blanks left blank."""
    text = out.copy()
    text["dau_28_ma"] = text["dau_28_ma"].round().astype("Int64")
    for column in DATE_COLUMNS:
        text[column] = pd.to_datetime(text[column]).dt.strftime("%Y-%m-%d")
    return text
