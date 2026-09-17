"""Checks on an assembled KPI-sheet update, plus the numbers worth reporting about it.

Every check raises ``ValueError`` with the offending block named; `describe_update` returns
the seam step, handoff-gap step and Dec-15 values the run log and `_index.md` quote.
"""
from __future__ import annotations

import pandas as pd

from mozaic_daily.kpi_sheet import (
    CURRENT, FORECAST_KIND, PRIOR_KIND, PRODUCTS, SHEET_COLUMNS, UpdatePlan,
    outgoing_forecast_start, rename_labels,
)

KEY = ["submission_date", "product", "forecast_name"]
DEC15_MONTH_DAY = (12, 15)


def _line(frame: pd.DataFrame, name: str, product: str) -> pd.DataFrame:
    return frame[(frame["forecast_name"] == name) & (frame["product"] == product)].sort_values("submission_date")


def check_carried_rows(out: pd.DataFrame, sheet: pd.DataFrame, plan: UpdatePlan) -> None:
    """Every input row survives field-for-field (labels renamed per the plan); in draft mode
    the input rows also come first and in their original order."""
    expected = rename_labels(sheet, plan.label_mapping())
    if plan.is_draft:
        head = out.head(len(sheet)).reset_index(drop=True)
        if not head.equals(expected.reset_index(drop=True)):
            raise ValueError("draft mode disturbed existing rows: the first "
                             f"{len(sheet)} output rows are not the input in order")
        return
    merged = expected.merge(out, on=KEY, how="left", suffixes=("_in", "_out"), indicator=True)
    lost = merged[merged["_merge"] != "both"]
    if len(lost):
        raise ValueError(f"{len(lost)} input rows missing from the output, e.g. {lost[KEY].head(3).values.tolist()}")
    for column in [c for c in SHEET_COLUMNS if c not in KEY]:
        a, b = merged[f"{column}_in"], merged[f"{column}_out"]
        changed = ~((a == b) | (a.isna() & b.isna()))
        if changed.any():
            raise ValueError(f"{int(changed.sum())} carried rows changed in {column!r}, "
                             f"e.g. {merged.loc[changed, KEY].head(3).values.tolist()}")


def check_no_duplicates(out: pd.DataFrame) -> None:
    dupes = out[out.duplicated(KEY, keep=False)]
    if len(dupes):
        raise ValueError(f"{len(dupes)} duplicate (date, product, forecast_name) rows, "
                         f"e.g. {dupes[KEY].head(3).values.tolist()}")


def check_new_forecast_line(out: pd.DataFrame, curves: pd.DataFrame, plan: UpdatePlan) -> None:
    """Seam .. last curve date, no blanks, Dec-15 equal to the locked literal."""
    expected_days = pd.Series(pd.date_range(plan.seam, curves["date"].max()))
    for product in plan.curve_columns:
        line = _line(out, f"{plan.install_as} {FORECAST_KIND}", product)
        if not line["submission_date"].reset_index(drop=True).equals(expected_days):
            raise ValueError(f"{plan.install_as} forecast {product}: dates are not "
                             f"{plan.seam.date()}..{expected_days.iloc[-1].date()} (n={len(line)})")
        if line["dau_28_ma"].isna().any():
            raise ValueError(f"{plan.install_as} forecast {product}: {int(line['dau_28_ma'].isna().sum())} blank DAU")
        if product in plan.expected_dec15:
            got = dec15_value(line)
            if got != plan.expected_dec15[product]:
                raise ValueError(f"Dec-15 {product} mismatch: expected {plan.expected_dec15[product]:,}, "
                                 f"got {got:,} — the curves file moved or the wrong column was read")


def check_new_prior_line(out: pd.DataFrame, sheet: pd.DataFrame, plan: UpdatePlan,
                         source_label: str = CURRENT) -> None:
    """Jan 1 .. seam-1; blanks are exactly the inherited handoffs plus the new one; the tail
    reproduces the outgoing forecast as published."""
    prev_seam = outgoing_forecast_start(sheet, source_label)
    gap_date = prev_seam - pd.Timedelta(days=1)
    for product in PRODUCTS:
        old_prior = _line(sheet, f"{source_label} {PRIOR_KIND}", product)
        old_forecast = _line(sheet, f"{source_label} {FORECAST_KIND}", product)
        line = _line(out, f"{plan.install_as} {PRIOR_KIND}", product)
        expected_days = pd.Series(pd.date_range(old_prior["submission_date"].min(), plan.prior_boundary))
        if not line["submission_date"].reset_index(drop=True).equals(expected_days):
            raise ValueError(f"{plan.install_as} prior forecasts {product}: dates are not "
                             f"{expected_days.iloc[0].date()}..{plan.prior_boundary.date()} (n={len(line)})")
        gaps = set(line.loc[line["dau_28_ma"].isna(), "submission_date"])
        expected_gaps = set(old_prior.loc[old_prior["dau_28_ma"].isna(), "submission_date"]) | {gap_date}
        if gaps != expected_gaps:
            raise ValueError(f"{plan.install_as} prior forecasts {product}: blank days "
                             f"{sorted(d.date() for d in gaps)} != expected {sorted(d.date() for d in expected_gaps)}")
        tail = line[line["submission_date"] >= prev_seam].set_index("submission_date")["dau_28_ma"]
        source = old_forecast[old_forecast["submission_date"] <= plan.prior_boundary]
        source = source.set_index("submission_date")["dau_28_ma"]
        if not tail.equals(source):
            raise ValueError(f"{plan.install_as} prior forecasts {product}: the {prev_seam.date()}.."
                             f"{plan.prior_boundary.date()} tail does not match the outgoing forecast rows")


def check_update(out: pd.DataFrame, sheet: pd.DataFrame, curves: pd.DataFrame, plan: UpdatePlan) -> None:
    check_no_duplicates(out)
    check_carried_rows(out, sheet, plan)
    check_new_forecast_line(out, curves, plan)
    check_new_prior_line(out, sheet, plan)


def dec15_value(line: pd.DataFrame) -> int:
    dates = line["submission_date"]
    at = line[(dates.dt.month == DEC15_MONTH_DAY[0]) & (dates.dt.day == DEC15_MONTH_DAY[1])]
    if len(at) != 1:
        raise ValueError(f"expected one Dec-15 row, found {len(at)}")
    return int(round(at["dau_28_ma"].iloc[0]))


def describe_update(out: pd.DataFrame, sheet: pd.DataFrame, plan: UpdatePlan) -> dict:
    """Numbers for the log and the cycle index: Dec-15s, the two steps per product, row counts."""
    prev_seam = outgoing_forecast_start(sheet)
    gap_date = prev_seam - pd.Timedelta(days=1)
    per_product = {}
    for product in PRODUCTS:
        forecast = _line(out, f"{plan.install_as} {FORECAST_KIND}", product).set_index("submission_date")["dau_28_ma"]
        prior = _line(out, f"{plan.install_as} {PRIOR_KIND}", product).set_index("submission_date")["dau_28_ma"]
        before_gap, after_gap = prior.get(gap_date - pd.Timedelta(days=1)), prior.get(prev_seam)
        per_product[product] = {
            "dec15": dec15_value(forecast.reset_index()),
            "outgoing_dec15": dec15_value(_line(sheet, f"{CURRENT} {FORECAST_KIND}", product)),
            "seam_step": int(round(forecast.iloc[0] - prior.iloc[-1])),
            "handoff_gap_step": int(round(after_gap - before_gap)),
            "forecast_rows": int(len(forecast)), "prior_rows": int(len(prior)),
        }
    return {
        "seam": plan.seam.date().isoformat(), "prior_boundary": plan.prior_boundary.date().isoformat(),
        "outgoing_seam": prev_seam.date().isoformat(), "handoff_gap_date": gap_date.date().isoformat(),
        "publish_date": plan.publish_date.date().isoformat(), "install_as": plan.install_as,
        "label_mapping": plan.label_mapping(), "rows_in": int(len(sheet)), "rows_out": int(len(out)),
        "products": per_product,
    }
