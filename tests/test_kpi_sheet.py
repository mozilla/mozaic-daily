"""Tests for `mozaic_daily.kpi_sheet` / `kpi_sheet_checks`: folding a cycle into the KPI tab.

A small synthetic tab (three vintages, full-year daily rows) exercises the promote and draft
schemes end to end; two regression tests reproduce the July 2026 promotion and the August 2026
draft byte-for-byte from the Sheets exports and outputs still on disk, and skip when absent.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from mozaic_daily.kpi_sheet import (
    CURRENT, FUTURE, UpdatePlan, assemble_update, block_inventory, format_for_sheet, month_label,
    read_sheet_export, rename_labels, split_name,
)
from mozaic_daily.kpi_sheet_checks import (
    check_carried_rows, check_new_prior_line, check_update, describe_update,
)

YEAR_DAYS = pd.date_range("2026-01-01", "2026-12-31")
OUTGOING_SEAM = pd.Timestamp("2026-02-10")   # mid-month, so the handoff gap must be inserted
NEW_SEAM = pd.Timestamp("2026-03-10")
DOWNLOADS = Path.home() / "Downloads"
REPO = Path(__file__).resolve().parents[1]


def _block(name, product, dates, values, created):
    return pd.DataFrame({"submission_date": dates, "product": product, "forecast_name": name, "year": 2026,
                         "quarter": 1, "created_on": pd.Timestamp(created), "updated_on": pd.Timestamp(created),
                         "dau_28_ma": values})


@pytest.fixture
def sheet() -> pd.DataFrame:
    """NO forecast, a JAN cycle, and the outgoing CURRENT cycle (seam Feb 10, created Feb 13),
    in the tab's own order: block by block, desktop before mobile."""
    bases = {"desktop": 40_000_000.0, "mobile": 15_000_000.0}
    prior_days = pd.date_range("2026-01-01", OUTGOING_SEAM - pd.Timedelta(days=1))
    fc_days = pd.date_range(OUTGOING_SEAM, "2026-12-31")
    blocks = []
    for product in ("desktop", "mobile"):
        blocks.append(_block("NO forecast", product, YEAR_DAYS, float("nan"), "2026-01-06"))
    for product, base in bases.items():
        blocks.append(_block("JAN forecast", product, YEAR_DAYS, base + pd.Series(range(365), dtype=float).values, "2026-01-06"))
    for product, base in bases.items():
        prior_vals = pd.Series(base + 10.0 * pd.Series(range(len(prior_days))).values)
        prior_vals[prior_days == pd.Timestamp("2026-01-31")] = float("nan")   # an inherited handoff blank
        blocks.append(_block("CURRENT prior forecasts", product, prior_days, prior_vals.values, "2026-02-13"))
    for product, base in bases.items():
        blocks.append(_block("CURRENT forecast", product, fc_days, base + 500_000.0 + pd.Series(range(len(fc_days))).values, "2026-02-13"))
    return pd.concat(blocks, ignore_index=True)


@pytest.fixture
def curves() -> pd.DataFrame:
    frame = pd.DataFrame({"date": YEAR_DAYS})
    for product, base in (("desktop", 41_000_000.0), ("mobile", 15_600_000.0)):
        col = pd.Series(base + 3.0 * pd.Series(range(365)).values, index=YEAR_DAYS)
        col[col.index < NEW_SEAM] = float("nan")
        frame[f"{product}_current_march"] = col.values
        frame[f"{product}_actuals"] = 1.0
    return frame


def _plan(**overrides) -> UpdatePlan:
    fields = dict(seam=NEW_SEAM, publish_date=pd.Timestamp("2026-03-12"),
                  curve_columns={"desktop": "desktop_current_march", "mobile": "mobile_current_march"},
                  demote_to="FEB")
    fields.update(overrides)
    return UpdatePlan(**fields)


def _line(frame, name, product):
    return frame[(frame["forecast_name"] == name) & (frame["product"] == product)].set_index("submission_date")["dau_28_ma"]


def test_promotion_renames_outgoing_and_installs_current(sheet, curves):
    out = assemble_update(sheet, curves, _plan())
    check_update(out, sheet, curves, _plan())
    names = list(dict.fromkeys(out["forecast_name"]))
    assert names == ["NO forecast", "JAN forecast", "FEB prior forecasts", "FEB forecast",
                     "CURRENT prior forecasts", "CURRENT forecast"]
    # Demoted rows keep their values and their February vintage.
    assert _line(out, "FEB forecast", "desktop").equals(_line(sheet, "CURRENT forecast", "desktop"))
    assert (out.loc[out["forecast_name"] == "FEB forecast", "created_on"] == pd.Timestamp("2026-02-13")).all()
    # New forecast line: seam .. Dec 31 from the curve column, stamped with the publish date.
    new_fc = _line(out, "CURRENT forecast", "mobile")
    assert new_fc.index[0] == NEW_SEAM and new_fc.index[-1] == pd.Timestamp("2026-12-31")
    assert new_fc.loc["2026-12-15"] == curves.set_index("date").loc["2026-12-15", "mobile_current_march"]
    assert (out.loc[out["forecast_name"] == "CURRENT forecast", "created_on"] == pd.Timestamp("2026-03-12")).all()


def test_prior_line_is_spliced_from_sheet_with_handoff_gap(sheet, curves):
    out = assemble_update(sheet, curves, _plan())
    prior = _line(out, "CURRENT prior forecasts", "desktop")
    assert prior.index[0] == pd.Timestamp("2026-01-01") and prior.index[-1] == NEW_SEAM - pd.Timedelta(days=1)
    blanks = set(prior[prior.isna()].index)
    assert blanks == {pd.Timestamp("2026-01-31"), OUTGOING_SEAM - pd.Timedelta(days=1)}
    tail = prior[prior.index >= OUTGOING_SEAM]
    source = _line(sheet, "CURRENT forecast", "desktop")
    assert tail.equals(source[source.index < NEW_SEAM])
    head = prior[prior.index < OUTGOING_SEAM - pd.Timedelta(days=1)]
    assert head.equals(_line(sheet, "CURRENT prior forecasts", "desktop").iloc[:-1])


def test_dec15_lock_catches_a_moved_curve(sheet, curves):
    plan = _plan(expected_dec15={"desktop": 1, "mobile": 2})
    out = assemble_update(sheet, curves, plan)
    with pytest.raises(ValueError, match="Dec-15 desktop mismatch"):
        check_update(out, sheet, curves, plan)


def test_carried_rows_check_catches_a_mutated_value(sheet, curves):
    plan = _plan()
    out = assemble_update(sheet, curves, plan)
    idx = out.index[(out["forecast_name"] == "JAN forecast") & (out["product"] == "mobile")][5]
    out.loc[idx, "dau_28_ma"] += 1
    with pytest.raises(ValueError, match="carried rows changed in 'dau_28_ma'"):
        check_carried_rows(out, sheet, plan)


def test_prior_line_check_catches_a_missing_gap(sheet, curves):
    plan = _plan()
    out = assemble_update(sheet, curves, plan)
    gap_rows = (out["forecast_name"] == "CURRENT prior forecasts") & (out["submission_date"] == OUTGOING_SEAM - pd.Timedelta(days=1))
    out.loc[gap_rows, "dau_28_ma"] = 1.0
    with pytest.raises(ValueError, match="blank days"):
        check_new_prior_line(out, sheet, plan)


def test_extra_rename_fixes_a_mislabelled_block(sheet, curves):
    out = assemble_update(sheet, curves, _plan(renames={"JAN": "DEC"}))
    assert "DEC forecast" in set(out["forecast_name"]) and "JAN forecast" not in set(out["forecast_name"])
    assert _line(out, "DEC forecast", "desktop").equals(_line(sheet, "JAN forecast", "desktop"))


def test_rename_rejects_absent_source_and_collision(sheet):
    with pytest.raises(ValueError, match="not in sheet"):
        rename_labels(sheet, {"SEP": "OCT"})
    with pytest.raises(ValueError, match="already used"):
        rename_labels(sheet, {"CURRENT": "JAN"})


def test_promotion_without_demote_label_raises(sheet, curves):
    with pytest.raises(ValueError, match="demote_to"):
        assemble_update(sheet, curves, _plan(demote_to=None))


def test_refuses_when_install_label_already_present(sheet, curves):
    with pytest.raises(ValueError, match="already carries"):
        assemble_update(sheet, curves, _plan(install_as="JAN", demote_to="FEB"))


def test_draft_mode_appends_future_and_leaves_input_in_place(sheet, curves):
    plan = _plan(install_as=FUTURE, demote_to=None)
    out = assemble_update(sheet, curves, plan)
    check_update(out, sheet, curves, plan)
    assert out.head(len(sheet)).reset_index(drop=True).equals(sheet.reset_index(drop=True))
    assert set(out["forecast_name"].iloc[len(sheet):]) == {"FUTURE prior forecasts", "FUTURE forecast"}
    assert "CURRENT forecast" in set(out["forecast_name"])


def test_describe_update_reports_steps_and_dec15(sheet, curves):
    plan = _plan()
    out = assemble_update(sheet, curves, plan)
    info = describe_update(out, sheet, plan)
    assert info["handoff_gap_date"] == "2026-02-09" and info["outgoing_seam"] == "2026-02-10"
    assert info["label_mapping"] == {CURRENT: "FEB"}
    d = info["products"]["desktop"]
    fc, prior = _line(out, "CURRENT forecast", "desktop"), _line(out, "CURRENT prior forecasts", "desktop")
    assert d["seam_step"] == int(round(fc.iloc[0] - prior.iloc[-1]))
    assert d["handoff_gap_step"] == int(round(prior.loc["2026-02-10"] - prior.loc["2026-02-08"]))
    assert d["dec15"] == int(round(fc.loc["2026-12-15"]))


def test_format_for_sheet_keeps_blanks_and_drops_decimals(sheet, curves):
    text = format_for_sheet(assemble_update(sheet, curves, _plan()))
    no_rows = text[text["forecast_name"] == "NO forecast"]["dau_28_ma"]
    assert no_rows.isna().all()
    written = text.loc[text["forecast_name"] == "CURRENT forecast", "dau_28_ma"].astype(str)
    assert not written.str.contains(r"\.").any()
    assert text["submission_date"].str.fullmatch(r"\d{4}-\d{2}-\d{2}").all()


def test_helpers():
    assert split_name("APR z forecast ex-Iran") == ("APR", "z forecast ex-Iran")
    assert month_label(pd.Timestamp("2026-08-10")) == "AUG"


def test_block_inventory_lists_blank_days(sheet):
    inv = block_inventory(sheet).set_index(["forecast_name", "product"])
    assert inv.loc[("CURRENT prior forecasts", "desktop"), "blank_days"] == "01-31"
    assert inv.loc[("CURRENT forecast", "mobile"), "rows"] == len(pd.date_range(OUTGOING_SEAM, "2026-12-31"))


# --- Regression against the real cycles still on disk ---------------------------------------

def _reproduce(export: Path, curves_path: Path, month: str, expected: Path, **plan_fields) -> None:
    if not (export.exists() and curves_path.exists() and expected.exists()):
        pytest.skip(f"regression inputs not on disk: {export.name}, {curves_path.name}, {expected.name}")
    sheet = read_sheet_export(export)
    curves = pd.read_csv(curves_path, parse_dates=["date"])
    columns = {p: f"{p}_current_{month}" for p in ("desktop", "mobile")}
    seam = min(curves.loc[curves[c].notna(), "date"].min() for c in columns.values())
    plan = UpdatePlan(seam=seam, curve_columns=columns, **plan_fields)
    out = assemble_update(sheet, curves, plan)
    check_update(out, sheet, curves, plan)
    got = format_for_sheet(out).astype(str).replace("<NA>", "").reset_index(drop=True)
    want = pd.read_csv(expected, dtype=str).fillna("").reset_index(drop=True)
    assert got.equals(want)


def test_reproduces_july_2026_promotion():
    _reproduce(DOWNLOADS / "2026 Firefox KPI Forecasts - Official Forecast Data.csv",
               REPO / "data-official/2026-07/csv/july_canonical_curves.csv", "july",
               REPO / "data-official/2026-07/kpi_sheet/official_forecast_data.2026-07-06.csv",
               publish_date=pd.Timestamp("2026-07-06"), demote_to="JUN")


def test_reproduces_august_2026_draft():
    _reproduce(DOWNLOADS / "2026 Firefox KPI Forecasts - Official Forecast Data(1).csv",
               REPO / "data-official/2026-08/csv/august_canonical_curves.csv", "august",
               REPO / "data-official/2026-08/kpi_sheet/official_forecast_data.2026-08-10.csv",
               publish_date=pd.Timestamp("2026-08-10"), install_as=FUTURE)
