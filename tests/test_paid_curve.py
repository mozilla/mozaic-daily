"""`mozaic_daily.paid_curve` — the weekly GMIO widget rows -> daily paid-DAU level transform.

The regression oracle is the frozen September 2026 build: the 2026-09-04 CSV pushed through this
module must reproduce the `paid_dau_level_daily` column of
`marketing_lift_model.gmio_uac_meta_total.2026-09-02.parquet` exactly. (That file also carries the
retired lift columns; this module no longer writes them.)
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from mozaic_daily.paid_curve import (
    basis_slug, build_daily_table, check_contract, compose_weekly, curve_stem,
    daily_type_labels, feed_tables, interpolate_weekly_to_daily, key_values, resolve_template_params,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SEPTEMBER_MARKETING = REPO_ROOT / "data-official" / "2026-09" / "marketing"
FROZEN_CSV = SEPTEMBER_MARKETING / "source_data" / "gmio_paid_dau_total_all.20260904.csv"
FROZEN_PARQUET = SEPTEMBER_MARKETING / "marketing_lift_model.gmio_uac_meta_total.2026-09-02.parquet"


def widget_rows(n_actual: int = 3, n_forecast: int = 3, meta_from: int = 2) -> pd.DataFrame:
    """Synthetic four-column output: UAC every week, UAC+Meta from week `meta_from`, one-week handoff overlap."""
    mondays = pd.date_range("2026-01-05", periods=n_actual + n_forecast, freq="7D")
    rows = []
    for i, monday in enumerate(mondays):
        uac = 1000.0 + 100 * i
        meta_total = uac + 50 * (i - meta_from + 1) if i >= meta_from else None
        is_actual = i < n_actual
        is_edge = i == n_actual - 1
        rows.append({
            "date": monday.date().isoformat(),
            "uac_actual": uac if is_actual else None,
            "uac_forecast": uac if (not is_actual or is_edge) else None,
            "uac_meta_actual": meta_total if (is_actual and meta_total is not None) else None,
            "uac_meta_forecast": meta_total if (meta_total is not None and (not is_actual or is_edge)) else None,
        })
    return pd.DataFrame(rows)


class TestTemplateParams:
    def test_substitutes_both_widget_params(self):
        sql = "SELECT CASE '{{metric}}' WHEN 'x' THEN 1 END, '{{country}}' = 'All'"
        out = resolve_template_params(sql, {"metric": "Total Paid DAU", "country": "All"})
        assert "{{" not in out and "'Total Paid DAU'" in out and "'All' = 'All'" in out

    def test_unknown_template_raises_and_names_it(self):
        with pytest.raises(ValueError, match="channel"):
            resolve_template_params("WHERE c = '{{channel}}' AND m = '{{metric}}'", {"metric": "x"})

    def test_feed_tables_lists_backticked_tables(self):
        sql = "FROM `mozdata.analysis.ahe_gmio_weekly_paid_dau_views_20260909` d JOIN `a.b.c` x"
        assert feed_tables(sql) == ["a.b.c", "mozdata.analysis.ahe_gmio_weekly_paid_dau_views_20260909"]


class TestContract:
    def test_extra_or_missing_column_halts(self):
        with pytest.raises(ValueError, match="expected exactly columns"):
            check_contract(widget_rows().drop(columns=["uac_meta_forecast"]))
        with pytest.raises(ValueError, match="expected exactly columns"):
            check_contract(widget_rows().assign(p025=1.0))

    def test_non_monday_row_halts(self):
        raw = widget_rows()
        raw.loc[1, "date"] = "2026-01-13"
        with pytest.raises(ValueError, match="ISO Mondays"):
            check_contract(raw)

    def test_skipped_week_halts(self):
        raw = widget_rows().drop(index=2)
        with pytest.raises(ValueError, match="not consecutive"):
            check_contract(raw)

    def test_handoff_disagreement_halts(self):
        raw = widget_rows()
        edge = raw.dropna(subset=["uac_actual", "uac_forecast"]).index[0]
        raw.loc[edge, "uac_forecast"] = raw.loc[edge, "uac_actual"] + 1
        with pytest.raises(ValueError, match="disagree on the handoff"):
            check_contract(raw)

    def test_actual_after_forecast_halts(self):
        raw = widget_rows(n_actual=2, n_forecast=3)
        raw.loc[4, "uac_actual"] = raw.loc[4, "uac_forecast"]
        with pytest.raises(ValueError, match="actual week follows a forecast week"):
            compose_weekly(check_contract(raw))


class TestComposition:
    def test_prefers_uac_meta_then_uac_and_actual_over_forecast(self):
        weekly = compose_weekly(check_contract(widget_rows(n_actual=3, n_forecast=2, meta_from=2)))
        assert weekly["basis"].tolist() == ["uac_actual", "uac_actual", "uac_meta_actual", "uac_meta_forecast", "uac_meta_forecast"]
        assert weekly["is_actual"].tolist() == [True, True, True, False, False]
        assert weekly.loc[2, "paid_dau_used"] == weekly.loc[2, "uac_meta_actual"]

    def test_every_value_is_one_of_the_query_columns_verbatim(self):
        weekly = compose_weekly(check_contract(widget_rows()))
        for _, row in weekly.iterrows():
            assert row["paid_dau_used"] == row[row["basis"]]


class TestDaily:
    def test_linear_between_mondays_and_flat_after_the_last(self):
        weekly = compose_weekly(check_contract(widget_rows(n_actual=2, n_forecast=1, meta_from=99)))
        level = interpolate_weekly_to_daily(weekly, pd.Timestamp("2026-01-31"))
        assert level.loc["2026-01-08"] == pytest.approx(1000 + 100 * 3 / 7)
        assert level.loc["2026-01-19"] == 1200.0
        assert (level.loc["2026-01-20":] == 1200.0).all()
        assert level.index.name == "target_date" and level.index.max() == pd.Timestamp("2026-01-31")

    def test_daily_end_before_last_row_halts(self):
        weekly = compose_weekly(check_contract(widget_rows()))
        with pytest.raises(ValueError, match="before the last weekly row"):
            interpolate_weekly_to_daily(weekly, pd.Timestamp("2026-01-10"))

    def test_daily_table_is_the_level_verbatim_plus_its_ma_and_nothing_else(self):
        """No lift, no anchor: the column `p` reads equals the interpolated level to the float."""
        weekly = compose_weekly(check_contract(widget_rows(n_actual=4, n_forecast=2, meta_from=99)))
        level = interpolate_weekly_to_daily(weekly, pd.Timestamp("2026-02-28"))
        daily = build_daily_table(level)
        assert list(daily.columns) == ["paid_dau_level_daily", "paid_dau_level_ma"]
        pd.testing.assert_series_equal(daily["paid_dau_level_daily"], level, check_names=False)
        assert daily["paid_dau_level_ma"].loc["2026-02-28"] == pytest.approx(level.loc["2026-02-01":"2026-02-28"].mean())
        assert daily.index.name == "target_date"

    def test_key_values_are_levels_only(self):
        weekly = compose_weekly(check_contract(widget_rows(n_actual=4, n_forecast=2, meta_from=99)))
        level = interpolate_weekly_to_daily(weekly, pd.Timestamp("2026-12-31"))
        values = key_values(level, "2026-02-02")
        assert set(values) == {"level_at_seam", "level_dec15", "level_year_end"}
        assert values["level_at_seam"] == level.loc["2026-02-02"]
        assert values["level_dec15"] == level.loc["2026-12-15"] == values["level_year_end"]

    def test_type_labels_switch_after_last_actual_monday(self):
        weekly = compose_weekly(check_contract(widget_rows(n_actual=2, n_forecast=2, meta_from=99)))
        level = interpolate_weekly_to_daily(weekly, pd.Timestamp("2026-01-31"))
        labels = daily_type_labels(build_daily_table(level), weekly)
        assert labels.loc["2026-01-12"] == "actuals" and labels.loc["2026-01-13"] == "forecast"


class TestNaming:
    def test_stem_carries_basis_seam_and_pull_date(self):
        assert curve_stem("total", "2026-09-02", "2026-09-09") == "marketing_lift_model.gmio_uac_meta_total.2026-09-02.pull2026-09-09"

    def test_basis_slugs(self):
        assert basis_slug("Total Paid DAU") == "total"
        assert basis_slug("2026-acquired Paid DAU") == "current_year"
        assert basis_slug("Rolling 12-month Paid DAU") == "rolling_12mo"
        with pytest.raises(ValueError, match="flow"):
            basis_slug("Attributed New Profiles")


@pytest.mark.skipif(not FROZEN_PARQUET.exists(), reason="September 2026 marketing build not on disk")
def test_reproduces_the_frozen_september_2026_level_exactly():
    """The 2026-09-04 producer is the oracle: same CSV in, byte-identical level column out."""
    weekly = compose_weekly(check_contract(pd.read_csv(FROZEN_CSV)))
    level = interpolate_weekly_to_daily(weekly, pd.Timestamp("2026-12-31"))
    rebuilt = build_daily_table(level)
    frozen = pd.read_parquet(FROZEN_PARQUET)
    pd.testing.assert_series_equal(rebuilt["paid_dau_level_daily"], frozen["paid_dau_level_daily"], check_freq=False)
    values = key_values(level, "2026-09-02")
    assert values["level_dec15"] == pytest.approx(1891001.857142857, abs=1e-6)
    assert values["level_at_seam"] == pytest.approx(1633937, abs=1.0)
