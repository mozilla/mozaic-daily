# -*- coding: utf-8 -*-
"""Tests for the Glean-history adapter (``glean_history.py``), its holiday calendar and enum wiring.

Synthetic export frames only. The fixture deliberately gives each OS x country cell a distinct
level and includes roll-up rows that must be dropped, so a wrong flag assignment or a leaked
roll-up changes a sum the tests check against a literal.
"""

import json
from datetime import date

import pandas as pd
import pytest

from mozaic_daily import glean_history as gh
from mozaic_daily.glean_history_holidays import LegacyOutage2025, LEGACY_OUTAGE_WINDOWS_2025
from mozaic_daily.data import get_queries
from mozaic_daily.queries import ADDITIONAL_HOLIDAYS, DataSource, Metric, QUERY_SPECS


def _export(days=3, start="2020-01-01", countries=("us", "de", "ROW")):
    """Cells with level = 100*os_rank + country_rank (+ day), plus consistent {os}_total roll-ups."""
    dates = pd.date_range(start, periods=days, freq="D")
    os_rank = {"win": 1, "mac": 2, "linux": 3}
    rows = []
    for d_i, d in enumerate(dates):
        totals = {}
        for prefix, r in os_rank.items():
            for c_i, c in enumerate(countries):
                y = 100 * r + c_i + d_i
                rows.append({"submission_date": d.date(), "cell": f"{prefix}_{c}", "glean_equivalent": float(y),
                             "legacy_dau": float(y) / 1.036})
                totals[prefix] = totals.get(prefix, 0.0) + y
        for prefix, t in totals.items():
            rows.append({"submission_date": d.date(), "cell": f"{prefix}_total", "glean_equivalent": t, "legacy_dau": t / 1.036})
        rows.append({"submission_date": d.date(), "cell": "global_all", "glean_equivalent": sum(totals.values()), "legacy_dau": 0.0})
    return pd.DataFrame(rows)


# ---- parse_cell ------------------------------------------------------------------------------

def test_parse_cell_maps_prefix_and_uppercases_country():
    assert gh.parse_cell("win_us") == gh.CellKey("windows", "US")
    assert gh.parse_cell("mac_ROW") == gh.CellKey("mac", "ROW")
    assert gh.parse_cell("linux_ir") == gh.CellKey("linux", "IR")


def test_parse_cell_rollups_are_none_and_unknown_prefix_raises():
    assert gh.parse_cell("global_ex_linux") is None
    assert gh.parse_cell("win_total") is None
    with pytest.raises(ValueError, match="Unrecognised export cell"):
        gh.parse_cell("android_us")


# ---- export_to_mozaic_frame ------------------------------------------------------------------

def test_frame_has_one_flag_per_row_and_drops_rollups():
    frame = gh.export_to_mozaic_frame(_export())
    assert list(frame.columns) == ["x", "country", "windows", "mac", "linux", "y"]
    assert str(frame["y"].dtype) == "int64"                 # whole people, as the overlay subtractor expects
    assert len(frame) == 3 * 3 * 3                      # days x OS x countries, no roll-ups
    assert (frame[["windows", "mac", "linux"]].sum(axis=1) == 1).all()
    assert set(frame["country"]) == {"US", "DE", "ROW"}


def test_frame_values_follow_the_os_flag_not_the_row_order():
    frame = gh.export_to_mozaic_frame(_export(days=1))
    us_mac = frame[(frame["country"] == "US") & frame["mac"]]["y"].item()
    assert us_mac == 200.0                               # 100*2 + country_rank 0 + day 0
    de_linux = frame[(frame["country"] == "DE") & frame["linux"]]["y"].item()
    assert de_linux == 301.0


def test_training_start_floor_is_applied():
    frame = gh.export_to_mozaic_frame(_export(days=5, start="2019-12-30"), training_start="2020-01-01")
    assert frame["x"].min() == pd.Timestamp("2020-01-01")
    assert frame["x"].max() == pd.Timestamp("2020-01-03")


def test_alternative_value_column_is_honoured():
    frame = gh.export_to_mozaic_frame(_export(days=1), value_column="legacy_dau")
    us_mac = frame[(frame["country"] == "US") & frame["mac"]]["y"].item()
    assert us_mac == 193                                 # round(200 / 1.036): whole people


def test_nan_value_raises():
    export = _export(days=1)
    export.loc[export["cell"] == "win_de", "glean_equivalent"] = float("nan")
    with pytest.raises(ValueError, match="NaN"):
        gh.export_to_mozaic_frame(export)


def test_missing_day_in_one_series_raises():
    export = _export(days=3)
    export = export[~((export["cell"] == "mac_de") & (export["submission_date"] == date(2020, 1, 2)))]
    with pytest.raises(ValueError, match="not dense"):
        gh.export_to_mozaic_frame(export)


# ---- check_sums_against_rollups --------------------------------------------------------------

def test_sums_match_rollups_and_a_perturbed_cell_is_caught():
    export = _export()
    frame = gh.export_to_mozaic_frame(export)
    diffs = gh.check_sums_against_rollups(frame, export)
    assert diffs == {"windows": 0.0, "mac": 0.0, "linux": 0.0}

    frame.loc[(frame["country"] == "US") & frame["windows"], "y"] += 5   # beyond the 0.5-per-tile rounding slack
    with pytest.raises(ValueError, match="windows: cells sum differs"):
        gh.check_sums_against_rollups(frame, export)


# ---- write_raw_checkpoints -------------------------------------------------------------------

def test_write_raw_checkpoints_writes_parquets_and_meta(tmp_path):
    export_dir = tmp_path / "export"
    export_dir.mkdir()
    _export().to_parquet(export_dir / gh.EXPORT_FILES["DAU"], index=False)
    _export().assign(glean_equivalent=lambda d: d["glean_equivalent"] * 10).to_parquet(
        export_dir / gh.EXPORT_FILES["MAU"], index=False)

    written = gh.write_raw_checkpoints(export_dir, tmp_path / "out")
    assert set(written) == {"DAU", "MAU"}
    assert written["DAU"].name == "mozaic_parts.raw.glean_history.desktop.DAU.parquet"
    mau = pd.read_parquet(written["MAU"])
    assert mau[(mau["country"] == "US") & mau["mac"] & (mau["x"] == "2020-01-01")]["y"].item() == 2000.0

    meta = json.loads((tmp_path / "out" / "glean_history_inputs.meta.json").read_text())
    assert meta["value_column"] == "glean_equivalent"
    assert meta["inputs"]["DAU"]["rows"] == 27
    assert len(meta["inputs"]["DAU"]["source_sha1"]) == 40
    assert meta["inputs"]["MAU"]["max_abs_diff_vs_os_total"] == {"windows": 0.0, "mac": 0.0, "linux": 0.0}


# ---- append_continuation -----------------------------------------------------------------------

def test_append_continuation_joins_exactly_and_keeps_history_columns():
    hist = _export(days=3, start="2020-01-01")
    cont = _export(days=2, start="2020-01-04")
    joined = gh.append_continuation(hist, cont)
    assert len(joined) == len(hist) + len(cont)
    assert list(joined.columns) == list(hist.columns)
    frame = gh.export_to_mozaic_frame(joined)
    assert frame["x"].max() == pd.Timestamp("2020-01-05")


def test_append_continuation_rejects_gap_overlap_and_cell_mismatch():
    hist = _export(days=3, start="2020-01-01")
    with pytest.raises(ValueError, match="day after"):
        gh.append_continuation(hist, _export(days=2, start="2020-01-05"))     # one-day gap
    with pytest.raises(ValueError, match="day after"):
        gh.append_continuation(hist, _export(days=2, start="2020-01-03"))     # overlap
    with pytest.raises(ValueError, match="cells differ"):
        gh.append_continuation(hist, _export(days=2, start="2020-01-04", countries=("us", "de")))


def test_write_raw_checkpoints_with_continuation(tmp_path):
    export_dir = tmp_path / "export"
    export_dir.mkdir()
    for metric in ("DAU", "MAU"):
        _export(days=3).to_parquet(export_dir / gh.EXPORT_FILES[metric], index=False)
        _export(days=2, start="2020-01-04").to_parquet(export_dir / gh.CONTINUATION_FILES[metric], index=False)
    written = gh.write_raw_checkpoints(export_dir, tmp_path / "out", with_continuation=True)
    dau = pd.read_parquet(written["DAU"])
    assert dau["x"].max() == pd.Timestamp("2020-01-05") and len(dau) == 5 * 9
    meta = json.loads((tmp_path / "out" / "glean_history_inputs.meta.json").read_text())
    assert len(meta["inputs"]["DAU"]["continuation_sha1"]) == 40


# ---- holiday calendar ------------------------------------------------------------------------

def test_legacy_outage_calendar_is_the_union_of_the_per_os_windows_in_2025_only():
    cal = LegacyOutage2025(years=[2024, 2025, 2026])
    days = sorted(cal.keys())
    assert days[0] == date(2025, 8, 16)                  # Windows window start
    assert days[-1] == date(2025, 12, 10)                # Mac window end
    assert len(days) == (date(2025, 12, 10) - date(2025, 8, 16)).days + 1
    assert all(d.year == 2025 for d in days)
    assert LEGACY_OUTAGE_WINDOWS_2025["windows"] == ("2025-08-16", "2025-11-06")
    assert LEGACY_OUTAGE_WINDOWS_2025["mac"] == ("2025-08-28", "2025-12-10")


# ---- enum wiring -----------------------------------------------------------------------------

def test_glean_history_source_is_desktop_glean_with_its_own_codes():
    ds = DataSource.GLEAN_HISTORY_DESKTOP
    assert ds.value == "glean_history_desktop"
    assert ds.short_code == "gh"
    assert ds.platform.value == "desktop"
    assert ds.telemetry_source.value == "glean"
    assert ds.display_name == "Desktop Glean-history"


def test_glean_history_source_has_no_query_spec_and_builds_no_sql():
    assert all(spec.data_source != DataSource.GLEAN_HISTORY_DESKTOP for spec in QUERY_SPECS.values())
    queries = get_queries("'US'", data_source_filter={DataSource.GLEAN_HISTORY_DESKTOP})
    assert all(not metrics for sources in queries.values() for metrics in sources.values())


def test_mau_metric_exists_with_short_code():
    assert Metric.MAU.value == "MAU"
    assert Metric.MAU.short_code == "M"


def test_glean_history_source_gets_the_outage_calendar_only():
    assert ADDITIONAL_HOLIDAYS[DataSource.GLEAN_HISTORY_DESKTOP] == [LegacyOutage2025]


# ---- project overlay loader (the `o` data correction) -------------------------------------------

def _overlay_spec(tmp_path, **overrides):
    import shutil
    spec_dir = tmp_path / "mozillaonline"
    spec_dir.mkdir()
    curve = pd.DataFrame({"date": pd.date_range("2026-06-01", "2027-12-31"), "mozillaonline_migration_dau_daily": 600000.0})
    curve.to_parquet(spec_dir / "curve.parquet", index=False)
    spec = {"type": "desktop_overlay", "platform": "desktop", "data_file": "curve.parquet",
            "value_column": "mozillaonline_migration_dau_daily",
            "allocation": {"key": "fixed_country_shares", "flag_column": "windows", "window_days": 28, "shares": {"CN": 0.93, "ROW": 0.07}},
            "scope": {"exclude_countries": ["IR"]}, "applies_to_forecast_start": "2026-10-10",
            "applies_to_data_source": "glean_history_desktop", "placeholder": False}
    spec.update(overrides)
    path = spec_dir / "mozillaonline.glean_history.json"
    path.write_text(json.dumps(spec))
    return path


def test_load_project_overlay_maps_directory_to_code_o(tmp_path):
    overlay = gh.load_project_overlay(_overlay_spec(tmp_path), "2026-10-10")
    assert overlay.code == "o"
    assert overlay.name == "mozillaonline_migration"
    assert overlay.flag_column == "windows"
    assert overlay.data_source.value == "glean_history_desktop"


@pytest.mark.parametrize("overrides, match", [
    ({"applies_to_forecast_start": "2026-09-09"}, "gated to"),
    ({"applies_to_data_source": "legacy_desktop"}, "applies to"),
    ({"allocation": {"key": "fixed_country_shares", "flag_column": "modern_windows", "window_days": 28, "shares": {"CN": 1.0}}}, "flag_column"),
])
def test_load_project_overlay_rejects_wrong_seam_source_or_flag(tmp_path, overrides, match):
    with pytest.raises(ValueError, match=match):
        gh.load_project_overlay(_overlay_spec(tmp_path, **overrides), "2026-10-10")


def test_production_resolver_cannot_see_the_glean_history_spec():
    """The draft's spec must never be picked up by a cycle run: its filename is outside the registry glob."""
    from mozaic_daily.overlays import resolve_overlays
    seen = {str(o.spec_path) for o in resolve_overlays("2026-10-10")}
    assert not any("2027-glean-draft" in p for p in seen), seen
