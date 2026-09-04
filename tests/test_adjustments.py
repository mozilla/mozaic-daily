"""Tests for src/mozaic_daily/adjustments.py.

These tests cover the filename-marker convention (.raw. / .adj-{codes}.),
sidecar meta round-tripping, state-consistency validation, the composite
adjustment-rendering math (linear_ramp / step / daily_series), and the
per-tile marketing-lift applier (subtract from training / add back to
forecast).
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from mozaic_daily.adjustments import (
    add_lift_to_forecast,
    add_marketing_lift_to_forecast,
    adjustment_spec_hash,
    apply_net_adjustment_to_series,
    build_adjustments_applied_list,
    canonical_codes,
    compute_country_shares,
    compute_fenix_country_shares,
    fixed_country_shares_from_spec,
    insert_state_marker,
    load_adjustments_from_dir,
    load_code_registry,
    load_forecast,
    load_lift_series,
    load_marketing_lift_series,
    load_marketing_spec,
    load_overlay_spec,
    meta_path,
    parse_state_from_path,
    read_meta,
    render_adjustment,
    state_marker,
    subtract_lift_from_training,
    subtract_marketing_lift_from_training,
    write_meta,
)


# --- code canonicalization -------------------------------------------------

def test_canonical_codes_sorts():
    assert canonical_codes(["t", "h"]) == "ht"


def test_canonical_codes_dedupes():
    assert canonical_codes(["h", "t", "h"]) == "ht"


def test_canonical_codes_empty():
    assert canonical_codes([]) == ""


def test_state_marker_raw():
    assert state_marker([]) == "raw"


def test_state_marker_single():
    assert state_marker(["h"]) == "adj-h"


def test_state_marker_combined_alphabetical():
    assert state_marker(["t", "h"]) == "adj-ht"


# --- filename parsing ------------------------------------------------------

def test_parse_state_raw():
    assert parse_state_from_path("foo.2026-05-13.ld-D.raw.parquet") == []


def test_parse_state_single_adj():
    assert parse_state_from_path("foo.2026-05-13.ld-D.adj-h.parquet") == ["h"]


def test_parse_state_multi_adj():
    assert parse_state_from_path("foo.2026-05-13.ld-D.adj-ht.plus_iran.parquet") == ["h", "t"]


def test_parse_state_missing_marker_raises():
    with pytest.raises(ValueError, match="No state marker"):
        parse_state_from_path("foo.2026-05-13.ld-D.parquet")


# --- filename construction -------------------------------------------------

def test_insert_marker_plain():
    out = insert_state_marker("foo.2026-05-13.ld-D.parquet", ["h"])
    assert out.name == "foo.2026-05-13.ld-D.adj-h.parquet"


def test_insert_marker_with_plus_iran():
    out = insert_state_marker("foo.2026-05-13.ld-D.plus_iran.parquet", ["h"])
    assert out.name == "foo.2026-05-13.ld-D.adj-h.plus_iran.parquet"


def test_insert_marker_raw():
    out = insert_state_marker("foo.2026-05-13.ld-D.parquet", [])
    assert out.name == "foo.2026-05-13.ld-D.raw.parquet"


def test_insert_marker_canonical_order():
    out = insert_state_marker("foo.parquet", ["t", "h"])
    assert out.name == "foo.adj-ht.parquet"


def test_meta_path_inserts_meta_json():
    assert meta_path("foo.adj-h.parquet").name == "foo.adj-h.parquet.meta.json"
    assert meta_path("foo.raw.csv").name == "foo.raw.csv.meta.json"


# --- adjustment math -------------------------------------------------------

HEADWIND_SPEC = {
    "type": "linear_ramp",
    "start_date": "2026-04-01",
    "anchor_date": "2026-12-15",
    "desktop_dau": -1497870,
    "mobile_dau": -27162,
}


def test_render_linear_ramp_endpoints():
    idx = pd.date_range("2026-04-01", "2026-12-15", freq="D")
    rendered = render_adjustment(HEADWIND_SPEC, idx)
    # Day 0 (start) is zero
    assert rendered["desktop"].iloc[0] == 0
    assert rendered["mobile"].iloc[0] == 0
    # Anchor date hits exact value
    assert rendered["desktop"].iloc[-1] == pytest.approx(-1497870)
    assert rendered["mobile"].iloc[-1] == pytest.approx(-27162)


def test_render_linear_ramp_midpoint_is_half():
    idx = pd.date_range("2026-04-01", "2026-12-15", freq="D")
    rendered = render_adjustment(HEADWIND_SPEC, idx)
    midpoint = (len(idx) - 1) // 2
    expected = HEADWIND_SPEC["desktop_dau"] * midpoint / (len(idx) - 1)
    assert rendered["desktop"].iloc[midpoint] == pytest.approx(expected)


def test_render_linear_ramp_before_start_is_zero():
    idx = pd.date_range("2026-01-01", "2026-12-15", freq="D")
    rendered = render_adjustment(HEADWIND_SPEC, idx)
    pre_start = rendered["desktop"].loc[rendered["desktop"].index < "2026-04-01"]
    assert (pre_start == 0).all()


def test_render_linear_ramp_clamps_at_anchor_only_when_asked():
    idx = pd.date_range("2026-09-02", "2027-12-31", freq="D")
    unclamped = render_adjustment({**HEADWIND_SPEC, "start_date": "2026-09-02"}, idx)["desktop"]
    clamped = render_adjustment({**HEADWIND_SPEC, "start_date": "2026-09-02", "clamp_at_anchor": True}, idx)["desktop"]
    dec15, dec31 = pd.Timestamp("2026-12-15"), pd.Timestamp("2026-12-31")
    assert unclamped[dec15] == clamped[dec15] == pytest.approx(HEADWIND_SPEC["desktop_dau"])
    assert unclamped[dec31] < unclamped[dec15]                       # keeps falling (the published behaviour)
    assert clamped[dec31] == clamped[pd.Timestamp("2027-12-31")] == pytest.approx(HEADWIND_SPEC["desktop_dau"])
    assert (clamped[idx < dec15] > clamped[dec15]).all()


def test_render_step():
    idx = pd.date_range("2026-01-01", "2026-12-31", freq="D")
    spec = {
        "type": "step",
        "start_date": "2026-06-01",
        "end_date": "2026-06-30",
        "desktop_dau": -100000,
        "mobile_dau": -5000,
    }
    rendered = render_adjustment(spec, idx)
    in_window = rendered["desktop"].loc["2026-06-15"]
    out_window = rendered["desktop"].loc["2026-07-15"]
    assert in_window == -100000
    assert out_window == 0


def test_render_daily_series():
    idx = pd.date_range("2026-05-01", "2026-05-10", freq="D")
    spec = {
        "type": "daily_series",
        "series": {
            "2026-05-03": {"desktop_dau": -50000, "mobile_dau": -1000},
            "2026-05-07": {"desktop_dau": -75000, "mobile_dau": -1500},
        },
    }
    rendered = render_adjustment(spec, idx)
    assert rendered["desktop"].loc["2026-05-03"] == -50000
    assert rendered["desktop"].loc["2026-05-07"] == -75000
    assert rendered["desktop"].loc["2026-05-05"] == 0


def _write_daily_file_spec(tmp_path, *, platform="desktop", value=1400.0, start="2026-06-01", end="2027-12-31"):
    idx = pd.date_range(start, end, freq="D", name="target_date")
    pd.DataFrame({"curve_dau_daily": pd.Series(value, index=idx)}).to_parquet(tmp_path / "curve.parquet")
    spec = {"type": "daily_file", "platform": platform, "data_file": "curve.parquet",
            "value_column": "curve_dau_daily"}
    (tmp_path / "curve.json").write_text(json.dumps(spec))
    return spec


def test_render_daily_file_applies_trailing_28d_mean_to_one_platform(tmp_path):
    spec = _write_daily_file_spec(tmp_path, platform="mobile", value=1400.0)
    idx = pd.date_range("2026-05-01", "2026-12-31", freq="D")
    rendered = render_adjustment(spec, idx, spec_dir=tmp_path)
    assert (rendered["desktop"] == 0).all()
    assert rendered["mobile"][pd.Timestamp("2026-05-31")] == 0.0          # before the file starts
    assert rendered["mobile"][pd.Timestamp("2026-06-01")] == 1400.0       # min_periods=1: first day is itself
    assert rendered["mobile"][pd.Timestamp("2026-12-15")] == 1400.0       # flat curve → flat MA


def test_render_daily_file_smooths_a_step_over_28_days(tmp_path):
    idx_file = pd.date_range("2026-06-01", "2027-12-31", freq="D", name="target_date")
    daily = pd.Series(0.0, index=idx_file)
    daily[idx_file >= "2026-07-01"] = 2800.0
    pd.DataFrame({"curve_dau_daily": daily}).to_parquet(tmp_path / "curve.parquet")
    spec = {"type": "daily_file", "platform": "desktop", "data_file": "curve.parquet", "value_column": "curve_dau_daily"}
    idx = pd.date_range("2026-06-01", "2026-12-31", freq="D")
    rendered = render_adjustment(spec, idx, spec_dir=tmp_path)["desktop"]
    assert rendered[pd.Timestamp("2026-07-14")] == pytest.approx(2800.0 * 14 / 28)  # half the window in
    assert rendered[pd.Timestamp("2026-07-28")] == pytest.approx(2800.0)


def test_render_daily_file_holds_last_value_past_the_file_end(tmp_path):
    spec = _write_daily_file_spec(tmp_path, value=900.0, end="2026-10-31")
    idx = pd.date_range("2026-10-01", "2026-12-31", freq="D")
    rendered = render_adjustment(spec, idx, spec_dir=tmp_path)["desktop"]
    assert rendered[pd.Timestamp("2026-12-31")] == 900.0


def test_render_daily_file_requires_spec_dir_and_platform(tmp_path):
    spec = _write_daily_file_spec(tmp_path)
    idx = pd.date_range("2026-06-01", "2026-06-10", freq="D")
    with pytest.raises(ValueError, match="spec_dir"):
        render_adjustment(spec, idx)
    with pytest.raises(ValueError, match="platform='tablet'"):
        render_adjustment({**spec, "platform": "tablet"}, idx, spec_dir=tmp_path)


def test_load_adjustments_from_dir_renders_daily_file_next_to_ramp(tmp_path):
    _write_daily_file_spec(tmp_path, platform="desktop", value=1000.0)
    (tmp_path / "headwind.json").write_text(json.dumps(HEADWIND_SPEC))
    idx = pd.date_range("2026-04-01", "2026-12-15", freq="D")
    net = load_adjustments_from_dir(tmp_path, idx)
    ramp_only = render_adjustment(HEADWIND_SPEC, idx)["desktop"]
    assert net["desktop"][pd.Timestamp("2026-12-15")] == pytest.approx(ramp_only.iloc[-1] + 1000.0)
    assert net["mobile"][pd.Timestamp("2026-12-15")] == pytest.approx(-27162.0)


def test_load_adjustments_from_dir_empty_returns_zero_unless_required(tmp_path):
    idx = pd.date_range("2026-04-01", "2026-04-10", freq="D")
    assert (load_adjustments_from_dir(tmp_path, idx)["desktop"] == 0).all()
    with pytest.raises(FileNotFoundError, match="No adjustment specs"):
        load_adjustments_from_dir(tmp_path, idx, require_specs=True)


def test_render_unknown_type_raises():
    with pytest.raises(ValueError, match="Unknown adjustment spec type"):
        render_adjustment({"type": "bogus"}, pd.date_range("2026-01-01", "2026-01-10"))


def test_load_adjustments_from_dir_sums_specs(tmp_path):
    adj_dir = tmp_path / "adjustments"
    adj_dir.mkdir()
    (adj_dir / "headwind.json").write_text(json.dumps(HEADWIND_SPEC))
    extra_spec = {**HEADWIND_SPEC, "desktop_dau": -500000, "mobile_dau": -10000}
    (adj_dir / "extra.json").write_text(json.dumps(extra_spec))
    idx = pd.date_range("2026-04-01", "2026-12-15", freq="D")
    totals = load_adjustments_from_dir(adj_dir, idx)
    assert totals["desktop"].iloc[-1] == pytest.approx(-1497870 + -500000)
    assert totals["mobile"].iloc[-1] == pytest.approx(-27162 + -10000)


def test_apply_net_adjustment_anchored_at_forecast_start():
    idx = pd.date_range("2026-01-01", "2026-12-31", freq="D")
    ma = pd.Series(1_000_000.0, index=idx)
    desktop_adj = pd.Series(np.linspace(0, -1_497_870, len(idx)), index=idx)
    net = {"desktop": desktop_adj, "mobile": pd.Series(0.0, index=idx)}
    forecast_start = pd.Timestamp("2026-05-13")

    out = apply_net_adjustment_to_series(ma, net, "desktop", forecast_start=forecast_start)
    assert (out.loc[out.index < forecast_start] == 1_000_000.0).all()
    at = out.loc[forecast_start]
    assert at == pytest.approx(1_000_000.0 + desktop_adj.loc[forecast_start])


def test_apply_net_adjustment_uses_passed_forecast_start_not_default():
    """Regression: apply_net_adjustment must use the passed forecast_start, not a global.

    This pins the behavior fixed in commit 0b9dec8 (prior-forecast headwind start
    date) — the prior-forecast series needs PREV_FORECAST_START, not FORECAST_START.
    """
    idx = pd.date_range("2026-01-01", "2026-12-31", freq="D")
    ma = pd.Series(1_000_000.0, index=idx)
    net = {"desktop": pd.Series(-1000.0, index=idx), "mobile": pd.Series(0.0, index=idx)}

    out_a = apply_net_adjustment_to_series(ma, net, "desktop", forecast_start=pd.Timestamp("2026-04-01"))
    out_b = apply_net_adjustment_to_series(ma, net, "desktop", forecast_start=pd.Timestamp("2026-05-13"))
    # A starts shifting earlier than B, so April values must differ
    assert out_a.loc["2026-04-15"] != out_b.loc["2026-04-15"]
    # Both reach the same shifted value after 2026-05-13
    assert out_a.loc["2026-06-01"] == out_b.loc["2026-06-01"]


# --- registry --------------------------------------------------------------

def test_load_code_registry_returns_h():
    """The shipped registry must define the 'h' (headwinds) code."""
    registry = load_code_registry()
    assert "h" in registry
    assert registry["h"]["name"] == "headwinds"


def test_build_adjustments_applied_list_sorts_and_hashes(tmp_path):
    spec_file = tmp_path / "headwind.json"
    spec_file.write_text(json.dumps(HEADWIND_SPEC))
    expected_sha = adjustment_spec_hash(spec_file)
    fake_registry = {"h": {"name": "headwinds", "description": "...", "spec_glob": ""}}
    out = build_adjustments_applied_list(
        codes=["h"],
        code_to_spec_file={"h": spec_file},
        registry=fake_registry,
    )
    assert out == [{"code": "h", "name": "headwinds", "spec_file": str(spec_file), "spec_sha1": expected_sha}]


def test_build_adjustments_applied_list_rejects_unknown_code(tmp_path):
    fake_registry = {"h": {"name": "headwinds", "description": "", "spec_glob": ""}}
    with pytest.raises(KeyError, match="not in registry"):
        build_adjustments_applied_list(
            codes=["z"],
            code_to_spec_file={"z": tmp_path / "x.json"},
            registry=fake_registry,
        )


# --- meta round-trip + load_forecast ---------------------------------------

def _make_parquet(path):
    pd.DataFrame({"target_date": pd.date_range("2026-01-01", periods=3), "dau": [1, 2, 3]}).to_parquet(path)


def test_meta_round_trip(tmp_path):
    parquet = tmp_path / "foo.adj-h.parquet"
    _make_parquet(parquet)
    write_meta(
        parquet,
        forecast_start_date="2026-05-13",
        data_source="legacy_desktop",
        produced_by="test",
        model_config={"prophet_changepoint_prior_scale": 0.15983},
        adjustments_applied=[{"code": "h", "name": "headwinds", "spec_file": "x.json", "spec_sha1": "deadbeef"}],
    )
    meta = read_meta(parquet)
    assert meta["data_source"] == "legacy_desktop"
    assert meta["adjustments_applied"][0]["code"] == "h"
    assert meta["model_config"]["prophet_changepoint_prior_scale"] == 0.15983
    # artifact sha1 was recorded
    assert isinstance(meta["artifact_sha1"], str) and len(meta["artifact_sha1"]) == 40


def test_load_forecast_happy_path(tmp_path):
    parquet = tmp_path / "foo.adj-h.parquet"
    _make_parquet(parquet)
    write_meta(
        parquet,
        forecast_start_date="2026-05-13",
        data_source=None,
        produced_by="test",
        model_config={},
        adjustments_applied=[{"code": "h", "name": "headwinds", "spec_file": "x.json", "spec_sha1": "deadbeef"}],
    )
    df, meta = load_forecast(parquet)
    assert len(df) == 3
    assert meta["adjustments_applied"][0]["code"] == "h"


def test_load_forecast_require_state_matches(tmp_path):
    parquet = tmp_path / "foo.adj-h.parquet"
    _make_parquet(parquet)
    write_meta(
        parquet,
        forecast_start_date="2026-05-13",
        data_source=None,
        produced_by="test",
        model_config={},
        adjustments_applied=[{"code": "h", "name": "headwinds", "spec_file": "x.json", "spec_sha1": "deadbeef"}],
    )
    df, _ = load_forecast(parquet, require_state=["h"])
    assert len(df) == 3


def test_load_forecast_require_state_mismatches(tmp_path):
    parquet = tmp_path / "foo.adj-h.parquet"
    _make_parquet(parquet)
    write_meta(
        parquet,
        forecast_start_date="2026-05-13",
        data_source=None,
        produced_by="test",
        model_config={},
        adjustments_applied=[{"code": "h", "name": "headwinds", "spec_file": "x.json", "spec_sha1": "deadbeef"}],
    )
    with pytest.raises(ValueError, match="State mismatch"):
        load_forecast(parquet, require_state=["t"])


def test_load_forecast_filename_meta_drift_raises(tmp_path):
    """Filename says adj-h but meta says adj-t → must error."""
    parquet = tmp_path / "foo.adj-h.parquet"
    _make_parquet(parquet)
    write_meta(
        parquet,
        forecast_start_date="2026-05-13",
        data_source=None,
        produced_by="test",
        model_config={},
        adjustments_applied=[{"code": "t", "name": "tailwinds", "spec_file": "x.json", "spec_sha1": "deadbeef"}],
    )
    with pytest.raises(ValueError, match="State drift"):
        load_forecast(parquet)


def test_load_forecast_missing_meta_raises(tmp_path):
    parquet = tmp_path / "foo.adj-h.parquet"
    _make_parquet(parquet)
    with pytest.raises(FileNotFoundError, match="Sidecar meta missing"):
        load_forecast(parquet)


def test_load_forecast_no_marker_raises(tmp_path):
    parquet = tmp_path / "foo.parquet"
    _make_parquet(parquet)
    with pytest.raises(ValueError, match="No state marker"):
        load_forecast(parquet)


def test_load_forecast_raw_with_empty_adjustments(tmp_path):
    """Raw file: filename has .raw., meta has empty adjustments_applied."""
    parquet = tmp_path / "foo.raw.parquet"
    _make_parquet(parquet)
    write_meta(
        parquet,
        forecast_start_date="2026-05-13",
        data_source="legacy_desktop",
        produced_by="test",
        model_config={"prophet_changepoint_prior_scale": 0.15983},
        adjustments_applied=[],
    )
    df, meta = load_forecast(parquet)
    assert meta["adjustments_applied"] == []


def test_load_forecast_csv(tmp_path):
    csv_path = tmp_path / "foo.adj-h.csv"
    pd.DataFrame({"x": [1, 2]}).to_csv(csv_path, index=False)
    write_meta(
        csv_path,
        forecast_start_date="2026-05-13",
        data_source=None,
        produced_by="test",
        model_config={},
        adjustments_applied=[{"code": "h", "name": "headwinds", "spec_file": "x.json", "spec_sha1": "deadbeef"}],
    )
    df, _ = load_forecast(csv_path)
    assert list(df.columns) == ["x"]


# --- Marketing-lift applier (code `m`) -------------------------------------
#
# Synthetic 3-country × 4-app × 60-day mobile training fixture spanning
# campaign launch (2026-04-06). Each (date, country, app) tile has one row.

APP_FLAGS = ["fenix_android", "firefox_ios", "focus_android", "focus_ios"]
TEST_COUNTRIES = ["US", "DE", "BR"]
TEST_DATES = pd.date_range("2026-03-15", "2026-05-13", freq="D")  # straddles 2026-04-06
TRAINING_END = pd.Timestamp("2026-05-13")
FORECAST_START = pd.Timestamp("2026-05-14")
CAMPAIGN_LAUNCH = pd.Timestamp("2026-04-06")

# Per-app, per-country baseline DAU. Chosen so US >> DE > BR for Fenix.
FENIX_BASELINE = {"US": 1_000_000, "DE": 200_000, "BR": 50_000}
FFOX_BASELINE = {"US": 500_000, "DE": 100_000, "BR": 25_000}
FOCUS_A_BASELINE = {"US": 50_000, "DE": 10_000, "BR": 5_000}
FOCUS_I_BASELINE = {"US": 30_000, "DE": 8_000, "BR": 2_000}


def _make_training_fixture():
    rows = []
    for date in TEST_DATES:
        for country in TEST_COUNTRIES:
            for app, baseline_map in [
                ("fenix_android", FENIX_BASELINE),
                ("firefox_ios", FFOX_BASELINE),
                ("focus_android", FOCUS_A_BASELINE),
                ("focus_ios", FOCUS_I_BASELINE),
            ]:
                row = {"x": date.date(), "country": country, "y": baseline_map[country]}
                for flag in APP_FLAGS:
                    row[flag] = (flag == app)
                rows.append(row)
    df = pd.DataFrame(rows)
    df["y"] = df["y"].astype("Int64")
    return df


def _make_lift_series():
    idx = pd.date_range("2026-02-01", "2026-12-31", freq="D")
    values = np.where(idx >= CAMPAIGN_LAUNCH, 10_000.0, 0.0)
    return pd.Series(values, index=idx, name="marketing_lift_daily")


@pytest.fixture
def training_df():
    return _make_training_fixture()


@pytest.fixture
def lift_series():
    return _make_lift_series()


@pytest.fixture
def country_shares(training_df):
    return compute_fenix_country_shares(
        training_df, training_end_date=TRAINING_END, window_days=28
    )


# --- load_marketing_spec ---------------------------------------------------

def test_load_marketing_spec_validates_type(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"type": "linear_ramp"}))
    with pytest.raises(ValueError, match="expected 'marketing_lift'"):
        load_marketing_spec(bad)


def test_load_marketing_spec_requires_keys(tmp_path):
    incomplete = tmp_path / "incomplete.json"
    incomplete.write_text(json.dumps({"type": "marketing_lift", "data_file": "x.parquet"}))
    with pytest.raises(ValueError, match="missing required key"):
        load_marketing_spec(incomplete)


def test_load_marketing_spec_round_trip(tmp_path):
    good = {
        "type": "marketing_lift",
        "data_file": "lift.parquet",
        "value_column": "marketing_lift_daily",
        "allocation": {"app_flag_column": "fenix_android", "key": "trailing_dau_share", "window_days": 28},
    }
    path = tmp_path / "marketing.json"
    path.write_text(json.dumps(good))
    spec = load_marketing_spec(path)
    assert spec["data_file"] == "lift.parquet"
    assert spec["allocation"]["window_days"] == 28


# --- load_marketing_lift_series --------------------------------------------

def test_load_marketing_lift_series_indexes_and_zeros(tmp_path):
    series = _make_lift_series()
    data_path = tmp_path / "lift.parquet"
    series.to_frame("marketing_lift_daily").rename_axis("target_date").to_parquet(data_path)
    spec = {"data_file": "lift.parquet", "value_column": "marketing_lift_daily"}
    out = load_marketing_lift_series(spec, tmp_path)
    assert isinstance(out.index, pd.DatetimeIndex)
    assert out.loc[:"2026-04-05"].sum() == 0.0
    assert out.loc["2026-04-06"] == 10_000.0
    assert not out.isna().any()


def test_load_marketing_lift_series_rejects_missing_column(tmp_path):
    pd.DataFrame({"other_col": [1.0]}, index=pd.DatetimeIndex(["2026-01-01"], name="target_date")).to_parquet(
        tmp_path / "lift.parquet"
    )
    spec = {"data_file": "lift.parquet", "value_column": "marketing_lift_daily"}
    with pytest.raises(ValueError, match="not found"):
        load_marketing_lift_series(spec, tmp_path)


# --- compute_fenix_country_shares ------------------------------------------

def test_compute_fenix_shares_sum_to_one(country_shares):
    assert abs(country_shares.sum() - 1.0) < 1e-12


def test_compute_fenix_shares_match_baselines(country_shares):
    total = sum(FENIX_BASELINE.values())
    for country, baseline in FENIX_BASELINE.items():
        assert country_shares.loc[country] == pytest.approx(baseline / total)


def test_compute_fenix_shares_ignores_other_apps(training_df):
    # Inflate firefox_ios baselines hugely; Fenix shares must be unchanged.
    inflated = training_df.copy()
    ffox_mask = inflated["firefox_ios"] == True  # noqa: E712
    inflated.loc[ffox_mask, "y"] = inflated.loc[ffox_mask, "y"].astype("Int64") * 100
    shares_inflated = compute_fenix_country_shares(
        inflated, training_end_date=TRAINING_END, window_days=28
    )
    expected = compute_fenix_country_shares(
        training_df, training_end_date=TRAINING_END, window_days=28
    )
    pd.testing.assert_series_equal(shares_inflated, expected)


def test_compute_fenix_shares_uses_only_window(training_df):
    # Inflate Fenix US for dates BEFORE the 28d window; shares must be unchanged.
    inflated = training_df.copy()
    window_start = TRAINING_END - pd.Timedelta(days=27)
    fenix_us_pre_window = (
        (inflated["fenix_android"] == True)  # noqa: E712
        & (inflated["country"] == "US")
        & (pd.to_datetime(inflated["x"]) < window_start)
    )
    assert fenix_us_pre_window.any(), "fixture should contain pre-window rows"
    inflated.loc[fenix_us_pre_window, "y"] = (
        inflated.loc[fenix_us_pre_window, "y"].astype("Int64") * 100
    )
    shares_inflated = compute_fenix_country_shares(
        inflated, training_end_date=TRAINING_END, window_days=28
    )
    expected = compute_fenix_country_shares(
        training_df, training_end_date=TRAINING_END, window_days=28
    )
    pd.testing.assert_series_equal(shares_inflated, expected)


# --- subtract_marketing_lift_from_training ---------------------------------

def test_subtract_sets_idempotency_attr(training_df, lift_series, country_shares):
    out = subtract_marketing_lift_from_training(
        training_df, daily_lift_series=lift_series, country_shares=country_shares
    )
    assert out.attrs["marketing_lift_subtracted"] is True


def test_subtract_rejects_already_adjusted(training_df, lift_series, country_shares):
    out = subtract_marketing_lift_from_training(
        training_df, daily_lift_series=lift_series, country_shares=country_shares
    )
    with pytest.raises(RuntimeError, match="called twice"):
        subtract_marketing_lift_from_training(
            out, daily_lift_series=lift_series, country_shares=country_shares
        )


def test_subtract_only_modifies_fenix_rows(training_df, lift_series, country_shares):
    out = subtract_marketing_lift_from_training(
        training_df, daily_lift_series=lift_series, country_shares=country_shares
    )
    non_fenix_in = training_df[training_df["fenix_android"] == False]  # noqa: E712
    non_fenix_out = out[out["fenix_android"] == False]  # noqa: E712
    pd.testing.assert_series_equal(
        non_fenix_in["y"].reset_index(drop=True),
        non_fenix_out["y"].reset_index(drop=True),
    )


def test_subtract_zero_before_campaign_launch(training_df, lift_series, country_shares):
    out = subtract_marketing_lift_from_training(
        training_df, daily_lift_series=lift_series, country_shares=country_shares
    )
    pre_launch = pd.to_datetime(training_df["x"]) < CAMPAIGN_LAUNCH
    pd.testing.assert_series_equal(
        training_df.loc[pre_launch, "y"].reset_index(drop=True),
        out.loc[pre_launch, "y"].reset_index(drop=True),
    )


def test_subtract_preserves_int64_dtype(training_df, lift_series, country_shares):
    out = subtract_marketing_lift_from_training(
        training_df, daily_lift_series=lift_series, country_shares=country_shares
    )
    assert out["y"].dtype == pd.Int64Dtype()


def test_subtract_world_invariant(training_df, lift_series, country_shares):
    """Sum of Fenix subtractions across countries per date == daily_lift[date]."""
    out = subtract_marketing_lift_from_training(
        training_df, daily_lift_series=lift_series, country_shares=country_shares
    )
    fenix_in = training_df[training_df["fenix_android"] == True]  # noqa: E712
    fenix_out = out[out["fenix_android"] == True]  # noqa: E712
    delta_per_row = (fenix_in["y"].astype("int64").to_numpy()
                     - fenix_out["y"].astype("int64").to_numpy())
    deltas_per_date = (
        pd.DataFrame({"x": fenix_in["x"].values, "delta": delta_per_row})
        .groupby("x")["delta"].sum()
    )
    deltas_per_date.index = pd.DatetimeIndex(deltas_per_date.index).normalize()
    for date, delta in deltas_per_date.items():
        if date >= CAMPAIGN_LAUNCH:
            # daily_lift = 10_000 in fixture; sum of (10_000 * share[c]) over c == 10_000
            assert delta == pytest.approx(10_000, abs=2)
        else:
            assert delta == 0


def test_subtract_does_not_mutate_input(training_df, lift_series, country_shares):
    original_y = training_df["y"].copy()
    _ = subtract_marketing_lift_from_training(
        training_df, daily_lift_series=lift_series, country_shares=country_shares
    )
    pd.testing.assert_series_equal(training_df["y"], original_y)


# --- add_marketing_lift_to_forecast ----------------------------------------

def _make_forecast_fixture():
    """Build a synthetic mozaic-style granular forecast df.

    Spans training (source='actual', dates 2026-03-15..2026-05-13) and
    forecast (source='forecast', dates 2026-05-14..2026-06-15).
    """
    rows = []
    # Per-country, per-app rows + per-country ALL + per-app world + world ALL
    train_dates = pd.date_range("2026-03-15", "2026-05-13", freq="D")
    fcst_dates = pd.date_range("2026-05-14", "2026-06-15", freq="D")
    populations = ["fenix_android", "firefox_ios", "focus_android", "focus_ios", "ALL"]
    countries = TEST_COUNTRIES + ["ALL"]
    for source, dates in [("actual", train_dates), ("forecast", fcst_dates)]:
        for date in dates:
            for country in countries:
                for population in populations:
                    rows.append({
                        "target_date": date,
                        "country": country,
                        "population": population,
                        "source": source,
                        "DAU": 100_000.0,
                    })
    return pd.DataFrame(rows)


def test_addback_world_row_gets_full_lift(country_shares, lift_series):
    fdf = _make_forecast_fixture()
    out = add_marketing_lift_to_forecast(
        fdf,
        daily_lift_series=lift_series,
        country_shares=country_shares,
        forecast_start=FORECAST_START,
    )
    world_fcst = (
        (out["country"] == "ALL")
        & (out["population"] == "ALL")
        & (out["source"] == "forecast")
    )
    delta = out.loc[world_fcst, "DAU"] - fdf.loc[world_fcst, "DAU"]
    # 10_000 lift on every forecast date
    assert np.allclose(delta.values, 10_000.0)


def test_addback_per_country_uses_shares(country_shares, lift_series):
    fdf = _make_forecast_fixture()
    out = add_marketing_lift_to_forecast(
        fdf,
        daily_lift_series=lift_series,
        country_shares=country_shares,
        forecast_start=FORECAST_START,
    )
    for country in TEST_COUNTRIES:
        mask = (
            (out["country"] == country)
            & (out["population"] == "fenix_android")
            & (out["source"] == "forecast")
        )
        delta = (out.loc[mask, "DAU"] - fdf.loc[mask, "DAU"])
        expected = 10_000 * country_shares.loc[country]
        assert delta.values == pytest.approx(expected)


def test_addback_per_country_all_pop_also_uses_shares(country_shares, lift_series):
    fdf = _make_forecast_fixture()
    out = add_marketing_lift_to_forecast(
        fdf,
        daily_lift_series=lift_series,
        country_shares=country_shares,
        forecast_start=FORECAST_START,
    )
    for country in TEST_COUNTRIES:
        mask = (
            (out["country"] == country)
            & (out["population"] == "ALL")
            & (out["source"] == "forecast")
        )
        delta = (out.loc[mask, "DAU"] - fdf.loc[mask, "DAU"])
        expected = 10_000 * country_shares.loc[country]
        assert delta.values == pytest.approx(expected)


def test_addback_world_fenix_row_gets_full_lift(country_shares, lift_series):
    fdf = _make_forecast_fixture()
    out = add_marketing_lift_to_forecast(
        fdf,
        daily_lift_series=lift_series,
        country_shares=country_shares,
        forecast_start=FORECAST_START,
    )
    mask = (
        (out["country"] == "ALL")
        & (out["population"] == "fenix_android")
        & (out["source"] == "forecast")
    )
    delta = out.loc[mask, "DAU"] - fdf.loc[mask, "DAU"]
    assert np.allclose(delta.values, 10_000.0)


def test_addback_pre_launch_rows_unchanged(country_shares, lift_series):
    """Pre-campaign rows have lift == 0, so they should be byte-equal."""
    fdf = _make_forecast_fixture()
    out = add_marketing_lift_to_forecast(
        fdf,
        daily_lift_series=lift_series,
        country_shares=country_shares,
        forecast_start=FORECAST_START,
    )
    pre_launch_mask = pd.to_datetime(out["target_date"]) < CAMPAIGN_LAUNCH
    assert pre_launch_mask.any(), "fixture should include pre-launch rows"
    pd.testing.assert_series_equal(
        fdf.loc[pre_launch_mask, "DAU"].reset_index(drop=True),
        out.loc[pre_launch_mask, "DAU"].reset_index(drop=True),
    )


def test_addback_post_launch_training_rows_get_lift(country_shares, lift_series):
    """Training rows after the campaign launch must also get the lift added back.

    This is what makes the training→forecast transition coherent for any
    downstream rolling statistic (e.g. 28d MA).
    """
    fdf = _make_forecast_fixture()
    out = add_marketing_lift_to_forecast(
        fdf,
        daily_lift_series=lift_series,
        country_shares=country_shares,
        forecast_start=FORECAST_START,
    )
    # World ALL training rows post-campaign-launch should gain the full daily lift
    mask = (
        (out["country"] == "ALL")
        & (out["population"] == "ALL")
        & (out["source"] == "actual")
        & (pd.to_datetime(out["target_date"]) >= CAMPAIGN_LAUNCH)
    )
    assert mask.any(), "fixture should include post-launch training rows"
    delta = (out.loc[mask, "DAU"] - fdf.loc[mask, "DAU"])
    assert np.allclose(delta.values, 10_000.0)


def test_addback_non_fenix_populations_untouched(country_shares, lift_series):
    fdf = _make_forecast_fixture()
    out = add_marketing_lift_to_forecast(
        fdf,
        daily_lift_series=lift_series,
        country_shares=country_shares,
        forecast_start=FORECAST_START,
    )
    for non_fenix in ["firefox_ios", "focus_android", "focus_ios"]:
        mask = out["population"] == non_fenix
        pd.testing.assert_series_equal(
            fdf.loc[mask, "DAU"].reset_index(drop=True),
            out.loc[mask, "DAU"].reset_index(drop=True),
        )


def test_addback_does_not_mutate_input(country_shares, lift_series):
    fdf = _make_forecast_fixture()
    snapshot = fdf["DAU"].copy()
    _ = add_marketing_lift_to_forecast(
        fdf,
        daily_lift_series=lift_series,
        country_shares=country_shares,
        forecast_start=FORECAST_START,
    )
    pd.testing.assert_series_equal(fdf["DAU"], snapshot)


# --- Registry + filename canonicalization round-trips for `m` --------------

def test_marketing_code_canonical_with_headwind():
    assert canonical_codes(["m", "h"]) == "hm"
    assert state_marker(["h", "m"]) == "adj-hm"


def test_parse_state_from_path_adj_hm():
    codes = parse_state_from_path("foo.2026-05-13.gm-D.adj-hm.parquet")
    assert codes == ["h", "m"]


def test_marketing_code_in_registry():
    registry = load_code_registry()
    assert "m" in registry
    assert registry["m"]["name"] == "marketing_lift"


# --- Desktop overlay applier (launch at login for new users, code `l`) -------------------
#
# Exercises the generalized bidirectional appliers on the DESKTOP training
# schema: boolean `modern_windows` / `winX` segment columns (rows with both
# False are the "other" segment). The launch at login for new users overlay lands entirely in
# modern_windows. MozillaOnline (`o`) will reuse the same generic functions.

LOL_ROLLOUT = pd.Timestamp("2026-05-08")
# modern_windows baselines chosen so shares are clean: 800k/150k/50k -> .80/.15/.05
MW_BASELINE = {"US": 800_000, "DE": 150_000, "BR": 50_000}
WINX_BASELINE = {"US": 120_000, "DE": 40_000, "BR": 10_000}
OTHER_BASELINE = {"US": 300_000, "DE": 60_000, "BR": 20_000}  # mac/linux etc.


def _make_desktop_training_fixture():
    """Long-format desktop DAU training: one row per (date, country, segment).

    Segment encoding mirrors production: modern_windows/winX booleans, with the
    'other' segment being both False.
    """
    rows = []
    for date in TEST_DATES:
        for country in TEST_COUNTRIES:
            for seg, baseline_map, mw, wx in [
                ("modern_windows", MW_BASELINE, True, False),
                ("winX", WINX_BASELINE, False, True),
                ("other", OTHER_BASELINE, False, False),
            ]:
                rows.append({
                    "x": date.date(),
                    "country": country,
                    "modern_windows": mw,
                    "winX": wx,
                    "y": baseline_map[country],
                })
    df = pd.DataFrame(rows)
    df["y"] = df["y"].astype("Int64")
    return df


def _make_lol_lift_series():
    """Flat 15_000/day Launch at Login (new users) curve from rollout (stand-in for the capped curve)."""
    idx = pd.date_range("2026-02-01", "2026-12-31", freq="D")
    values = np.where(idx >= LOL_ROLLOUT, 15_000.0, 0.0)
    return pd.Series(values, index=idx, name="lol_lift_daily")


def _make_desktop_forecast_fixture():
    """mozaic-style granular desktop forecast: population = OS segment or ALL."""
    rows = []
    train_dates = pd.date_range("2026-03-15", "2026-05-13", freq="D")
    fcst_dates = pd.date_range("2026-05-14", "2026-06-15", freq="D")
    populations = ["modern_windows", "winX", "other", "ALL"]
    countries = TEST_COUNTRIES + ["ALL"]
    for source, dates in [("actual", train_dates), ("forecast", fcst_dates)]:
        for date in dates:
            for country in countries:
                for population in populations:
                    rows.append({
                        "target_date": date,
                        "country": country,
                        "population": population,
                        "source": source,
                        "DAU": 500_000.0,
                    })
    return pd.DataFrame(rows)


@pytest.fixture
def desktop_training_df():
    return _make_desktop_training_fixture()


@pytest.fixture
def lol_lift_series():
    return _make_lol_lift_series()


@pytest.fixture
def mw_country_shares(desktop_training_df):
    return compute_country_shares(
        desktop_training_df,
        training_end_date=TRAINING_END,
        window_days=28,
        flag_column="modern_windows",
    )


# --- load_overlay_spec -----------------------------------------------------

def test_load_overlay_spec_validates_type(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"type": "marketing_lift"}))
    with pytest.raises(ValueError, match="expected 'desktop_overlay'"):
        load_overlay_spec(bad)


def test_load_overlay_spec_requires_flag_column(tmp_path):
    incomplete = tmp_path / "lol.json"
    incomplete.write_text(json.dumps({
        "type": "desktop_overlay", "data_file": "x.parquet", "value_column": "lol_lift_daily",
        "allocation": {"key": "trailing_dau_share", "window_days": 28},
    }))
    with pytest.raises(ValueError, match="flag_column"):
        load_overlay_spec(incomplete)


def test_load_overlay_spec_round_trip(tmp_path):
    good = {
        "type": "desktop_overlay",
        "data_file": "lol.parquet",
        "value_column": "lol_lift_daily",
        "allocation": {"flag_column": "modern_windows", "key": "trailing_dau_share", "window_days": 28},
    }
    path = tmp_path / "lol.json"
    path.write_text(json.dumps(good))
    spec = load_overlay_spec(path)
    assert spec["allocation"]["flag_column"] == "modern_windows"


def test_load_lift_series_generic(tmp_path):
    series = _make_lol_lift_series()
    series.to_frame("lol_lift_daily").rename_axis("target_date").to_parquet(tmp_path / "lol.parquet")
    spec = {"data_file": "lol.parquet", "value_column": "lol_lift_daily"}
    out = load_lift_series(spec, tmp_path)
    assert out.loc[:"2026-05-07"].sum() == 0.0
    assert out.loc["2026-05-08"] == 15_000.0


# --- compute_country_shares (modern_windows) -------------------------------

def test_mw_shares_sum_to_one(mw_country_shares):
    assert abs(mw_country_shares.sum() - 1.0) < 1e-12


def test_mw_shares_match_baseline_proportions(mw_country_shares):
    assert mw_country_shares.loc["US"] == pytest.approx(0.80)
    assert mw_country_shares.loc["DE"] == pytest.approx(0.15)
    assert mw_country_shares.loc["BR"] == pytest.approx(0.05)


def test_mw_shares_exclude_countries_and_renormalize(desktop_training_df):
    shares = compute_country_shares(
        desktop_training_df, training_end_date=TEST_DATES[-1], window_days=28,
        flag_column="modern_windows", exclude_countries=["US"],
    )
    assert "US" not in shares.index
    assert shares.sum() == pytest.approx(1.0)
    assert shares["DE"] == pytest.approx(MW_BASELINE["DE"] / (MW_BASELINE["DE"] + MW_BASELINE["BR"]))


def test_mw_shares_excluding_everything_raises(desktop_training_df):
    with pytest.raises(ValueError, match="after excluding"):
        compute_country_shares(
            desktop_training_df, training_end_date=TEST_DATES[-1], window_days=28,
            flag_column="modern_windows", exclude_countries=TEST_COUNTRIES,
        )


def test_mw_shares_ignore_other_segments(desktop_training_df):
    """winX/other DAU must not affect modern_windows shares."""
    inflated = desktop_training_df.copy()
    winx_mask = inflated["winX"] == True  # noqa: E712
    inflated.loc[winx_mask, "y"] = inflated.loc[winx_mask, "y"].astype("Int64") + 9_000_000
    shares_a = compute_country_shares(
        desktop_training_df, training_end_date=TRAINING_END, window_days=28, flag_column="modern_windows"
    )
    shares_b = compute_country_shares(
        inflated, training_end_date=TRAINING_END, window_days=28, flag_column="modern_windows"
    )
    pd.testing.assert_series_equal(shares_a, shares_b)


# --- subtract_lift_from_training (modern_windows) --------------------------

def test_lol_subtract_sets_custom_sentinel(desktop_training_df, lol_lift_series, mw_country_shares):
    out = subtract_lift_from_training(
        desktop_training_df, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
        flag_column="modern_windows", sentinel_attr="launch_on_login_subtracted",
    )
    assert out.attrs["launch_on_login_subtracted"] is True
    # A different overlay (different sentinel) can still subtract from the same frame.
    assert not out.attrs.get("marketing_lift_subtracted")


def test_lol_subtract_rejects_same_sentinel_twice(desktop_training_df, lol_lift_series, mw_country_shares):
    out = subtract_lift_from_training(
        desktop_training_df, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
        flag_column="modern_windows", sentinel_attr="launch_on_login_subtracted",
    )
    with pytest.raises(RuntimeError, match="called twice"):
        subtract_lift_from_training(
            out, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
            flag_column="modern_windows", sentinel_attr="launch_on_login_subtracted",
        )


def test_lol_subtract_only_modifies_modern_windows(desktop_training_df, lol_lift_series, mw_country_shares):
    out = subtract_lift_from_training(
        desktop_training_df, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
        flag_column="modern_windows", sentinel_attr="lol",
    )
    non_mw_in = desktop_training_df[desktop_training_df["modern_windows"] == False]  # noqa: E712
    non_mw_out = out[out["modern_windows"] == False]  # noqa: E712
    pd.testing.assert_series_equal(
        non_mw_in["y"].reset_index(drop=True), non_mw_out["y"].reset_index(drop=True)
    )


def test_lol_subtract_world_invariant(desktop_training_df, lol_lift_series, mw_country_shares):
    """Per-date sum of modern_windows subtractions == daily_lift[date]."""
    out = subtract_lift_from_training(
        desktop_training_df, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
        flag_column="modern_windows", sentinel_attr="lol",
    )
    mw_in = desktop_training_df[desktop_training_df["modern_windows"] == True]  # noqa: E712
    mw_out = out[out["modern_windows"] == True]  # noqa: E712
    delta = mw_in["y"].astype("int64").to_numpy() - mw_out["y"].astype("int64").to_numpy()
    per_date = (pd.DataFrame({"x": mw_in["x"].values, "d": delta})
                .groupby("x")["d"].sum())
    per_date.index = pd.DatetimeIndex(per_date.index).normalize()
    for date, d in per_date.items():
        assert d == (pytest.approx(15_000, abs=2) if date >= LOL_ROLLOUT else 0)


def test_lol_subtract_does_not_mutate_input(desktop_training_df, lol_lift_series, mw_country_shares):
    original_y = desktop_training_df["y"].copy()
    _ = subtract_lift_from_training(
        desktop_training_df, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
        flag_column="modern_windows", sentinel_attr="lol",
    )
    pd.testing.assert_series_equal(desktop_training_df["y"], original_y)


# --- add_lift_to_forecast (modern_windows) ---------------------------------

def test_lol_addback_world_row_full_lift(mw_country_shares, lol_lift_series):
    fdf = _make_desktop_forecast_fixture()
    out = add_lift_to_forecast(
        fdf, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
        forecast_start=FORECAST_START, population_value="modern_windows",
    )
    mask = (out["country"] == "ALL") & (out["population"] == "ALL") & (out["source"] == "forecast")
    delta = out.loc[mask, "DAU"] - fdf.loc[mask, "DAU"]
    assert np.allclose(delta.values, 15_000.0)


def test_lol_addback_per_country_uses_shares(mw_country_shares, lol_lift_series):
    fdf = _make_desktop_forecast_fixture()
    out = add_lift_to_forecast(
        fdf, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
        forecast_start=FORECAST_START, population_value="modern_windows",
    )
    for country in TEST_COUNTRIES:
        mask = ((out["country"] == country) & (out["population"] == "modern_windows")
                & (out["source"] == "forecast"))
        delta = out.loc[mask, "DAU"] - fdf.loc[mask, "DAU"]
        assert delta.values == pytest.approx(15_000 * mw_country_shares.loc[country])


def test_lol_addback_winx_and_other_untouched(mw_country_shares, lol_lift_series):
    fdf = _make_desktop_forecast_fixture()
    out = add_lift_to_forecast(
        fdf, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
        forecast_start=FORECAST_START, population_value="modern_windows",
    )
    for seg in ["winX", "other"]:
        mask = out["population"] == seg
        pd.testing.assert_series_equal(
            fdf.loc[mask, "DAU"].reset_index(drop=True),
            out.loc[mask, "DAU"].reset_index(drop=True),
        )


def test_lol_addback_does_not_mutate_input(mw_country_shares, lol_lift_series):
    fdf = _make_desktop_forecast_fixture()
    snapshot = fdf["DAU"].copy()
    _ = add_lift_to_forecast(
        fdf, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
        forecast_start=FORECAST_START, population_value="modern_windows",
    )
    pd.testing.assert_series_equal(fdf["DAU"], snapshot)


def test_lol_subtract_addback_roundtrip_at_world(desktop_training_df, lol_lift_series, mw_country_shares):
    """Add-back of a flat lift restores the world-level total we subtracted."""
    fdf = _make_desktop_forecast_fixture()
    added = add_lift_to_forecast(
        fdf, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
        forecast_start=FORECAST_START, population_value="modern_windows",
    )
    # Sum of per-country modern_windows add-backs equals the world modern_windows add-back.
    day = pd.Timestamp("2026-06-01")
    per_country = added[(added["target_date"] == day) & (added["population"] == "modern_windows")
                        & (added["country"] != "ALL")]["DAU"].sum() - \
        fdf[(fdf["target_date"] == day) & (fdf["population"] == "modern_windows")
            & (fdf["country"] != "ALL")]["DAU"].sum()
    assert per_country == pytest.approx(15_000.0, abs=2)


# --- Registry + filename round-trips for `l` -------------------------------

def test_lol_code_in_registry():
    registry = load_code_registry()
    assert "l" in registry
    assert registry["l"]["name"] == "launch_at_login_new_users"  # renamed 2026-09-04


def test_lol_code_canonical_and_marker():
    assert canonical_codes(["m", "h", "l"]) == "hlm"
    assert state_marker(["l", "h", "m"]) == "adj-hlm"


def test_parse_state_from_path_adj_hl():
    assert parse_state_from_path("foo.2026-06-29.ld-D.adj-hl.parquet") == ["h", "l"]


def test_build_adjustments_applied_includes_lol(tmp_path):
    spec = tmp_path / "lol.json"
    spec.write_text(json.dumps({"type": "desktop_overlay"}))
    applied = build_adjustments_applied_list(["l"], {"l": spec})
    assert applied[0]["code"] == "l"
    assert applied[0]["name"] == "launch_at_login_new_users"


# --- MozillaOnline overlay applier (code `o`) ------------------------------
#
# `o` reuses the SAME generic desktop-overlay appliers as `l` (modern_windows
# segment) but with FIXED geo shares from the spec rather than trailing-DAU
# shares. These tests exercise fixed_country_shares_from_spec + confirm `o`
# stacks with `l` on the same modern_windows training frame via a distinct
# idempotency sentinel.

# Stand-in migration start inside the desktop fixture's date range (real spec
# starts 2026-06-02; using 2026-05-08 here keeps the subtract/add tests non-vacuous
# against the 2026-03-15..2026-05-13 training fixture).
MOZONLINE_START = pd.Timestamp("2026-05-08")


def _mozonline_spec(shares, exclude=("IR",)):
    """Minimal desktop-overlay spec dict carrying a fixed geo allocation."""
    return {
        "allocation": {"key": "fixed_country_shares", "shares": dict(shares)},
        "scope": {"exclude_countries": list(exclude)},
    }


def _make_mozonline_lift_series(daily=10_000.0):
    """Flat migration curve from MOZONLINE_START (stand-in for Brad's model)."""
    idx = pd.date_range("2026-02-01", "2026-12-31", freq="D")
    values = np.where(idx >= MOZONLINE_START, daily, 0.0)
    return pd.Series(values, index=idx, name="migration_dau_daily")


# --- fixed_country_shares_from_spec ----------------------------------------

def test_fixed_shares_excludes_and_renormalizes():
    spec = _mozonline_spec({"CN": 0.9277, "US": 0.0152, "IR": 0.02}, exclude=("IR",))
    shares = fixed_country_shares_from_spec(spec, present_countries=["CN", "US", "DE"])
    # IR excluded by scope; DE absent from spec shares -> dropped.
    assert "IR" not in shares.index
    assert set(shares.index) == {"CN", "US"}
    # Renormalized over the two kept countries.
    assert abs(shares.sum() - 1.0) < 1e-12
    assert shares.loc["CN"] == pytest.approx(0.9277 / (0.9277 + 0.0152))
    assert shares.loc["CN"] > shares.loc["US"]


def test_fixed_shares_cn_dominant_full_footprint():
    spec = _mozonline_spec(
        {"CN": 0.9277, "HK": 0.0225, "US": 0.0152, "JP": 0.0098, "ROW": 0.0103},
        exclude=(),
    )
    shares = fixed_country_shares_from_spec(
        spec, present_countries=["CN", "HK", "US", "JP", "ROW"]
    )
    assert abs(shares.sum() - 1.0) < 1e-12
    assert shares.loc["CN"] == pytest.approx(0.9277 / 0.9855, rel=1e-6)  # ~0.941
    assert shares.loc["CN"] > 0.9


def test_fixed_shares_raises_when_no_country_present():
    spec = _mozonline_spec({"CN": 0.9277, "IR": 0.05}, exclude=("IR",))
    with pytest.raises(ValueError, match="No overlay geo shares remain"):
        fixed_country_shares_from_spec(spec, present_countries=["US", "DE", "BR"])


# --- subtract/add with fixed shares on modern_windows ----------------------

@pytest.fixture
def mozonline_lift_series():
    return _make_mozonline_lift_series()


@pytest.fixture
def mozonline_fixed_shares():
    # Fixed footprint over the desktop fixture's countries (US/DE/BR); sum==1.
    spec = _mozonline_spec({"US": 0.7, "DE": 0.2, "BR": 0.1, "IR": 0.5}, exclude=("IR",))
    return fixed_country_shares_from_spec(spec, present_countries=TEST_COUNTRIES)


def test_o_subtract_world_invariant_fixed_shares(
    desktop_training_df, mozonline_lift_series, mozonline_fixed_shares
):
    """Per-date sum of modern_windows subtractions == daily migration lift."""
    out = subtract_lift_from_training(
        desktop_training_df, daily_lift_series=mozonline_lift_series,
        country_shares=mozonline_fixed_shares, flag_column="modern_windows",
        sentinel_attr="mozillaonline_subtracted",
    )
    mw_in = desktop_training_df[desktop_training_df["modern_windows"] == True]  # noqa: E712
    mw_out = out[out["modern_windows"] == True]  # noqa: E712
    delta = mw_in["y"].astype("int64").to_numpy() - mw_out["y"].astype("int64").to_numpy()
    per_date = (pd.DataFrame({"x": mw_in["x"].values, "d": delta}).groupby("x")["d"].sum())
    per_date.index = pd.DatetimeIndex(per_date.index).normalize()
    for date, d in per_date.items():
        assert d == (pytest.approx(10_000, abs=2) if date >= MOZONLINE_START else 0)


def test_o_addback_world_and_per_country(mozonline_lift_series, mozonline_fixed_shares):
    fdf = _make_desktop_forecast_fixture()
    out = add_lift_to_forecast(
        fdf, daily_lift_series=mozonline_lift_series, country_shares=mozonline_fixed_shares,
        forecast_start=FORECAST_START, population_value="modern_windows",
    )
    # World ALL/ALL forecast rows get the full daily lift.
    world = (out["country"] == "ALL") & (out["population"] == "ALL") & (out["source"] == "forecast")
    assert np.allclose((out.loc[world, "DAU"] - fdf.loc[world, "DAU"]).values, 10_000.0)
    # Per-country modern_windows rows get lift * fixed share.
    for country in TEST_COUNTRIES:
        mask = ((out["country"] == country) & (out["population"] == "modern_windows")
                & (out["source"] == "forecast"))
        delta = out.loc[mask, "DAU"] - fdf.loc[mask, "DAU"]
        assert delta.values == pytest.approx(10_000 * mozonline_fixed_shares.loc[country])


def test_o_stacks_with_l_distinct_sentinel(
    desktop_training_df, lol_lift_series, mozonline_lift_series, mw_country_shares
):
    """`l` then `o` subtract from the same modern_windows frame; effects sum."""
    after_l = subtract_lift_from_training(
        desktop_training_df, daily_lift_series=lol_lift_series, country_shares=mw_country_shares,
        flag_column="modern_windows", sentinel_attr="launch_on_login_subtracted",
    )
    after_o = subtract_lift_from_training(
        after_l, daily_lift_series=mozonline_lift_series, country_shares=mw_country_shares,
        flag_column="modern_windows", sentinel_attr="mozillaonline_subtracted",
    )
    # Both sentinels present; no double-subtract error was raised.
    assert after_o.attrs["launch_on_login_subtracted"] is True
    assert after_o.attrs["mozillaonline_subtracted"] is True
    # On a date after both overlays start (>= 2026-05-08), modern_windows total
    # subtracted = 15k (l) + 10k (o) = 25k.
    late = pd.Timestamp("2026-05-11")
    mw_in = desktop_training_df[(desktop_training_df["modern_windows"] == True)  # noqa: E712
                                & (pd.to_datetime(desktop_training_df["x"]) == late)]
    mw_out = after_o[(after_o["modern_windows"] == True)  # noqa: E712
                     & (pd.to_datetime(after_o["x"]) == late)]
    total_delta = (mw_in["y"].astype("int64").to_numpy().sum()
                   - mw_out["y"].astype("int64").to_numpy().sum())
    assert total_delta == pytest.approx(25_000, abs=2)
    # winX/other rows are untouched by either overlay.
    non_mw_in = desktop_training_df[desktop_training_df["modern_windows"] == False]  # noqa: E712
    non_mw_out = after_o[after_o["modern_windows"] == False]  # noqa: E712
    pd.testing.assert_series_equal(
        non_mw_in["y"].reset_index(drop=True), non_mw_out["y"].reset_index(drop=True)
    )


# --- Registry + filename round-trips for `o` -------------------------------

def test_o_code_in_registry():
    registry = load_code_registry()
    assert "o" in registry
    assert registry["o"]["name"] == "mozillaonline_migration"


def test_o_code_canonical_and_marker():
    assert canonical_codes(["o", "l", "m"]) == "lmo"
    assert state_marker(["o", "l", "m"]) == "adj-lmo"
    assert state_marker(["o", "l", "m", "h"]) == "adj-hlmo"


def test_parse_state_from_path_adj_lmo():
    assert parse_state_from_path("foo.2026-06-29.gm+ld-D.adj-lmo.parquet") == ["l", "m", "o"]


def test_build_adjustments_applied_includes_mozillaonline(tmp_path):
    spec = tmp_path / "mozillaonline.json"
    spec.write_text(json.dumps({"type": "desktop_overlay"}))
    applied = build_adjustments_applied_list(["o"], {"o": spec})
    assert applied[0]["code"] == "o"
    assert applied[0]["name"] == "mozillaonline_migration"


def test_real_mozillaonline_spec_loads_as_overlay():
    """The committed July spec must load via the generic overlay loader."""
    repo_root = Path(__file__).resolve().parents[1]
    spec_path = repo_root / "data-official" / "2026-07" / "mozillaonline" / "mozillaonline.json"
    spec = load_overlay_spec(spec_path)
    assert spec["type"] == "desktop_overlay"
    assert spec["allocation"]["flag_column"] == "modern_windows"
    assert spec["value_column"] == "migration_dau_daily"
    assert spec["applies_to_forecast_start"] == "2026-07-06"
    assert spec["scope"]["exclude_countries"] == ["IR"]


# --- Registry + filename canonicalization round-trips for `p` --------------
#
# `p` (paid_organic_split) replaces `m` for mobile from the 2026-08 cycle. `m` stays
# registered so July's and August's pre-swap artifacts keep loading; these tests pin both
# facts, because dropping `m` would silently break `verify_forecast_states.py` on every
# historical mobile parquet.

def test_paid_organic_split_code_in_registry():
    registry = load_code_registry()
    assert "p" in registry
    assert registry["p"]["name"] == "paid_organic_split"
    assert registry["p"]["spec_glob"] == "data-official/*/organic/organic.json"


def test_retired_marketing_code_is_still_registered():
    """Retired for new cycles, but old .adj-m. artifacts must still load and verify."""
    registry = load_code_registry()
    assert "m" in registry
    assert registry["m"]["name"] == "marketing_lift"


def test_paid_organic_code_canonical_with_headwind():
    assert canonical_codes(["p", "h"]) == "hp"
    assert state_marker(["p", "h"]) == "adj-hp"


def test_parse_state_from_path_adj_p():
    assert parse_state_from_path("foo.2026-07-28.gm-D.adj-p.parquet") == ["p"]


def test_parse_state_from_path_adj_hp():
    assert parse_state_from_path("august_composite_28ma.adj-hp.csv") == ["h", "p"]


def test_paid_and_marketing_codes_produce_distinct_markers():
    """A cycle must be tellable from its filename alone: .adj-m. and .adj-p. are different
    methodologies, not different vintages of the same one."""
    assert state_marker(["m"]) != state_marker(["p"])
    assert insert_state_marker("f.2026-07-28.gm-D.parquet", ["p"]).name.endswith(".adj-p.parquet")


def test_build_adjustments_applied_list_accepts_p(tmp_path):
    spec_file = tmp_path / "organic.json"
    spec_file.write_text(json.dumps({"type": "paid_organic_split"}))
    out = build_adjustments_applied_list(
        codes=["p"], code_to_spec_file={"p": spec_file},
    )
    assert out[0]["code"] == "p"
    assert out[0]["name"] == "paid_organic_split"
    assert out[0]["spec_sha1"] == adjustment_spec_hash(spec_file)


# --- committed September display-layer specs -----------------------------------

def test_september_adjustments_dir_renders_h_u_and_t():
    """2026-09/adjustments (2026-09-04): `h` is Brad's Dec-15 value -726,000 ramped from the seam and flat
    after; `u` (-27,162) and `t` (+299,000) are the mobile legs, netting +271,838 as August did."""
    from pathlib import Path as _Path
    repo = _Path(__file__).resolve().parents[1]
    idx = pd.date_range("2026-08-01", "2027-12-31", freq="D")
    net = load_adjustments_from_dir(repo / "data-official" / "2026-09" / "adjustments", idx, require_specs=True)
    assert net["desktop"][pd.Timestamp("2026-09-02")] == 0.0
    assert net["desktop"][pd.Timestamp("2026-12-15")] == pytest.approx(-726000.0)
    assert net["desktop"][pd.Timestamp("2027-12-31")] == pytest.approx(-726000.0)   # clamped, unlike August
    assert net["mobile"][pd.Timestamp("2026-09-02")] == 0.0
    assert net["mobile"][pd.Timestamp("2026-12-15")] == pytest.approx(-27162.0 + 299000.0)  # `u` + `t`, netting +271,838 as August did
