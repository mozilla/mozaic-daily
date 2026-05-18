"""Tests for src/mozaic_daily/adjustments.py.

These tests cover the filename-marker convention (.raw. / .adj-{codes}.),
sidecar meta round-tripping, state-consistency validation, and the actual
adjustment-rendering math (linear_ramp / step / daily_series).
"""
import json

import numpy as np
import pandas as pd
import pytest

from mozaic_daily.adjustments import (
    adjustment_spec_hash,
    apply_net_adjustment_to_series,
    build_adjustments_applied_list,
    canonical_codes,
    insert_state_marker,
    load_adjustments_from_dir,
    load_code_registry,
    load_forecast,
    meta_path,
    parse_state_from_path,
    read_meta,
    render_adjustment,
    state_marker,
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
