"""Tests for src/mozaic_daily/ladder.py -- the desktop adjustment ladder's pure logic."""
import json

import pandas as pd
import pytest

from mozaic_daily.ladder import (
    cumulative_curves,
    cumulative_subsets,
    fingerprint_overlay,
    ladder_rows,
    order_by_impact,
    rung_dir_name,
    rung_key,
    runs_required,
)

CONFIG = {"prophet_changepoint_prior_scale": 0.1649, "prophet_n_changepoints": 40}
FINGERPRINTS = {"i": "aaa", "j": "bbb", "o": "ccc"}


# --- fingerprint -------------------------------------------------------------------------

def _write_overlay(tmp_path, spec_bytes: bytes, curve_bytes: bytes):
    (tmp_path / "curve.parquet").write_bytes(curve_bytes)
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({"data_file": "curve.parquet", "payload": spec_bytes.decode()}))
    return spec


def test_fingerprint_changes_when_curve_changes_but_spec_does_not(tmp_path):
    spec = _write_overlay(tmp_path, b"same", b"curve-v1")
    before = fingerprint_overlay(spec)
    (tmp_path / "curve.parquet").write_bytes(b"curve-v2")
    assert fingerprint_overlay(spec) != before


def test_fingerprint_is_stable_for_identical_inputs(tmp_path):
    spec = _write_overlay(tmp_path, b"same", b"curve")
    assert fingerprint_overlay(spec) == fingerprint_overlay(spec)


def test_fingerprint_without_data_file_hashes_spec_only(tmp_path):
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({"type": "linear_ramp"}))
    assert len(fingerprint_overlay(spec)) == 40


# --- rung key ----------------------------------------------------------------------------

def test_rung_key_ignores_fingerprints_of_disabled_overlays():
    raw_before = rung_key(forecast_start="2026-09-02", model_config=CONFIG, enabled_codes=[], fingerprints=FINGERPRINTS)
    edited = {**FINGERPRINTS, "i": "i-edited"}
    raw_after = rung_key(forecast_start="2026-09-02", model_config=CONFIG, enabled_codes=[], fingerprints=edited)
    o_only_after = rung_key(forecast_start="2026-09-02", model_config=CONFIG, enabled_codes=["o"], fingerprints=edited)
    o_only_before = rung_key(forecast_start="2026-09-02", model_config=CONFIG, enabled_codes=["o"], fingerprints=FINGERPRINTS)
    assert raw_before == raw_after
    assert o_only_before == o_only_after


def test_rung_key_changes_when_an_enabled_overlay_is_edited():
    before = rung_key(forecast_start="2026-09-02", model_config=CONFIG, enabled_codes=["i", "o"], fingerprints=FINGERPRINTS)
    after = rung_key(forecast_start="2026-09-02", model_config=CONFIG, enabled_codes=["i", "o"],
                     fingerprints={**FINGERPRINTS, "i": "i-edited"})
    assert before != after


def test_rung_key_changes_with_config_and_seam():
    base = rung_key(forecast_start="2026-09-02", model_config=CONFIG, enabled_codes=["o"], fingerprints=FINGERPRINTS)
    assert rung_key(forecast_start="2026-09-03", model_config=CONFIG, enabled_codes=["o"], fingerprints=FINGERPRINTS) != base
    assert rung_key(forecast_start="2026-09-02", model_config={**CONFIG, "prophet_n_changepoints": 41},
                    enabled_codes=["o"], fingerprints=FINGERPRINTS) != base


def test_rung_key_is_order_independent_and_fixed_length():
    a = rung_key(forecast_start="2026-09-02", model_config=CONFIG, enabled_codes=["o", "i"], fingerprints=FINGERPRINTS)
    b = rung_key(forecast_start="2026-09-02", model_config=CONFIG, enabled_codes=["i", "o"], fingerprints=FINGERPRINTS)
    assert a == b and len(a) == 16


def test_rung_key_requires_fingerprint_for_every_enabled_code():
    with pytest.raises(KeyError, match="'z'"):
        rung_key(forecast_start="2026-09-02", model_config=CONFIG, enabled_codes=["z"], fingerprints=FINGERPRINTS)


def test_rung_dir_name_reads_as_codes_then_key():
    assert rung_dir_name([], "abc") == "raw.abc"
    assert rung_dir_name(["o", "i"], "abc") == "i+o.abc"


# --- ordering ----------------------------------------------------------------------------

def test_order_by_impact_is_absolute_value_descending_with_code_tiebreak():
    effects = {"h": -726_000, "o": 101_290, "j": 67_094, "i": 41_945, "x": -67_094}
    assert order_by_impact(effects) == ["h", "o", "j", "x", "i"]


def test_cumulative_subsets_only_grow_on_overlay_codes():
    subsets = cumulative_subsets(["h", "o", "j", "i"], overlay_codes={"o", "j", "i"})
    assert subsets == [frozenset(), frozenset(), {"o"}, {"o", "j"}, {"o", "j", "i"}]


def test_runs_required_dedupes_and_orders_raw_first():
    subsets = cumulative_subsets(["h", "o", "u", "j"], overlay_codes={"o", "j"})
    assert runs_required(subsets) == [frozenset(), frozenset({"o"}), frozenset({"j", "o"})]


# --- rows and curves ---------------------------------------------------------------------

RUN_DEC15 = {frozenset(): 48_000_000.0, frozenset({"o"}): 48_100_000.0, frozenset({"o", "j"}): 48_170_000.0}


def test_ladder_rows_accumulate_display_effects_and_steps():
    rows = ladder_rows(["h", "o", "j"], overlay_codes={"o", "j"}, run_dec15=RUN_DEC15,
                       display_effects_dec15={"h": -726_000.0})
    assert [r["added"] for r in rows] == [None, "h", "o", "j"]
    assert [r["dec15"] for r in rows] == [48_000_000.0, 47_274_000.0, 47_374_000.0, 47_444_000.0]
    assert rows[0]["step"] is None
    assert [r["step"] for r in rows[1:]] == [-726_000.0, 100_000.0, 70_000.0]
    assert rows[-1]["dec15"] == RUN_DEC15[frozenset({"o", "j"})] - 726_000.0


def test_cumulative_curves_apply_display_layer_only_from_seam():
    idx = pd.date_range("2026-08-30", "2026-09-05", freq="D")
    seam = pd.Timestamp("2026-09-02")
    run_curves = {
        frozenset(): pd.Series(100.0, index=idx),
        frozenset({"o"}): pd.Series(110.0, index=idx),
    }
    display = {"h": pd.Series(-5.0, index=idx)}   # renderer gives a value on every date
    curves = cumulative_curves(["h", "o"], overlay_codes={"o"}, run_curves=run_curves,
                               display_curves=display, seam=seam)
    assert list(curves) == ["raw", "+h", "+o"]
    assert (curves["raw"] == 100.0).all()
    assert (curves["+h"][idx < seam] == 100.0).all()
    assert (curves["+h"][idx >= seam] == 95.0).all()
    assert (curves["+o"][idx >= seam] == 105.0).all()   # overlay run + accumulated display layer
