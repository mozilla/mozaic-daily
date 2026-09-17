"""Tests for ``mozaic_daily.adjustment_effects`` -- the per-code views over the combinatorics subset table."""
import pandas as pd
import pytest

from mozaic_daily.adjustment_effects import (
    RunCode,
    compare_effects,
    country_daily_series,
    curves_long,
    dec15_by_country_table,
    effects_table,
    marginal_effects,
    pairwise_interactions,
    pass_through,
    shapley_values,
    single_effects,
    staleness_problems,
    subsets_table,
    world_daily_series,
)

# Three interacting codes. Hand-built so every expected number below is arithmetic on these literals.
VALUES = {
    frozenset(): 100.0,
    frozenset("a"): 110.0,          # a alone: +10
    frozenset("b"): 95.0,           # b alone: -5
    frozenset("c"): 103.0,          # c alone: +3
    frozenset("ab"): 108.0,         # a+b interact: 108 - 110 - 95 + 100 = +3
    frozenset("ac"): 113.0,         # additive
    frozenset("bc"): 98.0,          # additive
    frozenset("abc"): 112.0,
}


def test_single_effects_are_each_code_added_to_raw():
    assert single_effects(VALUES, "abc") == {"a": 10.0, "b": -5.0, "c": 3.0}


def test_marginal_effects_are_each_code_removed_from_all_in():
    # all-in 112: drop a -> bc 98 (a marginal +14); drop b -> ac 113 (b marginal -1); drop c -> ab 108 (+4)
    assert marginal_effects(VALUES, "abc") == {"a": 14.0, "b": -1.0, "c": 4.0}


def test_shapley_sums_to_all_in_minus_raw_and_splits_the_interaction_evenly():
    shap = shapley_values(VALUES, "abc")
    assert sum(shap.values()) == pytest.approx(112.0 - 100.0)
    # c never interacts, so its Shapley value is its single effect; a and b share the +3 interaction
    # (+1.5 each) plus a's +10 / b's -5 alone... but a+b+c = 112 not 111, so the three-way term (+1)
    # is shared equally: a = 10 + 1.5 + 1/3, b = -5 + 1.5 + 1/3, c = 3 + 1/3.
    assert shap["c"] == pytest.approx(3.0 + 1 / 3)
    assert shap["a"] == pytest.approx(10.0 + 1.5 + 1 / 3)
    assert shap["b"] == pytest.approx(-5.0 + 1.5 + 1 / 3)


def test_pairwise_interactions_flag_only_the_interacting_pair():
    inter = pairwise_interactions(VALUES, "abc")
    assert inter[("a", "b")] == pytest.approx(3.0)
    assert inter[("a", "c")] == pytest.approx(0.0)
    assert inter[("b", "c")] == pytest.approx(0.0)


def test_missing_subset_raises_with_its_label():
    partial = {k: v for k, v in VALUES.items() if k != frozenset("ac")}
    with pytest.raises(KeyError, match="'a\\+c'"):
        shapley_values(partial, "abc")


def test_pass_through_is_realized_over_nominal_and_none_without_a_curve():
    assert pass_through(50.0, 200.0) == 0.25
    assert pass_through(50.0, 0.0) is None


def test_effects_table_rows_per_code_with_display_layer_appended():
    table = effects_table(
        cycle="2026-09", seam="2026-09-09", platform="desktop", values=VALUES,
        run_codes=[RunCode("a", nominal_dec15=20.0, fingerprint="fa"), RunCode("b", 10.0, "fb"), RunCode("c", 3.0, "fc")],
        display_effects={"h": -7.0},
    )
    assert list(table["code"]) == ["a", "b", "c", "h"]
    a = table.set_index("code").loc["a"]
    assert a["single_dec15"] == 10.0 and a["marginal_dec15"] == 14.0
    assert a["pass_through_single"] == pytest.approx(0.5)
    assert a["kind"] == "model_run" and a["fingerprint"] == "fa"
    h = table.set_index("code").loc["h"]
    assert h["kind"] == "display_layer"
    assert h["nominal_dec15"] == h["single_dec15"] == h["shapley_dec15"] == -7.0
    assert h["pass_through_shapley"] == 1.0


def test_effects_table_rejects_a_non_finite_subset_value():
    broken = dict(VALUES)
    broken[frozenset("abc")] = float("nan")
    with pytest.raises(ValueError, match="non-finite"):
        effects_table(cycle="c", seam="s", platform="desktop", values=broken,
                      run_codes=[RunCode("a", 1.0), RunCode("b", 1.0), RunCode("c", 1.0)], display_effects={})


def test_subsets_table_adds_display_total_and_orders_by_size():
    table = subsets_table(cycle="c", seam="s", platform="desktop", values=VALUES, display_total_dec15=-7.0,
                          parquets={frozenset(): "raw.parquet"})
    assert list(table["label"])[:4] == ["raw", "a", "b", "c"]
    assert list(table["label"])[-1] == "a+b+c"
    raw = table.iloc[0]
    assert raw["dec15_with_display_layer"] == 93.0 and raw["parquet"] == "raw.parquet"
    assert table.iloc[-1]["codes"] == "abc" and table.iloc[-1]["n_codes"] == 3


def test_curves_long_adds_display_series_only_where_defined():
    idx = pd.date_range("2026-12-01", periods=3)
    display = pd.Series([1.0, 2.0], index=idx[:2])  # no value on the third day -> treated as 0
    out = curves_long(cycle="c", platform="desktop",
                      run_curves={"raw": pd.Series([10.0, 10.0, 10.0], index=idx)}, display_series=display)
    assert list(out["ma28_with_display_layer"]) == [11.0, 12.0, 10.0]
    assert list(out["label"].unique()) == ["raw"]


def test_dec15_by_country_table_is_long():
    out = dec15_by_country_table(cycle="c", platform="desktop",
                                 per_country={"raw": pd.Series({"ALL": 5.0, "US": 2.0})})
    assert out.shape[0] == 2
    assert set(out["country"]) == {"ALL", "US"}


def _forecast_frame():
    return pd.DataFrame({
        "target_date": ["2026-12-01", "2026-12-02", "2026-12-01", "2026-12-01", "2026-12-01"],
        "country": ["ALL", "ALL", "US", "ALL", "ALL"],
        "segment": ['{"os": "ALL"}'] * 3 + ['{"os": "winX"}', "{}"],
        "data_source": ["legacy_desktop"] * 4 + ["glean_mobile"],
        "app_name": ["desktop"] * 4 + ["ALL MOBILE"],
        "dau": [50.0, 51.0, 20.0, 9.0, 17.0],
    })


def test_world_daily_series_picks_the_platform_world_row_only():
    world = world_daily_series(_forecast_frame(), "desktop")
    assert list(world) == [50.0, 51.0]
    assert list(world_daily_series(_forecast_frame(), "mobile")) == [17.0]


def test_country_daily_series_excludes_other_segments():
    per_country = country_daily_series(_forecast_frame(), "desktop")
    assert set(per_country) == {"ALL", "US"}
    assert list(per_country["US"]) == [20.0]


def _effects(cycle, code_rows):
    return pd.DataFrame([{
        "cycle": cycle, "seam": "s", "platform": "desktop", "code": code, "kind": kind,
        "nominal_dec15": nominal, "single_dec15": effect, "marginal_dec15": effect, "shapley_dec15": effect,
        "pass_through_single": effect / nominal, "pass_through_shapley": effect / nominal, "fingerprint": None,
    } for code, kind, nominal, effect in code_rows])


def test_compare_effects_splits_delta_into_curve_and_pass_through_terms():
    prior = _effects("2026-08", [("o", "model_run", 500.0, 400.0), ("h", "display_layer", -1000.0, -1000.0)])
    current = _effects("2026-09", [("o", "model_run", 600.0, 300.0), ("h", "display_layer", -800.0, -800.0),
                                   ("j", "model_run", 40.0, 30.0)])
    out = compare_effects(prior, current).set_index("code")
    o = out.loc["o"]
    assert o["status"] == "carried" and o["delta_effect"] == pytest.approx(-100.0)
    # e1 = 500*0.8, e2 = 600*0.5: curve term (600-500)*0.8 = 80, pass-through term 600*(0.5-0.8) = -180
    assert o["from_curve_change"] == pytest.approx(80.0)
    assert o["from_pass_through_change"] == pytest.approx(-180.0)
    assert o["from_curve_change"] + o["from_pass_through_change"] == pytest.approx(o["delta_effect"])
    h = out.loc["h"]
    assert h["from_curve_change"] == pytest.approx(200.0) and h["from_pass_through_change"] == pytest.approx(0.0)
    j = out.loc["j"]
    assert j["status"] == "added" and j["delta_effect"] == 30.0 and pd.isna(j["from_curve_change"])


def test_compare_effects_marks_removed_codes():
    prior = _effects("2026-08", [("m", "model_run", 10.0, 5.0)])
    current = _effects("2026-09", [("p", "model_run", 10.0, 6.0)])
    out = compare_effects(prior, current).set_index("code")
    assert out.loc["m", "status"] == "removed" and out.loc["m", "delta_effect"] == -5.0
    assert out.loc["p", "status"] == "added"


def test_compare_effects_rejects_unknown_view():
    with pytest.raises(ValueError, match="view"):
        compare_effects(_effects("a", []), _effects("b", []), view="median")


# --- staleness -----------------------------------------------------------------------------------

def _manifest():
    return {
        "forecast_start": "2026-09-09", "model_config": {"cps": 0.1},
        "droppable_codes": ["i", "j"], "pinned_overlay_codes": ["l"],
        "overlay_fingerprints": {"i": "fi", "j": "fj", "l": "fl"},
        "runs": {"i+j+l": {"artifact_sha1": "sha-all"}},
        "mobile_runs": {"raw": {"artifact_sha1": "sha-raw"}, "p": {"artifact_sha1": "sha-p"}},
        "mobile_model_config": {"cps": 0.035},
    }


def _desktop_meta():
    return {"forecast_start_date": "2026-09-09", "model_config": {"cps": 0.1}, "artifact_sha1": "sha-all",
            "adjustments_applied": [{"code": "i"}, {"code": "j"}, {"code": "l"}]}


def test_staleness_is_empty_when_everything_matches():
    assert staleness_problems(_manifest(), _desktop_meta(), {"i": "fi", "j": "fj", "l": "fl"}, platform="desktop") == []
    mobile_meta = {"forecast_start_date": "2026-09-09", "model_config": {"cps": 0.035}, "artifact_sha1": "sha-p",
                   "adjustments_applied": [{"code": "p"}]}
    assert staleness_problems(_manifest(), mobile_meta, {}, platform="mobile") == []


def test_staleness_reports_a_moved_seam_and_a_changed_curve():
    meta = _desktop_meta()
    meta["forecast_start_date"] = "2026-09-16"
    problems = staleness_problems(_manifest(), meta, {"i": "fi", "j": "fj-edited", "l": "fl"}, platform="desktop")
    assert any("seam" in p for p in problems)
    assert any(p.startswith("overlay j") for p in problems)
    assert not any(p.startswith("overlay i") for p in problems)


def test_staleness_reports_a_different_overlay_set_and_a_non_canonical_all_in_run():
    meta = _desktop_meta()
    meta["adjustments_applied"].append({"code": "e"})
    problems = staleness_problems(_manifest(), meta, {"i": "fi", "j": "fj", "l": "fl", "e": "fe"}, platform="desktop")
    assert any("overlay set" in p for p in problems)
    meta = _desktop_meta()
    meta["artifact_sha1"] = "rebuilt"
    problems = staleness_problems(_manifest(), meta, {"i": "fi", "j": "fj", "l": "fl"}, platform="desktop")
    assert problems == ["desktop all-in run is not the canonical parquet (artifact_sha1 differs)"]


def test_staleness_mobile_requires_the_block_and_the_canonical_p_run():
    manifest = _manifest()
    del manifest["mobile_runs"]
    meta = {"forecast_start_date": "2026-09-09", "model_config": {"cps": 0.035}, "artifact_sha1": "sha-p",
            "adjustments_applied": [{"code": "p"}]}
    assert staleness_problems(manifest, meta, {}, platform="mobile") == ["manifest has no mobile_runs block"]
    meta["artifact_sha1"] = "other"
    assert staleness_problems(_manifest(), meta, {}, platform="mobile") == [
        "mobile p run is not the canonical parquet (artifact_sha1 differs)"]
