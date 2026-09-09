"""Tests for ``mozaic_daily.combinatorics`` (adjustment subset enumeration and the Dec-15 table)."""
import pytest

from mozaic_daily.combinatorics import (
    all_subsets, combination_rows, drop_groups, dropped_codes, subset_label,
)


def test_all_subsets_enumerates_every_subset_once_smallest_first():
    subsets = all_subsets(["o", "i", "j"])
    assert len(subsets) == 8
    assert len(set(subsets)) == 8
    assert subsets[0] == frozenset()
    assert [len(s) for s in subsets] == [0, 1, 1, 1, 2, 2, 2, 3]
    # alphabetical inside a size band even though the input was unsorted
    assert [subset_label(s) for s in subsets[1:4]] == ["i", "j", "o"]
    assert [subset_label(s) for s in subsets[4:7]] == ["i+j", "i+o", "j+o"]


def test_all_subsets_dedupes_input_codes():
    assert len(all_subsets(["i", "i", "j"])) == 4


def test_subset_label_matches_ladder_cache_naming():
    assert subset_label([]) == "raw"
    assert subset_label(["o", "j"]) == "j+o"


def test_dropped_codes_is_the_complement():
    assert dropped_codes(["j"], ["i", "j", "l", "o"]) == ["i", "l", "o"]
    assert dropped_codes(["i", "j", "l", "o"], ["i", "j", "l", "o"]) == []


def _run_values():
    codes = ["a", "b"]
    return codes, {
        frozenset(): 100.0,
        frozenset({"a"}): 110.0,
        frozenset({"b"}): 95.0,
        frozenset({"a", "b"}): 108.0,
    }


def test_combination_rows_adds_display_layer_and_computes_deltas():
    codes, run_dec15 = _run_values()
    rows = combination_rows(all_codes=codes, run_dec15=run_dec15, display_total_dec15=-10.0,
                            prior_dec15=90.0, full_dec15=98.0, targets={"Low": 80.0, "Stretch": 120.0})
    by_label = {r["label"]: r for r in rows}
    full = by_label["a+b"]
    assert full["dec15"] == pytest.approx(98.0)         # 108 - 10
    assert full["delta_vs_prior"] == pytest.approx(8.0)
    assert full["pct_vs_prior"] == pytest.approx(8.0 / 90.0 * 100)
    assert full["delta_vs_full"] == pytest.approx(0.0)
    assert full["delta_vs_target_low"] == pytest.approx(18.0)
    assert full["delta_vs_target_stretch"] == pytest.approx(-22.0)
    raw = by_label["raw"]
    assert raw["dec15"] == pytest.approx(90.0)
    assert raw["delta_vs_full"] == pytest.approx(-8.0)
    assert raw["dropped"] == ["a", "b"] and raw["n_dropped"] == 2


def test_combination_rows_ordered_by_number_dropped_then_label():
    codes, run_dec15 = _run_values()
    rows = combination_rows(all_codes=codes, run_dec15=run_dec15, display_total_dec15=0.0,
                            prior_dec15=1.0, full_dec15=1.0, targets={})
    assert [r["label"] for r in rows] == ["a+b", "a", "b", "raw"]
    assert [r["n_dropped"] for r in rows] == [0, 1, 1, 2]


def test_combination_rows_raises_when_a_subset_has_no_run():
    codes, run_dec15 = _run_values()
    del run_dec15[frozenset({"b"})]
    with pytest.raises(KeyError, match=r"\['b'\]"):
        combination_rows(all_codes=codes, run_dec15=run_dec15, display_total_dec15=0.0,
                         prior_dec15=1.0, full_dec15=1.0, targets={})


def test_drop_groups_partitions_rows_by_count_in_order():
    codes, run_dec15 = _run_values()
    rows = combination_rows(all_codes=codes, run_dec15=run_dec15, display_total_dec15=0.0,
                            prior_dec15=1.0, full_dec15=1.0, targets={})
    groups = drop_groups(rows)
    assert list(groups) == [0, 1, 2]
    assert [r["label"] for r in groups[1]] == ["a", "b"]
    assert sum(len(g) for g in groups.values()) == len(rows)
