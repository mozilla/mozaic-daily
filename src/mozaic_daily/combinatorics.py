"""Adjustment combinatorics: every subset of the droppable overlays, scored at Dec-15.

The ladder (``ladder.py``) answers "what does each adjustment add, in impact order". This
module answers the planning question behind it: **if only some adjustments ship this cycle,
which subset should it be?** Every subset of the droppable per-tile overlays is a real model
run (cached with the ladder's content keys, so the two share runs); the pinned display-layer
codes (``h``) are exact and added at assembly time.

Pure logic only. ``scripts/build_adjustment_combinatorics.py`` owns the model runs and the
manifest; ``scripts/render_adjustment_combinatorics.py`` turns the manifest into the HTML
report.
"""
from __future__ import annotations

from itertools import combinations
from typing import Iterable, Mapping, Sequence

RAW_LABEL = "raw"


def all_subsets(codes: Iterable[str]) -> list[frozenset[str]]:
    """Every subset of ``codes`` (2^N of them), empty first, then by size, then alphabetically."""
    ordered = sorted(set(codes))
    subsets: list[frozenset[str]] = []
    for size in range(len(ordered) + 1):
        subsets.extend(frozenset(c) for c in combinations(ordered, size))
    return subsets


def subset_label(subset: Iterable[str]) -> str:
    """``raw`` for the empty set, else ``i+j+o`` -- the same label the ladder cache uses."""
    codes = sorted(subset)
    return "+".join(codes) if codes else RAW_LABEL


def dropped_codes(subset: Iterable[str], all_codes: Iterable[str]) -> list[str]:
    """The codes NOT in ``subset``; the row's "what was left out" column."""
    return sorted(set(all_codes) - set(subset))


def combination_rows(
    *,
    all_codes: Iterable[str],
    run_dec15: Mapping[frozenset[str], float],
    display_total_dec15: float,
    prior_dec15: float,
    full_dec15: float,
    targets: Mapping[str, float],
) -> list[dict]:
    """One row per subset: Dec-15 with the pinned display layer, and the deltas the table shows.

    ``run_dec15`` is each run's plain Dec-15 28d-MA (overlays baked in, no display layer);
    ``display_total_dec15`` is the summed Dec-15 effect of the pinned display-layer codes;
    ``prior_dec15`` is the previous cycle's delivered figure; ``full_dec15`` is the all-in
    build of this cycle, so ``delta_vs_full`` reads as "what leaving these out costs".
    Rows are sorted by the number of dropped codes, then by label, so the table reads as
    drop-none, drop-one, ..., drop-all.
    """
    codes = sorted(set(all_codes))
    missing = [s for s in all_subsets(codes) if s not in run_dec15]
    if missing:
        raise KeyError(f"no Dec-15 value for subset(s) {[subset_label(s) for s in missing]}")
    rows = []
    for subset in all_subsets(codes):
        dec15 = run_dec15[subset] + display_total_dec15
        dropped = dropped_codes(subset, codes)
        row = {
            "label": subset_label(subset),
            "included": sorted(subset),
            "dropped": dropped,
            "n_dropped": len(dropped),
            "dec15": dec15,
            "delta_vs_prior": dec15 - prior_dec15,
            "pct_vs_prior": (dec15 / prior_dec15 - 1) * 100,
            "delta_vs_full": dec15 - full_dec15,
        }
        for name, value in targets.items():
            row[f"delta_vs_target_{name.lower()}"] = dec15 - value
        rows.append(row)
    rows.sort(key=lambda r: (r["n_dropped"], r["label"]))
    return rows


def drop_groups(rows: Sequence[Mapping]) -> dict[int, list[Mapping]]:
    """Rows grouped by how many codes were dropped, for the report's one-section-per-count layout."""
    groups: dict[int, list[Mapping]] = {}
    for row in rows:
        groups.setdefault(row["n_dropped"], []).append(row)
    return dict(sorted(groups.items()))
