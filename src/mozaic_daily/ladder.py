"""Desktop adjustment ladder: cache keys, impact ordering, and curve assembly.

The ladder chart starts from the raw model curve and adds each adjustment in order of its
Dec-15 impact, largest first, ending at the published curve. Two kinds of adjustment take
part and they are handled differently:

* **Per-tile overlays** (``l``, ``o``, ``j``, ``i``): baked into the parquet, so every
  cumulative rung that adds one is a real model run. Runs are cached under a content key
  built from the seam, the model config and the fingerprints of *only the overlays enabled
  in that rung*, so editing one overlay's spec or curve invalidates just the rungs that
  contain it.
* **Display-layer adjustments** (``h``, ...): exact at Dec-15 and applied to the 28d MA
  after the model, so they need no run. They are added to a rung's curve at assembly time.

This module is pure logic. ``scripts/build_adjustment_ladder.py`` owns the model runs and
writes the manifest; the canonical notebook reads the manifest and calls
:func:`cumulative_curves`.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pandas as pd

KEY_LENGTH = 16
RAW_LABEL = "raw"


def fingerprint_overlay(spec_path: str | Path) -> str:
    """sha1 over the spec file plus the curve parquet it names (``data_file``), if any."""
    spec_path = Path(spec_path)
    digest = hashlib.sha1(spec_path.read_bytes())
    data_file = json.loads(spec_path.read_text()).get("data_file")
    if data_file:
        digest.update((spec_path.parent / data_file).read_bytes())
    return digest.hexdigest()


def rung_key(
    *,
    forecast_start: str,
    model_config: Mapping,
    enabled_codes: Iterable[str],
    fingerprints: Mapping[str, str],
) -> str:
    """Content key for one model run: seam + config + the enabled overlays' fingerprints.

    Overlays that are *not* enabled do not enter the key, which is what lets a spec edit to
    ``i`` leave the ``raw`` and ``o``-only rungs cached.
    """
    enabled = sorted(enabled_codes)
    missing = [code for code in enabled if code not in fingerprints]
    if missing:
        raise KeyError(f"no fingerprint for enabled overlay code(s) {missing}")
    payload = {
        "forecast_start": forecast_start,
        "model_config": dict(sorted(model_config.items())),
        "overlays": [(code, fingerprints[code]) for code in enabled],
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:KEY_LENGTH]


def rung_dir_name(enabled_codes: Iterable[str], key: str) -> str:
    """``raw.<key>`` or ``j+o.<key>`` -- readable prefix, unique suffix."""
    codes = sorted(enabled_codes)
    return f"{'+'.join(codes) if codes else RAW_LABEL}.{key}"


def order_by_impact(effects: Mapping[str, float]) -> list[str]:
    """Codes sorted by absolute Dec-15 effect, largest first; ties broken by code."""
    return sorted(effects, key=lambda code: (-abs(effects[code]), code))


def cumulative_subsets(order: Sequence[str], overlay_codes: Iterable[str]) -> list[frozenset[str]]:
    """The overlay subset each rung needs, rung 0 (raw) through rung len(order).

    Display-layer codes in ``order`` do not change the subset, so consecutive rungs can share
    a model run.
    """
    overlays = set(overlay_codes)
    subsets = [frozenset()]
    for code in order:
        previous = subsets[-1]
        subsets.append(previous | {code} if code in overlays else previous)
    return subsets


def runs_required(subsets: Iterable[frozenset[str]]) -> list[frozenset[str]]:
    """Distinct model runs behind a list of rung subsets, raw first, then by size."""
    return sorted(set(subsets), key=lambda s: (len(s), sorted(s)))


def ladder_rows(
    order: Sequence[str],
    overlay_codes: Iterable[str],
    run_dec15: Mapping[frozenset[str], float],
    display_effects_dec15: Mapping[str, float],
) -> list[dict]:
    """One row per rung: the code added, the cumulative Dec-15 value, and the step.

    ``run_dec15`` maps each overlay subset to that run's Dec-15 28d-MA; display-layer codes
    contribute their exact Dec-15 effect on top.
    """
    subsets = cumulative_subsets(order, overlay_codes)
    rows = []
    display_total = 0.0
    previous = None
    for index, subset in enumerate(subsets):
        added = None if index == 0 else order[index - 1]
        if added is not None and added not in set(overlay_codes):
            display_total += display_effects_dec15[added]
        value = run_dec15[subset] + display_total
        rows.append({
            "rung": index,
            "added": added,
            "overlay_subset": sorted(subset),
            "dec15": value,
            "step": None if previous is None else value - previous,
        })
        previous = value
    return rows


def cumulative_curves(
    order: Sequence[str],
    overlay_codes: Iterable[str],
    run_curves: Mapping[frozenset[str], pd.Series],
    display_curves: Mapping[str, pd.Series],
    seam: pd.Timestamp,
) -> dict[str, pd.Series]:
    """Rung label -> 28d-MA curve, display-layer pieces added from the seam forward.

    ``run_curves`` are the per-run display MAs (already seam-smoothed); ``display_curves`` are
    the rendered display-layer series on the same index. Labels are ``raw`` then
    ``+<code>`` so the plot legend reads as the build-up.
    """
    overlays = set(overlay_codes)
    subsets = cumulative_subsets(order, overlay_codes)
    base_index = run_curves[frozenset()].index
    display_total = pd.Series(0.0, index=base_index)
    curves = {}
    for index, subset in enumerate(subsets):
        label = RAW_LABEL if index == 0 else f"+{order[index - 1]}"
        added = None if index == 0 else order[index - 1]
        if added is not None and added not in overlays:
            piece = display_curves[added].reindex(base_index, fill_value=0.0)
            display_total = display_total + piece.where(base_index >= seam, 0.0)
        curves[label] = run_curves[subset].reindex(base_index) + display_total
    return curves
