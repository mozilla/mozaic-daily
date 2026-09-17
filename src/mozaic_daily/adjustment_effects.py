"""Adjustment effects: the per-code Dec-15 views derived from the combinatorics subset table.

The combinatorics build (``combinatorics.py`` + ``scripts/build_adjustment_combinatorics.py``)
forecasts every subset of a cycle's model-run adjustments. That table of subset values is the
one *fact*; everything a future cycle wants to ask -- "what did X add", "what would dropping X
cost", "how did X's impact move since last cycle" -- is a *view* derived here, so the tracked
export never restates a number by hand:

* **single**: ``f({X}) - f(raw)`` -- X added alone to the raw model.
* **marginal**: ``f(all) - f(all - {X})`` -- X taken away from the all-in build.
* **shapley**: the order-free average of X's marginal contribution over every subset. Sums
  exactly to ``f(all) - f(raw)``, which is what makes it the headline attribution when the
  overlays interact through the Prophet fit (single and marginal do not sum to anything).

Every code also carries its **nominal** Dec-15 value -- what the curve itself adds back (a
per-tile overlay's 28d-MA at Dec-15, the paid level for ``p``, the spec value for a
display-layer code) -- so the export stores a **pass-through ratio** (realized / nominal). That
ratio is what lets :func:`compare_effects` split a cycle-over-cycle change into "the curve moved"
and "the model absorbed a different fraction of it". Display-layer codes are exact by
construction: realized == nominal, pass-through 1.

Pure logic only; reading parquets and specs lives in ``scripts/export_adjustment_effects.py``.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import combinations
from math import factorial
from typing import Iterable, Mapping, Sequence

import pandas as pd

from mozaic_daily.combinatorics import RAW_LABEL, subset_label

EFFECT_VIEWS = ("single", "marginal", "shapley")
MODEL_RUN_KIND = "model_run"
DISPLAY_LAYER_KIND = "display_layer"

#: Row identity of the world-total series in a forecast parquet, per platform.
WORLD_FILTERS = {
    "desktop": dict(country="ALL", segment='{"os": "ALL"}', data_source="legacy_desktop", app_name="desktop"),
    "mobile": dict(country="ALL", segment="{}", data_source="glean_mobile", app_name="ALL MOBILE"),
}
#: Row identity of the per-country totals (all segments / all apps), per platform.
COUNTRY_FILTERS = {
    "desktop": dict(segment='{"os": "ALL"}', data_source="legacy_desktop", app_name="desktop"),
    "mobile": dict(segment="{}", data_source="glean_mobile", app_name="ALL MOBILE"),
}


@dataclass(frozen=True)
class RunCode:
    """One adjustment that is varied through model runs (a per-tile overlay, or ``p`` on mobile)."""

    code: str
    nominal_dec15: float
    fingerprint: str | None = None


# --- Subset-table views --------------------------------------------------------------------

def subset_value(values: Mapping[frozenset[str], float], subset: Iterable[str]) -> float:
    """``values[subset]`` with a readable error naming the missing subset."""
    key = frozenset(subset)
    if key not in values:
        raise KeyError(f"no Dec-15 value for subset {subset_label(key)!r}; run the combinatorics build first")
    return values[key]


def single_effects(values: Mapping[frozenset[str], float], codes: Iterable[str]) -> dict[str, float]:
    """Each code added alone to the raw model: ``f({c}) - f(raw)``."""
    raw = subset_value(values, ())
    return {c: subset_value(values, {c}) - raw for c in sorted(codes)}


def marginal_effects(values: Mapping[frozenset[str], float], codes: Iterable[str]) -> dict[str, float]:
    """Each code removed from the all-in build: ``f(all) - f(all - {c})``."""
    all_codes = frozenset(codes)
    full = subset_value(values, all_codes)
    return {c: full - subset_value(values, all_codes - {c}) for c in sorted(all_codes)}


def shapley_values(values: Mapping[frozenset[str], float], codes: Iterable[str]) -> dict[str, float]:
    """Exact Shapley attribution of ``f(all) - f(raw)`` over all 2^N subsets.

    For each code ``c`` and each subset ``S`` not containing it, the marginal ``f(S+c) - f(S)`` is
    weighted by ``|S|! (N-|S|-1)! / N!``. Needs every subset present.
    """
    ordered = sorted(set(codes))
    n = len(ordered)
    result: dict[str, float] = {}
    for code in ordered:
        others = [c for c in ordered if c != code]
        total = 0.0
        for size in range(n):
            weight = factorial(size) * factorial(n - size - 1) / factorial(n)
            for subset in combinations(others, size):
                without = frozenset(subset)
                total += weight * (subset_value(values, without | {code}) - subset_value(values, without))
        result[code] = total
    return result


def pairwise_interactions(values: Mapping[frozenset[str], float], codes: Iterable[str]) -> dict[tuple[str, str], float]:
    """``f({a,b}) - f({a}) - f({b}) + f(raw)`` for every pair; zero means the pair adds up."""
    raw = subset_value(values, ())
    result = {}
    for a, b in combinations(sorted(set(codes)), 2):
        result[(a, b)] = subset_value(values, {a, b}) - subset_value(values, {a}) - subset_value(values, {b}) + raw
    return result


def pass_through(realized: float, nominal: float) -> float | None:
    """Realized effect as a fraction of the curve's own value; ``None`` when there is no curve."""
    if nominal == 0:
        return None
    return realized / nominal


# --- Tracked tables ----------------------------------------------------------------------------

def effects_table(
    *,
    cycle: str,
    seam: str,
    platform: str,
    values: Mapping[frozenset[str], float],
    run_codes: Sequence[RunCode],
    display_effects: Mapping[str, float],
) -> pd.DataFrame:
    """One row per adjustment code: nominal, the three realized views, pass-through, fingerprint.

    ``values`` maps every subset of the run codes to that run's Dec-15 28d-MA (no display
    layer). Display-layer codes are appended with realized == nominal. Raises if the Shapley
    values do not sum to ``f(all) - f(raw)`` -- that identity is the module's own check.
    """
    codes = [rc.code for rc in run_codes]
    single = single_effects(values, codes)
    marginal = marginal_effects(values, codes)
    shapley = shapley_values(values, codes)
    span = subset_value(values, codes) - subset_value(values, ())
    if not all(math.isfinite(v) for v in values.values()):
        raise ValueError("non-finite Dec-15 value in the subset table; a run failed or was not scored")
    if abs(sum(shapley.values()) - span) > 1e-6 * max(1.0, abs(span)):
        raise ValueError(f"Shapley values sum to {sum(shapley.values()):,.3f} but all-in minus raw is {span:,.3f}")
    rows = []
    for rc in sorted(run_codes, key=lambda r: r.code):
        rows.append({
            "cycle": cycle, "seam": seam, "platform": platform, "code": rc.code, "kind": MODEL_RUN_KIND,
            "nominal_dec15": rc.nominal_dec15,
            "single_dec15": single[rc.code], "marginal_dec15": marginal[rc.code], "shapley_dec15": shapley[rc.code],
            "pass_through_single": pass_through(single[rc.code], rc.nominal_dec15),
            "pass_through_shapley": pass_through(shapley[rc.code], rc.nominal_dec15),
            "fingerprint": rc.fingerprint,
        })
    for code in sorted(display_effects):
        value = float(display_effects[code])
        rows.append({
            "cycle": cycle, "seam": seam, "platform": platform, "code": code, "kind": DISPLAY_LAYER_KIND,
            "nominal_dec15": value, "single_dec15": value, "marginal_dec15": value, "shapley_dec15": value,
            "pass_through_single": 1.0, "pass_through_shapley": 1.0, "fingerprint": None,
        })
    return pd.DataFrame(rows)


def subsets_table(
    *,
    cycle: str,
    seam: str,
    platform: str,
    values: Mapping[frozenset[str], float],
    display_total_dec15: float,
    parquets: Mapping[frozenset[str], str] | None = None,
) -> pd.DataFrame:
    """One row per subset: label, codes, Dec-15 without and with the display layer, run parquet."""
    rows = []
    for subset in sorted(values, key=lambda s: (len(s), sorted(s))):
        rows.append({
            "cycle": cycle, "seam": seam, "platform": platform,
            "label": subset_label(subset), "codes": "".join(sorted(subset)), "n_codes": len(subset),
            "dec15_no_display_layer": values[subset],
            "display_layer_dec15": display_total_dec15,
            "dec15_with_display_layer": values[subset] + display_total_dec15,
            "parquet": (parquets or {}).get(subset),
        })
    return pd.DataFrame(rows)


def curves_long(
    *,
    cycle: str,
    platform: str,
    run_curves: Mapping[str, pd.Series],
    display_series: pd.Series,
) -> pd.DataFrame:
    """Long frame: one row per (subset label, date) with the 28d-MA without and with the display layer.

    ``run_curves`` are date-indexed series (already the display MA); ``display_series`` is the
    summed display-layer series on a superset index, zero where it does not apply.
    """
    frames = []
    for label, curve in run_curves.items():
        display = display_series.reindex(curve.index, fill_value=0.0)
        frames.append(pd.DataFrame({
            "cycle": cycle, "platform": platform, "label": label, "date": curve.index,
            "ma28_no_display_layer": curve.to_numpy(),
            "ma28_with_display_layer": (curve + display).to_numpy(),
        }))
    return pd.concat(frames, ignore_index=True)


def dec15_by_country_table(*, cycle: str, platform: str, per_country: Mapping[str, pd.Series]) -> pd.DataFrame:
    """Long frame: one row per (subset label, country) with the plain Dec-15 28d-MA, no display layer."""
    frames = []
    for label, series in per_country.items():
        frames.append(pd.DataFrame({
            "cycle": cycle, "platform": platform, "label": label,
            "country": series.index, "dec15_no_display_layer": series.to_numpy(),
        }))
    return pd.concat(frames, ignore_index=True)


def world_daily_series(df: pd.DataFrame, platform: str) -> pd.Series:
    """The world-total daily DAU (training + forecast rows) of a forecast frame, date-indexed."""
    rows = _filter_rows(df, WORLD_FILTERS[platform])
    if rows.empty:
        raise ValueError(f"no world row for platform {platform!r} with {WORLD_FILTERS[platform]}")
    return _daily(rows)


def country_daily_series(df: pd.DataFrame, platform: str) -> dict[str, pd.Series]:
    """Per-country daily DAU (including ``ALL``), date-indexed, for the platform's total rows."""
    rows = _filter_rows(df, COUNTRY_FILTERS[platform])
    return {country: _daily(group) for country, group in rows.groupby("country", sort=True)}


def _filter_rows(df: pd.DataFrame, filters: Mapping[str, str]) -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    for column, value in filters.items():
        mask &= df[column] == value
    return df[mask]


def _daily(rows: pd.DataFrame) -> pd.Series:
    out = rows.assign(target_date=pd.to_datetime(rows["target_date"])).sort_values("target_date")
    return out.set_index("target_date")["dau"].astype(float)


# --- Cycle-over-cycle comparison ------------------------------------------------------------------

def compare_effects(prior: pd.DataFrame, current: pd.DataFrame, view: str = "shapley") -> pd.DataFrame:
    """Per (platform, code): how the realized Dec-15 effect moved, split into curve change and pass-through change.

    With ``e = nominal * pass_through`` on each side, the exact identity
    ``e2 - e1 = (n2 - n1) * pt1 + n2 * (pt2 - pt1)`` gives ``from_curve_change`` and
    ``from_pass_through_change``. Codes present on one side only are ``added`` / ``removed``
    and their whole effect is the delta. ``view`` picks the realized column (default Shapley).
    """
    if view not in EFFECT_VIEWS:
        raise ValueError(f"view must be one of {EFFECT_VIEWS}, got {view!r}")
    effect_col, pt_col = f"{view}_dec15", f"pass_through_{view}"
    keys = ["platform", "code"]
    left = _side(prior, effect_col, pt_col, "prior")
    right = _side(current, effect_col, pt_col, "current")
    merged = left.merge(right, on=keys, how="outer", suffixes=("", ""))
    merged["kind"] = merged["kind_current"].fillna(merged["kind_prior"])
    merged["status"] = "carried"
    merged.loc[merged["prior_effect"].isna(), "status"] = "added"
    merged.loc[merged["current_effect"].isna(), "status"] = "removed"
    merged["delta_effect"] = merged["current_effect"].fillna(0.0) - merged["prior_effect"].fillna(0.0)
    carried = merged["status"] == "carried"
    delta_nominal = merged["current_nominal"] - merged["prior_nominal"]
    delta_pt = merged["current_pass_through"] - merged["prior_pass_through"]
    merged["from_curve_change"] = (delta_nominal * merged["prior_pass_through"]).where(carried)
    merged["from_pass_through_change"] = (merged["current_nominal"] * delta_pt).where(carried)
    merged["view"] = view
    columns = ["platform", "code", "kind", "status", "view", "prior_cycle", "current_cycle",
               "prior_nominal", "current_nominal", "prior_pass_through", "current_pass_through",
               "prior_effect", "current_effect", "delta_effect", "from_curve_change", "from_pass_through_change"]
    return merged[columns].sort_values(["platform", "code"]).reset_index(drop=True)


def _side(table: pd.DataFrame, effect_col: str, pt_col: str, side: str) -> pd.DataFrame:
    if pt_col not in table.columns:  # marginal has no stored pass-through; derive it
        pt = table[effect_col] / table["nominal_dec15"].where(table["nominal_dec15"] != 0)
    else:
        pt = table[pt_col]
    return pd.DataFrame({
        "platform": table["platform"], "code": table["code"], f"kind_{side}": table["kind"],
        f"{side}_cycle": table["cycle"], f"{side}_nominal": table["nominal_dec15"].astype(float),
        f"{side}_pass_through": pt.astype(float), f"{side}_effect": table[effect_col].astype(float),
    })


# --- Currency check ----------------------------------------------------------------------------------

def staleness_problems(
    manifest: Mapping,
    canonical_meta: Mapping,
    live_fingerprints: Mapping[str, str],
    *,
    platform: str,
) -> list[str]:
    """Why a combinatorics manifest no longer describes the canonical build; empty means current.

    Desktop compares the manifest's seam, model config and the overlay set + fingerprints against
    the canonical sidecar (``forecast_start_date``, ``model_config``, ``adjustments_applied``) and
    the fingerprints recomputed from the specs on disk. Mobile compares the seam, the mobile
    config and that ``p`` is the (only) code, against the manifest's ``mobile_runs`` block.
    """
    problems: list[str] = []
    seam = canonical_meta.get("forecast_start_date")
    if manifest.get("forecast_start") != seam:
        problems.append(f"seam: manifest {manifest.get('forecast_start')} vs canonical {seam}")
    canonical_codes = sorted(a["code"] for a in canonical_meta.get("adjustments_applied", []))
    if platform == "desktop":
        if manifest.get("model_config") != canonical_meta.get("model_config"):
            problems.append("desktop model config differs from the canonical sidecar")
        manifest_codes = sorted(set(manifest.get("droppable_codes", [])) | set(manifest.get("pinned_overlay_codes", [])))
        if manifest_codes != canonical_codes:
            problems.append(f"desktop overlay set: manifest {manifest_codes} vs canonical {canonical_codes}")
        recorded = manifest.get("overlay_fingerprints", {})
        for code in canonical_codes:
            if recorded.get(code) != live_fingerprints.get(code):
                problems.append(f"overlay {code}: spec or curve changed since the manifest was built")
        all_in = manifest.get("runs", {}).get(subset_label(canonical_codes), {})
        _check_artifact(all_in, canonical_meta, "desktop all-in run", problems)
    elif platform == "mobile":
        mobile = manifest.get("mobile_runs")
        if not mobile:
            problems.append("manifest has no mobile_runs block")
            return problems
        if manifest.get("mobile_model_config") != canonical_meta.get("model_config"):
            problems.append("mobile model config differs from the canonical sidecar")
        if canonical_codes != ["p"]:
            problems.append(f"mobile canonical carries {canonical_codes}, expected ['p']")
        if set(mobile) != {RAW_LABEL, "p"}:
            problems.append(f"mobile_runs has {sorted(mobile)}, expected ['p', 'raw']")
        _check_artifact(mobile.get("p", {}), canonical_meta, "mobile p run", problems)
    else:
        raise ValueError(f"unknown platform {platform!r}")
    return problems


def _check_artifact(run: Mapping, canonical_meta: Mapping, what: str, problems: list[str]) -> None:
    """The manifest's run must be the canonical parquet itself (same ``artifact_sha1``)."""
    recorded, canonical = run.get("artifact_sha1"), canonical_meta.get("artifact_sha1")
    if recorded is None or canonical is None:
        problems.append(f"{what}: no artifact_sha1 to compare (manifest {recorded}, canonical {canonical})")
    elif recorded != canonical:
        problems.append(f"{what} is not the canonical parquet (artifact_sha1 differs)")
