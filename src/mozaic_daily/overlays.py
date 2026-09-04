"""Registry-driven dispatch for per-tile overlay adjustments (``desktop_overlay`` specs).

An overlay is a daily DAU curve that is subtracted from one segment's training rows
before mozaic runs and added back to the per-tile forecast afterwards, so the model
does not extrapolate the effect implicitly. ``l`` (launch at login for new users) and ``o``
(MozillaOnline) are the references.

Before this module existed, ``main.py`` hand-wired every overlay code through seven
touch points (finder, pre-mozaic applier, parameter, docstring, pre call, post call,
CLI flag). Now the registry drives it: any code in ``data-official/adjustment_codes.yaml``
with ``applier: per_tile_overlay`` is discovered here, its spec located by
``spec_glob`` and gated on ``applies_to_forecast_start``, and applied to the data
source named in the spec's ``applies_to_data_source``. Adding a curve is a registry
entry plus a spec plus a data file, and no Python.

Two allocation keys decide how a world-total curve is split across country tiles:

- ``trailing_dau_share`` — proportional to each country's recent DAU in the flagged
  segment (``l``). Right for effects that hit users in proportion to population.
- ``fixed_country_shares`` — an explicit per-country dict in the spec (``o``, ~93% CN).
  Right for effects localized to specific countries.

Both honour ``scope.exclude_countries`` and renormalize the rest to 1.0.
"""
from __future__ import annotations

import glob
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from .adjustments import (
    add_lift_to_forecast,
    compute_country_shares,
    fixed_country_shares_from_spec,
    load_code_registry,
    load_lift_series,
    load_overlay_spec,
    subtract_lift_from_training,
)
from .queries import DataSource, Metric

PER_TILE_OVERLAY_APPLIER = "per_tile_overlay"
KNOWN_APPLIERS = ("display_layer", PER_TILE_OVERLAY_APPLIER, "marketing_lift", "paid_organic_split")
ALLOCATION_TRAILING = "trailing_dau_share"
ALLOCATION_FIXED = "fixed_country_shares"
ALLOCATION_KEYS = (ALLOCATION_TRAILING, ALLOCATION_FIXED)


def repo_root() -> Path:
    """The mozaic-daily checkout this package is imported from."""
    return Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class ResolvedOverlay:
    """One overlay spec that gates on the current forecast start."""

    code: str
    name: str
    spec_path: Path
    spec: dict

    @property
    def data_source(self) -> DataSource:
        return DataSource(self.spec["applies_to_data_source"])

    @property
    def sentinel_attr(self) -> str:
        """Idempotency marker set on the training frame; distinct per overlay so several stack."""
        return f"{self.name}_subtracted"

    @property
    def flag_column(self) -> str:
        return self.spec["allocation"]["flag_column"]

    @property
    def exclude_countries(self) -> list[str]:
        return list(self.spec.get("scope", {}).get("exclude_countries", []))


# --- Registry ----------------------------------------------------------------

def registered_overlay_codes(registry: Optional[dict] = None, registry_path: Path | None = None) -> dict[str, dict]:
    """Registry entries whose ``applier`` is ``per_tile_overlay``, keyed by code.

    Every entry must declare an ``applier`` from :data:`KNOWN_APPLIERS`; a missing or
    unknown value raises so a new code cannot be silently left out of dispatch.
    """
    if registry is None:
        registry = load_code_registry(registry_path)
    overlays = {}
    for code, entry in registry.items():
        applier = entry.get("applier")
        if applier not in KNOWN_APPLIERS:
            raise ValueError(
                f"adjustment code {code!r} has applier={applier!r}; every registry entry needs "
                f"one of {KNOWN_APPLIERS}"
            )
        if applier == PER_TILE_OVERLAY_APPLIER:
            overlays[code] = entry
    return overlays


def find_spec_for_forecast(
    pattern: str,
    forecast_start_date: str,
    label: str,
    root: Path | None = None,
) -> Optional[Path]:
    """Locate the single spec under ``root`` whose date gate matches.

    ``pattern`` is a glob relative to ``root`` (the repo root by default). Returns
    ``None`` when nothing matches — the "this adjustment does not apply to this cycle"
    path, which is not an error — and raises when more than one spec claims the same
    ``forecast_start_date``.

    The exact string match is deliberate and is the safety gate for the whole overlay
    system: a run at a date no spec claims applies *no* overlays and emits ``.raw.``,
    which the canonical notebook then rejects via ``load_forecast(..., require_state=[...])``.
    """
    root = root if root is not None else repo_root()
    matches = []
    for candidate in sorted(glob.glob(str(root / pattern))):
        with open(candidate) as f:
            spec = json.load(f)
        if spec.get("applies_to_forecast_start") == forecast_start_date:
            matches.append(Path(candidate))
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(
            f"Multiple {label} specs claim applies_to_forecast_start="
            f"{forecast_start_date!r}: {[str(m) for m in matches]}"
        )
    return matches[0]


def spec_globs(entry: dict) -> list[str]:
    """A registry entry's ``spec_glob`` as a list: the first is the current layout, later ones legacy."""
    globs = entry["spec_glob"]
    return [globs] if isinstance(globs, str) else list(globs)


def _find_across_globs(patterns: list[str], forecast_start_date: str, label: str, root: Path) -> Optional[Path]:
    """One spec across every layout a code has ever used; two layouts claiming one date is an error."""
    matches = [m for m in (find_spec_for_forecast(g, forecast_start_date, label, root) for g in patterns) if m is not None]
    if len(matches) > 1:
        raise ValueError(f"Multiple {label} specs claim applies_to_forecast_start={forecast_start_date!r} "
                         f"across layouts: {[str(m) for m in matches]}")
    return matches[0] if matches else None


def resolve_overlays(
    forecast_start_date: str,
    *,
    disabled_codes: Iterable[str] = (),
    registry: Optional[dict] = None,
    root: Path | None = None,
) -> list[ResolvedOverlay]:
    """Every registered overlay whose spec gates on ``forecast_start_date``, sorted by code.

    ``disabled_codes`` are skipped even when their spec matches (the ``--disable-adjustment``
    path). Each resolved spec is validated by ``load_overlay_spec`` and must also name
    ``applies_to_data_source`` and a known ``allocation.key``.
    """
    root = root if root is not None else repo_root()
    disabled = set(disabled_codes)
    resolved = []
    for code, entry in sorted(registered_overlay_codes(registry).items()):
        if code in disabled:
            continue
        spec_path = _find_across_globs(spec_globs(entry), forecast_start_date, entry["name"], root)
        if spec_path is None:
            continue
        spec = load_overlay_spec(spec_path)
        _validate_dispatch_keys(spec, spec_path)
        resolved.append(ResolvedOverlay(code=code, name=entry["name"], spec_path=spec_path, spec=spec))
    return resolved


def _validate_dispatch_keys(spec: dict, spec_path: Path) -> None:
    source = spec.get("applies_to_data_source")
    valid_sources = [ds.value for ds in DataSource]
    if source not in valid_sources:
        raise ValueError(
            f"{spec_path} has applies_to_data_source={source!r}; expected one of {valid_sources}"
        )
    key = spec["allocation"].get("key")
    if key not in ALLOCATION_KEYS:
        raise ValueError(f"{spec_path} has allocation.key={key!r}; expected one of {ALLOCATION_KEYS}")
    if key == ALLOCATION_FIXED and "shares" not in spec["allocation"]:
        raise ValueError(f"{spec_path} uses {ALLOCATION_FIXED} but has no allocation.shares")


# --- Applying -----------------------------------------------------------------

def overlay_country_shares(
    overlay: ResolvedOverlay,
    training_df: pd.DataFrame,
    training_end_date: pd.Timestamp,
) -> pd.Series:
    """Per-country allocation of the overlay's world total, by the spec's ``allocation.key``."""
    allocation = overlay.spec["allocation"]
    if allocation["key"] == ALLOCATION_TRAILING:
        return compute_country_shares(
            training_df,
            training_end_date=pd.Timestamp(training_end_date),
            window_days=allocation["window_days"],
            flag_column=overlay.flag_column,
            exclude_countries=overlay.exclude_countries,
        )
    present_countries = training_df.loc[training_df[overlay.flag_column] == True, "country"].unique()  # noqa: E712
    return fixed_country_shares_from_spec(overlay.spec, present_countries)


def subtract_overlays_pre_mozaic(
    source_data: dict,
    overlays: list[ResolvedOverlay],
    training_end_date: str,
) -> tuple[dict, list[dict]]:
    """Subtract each overlay's curve from the DAU training rows before mozaic runs.

    Returns ``(modified_source_data, contexts)``; each context carries the exact
    series and shares the add-back must reuse so subtract and add stay symmetric.
    """
    metric_key = Metric.DAU.value
    modified = dict(source_data)
    contexts = []
    for overlay in overlays:
        training_df = modified[metric_key]
        shares = overlay_country_shares(overlay, training_df, pd.Timestamp(training_end_date))
        daily_lift_series = load_lift_series(overlay.spec, overlay.spec_path.parent)
        n_flagged = int((training_df[overlay.flag_column] == True).sum())  # noqa: E712
        print(
            f"Overlay `{overlay.code}` ({overlay.name}): subtracting from {metric_key} training "
            f"({len(shares)} countries by {overlay.spec['allocation']['key']}, "
            f"{n_flagged} {overlay.flag_column} rows)"
        )
        modified[metric_key] = subtract_lift_from_training(
            training_df,
            daily_lift_series=daily_lift_series,
            country_shares=shares,
            flag_column=overlay.flag_column,
            sentinel_attr=overlay.sentinel_attr,
        )
        contexts.append({"overlay": overlay, "daily_lift_series": daily_lift_series, "country_shares": shares})
    return modified, contexts


def add_overlays_post_mozaic(
    df_combined: pd.DataFrame,
    contexts: list[dict],
    forecast_start: pd.Timestamp,
) -> pd.DataFrame:
    """Add each subtracted curve back to the per-tile forecast (before the format function)."""
    metric_key = Metric.DAU.value
    if metric_key not in df_combined.columns:
        return df_combined
    for context in contexts:
        overlay = context["overlay"]
        df_combined = add_lift_to_forecast(
            df_combined,
            daily_lift_series=context["daily_lift_series"],
            country_shares=context["country_shares"],
            forecast_start=pd.Timestamp(forecast_start),
            metric_column=metric_key,
            population_value=overlay.flag_column,
        )
        n_total = len(df_combined)
        n_forecast = int((df_combined["source"] == "forecast").sum())
        print(
            f"Overlay `{overlay.code}` ({overlay.name}): added back across {n_total} rows "
            f"({n_forecast} forecast + {n_total - n_forecast} training/actual)"
        )
    return df_combined
