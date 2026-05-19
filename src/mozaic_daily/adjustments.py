"""Forecast-adjustment application and provenance tracking.

Adjustments shift a base forecast by some DAU amount. Each adjustment has a
one-letter code registered in ``data-official/adjustment_codes.yaml``. When
applied, the code appears in the output filename's state marker so the
adjustment state of any forecast artifact is always visible:

    forecast.2026-05-13.ld-D.raw.parquet              (no adjustments)
    forecast.2026-05-13.ld-D.adj-h.parquet            (headwinds)
    forecast.2026-05-13.gm-D.adj-m.parquet            (marketing-lift)
    forecast.2026-05-13.gm-D.adj-hm.parquet           (headwinds + marketing-lift)

Two applier styles live here:

1. **Composite post-forecast applier** (``apply_net_adjustment_to_series``) —
   mutates a 28d-MA composite ``Series`` after the base forecast is in hand.
   Example: ``h`` (headwinds). Spec types: ``linear_ramp``, ``step``,
   ``daily_series``; rendered via ``render_adjustment()``.

2. **Per-tile bidirectional applier** (``subtract_marketing_lift_from_training``
   + ``add_marketing_lift_to_forecast``) — mutates the long-format training
   ``DataFrame`` before mozaic runs and the long-format granular forecast
   ``DataFrame`` after mozaic runs, so the model itself learns the no-adjustment
   dynamic. Example: ``m`` (marketing-lift).

Every artifact also has a sidecar ``<name>.meta.json`` recording full provenance
(model config, adjustments applied with hashes, parent file, git commit). Use
``load_forecast()`` to load an artifact — it validates marker/meta consistency
and refuses to load anything without a sidecar.
"""
from __future__ import annotations

import glob as _glob
import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

import numpy as np
import pandas as pd
import yaml


_REGISTRY_PATH_DEFAULT = Path("data-official/adjustment_codes.yaml")
_MARKER_RAW = "raw"
_MARKER_ADJ_PREFIX = "adj-"


# --- Code registry (YAML) --------------------------------------------------

def load_code_registry(path: Path | None = None) -> dict[str, dict]:
    """Load the adjustment-code registry from YAML. Returns the ``codes`` dict."""
    path = Path(path) if path is not None else _REGISTRY_PATH_DEFAULT
    with open(path) as f:
        doc = yaml.safe_load(f)
    return doc.get("codes", {})


def canonical_codes(codes: Iterable[str]) -> str:
    """Return alphabetically sorted, deduped, concatenated code string."""
    return "".join(sorted(set(codes)))


def state_marker(codes: Iterable[str]) -> str:
    """Filename marker: ``raw`` for empty input, ``adj-{codes}`` otherwise."""
    canon = canonical_codes(codes)
    return _MARKER_RAW if not canon else f"{_MARKER_ADJ_PREFIX}{canon}"


# --- Filename parsing / building -------------------------------------------

def parse_state_from_path(path: str | Path) -> list[str]:
    """Return the adjustment codes encoded in a forecast filename.

    Returns ``[]`` for ``.raw.`` files. Raises ``ValueError`` if no marker is present.
    """
    name = Path(path).name
    for part in name.split("."):
        if part == _MARKER_RAW:
            return []
        if part.startswith(_MARKER_ADJ_PREFIX):
            return sorted(part[len(_MARKER_ADJ_PREFIX):])
    raise ValueError(
        f"No state marker in filename: {name}. "
        f"Expected '.raw.' or '.adj-{{codes}}.'"
    )


def insert_state_marker(path: str | Path, codes: Iterable[str]) -> Path:
    """Return ``path`` with a state marker inserted before the file extension.

    If the filename contains ``.plus_iran.``, the marker is inserted before it
    so the canonical order is ``...{marker}.plus_iran.{ext}``.
    """
    p = Path(path)
    marker = state_marker(codes)
    name = p.name
    if ".plus_iran." in name:
        head, tail = name.split(".plus_iran.", 1)
        new_name = f"{head}.{marker}.plus_iran.{tail}"
    else:
        new_name = f"{p.stem}.{marker}{p.suffix}"
    return p.with_name(new_name)


# --- Sidecar metafile ------------------------------------------------------

def meta_path(artifact_path: str | Path) -> Path:
    """Return sidecar meta path: ``foo.adj-h.parquet`` -> ``foo.adj-h.parquet.meta.json``."""
    p = Path(artifact_path)
    return p.with_suffix(p.suffix + ".meta.json")


def _sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_commit(repo_path: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def write_meta(
    artifact_path: str | Path,
    *,
    forecast_start_date: str,
    data_source: str | None,
    produced_by: str,
    model_config: dict | None,
    adjustments_applied: list[dict],
    parent_file: str | Path | None = None,
    extra: dict | None = None,
) -> Path:
    """Write a sidecar ``.meta.json`` for a forecast artifact.

    ``adjustments_applied`` is a list of ``{code, name, spec_file, spec_sha1}``
    dicts — one per adjustment applied, in code-alphabetical order.

    For files renamed under the new convention but whose original provenance is
    unknown, pass ``extra={"provenance": "reconstructed"}``.
    """
    artifact_path = Path(artifact_path)
    meta = {
        "forecast_start_date": forecast_start_date,
        "data_source": data_source,
        "produced_by": produced_by,
        "produced_at": pd.Timestamp.utcnow().isoformat() + "Z",
        "model_config": model_config,
        "adjustments_applied": adjustments_applied,
        "artifact_sha1": _sha1_file(artifact_path) if artifact_path.exists() else None,
        "mozaic_daily_commit": _git_commit(Path(__file__).resolve().parents[2]),
    }
    if parent_file is not None:
        parent = Path(parent_file)
        meta["parent_file"] = str(parent)
        if parent.exists():
            meta["parent_file_sha1"] = _sha1_file(parent)
    if extra:
        meta.update(extra)
    out = meta_path(artifact_path)
    out.write_text(json.dumps(meta, indent=2, sort_keys=True))
    return out


def read_meta(artifact_path: str | Path) -> dict:
    """Read sidecar meta. Raises ``FileNotFoundError`` if missing."""
    return json.loads(meta_path(artifact_path).read_text())


# --- Loader ----------------------------------------------------------------

def load_forecast(
    path: str | Path,
    *,
    require_state: Iterable[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Load a forecast artifact and its sidecar meta with state validation.

    - Filename must carry a ``.raw.`` or ``.adj-{codes}.`` marker.
    - Sidecar ``.meta.json`` must exist alongside.
    - Codes in filename must match codes in ``meta["adjustments_applied"]``.
    - If ``require_state`` is given, filename codes must equal that set.

    Returns ``(df, meta)``.
    """
    path = Path(path)
    filename_codes = parse_state_from_path(path)
    if require_state is not None:
        expected = sorted(set(require_state))
        if filename_codes != expected:
            raise ValueError(
                f"State mismatch: {path.name} has codes {filename_codes}, "
                f"required {expected}"
            )
    if not meta_path(path).exists():
        raise FileNotFoundError(
            f"Sidecar meta missing for {path}. Expected at {meta_path(path)}. "
            f"All forecast artifacts must have a .meta.json."
        )
    meta = read_meta(path)
    meta_codes = sorted({a["code"] for a in meta.get("adjustments_applied", [])})
    if filename_codes != meta_codes:
        raise ValueError(
            f"State drift: {path.name} filename codes {filename_codes} "
            f"!= meta codes {meta_codes}"
        )
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")
    return df, meta


# --- Adjustment rendering --------------------------------------------------

def render_adjustment(spec: dict, date_index) -> dict[str, pd.Series]:
    """Convert a single adjustment spec to net DAU Series for desktop and mobile.

    Supports spec types: ``linear_ramp``, ``step``, ``daily_series``.
    Returns ``{"desktop": Series, "mobile": Series}`` indexed by date.
    """
    idx = pd.DatetimeIndex(date_index)
    desktop = pd.Series(0.0, index=idx)
    mobile = pd.Series(0.0, index=idx)

    if spec["type"] == "linear_ramp":
        start = pd.Timestamp(spec["start_date"])
        anchor = pd.Timestamp(spec["anchor_date"])
        total_days = (anchor - start).days
        elapsed = np.maximum(0, (idx - start).days)
        desktop[:] = spec.get("desktop_dau", 0) * elapsed / total_days
        mobile[:] = spec.get("mobile_dau", 0) * elapsed / total_days

    elif spec["type"] == "step":
        start = pd.Timestamp(spec["start_date"])
        end = pd.Timestamp(spec["end_date"]) if "end_date" in spec else idx[-1]
        mask = (idx >= start) & (idx <= end)
        desktop[mask] = spec.get("desktop_dau", 0)
        mobile[mask] = spec.get("mobile_dau", 0)

    elif spec["type"] == "daily_series":
        for date_str, values in spec["series"].items():
            date = pd.Timestamp(date_str)
            if date in idx:
                loc = idx.get_loc(date)
                desktop.iloc[loc] = values.get("desktop_dau", 0)
                mobile.iloc[loc] = values.get("mobile_dau", 0)

    else:
        raise ValueError(f"Unknown adjustment spec type: {spec['type']}")

    return {"desktop": desktop, "mobile": mobile}


def load_adjustments_from_dir(adjustments_dir: str | Path, date_index) -> dict[str, pd.Series]:
    """Sum every ``*.json`` adjustment spec in ``adjustments_dir`` into net DAU series."""
    idx = pd.DatetimeIndex(date_index)
    desktop_total = pd.Series(0.0, index=idx)
    mobile_total = pd.Series(0.0, index=idx)
    for path in sorted(_glob.glob(f"{adjustments_dir}/*.json")):
        with open(path) as f:
            spec = json.load(f)
        rendered = render_adjustment(spec, idx)
        desktop_total += rendered["desktop"]
        mobile_total += rendered["mobile"]
    return {"desktop": desktop_total, "mobile": mobile_total}


def apply_net_adjustment_to_series(
    ma_series: pd.Series,
    net_adjustments: dict[str, pd.Series],
    platform: str,
    forecast_start: pd.Timestamp,
) -> pd.Series:
    """Apply net adjustment to a 28-day MA series, starting at ``forecast_start``.

    ``forecast_start`` is required (no default) — for the N-1 comparison series,
    pass the *prior* forecast's start so the ramp anchors correctly.
    """
    result = ma_series.copy()
    forecast_mask = result.index >= forecast_start
    adj = net_adjustments[platform].reindex(result.index, fill_value=0.0)
    result[forecast_mask] += adj[forecast_mask]
    return result


# --- Adjustment spec hashing (for meta provenance) ------------------------

def adjustment_spec_hash(spec_file: str | Path) -> str:
    """SHA1 of an adjustment spec JSON file; used in meta ``adjustments_applied``."""
    return _sha1_file(Path(spec_file))


def build_adjustments_applied_list(
    codes: Iterable[str],
    code_to_spec_file: dict[str, str | Path],
    *,
    registry: dict[str, dict] | None = None,
) -> list[dict]:
    """Build the ``adjustments_applied`` field for ``write_meta()``.

    Output is sorted by code so meta files are byte-stable across runs.
    """
    if registry is None:
        registry = load_code_registry()
    out = []
    for code in sorted(set(codes)):
        if code not in registry:
            raise KeyError(f"Adjustment code {code!r} not in registry")
        spec_file = code_to_spec_file[code]
        out.append({
            "code": code,
            "name": registry[code]["name"],
            "spec_file": str(spec_file),
            "spec_sha1": adjustment_spec_hash(spec_file),
        })
    return out


# --- Per-tile marketing-lift applier --------------------------------------
#
# Code ``m``. Applied bidirectionally around the mobile mozaic run:
#
#   training_df = subtract_marketing_lift_from_training(training_df, ...)
#   forecast_df = mozaic(training_df)
#   forecast_df = add_marketing_lift_to_forecast(forecast_df, ...)
#
# The same ``country_shares`` Series is used for both halves so the subtraction
# and add-back are byte-symmetric per (date, country, fenix_android) tile.
# ``country_shares`` is a trailing-28d Fenix Android DAU share, computed once
# from training data and frozen for the entire forecast horizon (stationary
# country-mix is a v1 assumption — documented in
# data-official/{YYYY-MM}/marketing/README.md).
#
# Column conventions:
# - Training DataFrame: ``x`` (dbdate), ``country`` (str), one boolean column
#   per app (``fenix_android``, ``firefox_ios``, ``focus_android``,
#   ``focus_ios``), ``y`` (Int64). Exactly one app flag is True per row.
# - Forecast DataFrame (after ``combine_tables``, before ``update_mobile_format``):
#   ``target_date``, ``country`` (str, ``"ALL"`` for world rollup), ``population``
#   (app name or ``"ALL"`` for all-apps rollup), ``source`` (``actual`` or
#   ``forecast``), plus a metric column per metric (``DAU``, etc.).


def load_marketing_spec(spec_path: str | Path) -> dict:
    """Load a marketing-lift spec JSON and validate its required fields.

    Raises ``ValueError`` if the spec's ``type`` is not ``marketing_lift`` so
    we don't accidentally pick up a different adjustment spec parked in the
    same directory.
    """
    spec_path = Path(spec_path)
    with open(spec_path) as f:
        spec = json.load(f)
    if spec.get("type") != "marketing_lift":
        raise ValueError(
            f"Spec at {spec_path} has type={spec.get('type')!r}; "
            f"expected 'marketing_lift'"
        )
    for required_key in ("data_file", "value_column", "allocation"):
        if required_key not in spec:
            raise ValueError(f"marketing_lift spec missing required key {required_key!r}")
    for required_alloc in ("app_flag_column", "key", "window_days"):
        if required_alloc not in spec["allocation"]:
            raise ValueError(
                f"marketing_lift spec.allocation missing required key {required_alloc!r}"
            )
    return spec


def load_marketing_lift_series(spec: dict, spec_dir: str | Path) -> pd.Series:
    """Load the daily DAU lift series from the spec's ``data_file``.

    Returns a Series indexed by ``DatetimeIndex`` (normalized to midnight), with
    the value column selected from the spec. Index must be unique and sorted.
    """
    spec_dir = Path(spec_dir)
    data_path = spec_dir / spec["data_file"]
    df = pd.read_parquet(data_path)
    value_column = spec["value_column"]
    if value_column not in df.columns:
        raise ValueError(
            f"value_column {value_column!r} not found in {data_path}; "
            f"available: {list(df.columns)}"
        )
    series = df[value_column]
    series.index = pd.DatetimeIndex(series.index).normalize()
    if not series.index.is_unique:
        raise ValueError(f"Non-unique dates in marketing-lift series at {data_path}")
    series = series.sort_index()
    return series


def compute_fenix_country_shares(
    mobile_dau_training: pd.DataFrame,
    *,
    training_end_date: pd.Timestamp,
    window_days: int,
    app_flag_column: str = "fenix_android",
) -> pd.Series:
    """Compute per-country share of Fenix Android DAU in a trailing window.

    Sums ``y`` over the trailing ``window_days`` ending at ``training_end_date``
    for rows where ``app_flag_column == True``, groups by ``country``, then
    normalizes so the result sums to 1.0.

    Returned Series is indexed by country code. Countries with zero DAU in
    the window are dropped.
    """
    end_date = pd.Timestamp(training_end_date).normalize()
    start_date = end_date - pd.Timedelta(days=window_days - 1)
    x_as_ts = pd.to_datetime(mobile_dau_training["x"])
    in_window = (x_as_ts >= start_date) & (x_as_ts <= end_date)
    fenix_only = mobile_dau_training[app_flag_column] == True  # noqa: E712
    sub = mobile_dau_training.loc[in_window & fenix_only, ["country", "y"]]
    totals = sub.groupby("country")["y"].sum().astype("float64")
    totals = totals[totals > 0]
    if totals.empty:
        raise ValueError(
            f"No {app_flag_column} DAU found in window "
            f"{start_date.date()} → {end_date.date()}; cannot compute shares."
        )
    shares = totals / totals.sum()
    shares.name = "fenix_country_share"
    return shares


def subtract_marketing_lift_from_training(
    mobile_dau_training: pd.DataFrame,
    *,
    daily_lift_series: pd.Series,
    country_shares: pd.Series,
    app_flag_column: str = "fenix_android",
) -> pd.DataFrame:
    """Subtract marketing-attributable DAU from the Fenix Android training rows.

    For each training row with ``app_flag_column == True``, subtracts
    ``daily_lift_series[date] * country_shares[country]`` (rounded) from ``y``.

    - Non-Fenix rows are returned unchanged.
    - Rows whose date is not in ``daily_lift_series`` contribute zero (the
      series is expected to be pre-launch zero, so this is a no-op for early
      training dates).
    - Countries not in ``country_shares`` contribute zero (typically because
      they had no Fenix DAU in the share-window — the lift attributed to
      them is zero by construction).
    - Sets ``df.attrs['marketing_lift_subtracted'] = True`` as an idempotency
      sentinel. Re-applying to the same DataFrame raises ``RuntimeError``.
    - Returns a copy; never mutates ``mobile_dau_training``.
    """
    if mobile_dau_training.attrs.get("marketing_lift_subtracted"):
        raise RuntimeError(
            "subtract_marketing_lift_from_training called twice on the same "
            "DataFrame; this would double-subtract. Pass the original "
            "training data instead."
        )
    result = mobile_dau_training.copy()
    result.attrs = dict(mobile_dau_training.attrs)
    fenix_mask = result[app_flag_column] == True  # noqa: E712
    if not fenix_mask.any():
        result.attrs["marketing_lift_subtracted"] = True
        return result
    date_index_for_join = pd.to_datetime(result.loc[fenix_mask, "x"]).dt.normalize()
    lift_at_row = date_index_for_join.map(daily_lift_series).fillna(0.0).to_numpy()
    share_at_row = result.loc[fenix_mask, "country"].map(country_shares).fillna(0.0).to_numpy()
    to_subtract = np.round(lift_at_row * share_at_row).astype("int64")
    new_y = result.loc[fenix_mask, "y"].astype("Int64") - pd.array(to_subtract, dtype="Int64")
    result.loc[fenix_mask, "y"] = new_y
    result["y"] = result["y"].astype("Int64")
    result.attrs["marketing_lift_subtracted"] = True
    return result


def add_marketing_lift_to_forecast(
    forecast_df: pd.DataFrame,
    *,
    daily_lift_series: pd.Series,
    country_shares: pd.Series,
    forecast_start: pd.Timestamp,
    metric_column: str = "DAU",
    app_population_value: str = "fenix_android",
    all_population_value: str = "ALL",
    all_country_value: str = "ALL",
) -> pd.DataFrame:
    """Add marketing-attributable DAU back into a per-tile mobile forecast.

    Called *after* mozaic, *before* ``update_mobile_format`` (i.e. while the
    population column is still ``population`` with values like
    ``fenix_android`` and ``ALL``, not yet ``app_name``).

    Row patterns and add-values (only rows with ``source == 'forecast'`` are
    touched):

    +-----------------------------------------+---------------------------+
    | row pattern                             | add to ``metric_column``  |
    +=========================================+===========================+
    | country=X, population='fenix_android'   | daily_lift * shares[X]    |
    | (X != 'ALL')                            |                           |
    +-----------------------------------------+---------------------------+
    | country=X, population='ALL'             | daily_lift * shares[X]    |
    | (X != 'ALL')                            |                           |
    +-----------------------------------------+---------------------------+
    | country='ALL', population='fenix_android'| daily_lift (full)        |
    +-----------------------------------------+---------------------------+
    | country='ALL', population='ALL'         | daily_lift (full)         |
    +-----------------------------------------+---------------------------+
    | other (non-Fenix populations)           | unchanged                 |
    +-----------------------------------------+---------------------------+

    Returns a copy; never mutates ``forecast_df``.
    """
    result = forecast_df.copy()
    if metric_column not in result.columns:
        return result

    target_dates = pd.to_datetime(result["target_date"]).dt.normalize()
    in_forecast_period = (target_dates >= pd.Timestamp(forecast_start).normalize()) & (
        result["source"] == "forecast"
    )
    if not in_forecast_period.any():
        return result

    lift_at_row = target_dates.map(daily_lift_series).fillna(0.0)

    country_share_at_row = result["country"].map(country_shares).fillna(0.0)
    is_world_country = result["country"] == all_country_value
    effective_share = country_share_at_row.where(~is_world_country, 1.0)

    is_fenix_population = result["population"] == app_population_value
    is_all_population = result["population"] == all_population_value
    is_eligible_population = is_fenix_population | is_all_population

    add_value = lift_at_row * effective_share
    apply_mask = in_forecast_period & is_eligible_population
    result.loc[apply_mask, metric_column] = (
        result.loc[apply_mask, metric_column].astype("float64") + add_value.loc[apply_mask]
    )
    return result

