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

2. **Per-tile bidirectional applier** (``subtract_lift_from_training`` +
   ``add_lift_to_forecast``) — mutates the long-format training ``DataFrame``
   before mozaic runs and the long-format granular forecast ``DataFrame`` after
   mozaic runs, so the model itself learns the no-adjustment dynamic. Examples:
   ``l``, ``o`` (``desktop_overlay`` specs, dispatched from the registry by
   ``overlays.py``) and the retired ``m`` (marketing-lift).

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
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import yaml


# Anchored to the checkout, not the cwd: main() is run from scan dirs and temp dirs.
_REGISTRY_PATH_DEFAULT = Path(__file__).resolve().parents[2] / "data-official" / "adjustment_codes.yaml"
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

DISPLAY_LAYER_SPEC_TYPES = ("linear_ramp", "step", "daily_series", "daily_file")
DAILY_FILE_PLATFORMS = ("desktop", "mobile")
DISPLAY_MA_WINDOW_DAYS = 28


def render_adjustment(spec: dict, date_index, spec_dir: str | Path | None = None) -> dict[str, pd.Series]:
    """Convert a single adjustment spec to net DAU Series for desktop and mobile.

    Supports spec types: ``linear_ramp``, ``step``, ``daily_series``, ``daily_file``.
    Returns ``{"desktop": Series, "mobile": Series}`` indexed by date.

    ``daily_file`` reads a curve from a parquet next to the spec (``data_file``,
    ``value_column``, ``platform``) and applies its trailing 28-day mean, because the
    display layer these adjustments land on is itself a 28-day MA. A producer who already
    delivered the 28-day series sets ``values_are_28d_ma: true`` and the values are used
    as-is. ``spec_dir`` is required for that type and ignored by the others.
    """
    idx = pd.DatetimeIndex(date_index)
    desktop = pd.Series(0.0, index=idx)
    mobile = pd.Series(0.0, index=idx)

    if spec["type"] == "linear_ramp":
        start = pd.Timestamp(spec["start_date"])
        anchor = pd.Timestamp(spec["anchor_date"])
        total_days = (anchor - start).days
        elapsed = np.maximum(0, (idx - start).days)
        if spec.get("clamp_at_anchor", False):
            # Hold at the anchor value past anchor_date. Off by default: every cycle through
            # 2026-08 published the unclamped ramp, and their specs must keep rendering as they did.
            elapsed = np.minimum(elapsed, total_days)
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

    elif spec["type"] == "daily_file":
        curve = _render_daily_file_curve(spec, idx, spec_dir)
        if spec["platform"] == "desktop":
            desktop[:] = curve.values
        else:
            mobile[:] = curve.values

    else:
        raise ValueError(f"Unknown adjustment spec type: {spec['type']}")

    return {"desktop": desktop, "mobile": mobile}


def _render_daily_file_curve(spec: dict, idx: pd.DatetimeIndex, spec_dir: str | Path | None) -> pd.Series:
    """Trailing 28-day mean of a ``daily_file`` spec's curve, aligned to ``idx``.

    Dates before the file starts contribute zero; dates after it ends hold the last
    value (the ingest step already extends every curve to the forecast horizon, so
    this only matters for an index that outruns the file).
    """
    if spec_dir is None:
        raise ValueError(
            "daily_file adjustment specs need spec_dir so data_file can be resolved; "
            "pass the directory the spec JSON lives in"
        )
    platform = spec.get("platform")
    if platform not in DAILY_FILE_PLATFORMS:
        raise ValueError(
            f"daily_file spec platform={platform!r}; expected one of {DAILY_FILE_PLATFORMS}"
        )
    series = load_lift_series(spec, spec_dir)
    if not spec.get("values_are_28d_ma", False):
        # A daily curve: the display layer is a 28d MA, so smooth it the same way.
        series = series.rolling(DISPLAY_MA_WINDOW_DAYS, min_periods=1).mean()
    return series.reindex(idx).ffill().fillna(0.0)


def load_adjustments_from_dir(
    adjustments_dir: str | Path,
    date_index,
    *,
    require_specs: bool = False,
) -> dict[str, pd.Series]:
    """Sum every ``*.json`` adjustment spec in ``adjustments_dir`` into net DAU series.

    Specs here are live by presence — there is no date gate. ``require_specs=True``
    raises on an empty directory instead of returning zeros; the canonical notebook
    wants that, because an empty ``adjustments/`` would silently publish a
    pre-headwind number.
    """
    idx = pd.DatetimeIndex(date_index)
    desktop_total = pd.Series(0.0, index=idx)
    mobile_total = pd.Series(0.0, index=idx)
    spec_paths = sorted(_glob.glob(f"{adjustments_dir}/*.json"))
    if require_specs and not spec_paths:
        raise FileNotFoundError(
            f"No adjustment specs in {adjustments_dir}; refusing to render a zero adjustment"
        )
    for path in spec_paths:
        with open(path) as f:
            spec = json.load(f)
        rendered = render_adjustment(spec, idx, spec_dir=Path(path).parent)
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


# --- Per-tile bidirectional overlay appliers (generic) --------------------
#
# Shared machinery for per-tile bidirectional adjustments. Applied around a
# mozaic run:
#
#   training_df = subtract_lift_from_training(training_df, flag_column=..., ...)
#   forecast_df = mozaic(training_df)
#   forecast_df = add_lift_to_forecast(forecast_df, population_value=..., ...)
#
# Consumers (thin wrappers / callers, defined below or in main.py):
#   ``m`` marketing_lift   — mobile, flag_column ``fenix_android``
#   ``l`` launch_on_login  — desktop, flag_column ``modern_windows``
#   ``o`` mozillaonline     — desktop, flag_column ``modern_windows``, fixed shares
#
# The same ``country_shares`` Series is used for both halves so the subtraction
# and add-back are byte-symmetric per (date, country, segment) tile. Shares are
# either computed from a trailing DAU window (``compute_country_shares``, e.g.
# ``m``/``l``) or supplied fixed from a spec (``fixed_country_shares_from_spec``,
# e.g. ``o``); the appliers don't care which. A v1 assumption is a stationary
# country mix frozen for the horizon.
#
# Column conventions:
# - Training DataFrame: ``x`` (date), ``country`` (str), boolean segment columns
#   (mobile: ``fenix_android``…; desktop: ``modern_windows``/``winX``), ``y``
#   (Int64). Rows are selected by ``flag_column == True``.
# - Forecast DataFrame (after ``combine_tables``, before the platform format fn):
#   ``target_date``, ``country`` (str, ``"ALL"`` for world rollup), ``population``
#   (segment name — app for mobile, OS for desktop — or ``"ALL"`` for the rollup),
#   ``source`` (``actual``/``forecast``), plus a metric column per metric.


def _load_lift_spec(
    spec_path: str | Path,
    *,
    expected_type: str,
    allocation_flag_key: str,
) -> dict:
    """Load and validate a bidirectional-overlay lift spec JSON.

    Shared by ``load_marketing_spec`` (``marketing_lift`` / ``app_flag_column``)
    and ``load_overlay_spec`` (``desktop_overlay`` / ``flag_column``). Raises
    ``ValueError`` if the spec's ``type`` doesn't match so we never pick up a
    different adjustment spec parked in the same directory.
    """
    spec_path = Path(spec_path)
    with open(spec_path) as f:
        spec = json.load(f)
    if spec.get("type") != expected_type:
        raise ValueError(
            f"Spec at {spec_path} has type={spec.get('type')!r}; "
            f"expected {expected_type!r}"
        )
    for required_key in ("data_file", "value_column", "allocation"):
        if required_key not in spec:
            raise ValueError(f"{expected_type} spec missing required key {required_key!r}")
    for required_alloc in (allocation_flag_key, "key", "window_days"):
        if required_alloc not in spec["allocation"]:
            raise ValueError(
                f"{expected_type} spec.allocation missing required key {required_alloc!r}"
            )
    return spec


def load_marketing_spec(spec_path: str | Path) -> dict:
    """Load a marketing-lift spec JSON and validate its required fields.

    Raises ``ValueError`` if the spec's ``type`` is not ``marketing_lift`` so
    we don't accidentally pick up a different adjustment spec parked in the
    same directory.
    """
    return _load_lift_spec(
        spec_path, expected_type="marketing_lift", allocation_flag_key="app_flag_column"
    )


def load_overlay_spec(spec_path: str | Path) -> dict:
    """Load a ``desktop_overlay`` spec JSON and validate its required fields.

    Desktop overlays (e.g. launch-on-login ``l``, MozillaOnline ``o``) share the
    marketing-lift bidirectional machinery but key allocation off a generic
    ``allocation.flag_column`` (a boolean segment column such as
    ``modern_windows``) rather than the mobile-specific ``app_flag_column``.
    """
    return _load_lift_spec(
        spec_path, expected_type="desktop_overlay", allocation_flag_key="flag_column"
    )


def load_lift_series(spec: dict, spec_dir: str | Path) -> pd.Series:
    """Load the daily DAU lift/overlay series from the spec's ``data_file``.

    Returns a Series indexed by ``DatetimeIndex`` (normalized to midnight), with
    the value column selected from the spec. Index must be unique and sorted.
    Shared by the marketing-lift and desktop-overlay appliers.
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
        raise ValueError(f"Non-unique dates in lift series at {data_path}")
    series = series.sort_index()
    return series


# Back-compat alias: the marketing series loader is fully generic.
load_marketing_lift_series = load_lift_series


def compute_country_shares(
    dau_training: pd.DataFrame,
    *,
    training_end_date: pd.Timestamp,
    window_days: int,
    flag_column: str = "fenix_android",
    exclude_countries: Iterable[str] = (),
) -> pd.Series:
    """Compute per-country share of a segment's DAU in a trailing window.

    Sums ``y`` over the trailing ``window_days`` ending at ``training_end_date``
    for rows where ``flag_column == True``, groups by ``country``, then
    normalizes so the result sums to 1.0. The ``flag_column`` is a boolean
    segment flag: ``fenix_android`` for mobile marketing-lift, ``modern_windows``
    for the launch-on-login desktop overlay, etc.

    ``exclude_countries`` are dropped before normalizing, so none of the curve is
    allocated to them and the remaining countries still sum to 1.0 — the same
    contract :func:`fixed_country_shares_from_spec` gives ``scope.exclude_countries``.

    Returned Series is indexed by country code. Countries with zero DAU in
    the window are dropped.
    """
    end_date = pd.Timestamp(training_end_date).normalize()
    start_date = end_date - pd.Timedelta(days=window_days - 1)
    x_as_ts = pd.to_datetime(dau_training["x"])
    in_window = (x_as_ts >= start_date) & (x_as_ts <= end_date)
    flag_only = dau_training[flag_column] == True  # noqa: E712
    sub = dau_training.loc[in_window & flag_only, ["country", "y"]]
    totals = sub.groupby("country")["y"].sum().astype("float64")
    totals = totals[totals > 0]
    totals = totals.drop(index=[c for c in exclude_countries if c in totals.index])
    if totals.empty:
        raise ValueError(
            f"No {flag_column} DAU found in window "
            f"{start_date.date()} → {end_date.date()} after excluding "
            f"{sorted(exclude_countries)}; cannot compute shares."
        )
    shares = totals / totals.sum()
    shares.name = "country_share"
    return shares


def compute_fenix_country_shares(
    mobile_dau_training: pd.DataFrame,
    *,
    training_end_date: pd.Timestamp,
    window_days: int,
    app_flag_column: str = "fenix_android",
) -> pd.Series:
    """Fenix Android trailing-window country shares (marketing-lift ``m``).

    Thin wrapper over :func:`compute_country_shares`; preserves the historical
    ``fenix_country_share`` Series name.
    """
    shares = compute_country_shares(
        mobile_dau_training,
        training_end_date=training_end_date,
        window_days=window_days,
        flag_column=app_flag_column,
    )
    shares.name = "fenix_country_share"
    return shares


def fixed_country_shares_from_spec(
    spec: dict,
    present_countries: Iterable[str],
) -> pd.Series:
    """Build renormalized fixed country shares from a desktop-overlay spec.

    For overlays whose geo footprint is a *fixed* allocation rather than a
    data-derived trailing-DAU share (e.g. MozillaOnline ``o``, ~93% China):
    reads ``spec["allocation"]["shares"]``, drops any
    ``spec["scope"]["exclude_countries"]`` (e.g. IR), restricts to the countries
    actually present in the training frame, then **renormalizes to sum 1.0**.

    Renormalization keeps the subtract⇄add-back symmetric: the ALL-country
    add-back is the full daily lift, so the per-country subtraction must also sum
    to the full daily lift over the countries that exist as tiles. This mirrors
    the sum-to-1 guarantee that :func:`compute_country_shares` provides for the
    trailing-DAU appliers. Countries named in the spec but absent from training
    (e.g. a market folded into ``ROW``) have their share redistributed
    proportionally across the present countries.

    Returned Series is indexed by country code and sums to 1.0.
    """
    raw_shares = spec["allocation"]["shares"]
    excluded = set(spec.get("scope", {}).get("exclude_countries", []))
    present = set(present_countries)
    kept = {
        country: float(share)
        for country, share in raw_shares.items()
        if country not in excluded and country in present
    }
    total = sum(kept.values())
    if total <= 0:
        raise ValueError(
            f"No overlay geo shares remain after excluding {sorted(excluded)} and "
            f"restricting to {len(present)} training countries; cannot allocate."
        )
    shares = pd.Series({country: share / total for country, share in kept.items()})
    shares.name = "country_share"
    return shares.sort_index()


def subtract_lift_from_training(
    dau_training: pd.DataFrame,
    *,
    daily_lift_series: pd.Series,
    country_shares: pd.Series,
    flag_column: str = "fenix_android",
    sentinel_attr: str = "lift_subtracted",
) -> pd.DataFrame:
    """Subtract adjustment-attributable DAU from a segment's training rows.

    For each training row with ``flag_column == True``, subtracts
    ``daily_lift_series[date] * country_shares[country]`` (rounded) from ``y``.
    ``flag_column`` selects the segment: ``fenix_android`` (marketing-lift) or
    ``modern_windows`` (launch-on-login desktop overlay).

    - Rows outside the flagged segment are returned unchanged.
    - Rows whose date is not in ``daily_lift_series`` contribute zero (the
      series is expected to be pre-launch zero, so this is a no-op for early
      training dates).
    - Countries not in ``country_shares`` contribute zero (typically because
      they had no DAU in the share-window — the lift attributed to them is zero
      by construction).
    - Sets ``df.attrs[sentinel_attr] = True`` as an idempotency sentinel;
      re-applying with the same ``sentinel_attr`` raises ``RuntimeError``. Two
      distinct overlays on the same DataFrame pass distinct ``sentinel_attr``.
    - Returns a copy; never mutates ``dau_training``.
    """
    if dau_training.attrs.get(sentinel_attr):
        raise RuntimeError(
            f"subtract_lift_from_training called twice with sentinel "
            f"{sentinel_attr!r} on the same DataFrame; this would "
            f"double-subtract. Pass the original training data instead."
        )
    result = dau_training.copy()
    result.attrs = dict(dau_training.attrs)
    segment_mask = result[flag_column] == True  # noqa: E712
    if not segment_mask.any():
        result.attrs[sentinel_attr] = True
        return result
    date_index_for_join = pd.to_datetime(result.loc[segment_mask, "x"]).dt.normalize()
    lift_at_row = date_index_for_join.map(daily_lift_series).fillna(0.0).to_numpy()
    share_at_row = result.loc[segment_mask, "country"].map(country_shares).fillna(0.0).to_numpy()
    to_subtract = np.round(lift_at_row * share_at_row).astype("int64")
    new_y = result.loc[segment_mask, "y"].astype("Int64") - pd.array(to_subtract, dtype="Int64")
    result.loc[segment_mask, "y"] = new_y
    result["y"] = result["y"].astype("Int64")
    result.attrs[sentinel_attr] = True
    return result


def subtract_marketing_lift_from_training(
    mobile_dau_training: pd.DataFrame,
    *,
    daily_lift_series: pd.Series,
    country_shares: pd.Series,
    app_flag_column: str = "fenix_android",
) -> pd.DataFrame:
    """Marketing-lift subtract (``m``). Thin wrapper over
    :func:`subtract_lift_from_training` with the historical
    ``marketing_lift_subtracted`` idempotency sentinel.
    """
    return subtract_lift_from_training(
        mobile_dau_training,
        daily_lift_series=daily_lift_series,
        country_shares=country_shares,
        flag_column=app_flag_column,
        sentinel_attr="marketing_lift_subtracted",
    )


def add_lift_to_forecast(
    forecast_df: pd.DataFrame,
    *,
    daily_lift_series: pd.Series,
    country_shares: pd.Series,
    forecast_start: pd.Timestamp,
    metric_column: str = "DAU",
    population_value: str = "fenix_android",
    all_population_value: str = "ALL",
    all_country_value: str = "ALL",
) -> pd.DataFrame:
    """Add adjustment-attributable DAU back into a per-tile forecast.

    Called *after* mozaic, *before* the platform format function (i.e. while the
    population column is still ``population`` with segment values like
    ``fenix_android`` / ``modern_windows`` and ``ALL``). ``population_value`` is
    the segment the overlay lands in: ``fenix_android`` (marketing-lift) or
    ``modern_windows`` (launch-on-login desktop overlay).

    Row patterns and add-values (``S`` = ``population_value``):

    +-----------------------------------------+---------------------------+
    | row pattern                             | add to ``metric_column``  |
    +=========================================+===========================+
    | country=X (!=ALL), population=S         | daily_lift * shares[X]    |
    +-----------------------------------------+---------------------------+
    | country=X (!=ALL), population='ALL'     | daily_lift * shares[X]    |
    +-----------------------------------------+---------------------------+
    | country='ALL', population=S             | daily_lift (full)         |
    +-----------------------------------------+---------------------------+
    | country='ALL', population='ALL'         | daily_lift (full)         |
    +-----------------------------------------+---------------------------+
    | other populations                       | unchanged                 |
    +-----------------------------------------+---------------------------+

    The add-back applies to **every** row (training AND forecast), restoring
    the lift we subtracted from post-launch training rows. The lift series is
    zero pre-launch and absent (filled with zero) outside its coverage, so rows
    in those date ranges are unchanged.

    Restoring training rows is essential for downstream consumers that compute
    rolling statistics across the training→forecast boundary (e.g. a 28-day
    moving average): without it, the MA at the boundary would average
    lift-subtracted training values with lift-added forecast values and look
    artificially depressed.

    ``forecast_start`` is accepted for parity with the other applier signatures
    and used only for logging/diagnostics; row gating is by lift value, not date.

    Returns a copy; never mutates ``forecast_df``.
    """
    result = forecast_df.copy()
    if metric_column not in result.columns:
        return result

    target_dates = pd.to_datetime(result["target_date"]).dt.normalize()
    lift_at_row = target_dates.map(daily_lift_series).fillna(0.0)

    country_share_at_row = result["country"].map(country_shares).fillna(0.0)
    is_world_country = result["country"] == all_country_value
    effective_share = country_share_at_row.where(~is_world_country, 1.0)

    is_segment_population = result["population"] == population_value
    is_all_population = result["population"] == all_population_value
    is_eligible_population = is_segment_population | is_all_population

    add_value = lift_at_row * effective_share
    apply_mask = is_eligible_population & (add_value != 0)
    result.loc[apply_mask, metric_column] = (
        result.loc[apply_mask, metric_column].astype("float64") + add_value.loc[apply_mask]
    )
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
    """Marketing-lift add-back (``m``). Thin wrapper over
    :func:`add_lift_to_forecast` (``app_population_value`` → ``population_value``).
    """
    return add_lift_to_forecast(
        forecast_df,
        daily_lift_series=daily_lift_series,
        country_shares=country_shares,
        forecast_start=forecast_start,
        metric_column=metric_column,
        population_value=app_population_value,
        all_population_value=all_population_value,
        all_country_value=all_country_value,
    )

