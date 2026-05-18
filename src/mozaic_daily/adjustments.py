"""Forecast-adjustment application and provenance tracking.

Adjustments (headwinds, tailwinds, etc.) shift a base forecast by some net DAU
amount over the forecast period. Each adjustment has a one-letter code
registered in ``data-official/adjustment_codes.yaml``. When applied, the code
appears in the output filename's state marker so the adjustment state of any
forecast artifact is always visible:

    forecast.2026-05-13.ld-D.raw.parquet            (no adjustments)
    forecast.2026-05-13.ld-D.adj-h.parquet          (headwinds)
    forecast.2026-05-13.ld-D.adj-ht.plus_iran.parquet (headwinds + tailwinds, Iran added back)

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
