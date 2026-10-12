# -*- coding: utf-8 -*-
"""Adapter: the legacy-to-glean per-cell export -> mozaic raw frames (OS x country tiles).

The 2027 Glean-history forecast (``data-official/2027-glean-draft/``) trains on the
legacy-to-glean project's *Glean-equivalent* history instead of a BigQuery pull. This module is
the only place that knows the export's layout. It turns the ``fc15`` per-cell parquet (one row
per ``submission_date`` x ``cell``, where ``cell`` is ``win_us`` / ``mac_ROW`` / ``global_all``
...) into the frame ``populate_tiles`` expects: ``x, country, windows, mac, linux, y`` with
exactly one OS flag True per row. The population labels mozaic derives are therefore
``windows`` / ``mac`` / ``linux`` -- not production's ``modern_windows`` / ``winX`` / ``other``,
because the translation has no Windows-version dimension.

Pure functions, then one writer (``write_raw_checkpoints``) that saves the frames under the
production checkpoint names so the forecast runner loads them like a cached BigQuery pull.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

# The headline column of the export (legacy-to-glean D-116): translated_from_legacy + glean_only_steady.
HEADLINE_COLUMN = "glean_equivalent"

# Production desktop DAU trains from 2020-01-01 (queries.py); the same floor applies here (decision 8).
DEFAULT_TRAINING_START = "2020-01-01"

OS_FLAGS = ("windows", "mac", "linux")

# Export cell prefix -> our OS flag column.
_PREFIX_TO_OS = {"win": "windows", "mac": "mac", "linux": "linux"}

# Export roll-up cells, kept only for the sum checks.
ROLLUP_CELLS = {"global_all", "global_ex_linux", "win_total", "mac_total", "linux_total"}

# Metric label -> export file stem. ``MAU`` is the rolling 28-day MAU of the export, a new
# Metric enum value (not ``Existing Engagement MAU``, which is a different population).
EXPORT_FILES = {
    "DAU": "glean_equivalent_dau_history_cells_fc15.parquet",
    "MAU": "glean_equivalent_mau_history_cells_fc15.parquet",
}

RAW_CHECKPOINT_TEMPLATE = "mozaic_parts.raw.glean_history.desktop.{metric}.parquet"


@dataclass(frozen=True)
class CellKey:
    os_flag: str
    country: str


def parse_cell(cell: str) -> CellKey | None:
    """``win_us`` -> (windows, US); ``mac_ROW`` -> (mac, ROW); roll-ups -> None."""
    if cell in ROLLUP_CELLS:
        return None
    prefix, _, country = cell.partition("_")
    if prefix not in _PREFIX_TO_OS or not country:
        raise ValueError(f"Unrecognised export cell {cell!r}; expected <win|mac|linux>_<country> or a roll-up")
    return CellKey(_PREFIX_TO_OS[prefix], country.upper())


def export_to_mozaic_frame(
    export: pd.DataFrame,
    *,
    value_column: str = HEADLINE_COLUMN,
    training_start: str = DEFAULT_TRAINING_START,
) -> pd.DataFrame:
    """Per-cell export rows -> ``x, country, windows, mac, linux, y`` (one flag True per row).

    Roll-up rows are dropped; dates before ``training_start`` are dropped; ``y`` is the chosen
    export column (the headline by default). Raises on NaN values or a non-dense date grid.
    """
    required = {"submission_date", "cell", value_column}
    missing = required - set(export.columns)
    if missing:
        raise ValueError(f"export is missing columns {sorted(missing)}; have {sorted(export.columns)}")

    keys = export["cell"].map(parse_cell)
    cells = export.loc[keys.notna()].copy()
    cells["country"] = keys[keys.notna()].map(lambda k: k.country).values
    cells["os_flag"] = keys[keys.notna()].map(lambda k: k.os_flag).values
    cells["x"] = pd.to_datetime(cells["submission_date"])
    cells = cells[cells["x"] >= pd.Timestamp(training_start)]
    if cells.empty:
        raise ValueError(f"no export rows on or after training_start={training_start}")

    for flag in OS_FLAGS:
        cells[flag] = cells["os_flag"] == flag
    cells["y"] = cells[value_column].astype(float)
    if cells["y"].isna().any():
        bad = cells.loc[cells["y"].isna(), ["x", "cell"]].head(5).to_dict("records")
        raise ValueError(f"{value_column} has NaN values in the export, e.g. {bad}")

    frame = cells[["x", "country", *OS_FLAGS, "y"]].sort_values(["x", "country", *OS_FLAGS]).reset_index(drop=True)
    check_one_flag_per_row(frame)
    check_dense_dates(frame)
    return frame


def check_one_flag_per_row(frame: pd.DataFrame) -> None:
    flags = frame[list(OS_FLAGS)].sum(axis=1)
    if not (flags == 1).all():
        raise ValueError(f"{(flags != 1).sum()} rows do not have exactly one OS flag set")


def check_dense_dates(frame: pd.DataFrame) -> None:
    """Every (country, OS) series must cover every day between the frame's first and last date."""
    expected = pd.date_range(frame["x"].min(), frame["x"].max(), freq="D")
    counts = frame.groupby(["country", *OS_FLAGS]).size()
    short = counts[counts != len(expected)]
    if not short.empty:
        raise ValueError(f"{len(short)} (country, OS) series are not dense over {expected[0].date()}..{expected[-1].date()}: "
                         f"{short.head(5).to_dict()}")


def check_sums_against_rollups(frame: pd.DataFrame, export: pd.DataFrame, *, value_column: str = HEADLINE_COLUMN,
                               tolerance: float = 1e-3) -> dict[str, float]:
    """Assert per-OS sums of the frame reproduce the export's ``{os}_total`` rows; returns max abs diff per OS."""
    export = export.assign(x=pd.to_datetime(export["submission_date"]))
    result = {}
    for prefix, flag in _PREFIX_TO_OS.items():
        ours = frame[frame[flag]].groupby("x")["y"].sum()
        theirs = export[export["cell"] == f"{prefix}_total"].set_index("x")[value_column].astype(float)
        theirs = theirs.reindex(ours.index)
        if theirs.isna().any():
            raise ValueError(f"export has no {prefix}_total row for some dates in the frame")
        diff = (ours - theirs).abs().max()
        result[flag] = float(diff)
        if diff > tolerance:
            raise ValueError(f"{flag}: cells sum differs from {prefix}_total by up to {diff:.3f} (> {tolerance})")
    return result


def _sha1(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_raw_checkpoints(
    export_dir: Path,
    out_dir: Path,
    *,
    metrics: tuple[str, ...] = ("DAU", "MAU"),
    value_column: str = HEADLINE_COLUMN,
    training_start: str = DEFAULT_TRAINING_START,
) -> dict[str, Path]:
    """Save one raw checkpoint per metric plus a sidecar; returns metric -> parquet path.

    Side effect: writes ``mozaic_parts.raw.glean_history.desktop.<metric>.parquet`` and
    ``glean_history_inputs.meta.json`` under ``out_dir``.
    """
    export_dir, out_dir = Path(export_dir), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    meta_inputs = {}
    for metric in metrics:
        source = export_dir / EXPORT_FILES[metric]
        export = pd.read_parquet(source)
        frame = export_to_mozaic_frame(export, value_column=value_column, training_start=training_start)
        sums = check_sums_against_rollups(frame, export, value_column=value_column)
        target = out_dir / RAW_CHECKPOINT_TEMPLATE.format(metric=metric)
        frame.to_parquet(target, index=False)
        written[metric] = target
        meta_inputs[metric] = {
            "source_file": str(source),
            "source_sha1": _sha1(source),
            "output_file": target.name,
            "rows": int(len(frame)),
            "countries": sorted(frame["country"].unique()),
            "date_range": [str(frame["x"].min().date()), str(frame["x"].max().date())],
            "max_abs_diff_vs_os_total": sums,
        }
    meta = {
        "produced_by": "mozaic_daily.glean_history.write_raw_checkpoints",
        "written_at": datetime.now().isoformat(timespec="seconds"),
        "value_column": value_column,
        "training_start": training_start,
        "populations": list(OS_FLAGS),
        "inputs": meta_inputs,
    }
    (out_dir / "glean_history_inputs.meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    return written
