"""Rename forecast artifacts to the new state-marker convention and write sidecar metas.

Reads ``tmp/inventory.csv`` (produced by ``scripts/verify_forecast_states.py``)
and for each row whose ``verified_state`` is ``raw`` or ``adj-h``:

  1. Renames the file to insert the state marker (``.raw.`` or ``.adj-h.``).
  2. Writes a sidecar ``.meta.json`` with reconstructed provenance — model config
     pulled from the sibling ``parameters.json``, adjustments from the matching
     ``adjustments/`` dir for adj-h files.

By default only the 11 load-bearing files (verified by bit-exact reproduction)
are migrated. Pass ``--include-scratch`` to also rename the 75 comparison/scan
files tagged by directory convention.

Always run with ``--dry-run`` first.

Usage:
    source .venv/bin/activate
    python scripts/migrate_forecast_names.py --dry-run
    python scripts/migrate_forecast_names.py            # execute
    python scripts/migrate_forecast_names.py --include-scratch --dry-run
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from mozaic_daily.adjustments import (  # noqa: E402
    build_adjustments_applied_list,
    insert_state_marker,
    load_code_registry,
    meta_path,
    write_meta,
)


# --- Provenance reconstruction ---------------------------------------------

def find_parameters_json(parquet_path: Path) -> Path | None:
    """Find the sibling parameters.json for an official forecast parquet."""
    candidate = parquet_path.parent / "parameters.json"
    return candidate if candidate.exists() else None


def find_adjustments_dir(file_path: Path) -> Path | None:
    """Find the matching adjustments/ dir for a forecast file.

    Checks parent directories first, then falls back to filename-based mapping
    for composite CSVs that live outside the forecast-date subdir.
    """
    for parent in file_path.parents:
        candidate = parent / "adjustments"
        if candidate.is_dir():
            return candidate
        if parent == REPO_ROOT:
            break
    name = file_path.name.lower()
    full_path_str = str(file_path)
    if "april" in name or "2026-04" in full_path_str:
        candidate = REPO_ROOT / "data-official" / "2026-04" / "adjustments"
        if candidate.is_dir():
            return candidate
    if "june" in name or "2026-06" in full_path_str:
        candidate = REPO_ROOT / "data-official" / "2026-06" / "adjustments"
        if candidate.is_dir():
            return candidate
    return None


def extract_model_config(parameters_json: Path | None, file_hint: str) -> dict | None:
    """Pull model config from a parameters.json file.

    ``file_hint`` is the parquet filename (e.g. 'ld-D' for desktop, 'gm-D' for mobile).
    Some parameters.json files have top-level desktop/mobile keys; others have a
    single ``platform`` field.
    """
    if parameters_json is None:
        return None
    data = json.loads(parameters_json.read_text())
    if "desktop" in data and "mobile" in data:
        return data["desktop"] if "ld-D" in file_hint else data["mobile"]
    if "platform" in data:
        return {k: v for k, v in data.items() if k != "platform"}
    return data


def data_source_from_hint(file_hint: str) -> str | None:
    if "ld-D" in file_hint:
        return "legacy_desktop"
    if "gm-D" in file_hint:
        return "glean_mobile"
    return None


def forecast_start_from_hint(file_hint: str) -> str | None:
    """Parse the date from filenames like mozaic_daily_forecast.2026-05-13.ld-D.parquet."""
    parts = file_hint.split(".")
    for p in parts:
        if len(p) == 10 and p.count("-") == 2:
            return p
    return None


# --- Migration -------------------------------------------------------------

def is_under_git(path: Path) -> bool:
    result = subprocess.run(
        ["git", "ls-files", "--error-unmatch", str(path)],
        cwd=REPO_ROOT,
        capture_output=True,
    )
    return result.returncode == 0


def rename_file(old: Path, new: Path, dry_run: bool) -> None:
    if not old.exists():
        raise FileNotFoundError(f"Source missing: {old}")
    if new.exists():
        raise FileExistsError(f"Target exists: {new}")
    if dry_run:
        return
    if is_under_git(old):
        subprocess.run(["git", "mv", str(old), str(new)], cwd=REPO_ROOT, check=True)
    else:
        shutil.move(str(old), str(new))


def migrate_load_bearing_row(row: dict, registry: dict, dry_run: bool) -> tuple[Path, Path, Path]:
    """Process one load-bearing inventory row. Returns (old, new, meta_path)."""
    old = REPO_ROOT / row["path"]
    state = row["verified_state"]
    codes = [] if state == "raw" else ["h"]
    new = insert_state_marker(old, codes)

    # Build meta
    fname = old.name
    params_json = find_parameters_json(old) if old.suffix == ".parquet" else None
    model_config = extract_model_config(params_json, fname) if params_json else None
    data_source = data_source_from_hint(fname)
    forecast_start = forecast_start_from_hint(fname)
    adjustments_dir = find_adjustments_dir(old) if codes else None
    if codes:
        spec_path = adjustments_dir / "headwind.json"
        adjustments_applied = build_adjustments_applied_list(
            codes=codes,
            code_to_spec_file={"h": spec_path},
            registry=registry,
        )
    else:
        adjustments_applied = []

    # Parent file (for adj-h CSVs): point at the underlying raw parquet(s) — leave as a note
    extra: dict = {"provenance": "reconstructed"}
    if codes and old.suffix == ".csv":
        extra["parent_files_note"] = (
            "Derived from the 4 raw parquets in data-official/{date}/{desktop,mobile}_*/ — "
            "see verify_forecast_states.py for the reproduction recipe."
        )

    meta_target = meta_path(new)
    if dry_run:
        return old, new, meta_target

    rename_file(old, new, dry_run=False)
    # When writing meta, the new file already exists at `new`
    write_meta(
        new,
        forecast_start_date=forecast_start or "unknown",
        data_source=data_source,
        produced_by=(
            "reconstructed by scripts/migrate_forecast_names.py — "
            "original producer: scripts/run_main.py for parquets, "
            "{april,june}_composite_forecast.ipynb for composite CSVs"
        ),
        model_config=model_config,
        adjustments_applied=adjustments_applied,
        extra=extra,
    )
    return old, new, meta_target


def migrate_scratch_row(row: dict, dry_run: bool) -> tuple[Path, Path]:
    """Scratch files: rename to .raw.{ext}, no sidecar meta (scratch dirs aren't authoritative)."""
    old = REPO_ROOT / row["path"]
    new = insert_state_marker(old, [])
    if dry_run:
        return old, new
    rename_file(old, new, dry_run=False)
    return old, new


# --- Main ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", default="tmp/inventory.csv")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--include-scratch",
        action="store_true",
        help="Also rename the 75 scratch comparison/scan files. Default: only load-bearing.",
    )
    args = parser.parse_args()

    inv_path = REPO_ROOT / args.inventory
    rows = list(csv.DictReader(open(inv_path)))
    registry = load_code_registry()

    load_bearing = [r for r in rows if r["verified_state"] in ("raw", "adj-h")]
    scratch = [r for r in rows if "directory convention" in r["verified_state"]]

    print(f"Load-bearing rows: {len(load_bearing)}")
    print(f"Scratch rows: {len(scratch)} (will {'include' if args.include_scratch else 'skip'})")
    print(f"Mode: {'DRY-RUN' if args.dry_run else 'EXECUTE'}\n")

    migrated_load_bearing = 0
    for row in load_bearing:
        try:
            old, new, meta = migrate_load_bearing_row(row, registry, args.dry_run)
            print(f"  {old.name}")
            print(f"    -> {new.name}")
            print(f"    +meta: {meta.name}")
            migrated_load_bearing += 1
        except (FileNotFoundError, FileExistsError) as e:
            print(f"  SKIP {row['path']}: {e}")

    migrated_scratch = 0
    if args.include_scratch:
        print(f"\n--- Scratch ---")
        for row in scratch:
            try:
                old, new = migrate_scratch_row(row, args.dry_run)
                print(f"  {old.relative_to(REPO_ROOT)} -> {new.name}")
                migrated_scratch += 1
            except (FileNotFoundError, FileExistsError) as e:
                print(f"  SKIP {row['path']}: {e}")

    print(f"\nSummary: load_bearing={migrated_load_bearing}, scratch={migrated_scratch}")
    if args.dry_run:
        print("(dry-run — no changes made)")


if __name__ == "__main__":
    main()
