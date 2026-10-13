#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Turn a country x OS DAU/MAU pull (bq_query.py --machine JSON) into Glean-history-style raw checkpoints.

The 2027 Glean-history draft is compared against the same model run on (a) Legacy telemetry and (b) real Glean
telemetry (PROCEDURE.html step 8). Both pulls use the SQL in ``tmp/query_{legacy,glean}_desktop_os_country.sql``
(15 named countries + ROW; OS folded to windows / mac / linux; ``other`` dropped here). This script writes the
frames the runner loads — ``mozaic_parts.raw.glean_history.desktop.{DAU,MAU}.parquet`` with columns
``x, country, windows, mac, linux, y`` (whole people) — plus ``glean_history_inputs.meta.json`` naming the
source and its sha1, so a comparison build's sidecar records what it was fed.

Usage:
    python scripts/build_comparison_raw_frames.py --json tmp/comparison_pulls/legacy_desktop_os_country.json \\
        --out-dir data-official/2027-glean-draft/raw_legacy_osxcountry_2026-10-09 --source-label "Legacy telemetry (telemetry.active_users_aggregates)"
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

OS_FLAGS = ("windows", "mac", "linux")


def load_pull(path: Path) -> pd.DataFrame:
    payload = json.loads(path.read_text())
    rows = payload["rows"] if isinstance(payload, dict) and "rows" in payload else payload
    df = pd.DataFrame(rows)
    required = {"x", "country", "os_bucket", "dau", "mau"}
    if not required <= set(df.columns):
        raise ValueError(f"pull is missing {sorted(required - set(df.columns))}; columns {sorted(df.columns)}")
    df["x"] = pd.to_datetime(df["x"])
    return df


def to_frames(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    df = df[df["os_bucket"].isin(OS_FLAGS)].copy()
    out = {}
    for metric, col in (("DAU", "dau"), ("MAU", "mau")):
        f = df[["x", "country", "os_bucket", col]].rename(columns={col: "y"})
        for flag in OS_FLAGS:
            f[flag] = f["os_bucket"] == flag
        f["y"] = pd.to_numeric(f["y"]).round().astype("int64")
        f = f[["x", "country", *OS_FLAGS, "y"]].sort_values(["x", "country", *OS_FLAGS]).reset_index(drop=True)
        # dense grid check per (country, os)
        expected = pd.date_range(f["x"].min(), f["x"].max(), freq="D")
        counts = f.groupby(["country", *OS_FLAGS]).size()
        short = counts[counts != len(expected)]
        if not short.empty:
            raise ValueError(f"{metric}: {len(short)} (country, OS) series are not dense: {short.head(5).to_dict()}")
        out[metric] = f
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--json", type=Path, required=True)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--source-label", required=True)
    args = p.parse_args()
    df = load_pull(args.json)
    dropped = int((~df["os_bucket"].isin(OS_FLAGS)).sum())
    frames = to_frames(df)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    meta_inputs = {}
    for metric, f in frames.items():
        target = args.out_dir / f"mozaic_parts.raw.glean_history.desktop.{metric}.parquet"
        f.to_parquet(target, index=False)
        meta_inputs[metric] = {"source_file": str(args.json), "source_sha1": hashlib.sha1(args.json.read_bytes()).hexdigest(),
                               "output_file": target.name, "rows": int(len(f)), "countries": sorted(f["country"].unique()),
                               "date_range": [str(f["x"].min().date()), str(f["x"].max().date())],
                               "world_last_day": float(f[f["x"] == f["x"].max()]["y"].sum())}
        print(f"{metric}: {len(f):,} rows, {f['x'].min().date()}..{f['x'].max().date()}, world on last day {meta_inputs[metric]['world_last_day']:,.0f}")
    meta = {"produced_by": "scripts/build_comparison_raw_frames.py", "written_at": datetime.now().isoformat(timespec="seconds"),
            "source": args.source_label, "value_column": "dau/mau from the aggregates table (whole people)",
            "rows_dropped_other_os": dropped, "populations": list(OS_FLAGS), "inputs": meta_inputs}
    (args.out_dir / "glean_history_inputs.meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(f"wrote {args.out_dir}")


if __name__ == "__main__":
    main()
