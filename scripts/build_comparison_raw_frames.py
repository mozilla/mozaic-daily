#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Turn a country x segment DAU/MAU pull (bq_query.py --machine JSON) into file-fed raw checkpoints.

Two layouts, one script:

``--layout desktop_os`` (default)
    The 2027 Glean-history draft's comparison inputs (PROCEDURE.html step 8): Legacy and real Glean desktop by
    country x OS from ``scripts/sql/glean_history_comparison_*_desktop_os_country.sql`` (15 named countries + ROW;
    OS folded to windows / mac / linux; ``other`` dropped here). Writes the frames the Glean-history runner loads —
    ``mozaic_parts.raw.glean_history.desktop.{DAU,MAU}.parquet`` with columns ``x, country, windows, mac, linux, y``.

``--layout mobile_app``
    The 2027 mobile MAU forecast's input (``scripts/sql/glean_mobile_mau_country_app.sql``): real Glean mobile by
    country x app. Writes the production mobile checkpoint names — ``mozaic_parts.raw.glean.mobile.{DAU,MAU}.parquet``
    with the production segment flags ``fenix_android, firefox_ios, focus_android, focus_ios`` — so
    ``scripts/run_mobile_mau_forecast.py`` loads them like a cached BigQuery pull.

Both write ``glean_history_inputs.meta.json`` naming the source and its sha1, so a build's sidecar records what it
was fed. Values are whole people (rounded to int64); every (country, segment) series must be dense.

Usage:
    python scripts/build_comparison_raw_frames.py --json tmp/comparison_pulls/legacy_desktop_os_country.json \\
        --out-dir data-official/2027-glean-draft/raw_legacy_osxcountry_2026-10-09 --source-label "Legacy telemetry (telemetry.active_users_aggregates)"
    python scripts/build_comparison_raw_frames.py --layout mobile_app --json tmp/mobile_mau_pull/glean_mobile_country_app.json \\
        --out-dir data-official/2027-glean-draft/raw_glean_mobile_countryxapp_2026-10-09 --source-label "Glean mobile (glean_telemetry.active_users_aggregates)"
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class Layout:
    """How a pull's segment column maps onto mozaic's boolean population flags, and what the checkpoint is called."""
    segment_column: str
    flag_for_value: dict[str, str]          # pull value -> flag column (unmapped values are dropped and counted)
    checkpoint_template: str                # metric -> file name
    require_dense: bool = True              # desktop comparisons must be gap-free; mobile apps start on different days

    @property
    def flags(self) -> tuple[str, ...]:
        return tuple(self.flag_for_value.values())


LAYOUTS = {
    "desktop_os": Layout(
        segment_column="os_bucket",
        flag_for_value={"windows": "windows", "mac": "mac", "linux": "linux"},
        checkpoint_template="mozaic_parts.raw.glean_history.desktop.{metric}.parquet",
    ),
    # The production mobile universe, exactly (queries.py: no "other" bucket); flag names follow _build_mobile_segment_columns.
    "mobile_app": Layout(
        segment_column="app_name",
        flag_for_value={"Fenix": "fenix_android", "Firefox iOS": "firefox_ios", "Focus Android": "focus_android", "Focus iOS": "focus_ios"},
        checkpoint_template="mozaic_parts.raw.glean.mobile.{metric}.parquet",
        require_dense=False,
    ),
}


def load_pull(path: Path, layout: Layout) -> pd.DataFrame:
    payload = json.loads(path.read_text())
    rows = payload["rows"] if isinstance(payload, dict) and "rows" in payload else payload
    df = pd.DataFrame(rows)
    required = {"x", "country", layout.segment_column, "dau", "mau"}
    if not required <= set(df.columns):
        raise ValueError(f"pull is missing {sorted(required - set(df.columns))}; columns {sorted(df.columns)}")
    df["x"] = pd.to_datetime(df["x"])
    return df


def to_frames(df: pd.DataFrame, layout: Layout) -> dict[str, pd.DataFrame]:
    """One dense ``x, country, <flags>, y`` frame per metric; rows whose segment value is unmapped are dropped."""
    seg = df[layout.segment_column].map(layout.flag_for_value)
    df = df[seg.notna()].assign(_flag=seg[seg.notna()])
    flags = list(layout.flags)
    out = {}
    for metric, col in (("DAU", "dau"), ("MAU", "mau")):
        f = df[["x", "country", "_flag", col]].rename(columns={col: "y"})
        for flag in flags:
            f[flag] = f["_flag"] == flag
        f["y"] = pd.to_numeric(f["y"]).round().astype("int64")
        f = f[["x", "country", *flags, "y"]].sort_values(["x", "country", *flags]).reset_index(drop=True)
        expected = pd.date_range(f["x"].min(), f["x"].max(), freq="D")
        counts = f.groupby(["country", *flags]).size()
        short = counts[counts != len(expected)]
        if not short.empty and layout.require_dense:
            raise ValueError(f"{metric}: {len(short)} (country, segment) series are not dense: {short.head(5).to_dict()}")
        if not short.empty:
            print(f"{metric}: {len(short)} (country, segment) series shorter than the full range (kept as is, like a production pull): "
                  f"{summarize_gaps(f, flags)}")
        out[metric] = f
    return out


def summarize_gaps(f: pd.DataFrame, flags: list[str]) -> str:
    """Per population: first day, and how many days inside its own range are missing (so a late-starting app reads as 0 gaps)."""
    parts = []
    for flag in flags:
        g = f[f[flag]]
        inside = pd.date_range(g["x"].min(), g["x"].max(), freq="D")
        missing = len(inside) * g["country"].nunique() - len(g)
        parts.append(f"{flag}: from {g['x'].min().date()}, {missing} country-days missing inside its range")
    return "; ".join(parts)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--json", type=Path, required=True)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--source-label", required=True)
    p.add_argument("--layout", choices=sorted(LAYOUTS), default="desktop_os")
    args = p.parse_args()
    layout = LAYOUTS[args.layout]
    df = load_pull(args.json, layout)
    dropped = int((~df[layout.segment_column].isin(layout.flag_for_value)).sum())
    frames = to_frames(df, layout)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    meta_inputs = {}
    for metric, f in frames.items():
        target = args.out_dir / layout.checkpoint_template.format(metric=metric)
        f.to_parquet(target, index=False)
        meta_inputs[metric] = {"source_file": str(args.json), "source_sha1": hashlib.sha1(args.json.read_bytes()).hexdigest(),
                               "output_file": target.name, "rows": int(len(f)), "countries": sorted(f["country"].unique()),
                               "date_range": [str(f["x"].min().date()), str(f["x"].max().date())],
                               "world_last_day": float(f[f["x"] == f["x"].max()]["y"].sum())}
        print(f"{metric}: {len(f):,} rows, {f['x'].min().date()}..{f['x'].max().date()}, world on last day {meta_inputs[metric]['world_last_day']:,.0f}")
    meta = {"produced_by": "scripts/build_comparison_raw_frames.py", "written_at": datetime.now().isoformat(timespec="seconds"),
            "layout": args.layout, "source": args.source_label, "value_column": "dau/mau from the aggregates table (whole people)",
            "rows_dropped_unmapped_segment": dropped, "populations": list(layout.flags), "inputs": meta_inputs}
    (args.out_dir / "glean_history_inputs.meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(f"wrote {args.out_dir}")


if __name__ == "__main__":
    main()
