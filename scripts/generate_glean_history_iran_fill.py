#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Iran counterfactual fill for the Glean-history desktop source (OS x country layout).

The legacy-to-glean export carries Iran's 2026 internet-shutdown crater untreated (Windows DAU at
~4% of its pre-shutdown level, MAU at ~27%). The packaged mozaic fill is keyed to the production
``modern_windows`` / ``winX`` layout and cannot splice onto ``windows`` / ``mac`` / ``linux``
frames, so this script regenerates it with the **same method** as ``generate_iran_fill.py``
(our method on their data): train mozaic on the translated IR cells before ``FORECAST_START``,
forecast the gap forward, harvest the per-population IR forecast, restore the weekly amplitude,
report the seam against the real recovery, and STOP for the human go/no-go on seam scaling.

Phase 1 (default):   raw checkpoints -> IR-only frames (cut at FORECAST_START) -> forecast ->
                     harvest + DOW restore -> drafts under <out>/_draft/, seam diagnostics + plots.
Phase 2 (--finalize --scale-spec FILE): apply the approved per-metric scale factors and write
                     ``iran_fill.glean_history_desktop.parquet`` + sidecar.

Usage:
    python scripts/generate_glean_history_iran_fill.py \\
        --raw-dir data-official/2027-glean-draft/raw_glean_history_2026-09-08
    python scripts/generate_glean_history_iran_fill.py --finalize \\
        --scale-spec data-official/2027-glean-draft/iran_fill/seam_scale_spec.json

Fill windows follow FILL_FORMAT_SPEC.md: DAU 2026-02-28..05-25; MAU 2026-02-28..06-21 (rolling-28).
The model config defaults to the October 2026 desktop lock (g01), overridable with the same knobs
as ``run_glean_history_forecast.py``.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
for sub in ("src", "scripts"):
    if str(repo_root / sub) not in sys.path:
        sys.path.insert(0, str(repo_root / sub))

import generate_iran_fill as gif  # noqa: E402  (the production producer's pure helpers)
from mozaic_daily.config import get_git_commit_hash  # noqa: E402
from mozaic_daily.forecast import get_desktop_forecast_dfs  # noqa: E402
from mozaic_daily.glean_history import OS_FLAGS, RAW_CHECKPOINT_TEMPLATE  # noqa: E402
from mozaic_daily.glean_history_holidays import LegacyOutage2025  # noqa: E402
from mozaic_daily.queries import DataSource, Metric, Platform  # noqa: E402
from run_glean_history_forecast import OCTOBER_G01_LOCK, add_knob_arguments, build_config  # noqa: E402

DATA_SOURCE = DataSource.GLEAN_HISTORY_DESKTOP
SEGMENT_COLUMNS = list(OS_FLAGS)
DEFAULT_OUTPUT_DIR = repo_root / "data-official" / "2027-glean-draft" / "iran_fill"
FILL_FILE = f"iran_fill.{DATA_SOURCE.value}.parquet"

# Rolling-28 MAU needs the same 28-day extension past recovery as Existing Engagement MAU.
gif.FILL_END_BY_METRIC.setdefault(Metric.MAU.value, gif.FILL_END_BY_METRIC[Metric.EXISTING_ENGAGEMENT_MAU.value])
gif.REAL_RESUMES_BY_METRIC.setdefault(Metric.MAU.value, gif.REAL_RESUMES_BY_METRIC[Metric.EXISTING_ENGAGEMENT_MAU.value])


def load_iran_frames(raw_dir: Path, metrics: list[str]) -> dict[str, pd.DataFrame]:
    """IR-only rows per metric from the raw checkpoints, all dates (the cut happens in the caller)."""
    frames = {}
    for metric in metrics:
        path = raw_dir / RAW_CHECKPOINT_TEMPLATE.format(metric=metric)
        if not path.exists():
            raise FileNotFoundError(f"raw checkpoint missing: {path}; run mozaic_daily.glean_history.write_raw_checkpoints first")
        df = pd.read_parquet(path)
        df["x"] = pd.to_datetime(df["x"])
        ir = df[df["country"] == "IR"].copy()
        if ir.empty:
            raise ValueError(f"{path} has no IR rows")
        frames[metric] = ir
    return frames


def forecast_iran(frames: dict[str, pd.DataFrame], config) -> dict[str, pd.DataFrame]:
    """Fit on IR before FORECAST_START only (the crater must not be in the training data)."""
    training = {m: df[df["x"] < pd.Timestamp(gif.FORECAST_START)] for m, df in frames.items()}
    result = get_desktop_forecast_dfs(
        training, gif.FORECAST_START, gif.FORECAST_END,
        additional_holidays=[LegacyOutage2025],
        config=config,
        data_source=DATA_SOURCE.value,   # no packaged fill is registered for this source
    )
    return result.dfs


def run_phase1(raw_dir: Path, out_dir: Path, metrics: list[str], config) -> None:
    draft_dir, plot_dir = out_dir / "_draft", out_dir / "seam_plots"
    draft_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()

    print(f"\n{'=' * 60}\nStep 1: IR frames from {raw_dir}\n{'=' * 60}")
    frames = load_iran_frames(raw_dir, metrics)
    for m, df in frames.items():
        print(f"  {m}: {len(df)} rows, {df['x'].min().date()}..{df['x'].max().date()}")

    print(f"\n{'=' * 60}\nStep 2: forecast IR forward from {gif.FORECAST_START} (config {config.to_slug()})\n{'=' * 60}")
    forecasts = forecast_iran(frames, config)

    print(f"\n{'=' * 60}\nStep 3: harvest + restore weekly amplitude\n{'=' * 60}")
    smooth = pd.concat(
        [gif.harvest_fill(fdf, DATA_SOURCE, m, SEGMENT_COLUMNS) for m, fdf in forecasts.items()],
        ignore_index=True,
    )
    fill = gif.reseasonalize_source(smooth, frames, Platform.DESKTOP, SEGMENT_COLUMNS)
    smooth.to_parquet(draft_dir / f"iran_fill.{DATA_SOURCE.value}.smooth.parquet", index=False)
    fill.to_parquet(draft_dir / f"iran_fill.{DATA_SOURCE.value}.unscaled.parquet", index=False)
    print(f"  drafts written to {draft_dir} ({len(fill)} rows)")

    print(f"\n{'=' * 60}\nStep 4: seam diagnostics (nothing applied)\n{'=' * 60}")
    rows, scale_spec = [], {DATA_SOURCE.value: {}}
    for metric in metrics:
        real_all = gif.real_iran_all_by_date(frames[metric])
        diag = gif.seam_diagnostics(fill[fill["metric"] == metric], real_all, metric)
        diag.update({"data_source": DATA_SOURCE.value, "metric": metric})
        rows.append(diag)
        scale_spec[DATA_SOURCE.value][metric] = 1.0
        plot_path = plot_dir / f"seam.{DATA_SOURCE.value}.{Metric(metric).short_code}.png"
        gif.render_seam_plot(fill[fill["metric"] == metric], real_all, DATA_SOURCE, metric, str(plot_path),
                             smooth_df=smooth[smooth["metric"] == metric])
        print(f"  {metric}: fill 7d {diag['fill_7d_mean']:,.0f} vs real 7d {diag['real_7d_mean']:,.0f} "
              f"= {diag['pct_delta_7d']:+.2f}% (candidate scale {diag['candidate_scale_7d']}); plot {plot_path}")
    pd.DataFrame(rows).to_csv(out_dir / "seam_diagnostics.csv", index=False)
    (out_dir / "seam_scale_spec.json").write_text(json.dumps(scale_spec, indent=2) + "\n")
    print(f"\nPhase 1 done in {(time.time() - started) / 60:.1f} min. Review seam_plots/ and seam_diagnostics.csv, "
          f"edit seam_scale_spec.json if a scale is approved, then run --finalize.")


def run_finalize(out_dir: Path, scale_spec_path: Path, config) -> None:
    scale_spec = json.loads(Path(scale_spec_path).read_text())
    draft = out_dir / "_draft" / f"iran_fill.{DATA_SOURCE.value}.unscaled.parquet"
    fill = pd.read_parquet(draft)
    applied = {}
    for metric, factor in scale_spec.get(DATA_SOURCE.value, {}).items():
        if factor != 1.0:
            fill.loc[fill["metric"] == metric, "y"] *= float(factor)
        applied[metric] = float(factor)
    fill["y"] = fill["y"].clip(lower=0)

    final = out_dir / FILL_FILE
    fill.to_parquet(final, index=False)
    try:
        commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        commit = "unknown"
    meta = {
        "producer": "scripts/generate_glean_history_iran_fill.py",
        "mozaic_daily_commit": commit,
        "mozaic_commit": get_git_commit_hash(),
        "data_source": DATA_SOURCE.value,
        "segment_columns": SEGMENT_COLUMNS,
        "forecast_start": gif.FORECAST_START,
        "forecast_end": gif.FORECAST_END,
        "fill_end_default": gif.FILL_END_DEFAULT,
        "fill_end_by_metric": {m: gif.FILL_END_BY_METRIC[m] for m in sorted(set(fill["metric"]))
                               if m in gif.FILL_END_BY_METRIC},
        "model_config": config.to_dict(),
        "additional_holidays": ["LegacyOutage2025"],
        "seam_scale_factors": applied,
        "weekly_reseasonalized": True,
        "dow_reference_window": [gif.CLEAN_DOW_START, gif.FORECAST_START],
        "country": "IR",
        "rows": int(len(fill)),
        "note": "IR is pool-translated on every OS in the export (factor_source win_mid / mac_small / linux_small); "
                "this fill is a counterfactual of that pooled translation.",
    }
    (out_dir / f"iran_fill.{DATA_SOURCE.value}.meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(f"wrote {final} (+ sidecar); scales={applied}")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw-dir", type=Path, default=repo_root / "data-official/2027-glean-draft/raw_glean_history_2026-09-08")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--metrics", nargs="+", default=[Metric.DAU.value, Metric.MAU.value])
    p.add_argument("--finalize", action="store_true")
    p.add_argument("--scale-spec", type=Path, default=None)
    add_knob_arguments(p, defaults=OCTOBER_G01_LOCK)
    return p.parse_args()


def main():
    args = parse_args()
    config = build_config(args)
    if args.finalize:
        if args.scale_spec is None:
            sys.exit("--finalize requires --scale-spec")
        run_finalize(args.output_dir, args.scale_spec, config)
    else:
        run_phase1(args.raw_dir, args.output_dir, args.metrics, config)


if __name__ == "__main__":
    main()
