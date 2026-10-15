#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Iran counterfactual fill for mobile rolling-28 MAU (the 2027 mobile MAU forecast's input).

The packaged mozaic fill for ``glean_mobile`` covers DAU, New Profiles and the two Existing-Engagement metrics, not the
rolling-28 ``MAU`` the 2027 mobile forecast trains on, so the 2026 Iran shutdown crater (IR mobile MAU 1.85M -> 0.5M)
would sit untreated in the history. This script regenerates the fill for that one metric with the **same method** as
``generate_iran_fill.py`` and ``generate_glean_history_iran_fill.py``: train the mobile model on real IR rows before
``FORECAST_START`` (2026-02-28), forecast the gap forward, harvest the per-app IR forecast over the MAU window
(2026-02-28..2026-06-21, rolling-28 blackout-free from 06-22), restore the weekly amplitude (mean-preserving; near-nil for
a 28-day quantity), report the seam against the real recovery, and STOP for the human go/no-go on seam scaling.

Phase 1 (default):   raw mobile checkpoints -> IR-only MAU frame (cut at FORECAST_START) -> forecast -> harvest + DOW
                     restore -> drafts under <out>/_draft/, seam diagnostics + plot, seam_scale_spec.json at 1.0.
Phase 2 (--finalize --scale-spec FILE): apply the approved scale and write ``iran_fill.glean_mobile.MAU.parquet`` + sidecar.

Usage:
    python scripts/generate_mobile_mau_iran_fill.py --raw-dir data-official/2027-glean-draft/raw_glean_mobile_countryxapp_2026-10-09
    python scripts/generate_mobile_mau_iran_fill.py --finalize --scale-spec data-official/2027-glean-draft/iran_fill_mobile/seam_scale_spec.json

The model config defaults to the October 2026 mobile lock (``run_mobile_mau_forecast.OCTOBER_MOBILE_LOCK``), overridable
with the same knobs as the runner. Holidays: the production ``MobileEvents`` calendar, as in every mobile run.
"""

from __future__ import annotations

import argparse
import json
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
from mozaic_daily.forecast import get_mobile_forecast_dfs  # noqa: E402
from mozaic_daily.queries import ADDITIONAL_HOLIDAYS, DataSource, Metric, Platform  # noqa: E402
from run_mobile_mau_forecast import OCTOBER_MOBILE_LOCK, RAW_CHECKPOINT_TEMPLATE, add_knob_arguments, build_config  # noqa: E402

DATA_SOURCE = DataSource.GLEAN_MOBILE
METRIC = Metric.MAU.value
SEGMENT_COLUMNS = gif.SEGMENT_COLUMNS[Platform.MOBILE]
DEFAULT_OUTPUT_DIR = repo_root / "data-official" / "2027-glean-draft" / "iran_fill_mobile"
FILL_FILE = f"iran_fill.{DATA_SOURCE.value}.MAU.parquet"

# Rolling-28 MAU needs the same 28-day extension past recovery as Existing Engagement MAU.
gif.FILL_END_BY_METRIC.setdefault(METRIC, gif.FILL_END_BY_METRIC[Metric.EXISTING_ENGAGEMENT_MAU.value])
gif.REAL_RESUMES_BY_METRIC.setdefault(METRIC, gif.REAL_RESUMES_BY_METRIC[Metric.EXISTING_ENGAGEMENT_MAU.value])


def load_iran_frame(raw_dir: Path) -> pd.DataFrame:
    """IR-only MAU rows from the raw mobile checkpoint, all dates (the cut happens in the caller)."""
    path = raw_dir / RAW_CHECKPOINT_TEMPLATE.format(metric=METRIC)
    if not path.exists():
        raise FileNotFoundError(f"raw checkpoint missing: {path}; run build_comparison_raw_frames.py --layout mobile_app first")
    df = pd.read_parquet(path)
    df["x"] = pd.to_datetime(df["x"])
    ir = df[df["country"] == "IR"].copy()
    if ir.empty:
        raise ValueError(f"{path} has no IR rows")
    return ir


def forecast_iran(ir: pd.DataFrame, config) -> pd.DataFrame:
    """Fit on IR before FORECAST_START only (the crater must not be in the training data)."""
    training = {METRIC: ir[ir["x"] < pd.Timestamp(gif.FORECAST_START)]}
    result = get_mobile_forecast_dfs(
        training, gif.FORECAST_START, gif.FORECAST_END,
        additional_holidays=ADDITIONAL_HOLIDAYS[DATA_SOURCE],
        config=config,
        data_source=DATA_SOURCE.value,   # the packaged fill has no MAU rows, so it is a no-op here
    )
    return result.dfs[METRIC]


def run_phase1(raw_dir: Path, out_dir: Path, config) -> None:
    draft_dir, plot_dir = out_dir / "_draft", out_dir / "seam_plots"
    draft_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()

    print(f"\n{'=' * 60}\nStep 1: IR MAU frame from {raw_dir}\n{'=' * 60}")
    ir = load_iran_frame(raw_dir)
    print(f"  {METRIC}: {len(ir)} rows, {ir['x'].min().date()}..{ir['x'].max().date()}")

    print(f"\n{'=' * 60}\nStep 2: forecast IR forward from {gif.FORECAST_START} (config {config.to_slug()})\n{'=' * 60}")
    forecast = forecast_iran(ir, config)

    print(f"\n{'=' * 60}\nStep 3: harvest + restore weekly amplitude\n{'=' * 60}")
    smooth = gif.harvest_fill(forecast, DATA_SOURCE, METRIC, SEGMENT_COLUMNS)
    fill = gif.reseasonalize_source(smooth, {METRIC: ir}, Platform.MOBILE, SEGMENT_COLUMNS)
    smooth.to_parquet(draft_dir / f"iran_fill.{DATA_SOURCE.value}.MAU.smooth.parquet", index=False)
    fill.to_parquet(draft_dir / f"iran_fill.{DATA_SOURCE.value}.MAU.unscaled.parquet", index=False)
    print(f"  drafts written to {draft_dir} ({len(fill)} rows)")

    print(f"\n{'=' * 60}\nStep 4: seam diagnostics (nothing applied)\n{'=' * 60}")
    real_all = gif.real_iran_all_by_date(ir)
    diag = gif.seam_diagnostics(fill, real_all, METRIC)
    diag.update({"data_source": DATA_SOURCE.value, "metric": METRIC})
    plot_path = plot_dir / f"seam.{DATA_SOURCE.value}.M.png"
    gif.render_seam_plot(fill, real_all, DATA_SOURCE, METRIC, str(plot_path), smooth_df=smooth)
    print(f"  {METRIC}: fill 7d {diag['fill_7d_mean']:,.0f} vs real 7d {diag['real_7d_mean']:,.0f} "
          f"= {diag['pct_delta_7d']:+.2f}% (candidate scale {diag['candidate_scale_7d']}); plot {plot_path}")
    pd.DataFrame([diag]).to_csv(out_dir / "seam_diagnostics.csv", index=False)
    (out_dir / "seam_scale_spec.json").write_text(json.dumps({DATA_SOURCE.value: {METRIC: 1.0}}, indent=2) + "\n")
    print(f"\nPhase 1 done in {(time.time() - started) / 60:.1f} min. Review seam_plots/ and seam_diagnostics.csv, "
          f"edit seam_scale_spec.json if a scale is approved, then run --finalize.")


def run_finalize(out_dir: Path, scale_spec_path: Path, config) -> None:
    scale_spec = json.loads(Path(scale_spec_path).read_text())
    fill = pd.read_parquet(out_dir / "_draft" / f"iran_fill.{DATA_SOURCE.value}.MAU.unscaled.parquet")
    factor = float(scale_spec.get(DATA_SOURCE.value, {}).get(METRIC, 1.0))
    if factor != 1.0:
        fill["y"] *= factor
    fill["y"] = fill["y"].clip(lower=0)

    final = out_dir / FILL_FILE
    fill.to_parquet(final, index=False)
    try:
        commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        commit = "unknown"
    meta = {
        "producer": "scripts/generate_mobile_mau_iran_fill.py",
        "mozaic_daily_commit": commit,
        "mozaic_commit": get_git_commit_hash(),
        "data_source": DATA_SOURCE.value,
        "metric": METRIC,
        "segment_columns": SEGMENT_COLUMNS,
        "forecast_start": gif.FORECAST_START,
        "forecast_end": gif.FORECAST_END,
        "fill_end": gif.FILL_END_BY_METRIC[METRIC],
        "real_resumes": gif.REAL_RESUMES_BY_METRIC[METRIC],
        "model_config": config.to_dict(),
        "additional_holidays": [h.__name__ for h in ADDITIONAL_HOLIDAYS[DATA_SOURCE]],
        "seam_scale_factors": {METRIC: factor},
        "weekly_reseasonalized": True,
        "dow_reference_window": [gif.CLEAN_DOW_START, gif.FORECAST_START],
        "country": "IR",
        "rows": int(len(fill)),
        "note": "Rolling-28 MAU counterfactual for the 2026 Iran shutdown on the four mobile apps; the packaged glean_mobile fill "
                "carries no MAU metric, so this one is passed to the runner as additional_fills.",
    }
    (out_dir / f"iran_fill.{DATA_SOURCE.value}.MAU.meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(f"wrote {final} (+ sidecar); scale={factor}")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw-dir", type=Path, default=repo_root / "data-official/2027-glean-draft/raw_glean_mobile_countryxapp_2026-10-09")
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--finalize", action="store_true")
    p.add_argument("--scale-spec", type=Path, default=None)
    add_knob_arguments(p, defaults=OCTOBER_MOBILE_LOCK)
    return p.parse_args()


def main():
    args = parse_args()
    config = build_config(args)
    if args.finalize:
        if args.scale_spec is None:
            sys.exit("--finalize requires --scale-spec")
        run_finalize(args.output_dir, args.scale_spec, config)
    else:
        run_phase1(args.raw_dir, args.output_dir, config)


if __name__ == "__main__":
    main()
