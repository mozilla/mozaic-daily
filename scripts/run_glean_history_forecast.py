#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""One desktop forecast on the Glean-history source (file-fed, OS x country tiles), DAU and MAU.

The 2027 Glean-history draft (``data-official/2027-glean-draft/``) trains on the legacy-to-glean
export instead of BigQuery. This runner is the only entry point for it:

1. loads the raw checkpoints written by ``mozaic_daily.glean_history.write_raw_checkpoints``
   (``mozaic_parts.raw.glean_history.desktop.{DAU,MAU}.parquet``) -- no BigQuery, no pre-flight;
2. optionally splices the regenerated Iran counterfactual fill (``--iran-fill``);
3. runs ``main.process_data_source`` for ``DataSource.GLEAN_HISTORY_DESKTOP`` with the configured
   ``DesktopModelConfig`` (default: the October 2026 g01 lock), no adjustments of any kind;
4. writes ``mozaic_daily_forecast.<seam>.gh-D+M.raw.parquet`` + sidecar meta, the fitted pickle,
   and ``parameters.json`` under ``<results-dir>/<config slug>/``; prints Dec-15 2026 / 2027 world values.

Usage:
    python scripts/run_glean_history_forecast.py \\
        --forecast-start-date 2026-09-09 \\
        --raw-dir data-official/2027-glean-draft/raw_glean_history_2026-09-08 \\
        --iran-fill data-official/2027-glean-draft/iran_fill/iran_fill.glean_history_desktop.parquet \\
        --results-dir data-official/2027-glean-draft/desktop_glean_history_g01_2026-09-09

MAU runs through the same desktop model as DAU unless knobs are overridden (Brendan, 2026-10-12:
be open to MAU-specific parameters; announce before running). ``--metrics DAU`` or ``--metrics MAU``
runs one metric; the filename's filter code follows (``gh-D`` / ``gh-M``).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
if str(repo_root / "src") not in sys.path:
    sys.path.insert(0, str(repo_root / "src"))

from mozaic.models import DesktopModelConfig  # noqa: E402
from mozaic_daily.adjustments import insert_state_marker, write_meta  # noqa: E402
from mozaic_daily.config import get_runtime_config  # noqa: E402
from mozaic_daily.glean_history import RAW_CHECKPOINT_TEMPLATE  # noqa: E402
from mozaic_daily.main import (  # noqa: E402
    get_checkpoint_filename, get_mozaic_objects_filename, process_data_source, save_mozaic_objects,
)
from mozaic_daily.queries import ADDITIONAL_HOLIDAYS, DataSource, Metric  # noqa: E402
from mozaic_daily.tables import format_output_table  # noqa: E402

DATA_SOURCE = DataSource.GLEAN_HISTORY_DESKTOP

# data-official/2026-10/desktop_g01_2026-10-05/*/parameters.json, field by field.
OCTOBER_G01_LOCK = {
    "prophet_changepoint_prior_scale": 0.1649,
    "prophet_changepoint_range": 0.814,
    "prophet_n_changepoints": 40,
    "prophet_recent_weeks": 17,
    "prophet_seasonality_prior_scale": 0.00825,
    "seasonality_regime": "multiplicative",
    "seasonality_corr_threshold": 0.0,
    "holiday_threshold": -0.032,
    "holiday_max_radius": 5,
    "holiday_min_radius": 3,
    "holiday_effect_floor": -0.6,
}

# CLI flag -> DesktopModelConfig field.
KNOBS = {
    "changepoint_prior_scale": ("prophet_changepoint_prior_scale", float),
    "changepoint_range": ("prophet_changepoint_range", float),
    "n_changepoints": ("prophet_n_changepoints", int),
    "recent_weeks": ("prophet_recent_weeks", int),
    "seasonality_prior_scale": ("prophet_seasonality_prior_scale", float),
    "seasonality_regime": ("seasonality_regime", str),
    "seasonality_corr_threshold": ("seasonality_corr_threshold", float),
    "holiday_threshold": ("holiday_threshold", float),
    "holiday_max_radius": ("holiday_max_radius", int),
    "holiday_min_radius": ("holiday_min_radius", int),
    "holiday_effect_floor": ("holiday_effect_floor", float),
}


def add_knob_arguments(parser: argparse.ArgumentParser, defaults: dict) -> None:
    """Model knobs with the given defaults (the g01 lock), so a plain run is the locked build."""
    g = parser.add_argument_group("Model knobs (default: October 2026 g01 lock)")
    for flag, (field, typ) in KNOBS.items():
        g.add_argument(f"--{flag.replace('_', '-')}", dest=flag, type=typ, default=defaults[field])


def build_config(args: argparse.Namespace) -> DesktopModelConfig:
    return DesktopModelConfig(**{field: getattr(args, flag) for flag, (field, _) in KNOBS.items()})


def load_raw_frames(raw_dir: Path, metrics: list[str]) -> dict[str, pd.DataFrame]:
    frames = {}
    for metric in metrics:
        path = raw_dir / RAW_CHECKPOINT_TEMPLATE.format(metric=metric)
        if not path.exists():
            raise FileNotFoundError(f"raw checkpoint missing: {path}")
        frames[metric] = pd.read_parquet(path)
    return frames


def world_value(df: pd.DataFrame, column: str, on: str) -> float:
    row = df[(df["country"] == "ALL") & (df["segment"] == '{"os": "ALL"}') & (df["target_date"] == on)]
    return float(row[column].item()) if len(row) == 1 else float("nan")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--forecast-start-date", required=True, help="seam, YYYY-MM-DD (training ends the day before)")
    p.add_argument("--raw-dir", type=Path, required=True)
    p.add_argument("--results-dir", type=Path, required=True)
    p.add_argument("--metrics", nargs="+", default=[Metric.DAU.value, Metric.MAU.value],
                   choices=[Metric.DAU.value, Metric.MAU.value])
    p.add_argument("--iran-fill", type=Path, default=None,
                   help="regenerated IR fill parquet (generate_glean_history_iran_fill.py); omit for no fill")
    p.add_argument("--dry-run", action="store_true")
    add_knob_arguments(p, defaults=OCTOBER_G01_LOCK)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    config = build_config(args)
    slug = config.to_slug()
    slug_dir = args.results_dir / slug
    slug_dir.mkdir(parents=True, exist_ok=True)
    metric_filter = {Metric(m) for m in args.metrics}

    runtime = get_runtime_config(forecast_start_date_override=args.forecast_start_date)
    print("=" * 60 + f"\nGlean-history desktop forecast\n" + "=" * 60)
    print(f"Seam / training end : {runtime['forecast_start_date']} / {runtime['training_end_date']}")
    print(f"Horizon end         : {runtime['forecast_end_date']}")
    print(f"Metrics             : {sorted(args.metrics)}")
    print(f"Raw dir             : {args.raw_dir}")
    print(f"Iran fill           : {args.iran_fill or 'none'}")
    print(f"Holidays            : {[h.__name__ for h in ADDITIONAL_HOLIDAYS.get(DATA_SOURCE, [])]}")
    print(f"Output dir          : {slug_dir}")
    print(f"Config              : {json.dumps(config.to_dict(), indent=2)}")

    (slug_dir / "parameters.json").write_text(json.dumps({
        "forecast_start_date": args.forecast_start_date,
        "slug": slug,
        "config": config.to_dict(),
        "data_source": DATA_SOURCE.value,
        "metrics": sorted(args.metrics),
        "raw_dir": str(args.raw_dir),
        "iran_fill": str(args.iran_fill) if args.iran_fill else None,
    }, indent=2) + "\n")
    if args.dry_run:
        print("[dry-run] stopping before the forecast")
        return

    frames = load_raw_frames(args.raw_dir, args.metrics)
    fills = [pd.read_parquet(args.iran_fill)] if args.iran_fill else None
    datasets = {"desktop": {"glean": frames}}

    started = time.time()
    df, mozaics = process_data_source(
        DATA_SOURCE, datasets,
        runtime["forecast_start_date"], runtime["forecast_end_date"],
        training_end_date=runtime["training_end_date"],
        model_configs={DATA_SOURCE: config},
        additional_fills=fills,
    )
    df = format_output_table(df, runtime["forecast_start_date"], runtime["forecast_run_dt"])
    print(f"\nForecast took {(time.time() - started) / 60:.1f} min")

    save_mozaic_objects(mozaics, get_mozaic_objects_filename(args.forecast_start_date, DATA_SOURCE, str(slug_dir)))
    unmarked = Path(get_checkpoint_filename(args.forecast_start_date, str(slug_dir),
                                            data_source_filter={DATA_SOURCE}, metric_filter=metric_filter))
    target = insert_state_marker(unmarked, [])
    df.to_parquet(target)
    inputs_meta_path = args.raw_dir / "glean_history_inputs.meta.json"
    write_meta(
        target,
        forecast_start_date=args.forecast_start_date,
        data_source=DATA_SOURCE.value,
        produced_by="scripts/run_glean_history_forecast.py",
        model_config=config.to_dict(),
        adjustments_applied=[],
        extra={
            "metrics": sorted(args.metrics),
            "populations": ["windows", "mac", "linux"],
            "additional_holidays": [h.__name__ for h in ADDITIONAL_HOLIDAYS.get(DATA_SOURCE, [])],
            "iran_fill": str(args.iran_fill) if args.iran_fill else None,
            "inputs_meta": json.loads(inputs_meta_path.read_text()) if inputs_meta_path.exists() else None,
            "run_at": datetime.now().isoformat(timespec="seconds"),
        },
    )
    print(f"Wrote {target} (+ sidecar)")

    print("\nWorld values (country ALL, os ALL):")
    for metric in sorted(args.metrics):
        col = {"DAU": "dau", "MAU": "mau"}[metric]
        for on in ("2026-12-15", "2027-12-15"):
            print(f"  {metric} {on}: {world_value(df, col, on):,.0f}")


if __name__ == "__main__":
    main()
