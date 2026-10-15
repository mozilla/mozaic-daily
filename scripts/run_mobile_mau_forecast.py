#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""One mobile MAU forecast on a file-fed country x app pull (the 2027 mobile companion to the desktop Glean-history draft).

Brendan, 2026-10-14: "we need to build one for mobile MAU over the same time period". Mobile is Glean-native, so the
history is the real production table from 2020-12-31 (``scripts/sql/glean_mobile_mau_country_app.sql`` ->
``build_comparison_raw_frames.py --layout mobile_app``), no translation. This runner:

1. loads ``mozaic_parts.raw.glean.mobile.MAU.parquet`` from ``--raw-dir`` -- no BigQuery, no pre-flight;
2. splices the regenerated Iran rolling-28 MAU fill (``--iran-fill``; the packaged mobile fill has no MAU metric);
3. optionally scales Fenix MAU training rows to their ORGANIC component (``--organic-split``: the measured paid/organic
   MAU split from ``build_fenix_organic_mau_split.py``, held flat backwards before its first day like `p`; IR excluded).
   Brendan, 2026-10-15: "we only forecast the organic part of mobile; marketing forecasts paid" — so there is NO add-back:
   the output is organic mobile MAU, history rows included. Without the flag it is a total-MAU forecast;
   3b. optionally applies the iOS-rate cap DATA CORRECTION (``--ios-rate-cap``: the parquet from ``build_ios_rate_cap.py``;
   India + Indonesia organic Fenix from 2026-04-01 capped at their own Firefox iOS growth, excess removed, no add-back;
   requires ``--organic-split``; rollback = drop the flag, see ``organic_mau/ios_rate_cap/IOS_RATE_CAP.md``);
4. runs ``main.process_data_source`` for ``DataSource.GLEAN_MOBILE`` with the configured ``MobileModelConfig``
   (default: the October 2026 mobile lock) and the production ``MobileEvents`` calendar;
5. writes ``mozaic_daily_forecast.<seam>.gm-M.raw.parquet`` + sidecar meta (``extra.organic_only`` records the split when
   one was applied; the filename marker stays ``.raw.`` because no registered adjustment is added back), the fitted
   pickle and ``parameters.json`` under ``<results-dir>/<config slug>/``; prints Dec-15 2026 / 2027 world values.

Usage:
    python scripts/run_mobile_mau_forecast.py \\
        --forecast-start-date 2026-10-10 \\
        --raw-dir data-official/2027-glean-draft/raw_glean_mobile_countryxapp_2026-10-09 \\
        --iran-fill data-official/2027-glean-draft/iran_fill_mobile/iran_fill.glean_mobile.MAU.parquet \\
        --results-dir data-official/2027-glean-draft/mobile_mau_2026-10-10

Knobs are the seven non-holiday ``MobileModelConfig`` fields plus the two holiday knobs the production lock carries
(threshold, effect floor); radii stay at the package defaults by policy. Announce before any probe (Brendan).
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

from mozaic.models import MobileModelConfig  # noqa: E402
from mozaic_daily.adjustments import insert_state_marker, write_meta  # noqa: E402
from mozaic_daily.config import get_runtime_config  # noqa: E402
from mozaic_daily.ios_rate_cap import apply_ios_rate_cap  # noqa: E402
from mozaic_daily.main import (  # noqa: E402
    get_checkpoint_filename, get_mozaic_objects_filename, process_data_source, save_mozaic_objects,
)
from mozaic_daily.organic import build_share_lookup, split_training_to_organic  # noqa: E402
from mozaic_daily.queries import ADDITIONAL_HOLIDAYS, DataSource, Metric  # noqa: E402
from mozaic_daily.tables import format_output_table  # noqa: E402

DATA_SOURCE = DataSource.GLEAN_MOBILE
METRIC = Metric.MAU
RAW_CHECKPOINT_TEMPLATE = "mozaic_parts.raw.glean.mobile.{metric}.parquet"
POPULATIONS = ["fenix_android", "firefox_ios", "focus_android", "focus_ios"]

# data-official/2026-10/mobile_cpr0725_paid1006_2026-10-05/*/parameters.json, field by field (the October 2026 mobile lock).
OCTOBER_MOBILE_LOCK = {
    "prophet_changepoint_prior_scale": 0.035,
    "prophet_changepoint_range": 0.725,
    "prophet_n_changepoints": 25,
    "prophet_recent_weeks": 13,
    "prophet_seasonality_prior_scale": 0.1,
    "seasonality_regime": "auto",
    "seasonality_corr_threshold": 0.0,
    "holiday_threshold": -0.055,
    "holiday_max_radius": 5,
    "holiday_min_radius": 3,
    "holiday_effect_floor": -0.6,
}

# CLI flag -> MobileModelConfig field. Radii are deliberately not exposed (holiday knobs are excluded from tuning by policy).
KNOBS = {
    "changepoint_prior_scale": ("prophet_changepoint_prior_scale", float),
    "changepoint_range": ("prophet_changepoint_range", float),
    "n_changepoints": ("prophet_n_changepoints", int),
    "recent_weeks": ("prophet_recent_weeks", int),
    "seasonality_prior_scale": ("prophet_seasonality_prior_scale", float),
    "seasonality_regime": ("seasonality_regime", str),
    "seasonality_corr_threshold": ("seasonality_corr_threshold", float),
    "holiday_threshold": ("holiday_threshold", float),
    "holiday_effect_floor": ("holiday_effect_floor", float),
}


def add_knob_arguments(parser: argparse.ArgumentParser, defaults: dict) -> None:
    """Model knobs with the given defaults (the October mobile lock), so a plain run is the locked config."""
    g = parser.add_argument_group("Model knobs (default: October 2026 mobile lock)")
    for flag, (field, typ) in KNOBS.items():
        g.add_argument(f"--{flag.replace('_', '-')}", dest=flag, type=typ, default=defaults[field])


def build_config(args: argparse.Namespace) -> MobileModelConfig:
    fields = {field: getattr(args, flag) for flag, (field, _) in KNOBS.items()}
    fields.update({k: v for k, v in OCTOBER_MOBILE_LOCK.items() if k not in fields})   # the un-exposed radii
    return MobileModelConfig(**fields)


def load_raw_frame(raw_dir: Path) -> pd.DataFrame:
    path = raw_dir / RAW_CHECKPOINT_TEMPLATE.format(metric=METRIC.value)
    if not path.exists():
        raise FileNotFoundError(f"raw checkpoint missing: {path}; run build_comparison_raw_frames.py --layout mobile_app")
    return pd.read_parquet(path)


def apply_organic_split(frame: pd.DataFrame, split_path: Path, exclude_countries: list[str]) -> tuple[pd.DataFrame, dict]:
    """Fenix MAU training rows x measured organic share (held flat backwards before the split starts); returns the frame and a record."""
    split = pd.read_parquet(split_path)
    split["submission_date"] = pd.to_datetime(split["submission_date"])
    lookup = build_share_lookup(split, share_column="organic_share", training_dates=pd.DatetimeIndex(pd.to_datetime(frame["x"]).unique()),
                                countries=sorted(frame["country"].unique()))
    organic, measured_paid = split_training_to_organic(frame, share_lookup=lookup, flag_column="fenix_android",
                                                       exclude_countries=exclude_countries, sentinel_attr="organic_mau_split_applied")
    last_day = pd.to_datetime(frame["x"]).max()
    fenix_last = frame[(pd.to_datetime(frame["x"]) == last_day) & frame["fenix_android"]]
    paid_last = measured_paid.xs(last_day, level="submission_date").sum() if not measured_paid.empty else 0
    record = {"split_file": str(split_path), "share_measured_from": str(split["submission_date"].min().date()),
              "share_held_flat_before": str(split["submission_date"].min().date()), "exclude_countries": exclude_countries,
              "fenix_world_total_mau_last_day": int(fenix_last["y"].sum()), "fenix_paid_mau_removed_last_day": int(paid_last),
              "fenix_paid_share_last_day": float(paid_last / fenix_last["y"].sum()) if len(fenix_last) else None}
    return organic, record


def world_value(df: pd.DataFrame, column: str, on: str) -> float:
    row = df[(df["country"] == "ALL") & (df["app_name"] == "ALL MOBILE") & (df["target_date"] == on)]
    return float(row[column].item()) if len(row) == 1 else float("nan")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--forecast-start-date", required=True, help="seam, YYYY-MM-DD (training ends the day before)")
    p.add_argument("--raw-dir", type=Path, required=True)
    p.add_argument("--results-dir", type=Path, required=True)
    p.add_argument("--iran-fill", type=Path, default=None,
                   help="rolling-28 MAU IR fill parquet (generate_mobile_mau_iran_fill.py); omit for no fill")
    p.add_argument("--organic-split", type=Path, default=None,
                   help="measured Fenix paid/organic MAU split parquet (build_fenix_organic_mau_split.py): forecast ORGANIC MAU, no add-back")
    p.add_argument("--organic-exclude", nargs="*", default=["IR"], help="countries left total under --organic-split (default IR, as in `p`)")
    p.add_argument("--ios-rate-cap", type=Path, default=None,
                   help="iOS-rate cap parquet (build_ios_rate_cap.py): subtract the recorded excess from the capped countries' organic Fenix rows; needs --organic-split")
    p.add_argument("--dry-run", action="store_true")
    add_knob_arguments(p, defaults=OCTOBER_MOBILE_LOCK)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    config = build_config(args)
    slug = config.to_slug()
    slug_dir = args.results_dir / slug
    slug_dir.mkdir(parents=True, exist_ok=True)

    if args.ios_rate_cap and not args.organic_split:
        raise SystemExit("--ios-rate-cap is a correction to the ORGANIC series; pass --organic-split with it")
    runtime = get_runtime_config(forecast_start_date_override=args.forecast_start_date)
    holiday_names = [h.__name__ for h in ADDITIONAL_HOLIDAYS.get(DATA_SOURCE, [])]
    print("=" * 60 + "\nMobile MAU forecast (file-fed)\n" + "=" * 60)
    print(f"Seam / training end : {runtime['forecast_start_date']} / {runtime['training_end_date']}")
    print(f"Horizon end         : {runtime['forecast_end_date']}")
    print(f"Raw dir             : {args.raw_dir}")
    print(f"Iran fill           : {args.iran_fill or 'none'}")
    print(f"Holidays            : {holiday_names}")
    print(f"Organic split       : {args.organic_split or 'none (total MAU)'}{' ex ' + ','.join(args.organic_exclude) if args.organic_split else ''}")
    print(f"iOS-rate cap        : {args.ios_rate_cap or 'none'}")
    print(f"Output dir          : {slug_dir}")
    print(f"Config              : {json.dumps(config.to_dict(), indent=2)}")

    (slug_dir / "parameters.json").write_text(json.dumps({
        "forecast_start_date": args.forecast_start_date,
        "slug": slug,
        "config": config.to_dict(),
        "data_source": DATA_SOURCE.value,
        "metrics": [METRIC.value],
        "raw_dir": str(args.raw_dir),
        "iran_fill": str(args.iran_fill) if args.iran_fill else None,
        "additional_holidays": holiday_names,
        "adjustment_code": None,
        "organic_split": str(args.organic_split) if args.organic_split else None,
        "organic_exclude": args.organic_exclude if args.organic_split else None,
        "ios_rate_cap": str(args.ios_rate_cap) if args.ios_rate_cap else None,
    }, indent=2) + "\n")
    if args.dry_run:
        print("[dry-run] stopping before the forecast")
        return

    frame = load_raw_frame(args.raw_dir)
    organic_record = None
    if args.organic_split:
        frame, organic_record = apply_organic_split(frame, args.organic_split, list(args.organic_exclude))
        print(f"Organic split: Fenix paid MAU removed on the last training day {organic_record['fenix_paid_mau_removed_last_day']:,} "
              f"of {organic_record['fenix_world_total_mau_last_day']:,} ({organic_record['fenix_paid_share_last_day']:.2%}); "
              f"share measured from {organic_record['share_measured_from']}, held flat before")
    cap_record = None
    if args.ios_rate_cap:
        frame, cap_record = apply_ios_rate_cap(frame, pd.read_parquet(args.ios_rate_cap))
        cap_record["cap_file"] = str(args.ios_rate_cap)
        print(f"iOS-rate cap: {cap_record['countries']} from {cap_record['from_date']}, removed on the last training day "
              f"{cap_record['removed_mau_last_day']} (total {cap_record['removed_mau_last_day_total']:,}), no add-back")
    fills = [pd.read_parquet(args.iran_fill)] if args.iran_fill else None
    datasets = {"mobile": {"glean": {METRIC.value: frame}}}

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
                                            data_source_filter={DATA_SOURCE}, metric_filter={METRIC}))
    target = insert_state_marker(unmarked, [])
    df.to_parquet(target)
    inputs_meta_path = args.raw_dir / "glean_history_inputs.meta.json"
    write_meta(
        target,
        forecast_start_date=args.forecast_start_date,
        data_source=DATA_SOURCE.value,
        produced_by="scripts/run_mobile_mau_forecast.py",
        model_config=config.to_dict(),
        adjustments_applied=[],
        extra={
            "metrics": [METRIC.value],
            "populations": POPULATIONS,
            "additional_holidays": holiday_names,
            "iran_fill": str(args.iran_fill) if args.iran_fill else None,
            "organic_only": organic_record,
            "ios_rate_cap": cap_record,
            "paid_organic_split": ("ORGANIC ONLY: Fenix training rows scaled by the measured organic MAU share, nothing added back"
                                   if organic_record else "not applied (total MAU)"),
            "inputs_meta": json.loads(inputs_meta_path.read_text()) if inputs_meta_path.exists() else None,
            "run_at": datetime.now().isoformat(timespec="seconds"),
        },
    )
    print(f"Wrote {target} (+ sidecar)")

    print("\nWorld values (country ALL, app ALL MOBILE):")
    for on in ("2026-12-15", "2027-12-15"):
        print(f"  MAU {on}: {world_value(df, 'mau', on):,.0f}")


if __name__ == "__main__":
    main()
