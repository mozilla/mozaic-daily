#!/usr/bin/env python3
"""Verify one per-tile overlay end-to-end with the three-curve isolation check.

Works for any code registered with ``applier: per_tile_overlay`` (``l``, ``o``, and every
curve ingested later). Reusing the cycle's cached raw DAU pull (no BigQuery), it forecasts:

  (a) raw / no-overlay      — forecast on original training (the effect left implicit)
  (b) subtract-and-forecast — forecast on subtracted training, NO add-back
  (c) full subtract+add     — (b) then add the curve back  == the adj-<code> output

and reports the Dec-15 deltas that answer the double-count question:

  (c)-(a) = net effect of the overlay on the KPI
  (a)-(b) = how much of the effect Prophet already extrapolated (the avoided double-count)
  (c)-(b) = the full add-back (the curve's own 28d-MA level)

Pass-through ``(c)-(a)`` / ``(c)-(b)`` should land in a plausible 0.5–1.5× band: near zero means the
add-back leg never ran, far above one means the training subtraction is reshaping the trend.

Run (from the repo root, with the cycle's raw pull cached in --raw-cache-dir):

    source .venv/bin/activate && python scripts/verify_overlay.py --code j --cycle 2026-09 \\
        --raw-cache-dir data-official/2026-09/desktop_rawpull_2026-09-02

Writes ``<name>_three_curve_numbers.json``, ``<name>_three_curves.parquet`` (cached so re-plotting
never re-runs the slow forecasts) and ``<name>_three_curve_isolation.png`` into the spec
directory's ``plots/``. This script forecasts twice (several minutes per run); the ingest skill
prints the invocation and leaves running it to you.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from mozaic_daily.adjustments import (  # noqa: E402
    add_lift_to_forecast, load_lift_series, subtract_lift_from_training,
)
from mozaic_daily.config import get_runtime_config  # noqa: E402
from mozaic_daily.data import get_aggregate_data, get_queries  # noqa: E402
from mozaic_daily.forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs  # noqa: E402
from mozaic_daily.overlays import (  # noqa: E402
    ResolvedOverlay, overlay_country_shares, registered_overlay_codes, resolve_overlays,
)
from mozaic_daily.queries import ADDITIONAL_HOLIDAYS, DataSource, Metric, Platform  # noqa: E402
from mozaic_daily.tables import combine_tables  # noqa: E402

MEASUREMENT_DATE = pd.Timestamp("2026-12-15")
MA_WINDOW_DAYS = 28
PASS_THROUGH_BAND = (0.5, 1.5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--code", required=True, help="one-letter overlay code, e.g. l, o, j")
    parser.add_argument("--cycle", required=True, help="cycle directory, e.g. 2026-09")
    parser.add_argument("--forecast-start-date", default=None,
                        help="defaults to the spec's applies_to_forecast_start")
    parser.add_argument("--raw-cache-dir", type=Path, default=Path("."),
                        help="directory holding the cached mozaic_parts.raw.* pull (default: cwd)")
    parser.add_argument("--project", default="mozdata", help="BigQuery project if the cache is cold")
    parser.add_argument("--measurement-date", default=str(MEASUREMENT_DATE.date()))
    return parser.parse_args()


def locate_overlay(code: str, cycle: str, forecast_start_date: str | None) -> ResolvedOverlay:
    """The registry-resolved overlay for ``code`` whose spec lives under ``data-official/<cycle>/``."""
    registry_entry = registered_overlay_codes().get(code)
    if registry_entry is None:
        raise SystemExit(f"`{code}` is not a per_tile_overlay code in data-official/adjustment_codes.yaml")
    from mozaic_daily.overlays import spec_globs
    cycle_specs = sorted(p for g in spec_globs(registry_entry) for p in (REPO / "data-official" / cycle).glob(g.split("/", 2)[2]))
    if not cycle_specs:
        raise SystemExit(f"no spec for `{code}` under data-official/{cycle}/ ({spec_globs(registry_entry)})")
    spec = json.loads(cycle_specs[0].read_text())
    start = forecast_start_date or spec.get("applies_to_forecast_start")
    if start is None:
        raise SystemExit(f"{cycle_specs[0]} has no applies_to_forecast_start; pass --forecast-start-date")
    matches = [o for o in resolve_overlays(start) if o.code == code]
    if not matches:
        raise SystemExit(f"`{code}` does not gate on forecast_start={start}")
    return matches[0]


def world_ma28(df_combined: pd.DataFrame) -> pd.Series:
    """28d-MA of the ALL-country / ALL-segment daily DAU, indexed by date."""
    world = df_combined[(df_combined["country"] == "ALL") & (df_combined["population"] == "ALL")].copy()
    world["target_date"] = pd.to_datetime(world["target_date"])
    daily = world.sort_values("target_date").set_index("target_date")["DAU"].astype(float)
    return daily.rolling(MA_WINDOW_DAYS, min_periods=MA_WINDOW_DAYS).mean()


def run_forecast(source_data: dict, data_source: DataSource, forecast_start: str) -> pd.DataFrame:
    cfg = get_runtime_config(forecast_start_date_override=forecast_start)
    forecast_func = get_desktop_forecast_dfs if data_source.platform == Platform.DESKTOP else get_mobile_forecast_dfs
    result = forecast_func(
        source_data, cfg["forecast_start_date"], cfg["forecast_end_date"],
        additional_holidays=ADDITIONAL_HOLIDAYS.get(data_source, []),
        data_source=data_source.value,
    )
    return combine_tables(result.dfs)


def three_curves(overlay: ResolvedOverlay, source_data: dict, forecast_start: str, training_end: str) -> pd.DataFrame:
    """The (a)/(b)/(c) world 28d-MA curves for one overlay."""
    data_source = overlay.data_source
    dau_key = Metric.DAU.value
    shares = overlay_country_shares(overlay, source_data[dau_key], pd.Timestamp(training_end))
    lift = load_lift_series(overlay.spec, overlay.spec_path.parent)

    print(f"[1/2] forecasting RAW (no `{overlay.code}`) ...", flush=True)
    raw_combined = run_forecast(source_data, data_source, forecast_start)

    print("[2/2] forecasting on SUBTRACTED training ...", flush=True)
    subtracted = dict(source_data)
    subtracted[dau_key] = subtract_lift_from_training(
        source_data[dau_key], daily_lift_series=lift, country_shares=shares,
        flag_column=overlay.flag_column, sentinel_attr=overlay.sentinel_attr,
    )
    sub_combined = run_forecast(subtracted, data_source, forecast_start)
    full_combined = add_lift_to_forecast(
        sub_combined, daily_lift_series=lift, country_shares=shares,
        forecast_start=pd.Timestamp(forecast_start), metric_column=dau_key,
        population_value=overlay.flag_column,
    )
    return pd.DataFrame({
        "a_raw": world_ma28(raw_combined),
        "b_subtract_only": world_ma28(sub_combined),
        "c_full": world_ma28(full_combined),
    })


def summarize(curves: pd.DataFrame, measurement_date: pd.Timestamp) -> dict:
    a, b, c = (curves[k].loc[measurement_date] for k in ("a_raw", "b_subtract_only", "c_full"))
    add_back = c - b
    pass_through = (c - a) / add_back if add_back else float("nan")
    return {
        "measurement_date": str(measurement_date.date()),
        "raw_no_overlay": float(a),
        "subtract_only": float(b),
        "full_subtract_add": float(c),
        "net_effect_c_minus_a": float(c - a),
        "already_extrapolated_a_minus_b": float(a - b),
        "add_back_c_minus_b": float(add_back),
        "pass_through_ratio": float(pass_through),
        "pass_through_in_band": bool(PASS_THROUGH_BAND[0] <= pass_through <= PASS_THROUGH_BAND[1]),
    }


def plot_isolation(curves: pd.DataFrame, overlay: ResolvedOverlay, forecast_start: pd.Timestamp,
                   measurement_date: pd.Timestamp, out_path: Path) -> None:
    """Levels on top (they overlap at world scale), the deltas that carry the story below."""
    shown = curves[curves.index >= forecast_start]
    fig, (ax, axd) = plt.subplots(2, 1, figsize=(12, 8), sharex=True, gridspec_kw={"height_ratios": [2, 1]})
    for column, label, color in [
        ("a_raw", f"(a) raw / no-{overlay.code}", "tab:gray"),
        ("b_subtract_only", "(b) subtract-and-forecast (no add-back)", "tab:orange"),
        ("c_full", f"(c) full subtract+add  (= adj-{overlay.code} output)", "tab:blue"),
    ]:
        ax.plot(shown.index, shown[column] / 1e6, label=label, color=color, lw=2)
    ax.axvline(measurement_date, color="tab:red", ls=":", lw=1, label="Dec-15 KPI")
    ax.set_ylabel(f"ALL-{overlay.data_source.platform.value} DAU, 28d-MA (M)")
    ax.set_title(f"`{overlay.code}` {overlay.name} — three-curve isolation ({overlay.data_source.value} DAU)")
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(alpha=0.3)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.2f}M"))

    deltas = {
        "(c)-(a): net effect on KPI": (shown["c_full"] - shown["a_raw"], "tab:blue", "-", 2),
        "(a)-(b): already extrapolated by Prophet": (shown["a_raw"] - shown["b_subtract_only"], "tab:gray", "--", 1.8),
        "(c)-(b): full add-back": (shown["c_full"] - shown["b_subtract_only"], "tab:blue", ":", 1),
    }
    for label, (series, color, style, width) in deltas.items():
        axd.plot(series.index, series / 1e3, color=color, ls=style, lw=width, label=label)
    axd.axvline(measurement_date, color="tab:red", ls=":", lw=1)
    axd.set_ylabel("Δ 28d-MA (thousands)")
    axd.legend(loc="upper right", fontsize=8)
    axd.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=130)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    measurement_date = pd.Timestamp(args.measurement_date)
    overlay = locate_overlay(args.code, args.cycle, args.forecast_start_date)
    forecast_start = overlay.spec["applies_to_forecast_start"] if args.forecast_start_date is None else args.forecast_start_date
    data_source = overlay.data_source
    print(f"Overlay `{overlay.code}` ({overlay.name}) on {data_source.value}, seam {forecast_start}", flush=True)

    out_dir = overlay.spec_path.parent / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    curves_cache = overlay.spec_path.parent / f"{overlay.name}_three_curves.parquet"

    if curves_cache.exists():
        print(f"Loading cached curves from {curves_cache} (skip forecasts)", flush=True)
        curves = pd.read_parquet(curves_cache)
    else:
        cfg = get_runtime_config(forecast_start_date_override=forecast_start)
        queries = get_queries(cfg["country_string"], data_source_filter={data_source}, metric_filter={Metric.DAU})
        datasets = get_aggregate_data(queries, project=args.project, checkpoints=True, clean=False,
                                      output_dir=str(args.raw_cache_dir))
        platform_key, source_key = data_source.platform.value, data_source.telemetry_source.value
        source_data = {m: df.assign(x=pd.to_datetime(df["x"])) for m, df in datasets[platform_key][source_key].items()}
        curves = three_curves(overlay, source_data, forecast_start, cfg["training_end_date"])
        curves.to_parquet(curves_cache)

    numbers = summarize(curves, measurement_date)
    print(json.dumps(numbers, indent=2))
    (out_dir / f"{overlay.name}_three_curve_numbers.json").write_text(json.dumps(numbers, indent=2, sort_keys=True))
    plot_path = out_dir / f"{overlay.name}_three_curve_isolation.png"
    plot_isolation(curves, overlay, pd.Timestamp(forecast_start), measurement_date, plot_path)
    band = "inside" if numbers["pass_through_in_band"] else "OUTSIDE"
    print(f"Pass-through {numbers['pass_through_ratio']:.2f}x is {band} the {PASS_THROUGH_BAND} band. "
          f"Plot: {plot_path}")


if __name__ == "__main__":
    main()
