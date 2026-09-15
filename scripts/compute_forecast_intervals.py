#!/usr/bin/env python3
"""Prediction intervals for one forecast build, from its fitted mozaic pickle.

Reads the ``mozaic_objects.*.pkl`` a forecast run saved, rebuilds the 1,000 world-total sample
paths, checks their median against the forecast parquet, and writes daily + 28d-MA bands, a
Dec-15 / trough summary, and plots. See ``mozaic_daily.intervals`` for the method and its two
rules (quantiles of the MA, not MA of the quantiles; median must reproduce the parquet).

Usage
-----
    source .venv/bin/activate
    python scripts/compute_forecast_intervals.py \\
        --pkl data-official/2026-08/desktop_raw_ci_2026-08-02/<slug>/mozaic_objects.legacy_desktop.2026-08-02.pkl \\
        --forecast-parquet data-official/2026-08/desktop_raw_ci_2026-08-02/<slug>/mozaic_daily_forecast.2026-08-02.ld-D.raw.parquet \\
        --out-dir data-official/2026-08/desktop_raw_ci_2026-08-02 \\
        --reference "published (adj-hlo)=48703443" \\
        --reference "published, h removed=50018443"

Outputs under ``--out-dir``::

    csv/<stem>_daily_bands.csv        date, actuals, median, lower_50, upper_50, lower_80, ... (daily DAU)
    csv/<stem>_28ma_bands.csv         same columns on the plain trailing 28d mean
    csv/<stem>_summary.csv            Dec-15 (and trough) median + bounds + half-widths, plus references
    <stem>_summary.json               the same, machine-readable, with provenance
    plots/<stem>_28ma_bands.png       full-year 28d-MA chart with shaded bands
    plots/<stem>_daily_bands.png      daily chart, seam onward
    plots/<stem>_dec15_zoom.png       Nov-Dec zoom on the 28d-MA

``<stem>`` defaults to ``<platform>_<state>`` read from the parquet filename, e.g. ``desktop_raw``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import cloudpickle
import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from mozaic_daily import intervals  # noqa: E402
from mozaic_daily.adjustments import load_forecast  # noqa: E402

WORLD_COUNTRY = "ALL"
# The world-total row differs by platform: desktop is one row per date with an OS key in the segment;
# mobile is unsegmented but carries one row per app, so the "ALL MOBILE" app row has to be selected too.
WORLD_SEGMENT_BY_PLATFORM = {"desktop": '{"os": "ALL"}', "mobile": "{}"}
WORLD_APP_BY_PLATFORM = {"desktop": None, "mobile": "ALL MOBILE"}
PLATFORM_BY_FILENAME_TAG = {"ld-D": "desktop", "gm-D": "mobile"}
BAND_ALPHAS = {"50": 0.45, "80": 0.30, "90": 0.18}
BAND_COLOR = "#1f77b4"
ACTUALS_COLOR = "#333333"
REFERENCE_COLORS = ("#d62728", "#ff7f0e", "#2ca02c", "#9467bd")


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_world_mozaic(pkl_path: Path, metric: str):
    with open(pkl_path, "rb") as handle:
        mozaics = cloudpickle.load(handle)
    if metric not in mozaics:
        raise KeyError(f"Metric {metric!r} not in pickle; available: {sorted(mozaics)}")
    return mozaics[metric]


def infer_platform(forecast_parquet: Path) -> str:
    """``mozaic_daily_forecast.<date>.ld-D.raw.parquet`` -> ``desktop``; ``gm-D`` -> ``mobile``."""
    tag = forecast_parquet.name.split(".")[2]
    if tag not in PLATFORM_BY_FILENAME_TAG:
        raise ValueError(f"Cannot infer platform from {forecast_parquet.name!r}: tag {tag!r} not in {sorted(PLATFORM_BY_FILENAME_TAG)}")
    return PLATFORM_BY_FILENAME_TAG[tag]


def parquet_world_series(forecast_parquet: Path) -> tuple[pd.Series, pd.Series, dict]:
    """(training actuals, forecast) for the world total, both indexed by date, plus the sidecar meta."""
    df, meta = load_forecast(str(forecast_parquet))
    platform = infer_platform(forecast_parquet)
    world_segment, world_app = WORLD_SEGMENT_BY_PLATFORM[platform], WORLD_APP_BY_PLATFORM[platform]
    is_world = (df["country"] == WORLD_COUNTRY) & (df["segment"] == world_segment)
    if world_app is not None:
        is_world &= df["app_name"] == world_app
    world = df[is_world].copy()
    if world.empty:
        raise ValueError(f"No world rows (country={WORLD_COUNTRY}, segment={world_segment}, app={world_app}) in {forecast_parquet}")
    if world["target_date"].duplicated().any():
        raise ValueError(f"World rows are not one per date in {forecast_parquet.name}; add a selector for the extra dimension.")
    world["target_date"] = pd.to_datetime(world["target_date"])
    world = world.set_index("target_date").sort_index()
    training = world.loc[world["data_type"] == "training", "dau"]
    forecast = world.loc[world["data_type"] == "forecast", "dau"]
    return training, forecast, meta


def infer_stem(forecast_parquet: Path) -> str:
    """``mozaic_daily_forecast.<date>.ld-D.raw.parquet`` -> ``desktop_raw``."""
    state = forecast_parquet.name.split(".")[3].replace("adj-", "adj_")
    return f"{infer_platform(forecast_parquet)}_{state}"


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def _format_millions_axis(ax) -> None:
    """Tick labels in millions with enough decimals that adjacent ticks differ."""
    ticks = ax.get_yticks()
    step = np.min(np.diff(ticks)) if len(ticks) > 1 else 1e6
    decimals = max(0, int(np.ceil(-np.log10(step / 1e6))) + 1) if step > 0 else 2
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v / 1e6:.{decimals}f}M"))


def _shade_bands(ax, bands: pd.DataFrame, levels: list[str]) -> None:
    for pct in sorted(levels, key=int, reverse=True):
        ax.fill_between(
            bands.index, bands[f"lower_{pct}"], bands[f"upper_{pct}"],
            color=BAND_COLOR, alpha=BAND_ALPHAS.get(pct, 0.2), linewidth=0, label=f"{pct}% interval",
        )


def plot_bands(
    bands: pd.DataFrame, actuals: pd.Series, levels: list[str], title: str, out_path: Path,
    anchor_date: pd.Timestamp, references: dict[str, float] | None = None,
) -> None:
    fig, ax = plt.subplots(figsize=(13, 6))
    if len(actuals):
        ax.plot(actuals.index, actuals.values, color=ACTUALS_COLOR, linewidth=1.2, label="actuals")
    _shade_bands(ax, bands, levels)
    ax.plot(bands.index, bands[intervals.MEDIAN_COLUMN], color=BAND_COLOR, linewidth=1.6, label="median (raw model)")
    ax.axvline(anchor_date, color="grey", linestyle=":", linewidth=1)
    for (label, value), color in zip((references or {}).items(), REFERENCE_COLORS):
        ax.scatter([anchor_date], [value], marker="D", s=45, color=color, edgecolor="white",
                   label=f"{label}: {value:,.0f}", zorder=5)
    ax.set_title(title)
    ax.set_ylabel("DAU")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d\n%Y"))
    ax.grid(alpha=0.3)
    ax.legend(loc="best", fontsize=9)
    fig.canvas.draw()
    _format_millions_axis(ax)
    fig.tight_layout()
    fig.savefig(out_path, dpi=130)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pkl", type=Path, required=True, help="mozaic_objects.*.pkl from the forecast run")
    parser.add_argument("--forecast-parquet", type=Path, required=True, help="the run's forecast parquet (with sidecar)")
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--metric", default="DAU")
    parser.add_argument("--levels", type=float, nargs="+", default=list(intervals.DEFAULT_LEVELS),
                        help="Central interval levels, e.g. 0.5 0.8 0.9")
    parser.add_argument("--anchor-date", default="2026-12-15")
    parser.add_argument("--curve-start", default="2026-01-01", help="First date written to the curve CSVs and plots")
    parser.add_argument("--curve-end", default="2026-12-31",
                        help="Last date written to the curve CSVs and plots (the run forecasts a further year)")
    parser.add_argument("--trough-window", nargs=2, default=["2026-08-01", "2026-09-30"], metavar=("START", "END"))
    parser.add_argument("--reference", action="append", default=[], metavar="LABEL=VALUE",
                        help="Point reference to print in the summary and mark on the plots (repeatable)")
    parser.add_argument("--stem", default=None, help="Output file stem (default from the parquet name)")
    parser.add_argument("--median-tolerance", type=float, default=1.0,
                        help="Max allowed |median path - parquet forecast| in DAU before aborting")
    return parser.parse_args()


def parse_references(items: list[str]) -> dict[str, float]:
    references = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"--reference must be LABEL=VALUE, got {item!r}")
        label, value = item.rsplit("=", 1)
        references[label.strip()] = float(value.replace(",", ""))
    return references


def main() -> None:
    args = parse_args()
    levels = list(args.levels)
    level_labels = [f"{round(l * 100):d}" for l in levels]
    anchor_date = pd.Timestamp(args.anchor_date)
    references = parse_references(args.reference)
    stem = args.stem or infer_stem(args.forecast_parquet)

    csv_dir, plot_dir = args.out_dir / "csv", args.out_dir / "plots"
    csv_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading pickle {args.pkl} ...")
    moz = load_world_mozaic(args.pkl, args.metric)
    training, parquet_forecast, meta = parquet_world_series(args.forecast_parquet)

    paths = intervals.world_sample_paths(moz)
    worst = intervals.assert_median_matches_forecast(paths, parquet_forecast, tolerance=args.median_tolerance)
    print(f"Median path reproduces the parquet world forecast (worst |dev| {worst:.4f} DAU) — {paths.shape[1]} paths")

    actuals = intervals.world_actuals(moz)
    if not np.allclose(actuals.reindex(training.index).to_numpy(), training.to_numpy(), atol=1.0):
        raise AssertionError("Pickle actuals differ from the parquet's training rows beyond 1 DAU — wrong pickle/parquet pair?")

    daily_bands = intervals.band_quantiles(paths, levels)
    spliced = intervals.splice_actuals_onto_paths(actuals, paths)
    ma_bands = intervals.band_quantiles(intervals.rolling_mean_paths(spliced), levels)
    ma_bands = ma_bands.loc[paths.index.min():]  # bands only where at least one forecast day is in the window
    actuals_ma = actuals.rolling(intervals.MA_WINDOW).mean()
    # The published convention is the trailing mean of the MEDIAN path (the parquet's point forecast).
    # It differs from the median of the per-path trailing means whenever the path distribution is
    # skewed, so both are written: `point_forecast_28ma` is the quotable number, `median` is the band centre.
    point_forecast_ma = pd.concat([actuals, parquet_forecast]).rolling(intervals.MA_WINDOW).mean().loc[paths.index.min():]
    ma_bands.insert(0, "point_forecast_28ma", point_forecast_ma)

    curve_start, curve_end = pd.Timestamp(args.curve_start), pd.Timestamp(args.curve_end)
    daily_bands, ma_bands = daily_bands.loc[:curve_end], ma_bands.loc[:curve_end]
    daily_csv = pd.concat([actuals.rename("actuals").loc[curve_start:], daily_bands], axis=1)
    ma_csv = pd.concat([actuals_ma.rename("actuals_28ma").loc[curve_start:], ma_bands], axis=1)
    daily_csv.rename_axis("date").to_csv(csv_dir / f"{stem}_daily_bands.csv", float_format="%.2f")
    ma_csv.rename_axis("date").to_csv(csv_dir / f"{stem}_28ma_bands.csv", float_format="%.2f")

    summary = {
        "produced_by": "scripts/compute_forecast_intervals.py",
        "pkl": str(args.pkl),
        "forecast_parquet": str(args.forecast_parquet),
        "forecast_start_date": meta.get("forecast_start_date"),
        "adjustments_applied": meta.get("adjustments_applied", []),
        "model_config": meta.get("model_config"),
        "n_sample_paths": int(paths.shape[1]),
        "levels": levels,
        "ma_window": intervals.MA_WINDOW,
        "method": "quantiles across sample paths; 28d-MA = plain trailing mean per path with actuals spliced in front",
        "interval_kind": "Prophet predictive (trend + changepoint simulation + observation noise); not calibrated",
        "median_vs_parquet_worst_abs_dev": worst,
        "anchor_28ma": {**intervals.point_summary(ma_bands, anchor_date, levels),
                        "point_forecast_28ma": float(ma_bands.loc[anchor_date, "point_forecast_28ma"])},
        "anchor_daily": intervals.point_summary(daily_bands, anchor_date, levels),
        "trough_28ma": intervals.trough_summary(ma_bands, *args.trough_window),
        "references": references,
    }
    (args.out_dir / f"{stem}_summary.json").write_text(json.dumps(summary, indent=2, default=str))

    rows = [{"series": f"{stem} 28d-MA", **summary["anchor_28ma"]}, {"series": f"{stem} daily", **summary["anchor_daily"]}]
    for label, value in references.items():
        rows.append({"series": label, "date": anchor_date.date().isoformat(), intervals.MEDIAN_COLUMN: value})
    pd.DataFrame(rows).to_csv(csv_dir / f"{stem}_summary.csv", index=False, float_format="%.0f")

    plot_bands(ma_bands, actuals_ma.loc[curve_start:], level_labels,
               f"{stem}: 28d-MA with predictive intervals (seam {paths.index.min().date()})",
               plot_dir / f"{stem}_28ma_bands.png", anchor_date, references)
    plot_bands(daily_bands, actuals.loc[paths.index.min() - pd.Timedelta(days=90):], level_labels,
               f"{stem}: daily DAU with predictive intervals", plot_dir / f"{stem}_daily_bands.png", anchor_date)
    zoom = ma_bands.loc[anchor_date - pd.Timedelta(days=45): anchor_date + pd.Timedelta(days=16)]
    plot_bands(zoom, actuals_ma.iloc[0:0], level_labels, f"{stem}: 28d-MA around {anchor_date.date()}",
               plot_dir / f"{stem}_dec15_zoom.png", anchor_date, references)

    a = summary["anchor_28ma"]
    print(f"\n{stem} 28d-MA on {anchor_date.date()}: point forecast {a['point_forecast_28ma']:,.0f} "
          f"(trailing mean of the median path); band-centre median {a['median']:,.0f}")
    for pct in level_labels:
        print(f"  {pct}%: [{a[f'lower_{pct}']:,.0f}, {a[f'upper_{pct}']:,.0f}]  half-width {a[f'halfwidth_{pct}']:,.0f}")
    for label, value in references.items():
        print(f"  reference {label}: {value:,.0f}")
    t = summary["trough_28ma"]
    print(f"Trough (28d-MA median) {t['trough_date']}: {t['median']:,.0f}")
    print(f"\nWrote {csv_dir}/{stem}_*.csv, {plot_dir}/{stem}_*.png, {args.out_dir}/{stem}_summary.json")


if __name__ == "__main__":
    main()
