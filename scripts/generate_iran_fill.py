#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Generate the Iran counterfactual-fill artifact for the mozaic shutdown gap.

Iran's internet shutdown collapsed native Firefox telemetry to near-zero from 2026-03-01
through 2026-05-25; it fully recovered on 2026-05-26. Fed raw into mozaic, the 86-day hole
corrupts Prophet. This script builds a **counterfactual fill** — what Iran would have been with
no shutdown — by *propagating the mozaic model forward*: train mozaic on clean pre-shutdown IR
data (everything before ``FORECAST_START``), forecast the gap forward, and harvest mozaic's own
**per-population** IR forecast as the fill. This is the same mechanism as the retired
``generate_iran_synthetic.py``, with three deltas: per-population (not ALL-level) harvest, the
long segment-boolean output schema (see ``data-official/2026-07/iran_fill/FILL_FORMAT_SPEC.md``),
and per-metric fill windows (MAU extends ~28d past recovery because it is a rolling-28 metric).

Two phases, separated by a **human go/no-go** on seam scaling (never automatic):

  Phase 1 (default):  query -> forecast -> harvest unscaled fill + compute seam diagnostics +
                      render seam plots. Persists drafts under ``<out>/_draft/`` and STOPS.
  Phase 2 (--finalize --scale-spec FILE):  read the drafts, apply the approved per-(source,metric)
                      scale factors, and write the final ``iran_fill.<source>.parquet`` + sidecar.

Usage:
    # Phase 1 — produce diagnostics for review:
    python scripts/generate_iran_fill.py
    # Phase 2 — after approving scales in seam_scale_spec.json:
    python scripts/generate_iran_fill.py --finalize --scale-spec <out>/seam_scale_spec.json
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
from google.cloud import bigquery

repo_root = Path(__file__).parent.parent
src_path = repo_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from mozaic_daily.config import STATIC_CONFIG
from mozaic_daily.data import query_to_dataframe
from mozaic_daily.forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs
from mozaic_daily.queries import (
    ADDITIONAL_HOLIDAYS,
    DataSource,
    Metric,
    Platform,
    QUERY_SPECS,
)
from mozaic.models import DesktopModelConfig, MobileModelConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Train on clean pre-shutdown IR (everything < FORECAST_START); forecast the gap forward.
# 2026-02-28 is the first fill day: real data through 2026-02-27 is clean pre-shutdown.
FORECAST_START = "2026-02-28"
FORECAST_END = "2026-12-31"

# Per-metric fill window ends (inclusive). DAU/NP/EED end at the last shutdown day; MAU extends
# ~28d because its rolling-28 window is blackout-free only from 2026-06-22 (see FILL_FORMAT_SPEC).
FILL_END_DEFAULT = "2026-05-25"
FILL_END_BY_METRIC = {
    Metric.EXISTING_ENGAGEMENT_MAU.value: "2026-06-21",
}
# First real day after each metric's fill window — used for the seam comparison.
REAL_RESUMES_DEFAULT = "2026-05-26"
REAL_RESUMES_BY_METRIC = {
    Metric.EXISTING_ENGAGEMENT_MAU.value: "2026-06-22",
}

DEFAULT_OUTPUT_DIR = os.path.join(repo_root, "data-official", "2026-07", "iran_fill")

# July params (reuse June): desktop differs from package default only in holiday_threshold=-0.05;
# mobile uses package defaults (cps=0.02, thresh=-0.032, recent_weeks=13). See forecast-parameters/.
DESKTOP_CONFIG = DesktopModelConfig(holiday_threshold=-0.05)
MOBILE_CONFIG = MobileModelConfig()

ALL_DATA_SOURCES = [DataSource.GLEAN_DESKTOP, DataSource.LEGACY_DESKTOP, DataSource.GLEAN_MOBILE]

FORECAST_FN_BY_PLATFORM = {
    Platform.DESKTOP: get_desktop_forecast_dfs,
    Platform.MOBILE: get_mobile_forecast_dfs,
}

# Segment-boolean columns per platform (must match populate_tiles input exactly).
SEGMENT_COLUMNS = {
    Platform.DESKTOP: ["modern_windows", "winX"],
    Platform.MOBILE: ["fenix_android", "firefox_ios", "focus_android", "focus_ios"],
}


def population_to_segments(population: str, platform: Platform) -> Dict[str, bool]:
    """Reverse-map a mozaic population label to its segment-boolean flags.

    Populations are single-label (OS buckets are mutually exclusive; an app row sets exactly one
    app flag), so at most one flag is True. ``population == 'other'`` (or any label not in the
    platform's segment set) maps to all-False.
    """
    cols = SEGMENT_COLUMNS[platform]
    return {col: (col == population) for col in cols}


# ---------------------------------------------------------------------------
# BQ queries
# ---------------------------------------------------------------------------

def query_iran_data(
    project: str,
    data_sources: List[DataSource],
) -> Dict[str, Dict[str, Dict[str, pd.DataFrame]]]:
    """Query BigQuery for Iran-only historical data, one frame per (source, metric).

    Strips the ``country != 'IR'`` exclusion that ``build_query`` adds for the main pipeline.
    Returns ``{platform_value: {source_value: {metric_value: DataFrame}}}`` matching the layout
    ``generate_forecasts`` expects.
    """
    datasets: Dict[str, Dict[str, Dict[str, pd.DataFrame]]] = {
        "desktop": {"glean": {}, "legacy": {}},
        "mobile": {"glean": {}},
    }

    specs_to_run = [
        spec for spec in QUERY_SPECS.values() if spec.data_source in data_sources
    ]

    for i, spec in enumerate(specs_to_run, 1):
        platform = spec.platform.value
        source = spec.telemetry_source.value
        metric = spec.metric.value

        print(f"[{i}/{len(specs_to_run)}] Querying {spec.data_source.display_name} {metric}")
        start = time.time()
        sql = spec.build_query("'IR'").replace("AND country != 'IR'", "")
        client = bigquery.Client(project)
        df = query_to_dataframe(client, sql, label=f"iran {spec.data_source.display_name} {metric}")
        elapsed = time.time() - start
        print(f"  -> {len(df)} rows in {elapsed:.1f}s")
        if df.empty:
            print(f"  WARNING: No data returned for {spec.data_source.display_name} {metric}")

        datasets[platform][source][metric] = df

    return datasets


# ---------------------------------------------------------------------------
# Forecasting
# ---------------------------------------------------------------------------

def generate_forecasts(
    datasets: Dict[str, Dict[str, Dict[str, pd.DataFrame]]],
    data_sources: List[DataSource],
) -> Dict[DataSource, Dict[str, pd.DataFrame]]:
    """Run the forward mozaic forecast per data source. Returns {DataSource: {metric: granular_df}}."""
    configs = {Platform.DESKTOP: DESKTOP_CONFIG, Platform.MOBILE: MOBILE_CONFIG}
    forecasts: Dict[DataSource, Dict[str, pd.DataFrame]] = {}

    for i, data_source in enumerate(data_sources, 1):
        metric_data = datasets[data_source.platform.value][data_source.telemetry_source.value]
        print(f"\n{'='*60}\n[{i}/{len(data_sources)}] Forecasting {data_source.display_name}\n{'='*60}")
        if not metric_data:
            print("  Skipping — no data available")
            continue

        result = FORECAST_FN_BY_PLATFORM[data_source.platform](
            metric_data,
            FORECAST_START,
            FORECAST_END,
            additional_holidays=ADDITIONAL_HOLIDAYS.get(data_source, []),
            config=configs[data_source.platform],
        )
        forecasts[data_source] = result.dfs

    return forecasts


# ---------------------------------------------------------------------------
# Harvest per-population fill
# ---------------------------------------------------------------------------

def harvest_fill(
    forecast_df: pd.DataFrame,
    data_source: DataSource,
    metric: str,
) -> pd.DataFrame:
    """Extract per-population IR forecast rows for the metric's fill window.

    Returns long rows: metric, x, country='IR', <segment booleans>, y. Excludes the 'ALL'
    population (mozaic recomputes ALL by summing tiles).
    """
    fill_end = FILL_END_BY_METRIC.get(metric, FILL_END_DEFAULT)
    platform = data_source.platform

    df = forecast_df[
        (forecast_df["country"] == "IR") & (forecast_df["population"] != "ALL")
    ].copy()
    df["target_date"] = pd.to_datetime(df["target_date"])
    df = df[(df["target_date"] >= pd.Timestamp(FORECAST_START)) & (df["target_date"] <= pd.Timestamp(fill_end))]

    if df.empty:
        print(f"  WARNING: no IR per-population rows for {data_source.value} {metric}")
        return pd.DataFrame()

    seg_rows = df["population"].apply(lambda p: pd.Series(population_to_segments(p, platform)))
    out = pd.concat([df[["target_date", "value"]].reset_index(drop=True), seg_rows.reset_index(drop=True)], axis=1)
    out = out.rename(columns={"target_date": "x", "value": "y"})
    # Prophet point forecasts can dip slightly negative for tiny-count populations (e.g. focus
    # New Profiles ~a few/day); counts can't be negative, so floor at 0.
    out["y"] = out["y"].clip(lower=0)
    out["metric"] = metric
    out["country"] = "IR"
    cols = ["metric", "x", "country"] + SEGMENT_COLUMNS[platform] + ["y"]
    return out[cols]


def real_iran_all_by_date(metric_df: pd.DataFrame) -> pd.Series:
    """Real IR-ALL daily total from a queried frame (sum y across IR segment rows per date).

    The query returns IR *and* everything-else-as-'ROW' (build_query maps non-listed countries to
    'ROW'), so we MUST filter country=='IR' before summing or we get the world total.
    """
    ir = metric_df[metric_df["country"] == "IR"]
    s = ir.groupby("x")["y"].sum()
    s.index = pd.to_datetime(s.index)
    return s.sort_index()


# ---------------------------------------------------------------------------
# Re-seasonalize: restore real day-of-week amplitude (Prophet damps the weekly swing)
# ---------------------------------------------------------------------------
# Prophet shrinks the weekday->weekend amplitude (measured: the fill's peak/trough swing is ~46%
# of real IR's). We re-impose the empirical day-of-week profile measured from clean pre-shutdown IR,
# per (metric, population) with a pooled-ALL fallback for sparse populations. Geomean-normalized, so
# the trend and weekly mean level (and the seam we validated) are preserved.

CLEAN_DOW_START = "2025-09-01"               # real reference window start for the DOW profile
JAN_BLACKOUT = ("2026-01-08", "2026-01-23")  # excluded from the reference (Jan mini-blackout)
MIN_DOW_POINTS = 42                          # >=6 clean weeks to trust a per-population profile


def _population_of_row(row, seg_cols) -> str:
    return next((c for c in seg_cols if row[c]), "other")


def _dow_profile(daily: pd.Series):
    """Geomean-normalized multiplicative day-of-week profile (index 0=Mon..6=Sun) for a daily series.

    Detrends in log space by a 28-day centered mean, then averages the residual by weekday.
    Returns (profile Series, n_effective_points) or (None, 0) if too sparse.
    """
    s = daily[daily > 0].sort_index()
    if len(s) < 14:
        return None, 0
    log = np.log(s)
    detr = (log - log.rolling(28, center=True, min_periods=10).mean()).dropna()
    if len(detr) < 14:
        return None, 0
    prof = detr.groupby(detr.index.dayofweek).mean()
    return np.exp(prof - prof.mean()), len(detr)


def _clean_window(daily: pd.Series) -> pd.Series:
    s = daily[(daily.index >= pd.Timestamp(CLEAN_DOW_START)) & (daily.index < pd.Timestamp(FORECAST_START))]
    return s[~((s.index >= pd.Timestamp(JAN_BLACKOUT[0])) & (s.index <= pd.Timestamp(JAN_BLACKOUT[1])))]


def _population_daily(df: pd.DataFrame, platform: Platform) -> Dict[str, pd.Series]:
    """Split a frame (x, segment bools, y) into {population: daily Series}, plus 'ALL' (IR total)."""
    seg_cols = SEGMENT_COLUMNS[platform]
    d = df.copy()
    d["x"] = pd.to_datetime(d["x"])
    d["pop"] = d.apply(lambda r: _population_of_row(r, seg_cols), axis=1)
    out = {}
    for pop, g in d.groupby("pop"):
        out[pop] = g.groupby("x")["y"].sum().sort_index()
    out["ALL"] = d.groupby("x")["y"].sum().sort_index()
    return out


def compute_real_dow_factors(real_metric_df: pd.DataFrame, platform: Platform) -> Dict[str, pd.Series]:
    """Per-population real DOW profiles from the clean pre-shutdown IR window; pooled-ALL fallback.

    Returns {population: profile} plus '__pooled__'. Sparse populations get the pooled profile.
    """
    ir = real_metric_df[real_metric_df["country"] == "IR"]
    series = _population_daily(ir, platform)
    pooled, _ = _dow_profile(_clean_window(series.get("ALL", pd.Series(dtype=float))))
    profiles: Dict[str, pd.Series] = {"__pooled__": pooled}
    for pop, s in series.items():
        if pop == "ALL":
            continue
        prof, n = _dow_profile(_clean_window(s))
        profiles[pop] = prof if (prof is not None and n >= MIN_DOW_POINTS) else pooled
    return profiles


def reseasonalize_source(
    fill_smooth: pd.DataFrame,
    real_metric_dfs: Dict[str, pd.DataFrame],
    platform: Platform,
) -> pd.DataFrame:
    """Re-impose real DOW amplitude per (metric, population) onto the smooth fill (mean-preserving).

    corrected = smooth / fill_DOW_factor[dow] * real_DOW_factor[dow]
    """
    seg_cols = SEGMENT_COLUMNS[platform]
    work = fill_smooth.copy()
    work["x"] = pd.to_datetime(work["x"])
    work["pop"] = work.apply(lambda r: _population_of_row(r, seg_cols), axis=1)
    work["dow"] = work["x"].dt.dayofweek

    corrected = []
    for metric, g in work.groupby("metric"):
        real_prof = compute_real_dow_factors(real_metric_dfs[metric], platform)
        g = g.copy()
        fill_prof, smooth_pop_mean = {}, {}
        for pop, gg in g.groupby("pop"):
            fill_prof[pop], _ = _dow_profile(gg.groupby("x")["y"].sum())
            smooth_pop_mean[pop] = gg["y"].mean()

        def factor(row):
            rp = real_prof.get(row["pop"], real_prof["__pooled__"])
            fp = fill_prof.get(row["pop"])
            if rp is None or fp is None or row["dow"] not in rp.index or row["dow"] not in fp.index:
                return 1.0
            return float(rp[row["dow"]] / fp[row["dow"]])

        g["y"] = g["y"] * g.apply(factor, axis=1)
        # Preserve each population's arithmetic mean: re-imposing amplitude lifts the arithmetic mean
        # at fixed geomean (Jensen), but a uniform per-population rescale leaves the DOW swing intact
        # while restoring the level the seam was validated at.
        for pop, gg in g.groupby("pop"):
            cur = gg["y"].mean()
            if cur > 0:
                g.loc[gg.index, "y"] = gg["y"] * (smooth_pop_mean[pop] / cur)
        corrected.append(g)

    return pd.concat(corrected, ignore_index=True)[list(fill_smooth.columns)]


# ---------------------------------------------------------------------------
# Seam diagnostics
# ---------------------------------------------------------------------------

def seam_diagnostics(
    fill_df: pd.DataFrame,
    real_all: pd.Series,
    metric: str,
) -> Dict[str, float]:
    """Compare the fill's right edge to the first real-recovery data (de-weekended 7-day means).

    Returns the point and 7-day-mean seam deltas plus the candidate scale factor that would make
    the fill's right edge meet real recovery. NOTHING is applied here — this is for the human
    go/no-go.
    """
    fill_end = pd.Timestamp(FILL_END_BY_METRIC.get(metric, FILL_END_DEFAULT))
    real_resume = pd.Timestamp(REAL_RESUMES_BY_METRIC.get(metric, REAL_RESUMES_DEFAULT))

    fill_all = fill_df.groupby("x")["y"].sum()
    fill_all.index = pd.to_datetime(fill_all.index)

    fill_point = float(fill_all.get(fill_end, float("nan")))
    real_point = float(real_all.get(real_resume, float("nan")))

    # 7-day means straddling the seam (last 7 fill days vs first 7 real-recovery days).
    fill_7 = fill_all[fill_all.index <= fill_end].tail(7).mean()
    real_7 = real_all[(real_all.index >= real_resume) & (real_all.index < real_resume + pd.Timedelta(days=7))].mean()

    pct_point = (real_point / fill_point - 1.0) * 100 if fill_point else float("nan")
    pct_7 = (real_7 / fill_7 - 1.0) * 100 if fill_7 else float("nan")
    scale_7 = (real_7 / fill_7) if fill_7 else float("nan")

    return {
        "fill_end": fill_end.date().isoformat(),
        "real_resume": real_resume.date().isoformat(),
        "fill_point": round(fill_point, 1),
        "real_point": round(real_point, 1),
        "pct_delta_point": round(pct_point, 2),
        "fill_7d_mean": round(float(fill_7), 1),
        "real_7d_mean": round(float(real_7), 1),
        "pct_delta_7d": round(pct_7, 2),
        "candidate_scale_7d": round(scale_7, 4),
    }


def render_seam_plot(
    fill_df: pd.DataFrame,
    real_all: pd.Series,
    data_source: DataSource,
    metric: str,
    out_path: str,
    smooth_df: pd.DataFrame = None,
) -> None:
    """Two-panel seam plot. Top: real IR (Feb–Jun, incl. gap + recovery) vs the counterfactual fill,
    with the real pre-shutdown window shown so weekly amplitude is comparable. Bottom: a 5-week
    mid-gap zoom of the fill (and, if given, the pre-correction smooth path) so the restored
    weekday->weekend swing is verifiable at a glance."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fill_all = fill_df.groupby("x")["y"].sum()
    fill_all.index = pd.to_datetime(fill_all.index)
    real_window = real_all[(real_all.index >= pd.Timestamp("2026-02-01")) & (real_all.index <= pd.Timestamp("2026-07-01"))]
    real_resume = pd.Timestamp(REAL_RESUMES_BY_METRIC.get(metric, REAL_RESUMES_DEFAULT))

    fig, (ax, axz) = plt.subplots(2, 1, figsize=(11, 7))
    ax.plot(real_window.index, real_window.values, color="#888", lw=1, label="real IR (incl. gap)")
    ax.plot(fill_all.index, fill_all.values, color="#d62728", lw=1.4, label="counterfactual fill")
    ax.axvline(real_resume, color="#1f77b4", ls="--", lw=1, label=f"real resumes {real_resume.date()}")
    ax.set_title(f"{data_source.value} — {metric}: counterfactual fill vs real")
    ax.set_ylabel("IR-ALL")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)

    # Zoom: a 5-week mid-gap window to inspect weekly amplitude.
    z0, z1 = pd.Timestamp("2026-03-30"), pd.Timestamp("2026-05-03")
    fz = fill_all[(fill_all.index >= z0) & (fill_all.index <= z1)]
    axz.plot(fz.index, fz.values, color="#d62728", lw=1.8, marker="o", ms=3, label="fill (DOW-restored)")
    if smooth_df is not None:
        sm = smooth_df.groupby("x")["y"].sum()
        sm.index = pd.to_datetime(sm.index)
        smz = sm[(sm.index >= z0) & (sm.index <= z1)]
        axz.plot(smz.index, smz.values, color="#888", lw=1.4, ls="--", marker=".", ms=3, label="fill (smooth, pre-fix)")
    axz.set_title("Mid-gap zoom — weekly amplitude (Fri trough should deepen vs smooth)")
    axz.set_ylabel("IR-ALL")
    axz.legend(fontsize=8)
    axz.grid(alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=110)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def run_phase1(project: str, out_dir: str, data_sources: List[DataSource]) -> None:
    """Forecast forward, harvest unscaled fill, persist drafts + seam diagnostics + plots. STOP."""
    draft_dir = os.path.join(out_dir, "_draft")
    plot_dir = os.path.join(out_dir, "seam_plots")
    os.makedirs(draft_dir, exist_ok=True)
    os.makedirs(plot_dir, exist_ok=True)
    overall = time.time()

    print(f"\n{'='*60}\nStep 1: Query IR data\n{'='*60}")
    datasets = query_iran_data(project, data_sources)

    print(f"\n{'='*60}\nStep 2: Forward forecast (FORECAST_START={FORECAST_START})\n{'='*60}")
    forecasts = generate_forecasts(datasets, data_sources)

    print(f"\n{'='*60}\nStep 3: Harvest + restore weekly DOW amplitude (write drafts)\n{'='*60}")
    fills_by_source: Dict[DataSource, pd.DataFrame] = {}
    smooth_by_source: Dict[DataSource, pd.DataFrame] = {}
    for data_source in data_sources:
        if data_source not in forecasts:
            continue
        per_metric_fills = [
            f for f in (harvest_fill(fdf, data_source, m) for m, fdf in forecasts[data_source].items())
            if not f.empty
        ]
        if not per_metric_fills:
            continue
        src_smooth = pd.concat(per_metric_fills, ignore_index=True)
        real_metric_dfs = datasets[data_source.platform.value][data_source.telemetry_source.value]
        src_fill = reseasonalize_source(src_smooth, real_metric_dfs, data_source.platform)
        smooth_by_source[data_source] = src_smooth
        fills_by_source[data_source] = src_fill
        src_smooth.to_parquet(os.path.join(draft_dir, f"iran_fill.{data_source.value}.smooth.parquet"), index=False)
        src_fill.to_parquet(os.path.join(draft_dir, f"iran_fill.{data_source.value}.unscaled.parquet"), index=False)
        print(f"  {data_source.value}: harvested + DOW-restored ({len(src_fill)} rows)")

    print(f"\n{'='*60}\nStep 4: Seam diagnostics + plots\n{'='*60}")
    emit_diagnostics(out_dir, plot_dir, fills_by_source, datasets, data_sources, smooth_by_source)
    print(f"\nTotal elapsed: {(time.time()-overall)/60:.1f} min")


def run_reseasonalize(project: str, out_dir: str, data_sources: List[DataSource]) -> None:
    """Re-impose real DOW amplitude on existing smooth Phase-1 drafts (no re-forecast).

    Treats an existing ``.smooth.parquet`` as the smooth source; if absent, the current
    ``.unscaled.parquet`` is taken as smooth and backed up to ``.smooth.parquet`` first. Writes the
    corrected fill back to ``.unscaled.parquet`` (what --finalize consumes) and refreshes diagnostics.
    """
    draft_dir = os.path.join(out_dir, "_draft")
    plot_dir = os.path.join(out_dir, "seam_plots")
    print(f"\n{'='*60}\nReseasonalize: re-query real IR + restore DOW amplitude on drafts\n{'='*60}")
    datasets = query_iran_data(project, data_sources)

    fills_by_source: Dict[DataSource, pd.DataFrame] = {}
    smooth_by_source: Dict[DataSource, pd.DataFrame] = {}
    for data_source in data_sources:
        unscaled_path = os.path.join(draft_dir, f"iran_fill.{data_source.value}.unscaled.parquet")
        smooth_path = os.path.join(draft_dir, f"iran_fill.{data_source.value}.smooth.parquet")
        if os.path.exists(smooth_path):
            src_smooth = pd.read_parquet(smooth_path)
        elif os.path.exists(unscaled_path):
            src_smooth = pd.read_parquet(unscaled_path)
            src_smooth.to_parquet(smooth_path, index=False)  # back up the smooth path once
        else:
            print(f"  skip {data_source.value}: no draft")
            continue
        real_metric_dfs = datasets[data_source.platform.value][data_source.telemetry_source.value]
        src_fill = reseasonalize_source(src_smooth, real_metric_dfs, data_source.platform)
        src_fill.to_parquet(unscaled_path, index=False)
        smooth_by_source[data_source] = src_smooth
        fills_by_source[data_source] = src_fill
        print(f"  {data_source.value}: DOW-restored ({len(src_fill)} rows)")

    emit_diagnostics(out_dir, plot_dir, fills_by_source, datasets, data_sources, smooth_by_source)


def emit_diagnostics(
    out_dir: str,
    plot_dir: str,
    fills_by_source: Dict[DataSource, pd.DataFrame],
    datasets: Dict[str, Dict[str, Dict[str, pd.DataFrame]]],
    data_sources: List[DataSource],
    smooth_by_source: Dict[DataSource, pd.DataFrame] = None,
) -> None:
    """Compute seam diagnostics + render plots from per-source fills vs real IR data; write CSV +
    a pre-seeded scale spec (all 1.0). Shared by Phase 1 / --diagnostics-only / --reseasonalize.
    Applies NO scaling. If smooth_by_source is given, the pre-correction path is overlaid in the
    zoom panel so the DOW-amplitude fix is verifiable."""
    os.makedirs(plot_dir, exist_ok=True)
    smooth_by_source = smooth_by_source or {}
    diag_rows = []
    for data_source in data_sources:
        if data_source not in fills_by_source:
            continue
        src_fill = fills_by_source[data_source]
        src_smooth = smooth_by_source.get(data_source)
        src_metric_data = datasets[data_source.platform.value][data_source.telemetry_source.value]
        for metric in src_fill["metric"].unique():
            fill = src_fill[src_fill["metric"] == metric]
            smooth = src_smooth[src_smooth["metric"] == metric] if src_smooth is not None else None
            real_all = real_iran_all_by_date(src_metric_data[metric])
            diag = seam_diagnostics(fill, real_all, metric)
            diag.update({"data_source": data_source.value, "metric": metric})
            diag_rows.append(diag)
            plot_path = os.path.join(plot_dir, f"seam.{data_source.value}.{Metric(metric).short_code}.png")
            render_seam_plot(fill, real_all, data_source, metric, plot_path, smooth_df=smooth)

    diag_df = pd.DataFrame(diag_rows)[
        ["data_source", "metric", "fill_end", "real_resume", "fill_7d_mean", "real_7d_mean",
         "pct_delta_7d", "candidate_scale_7d", "pct_delta_point"]
    ]
    diag_path = os.path.join(out_dir, "seam_diagnostics.csv")
    diag_df.to_csv(diag_path, index=False)

    scale_spec = {ds.value: {} for ds in data_sources}
    for r in diag_rows:
        scale_spec[r["data_source"]][r["metric"]] = 1.0
    spec_path = os.path.join(out_dir, "seam_scale_spec.json")
    with open(spec_path, "w") as f:
        json.dump(scale_spec, f, indent=2)

    print(f"\n{'='*60}\nSEAM DIAGNOSTICS (human go/no-go required)\n{'='*60}")
    with pd.option_context("display.width", 200, "display.max_columns", None):
        print(diag_df.to_string(index=False))
    print(f"\nDiagnostics: {diag_path}\nPlots: {plot_dir}\nScale spec (edit + approve): {spec_path}")
    print(f"\nSTOP — review seams, set factors in seam_scale_spec.json, then run "
          f"--finalize --scale-spec {spec_path}")


def run_diagnostics_only(project: str, out_dir: str, data_sources: List[DataSource]) -> None:
    """Recompute seam diagnostics + plots from the saved Phase-1 drafts (no re-forecast)."""
    draft_dir = os.path.join(out_dir, "_draft")
    plot_dir = os.path.join(out_dir, "seam_plots")
    print(f"\n{'='*60}\nDiagnostics-only: re-query real IR + recompute from drafts\n{'='*60}")
    datasets = query_iran_data(project, data_sources)
    fills_by_source: Dict[DataSource, pd.DataFrame] = {}
    for data_source in data_sources:
        draft_path = os.path.join(draft_dir, f"iran_fill.{data_source.value}.unscaled.parquet")
        if os.path.exists(draft_path):
            fills_by_source[data_source] = pd.read_parquet(draft_path)
    emit_diagnostics(out_dir, plot_dir, fills_by_source, datasets, data_sources)


def run_finalize(out_dir: str, scale_spec_path: str, data_sources: List[DataSource]) -> None:
    """Apply approved per-(source,metric) scale factors to the drafts; write final artifacts + sidecars."""
    import subprocess
    from mozaic_daily.config import get_git_commit_hash  # mozaic package commit

    try:
        mozaic_daily_commit = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"], text=True
        ).strip()
    except Exception:
        mozaic_daily_commit = "unknown"

    with open(scale_spec_path) as f:
        scale_spec = json.load(f)
    draft_dir = os.path.join(out_dir, "_draft")

    for data_source in data_sources:
        draft_path = os.path.join(draft_dir, f"iran_fill.{data_source.value}.unscaled.parquet")
        if not os.path.exists(draft_path):
            print(f"  skip {data_source.value}: no draft")
            continue
        fill = pd.read_parquet(draft_path)
        src_scales = scale_spec.get(data_source.value, {})
        applied = {}
        for metric, factor in src_scales.items():
            if factor != 1.0:
                fill.loc[fill["metric"] == metric, "y"] *= float(factor)
            applied[metric] = float(factor)
        fill["y"] = fill["y"].clip(lower=0)  # floor counts at 0 (drafts may predate the harvest clip)

        final_path = os.path.join(out_dir, f"iran_fill.{data_source.value}.parquet")
        fill.to_parquet(final_path, index=False)

        meta = {
            "producer": "scripts/generate_iran_fill.py",
            "mozaic_daily_commit": mozaic_daily_commit,
            "mozaic_commit": get_git_commit_hash(),
            "forecast_start": FORECAST_START,
            "forecast_end": FORECAST_END,
            "fill_end_default": FILL_END_DEFAULT,
            "fill_end_by_metric": FILL_END_BY_METRIC,
            "platform": data_source.platform.value,
            "model_config": (DESKTOP_CONFIG if data_source.platform == Platform.DESKTOP else MOBILE_CONFIG).to_dict(),
            "seam_scale_factors": applied,
            "weekly_reseasonalized": os.path.exists(
                os.path.join(draft_dir, f"iran_fill.{data_source.value}.smooth.parquet")
            ),
            "dow_reference_window": [CLEAN_DOW_START, FORECAST_START],
            "dow_granularity": "per-population, pooled-ALL fallback (>=6 clean weeks)",
            "country": "IR",
        }
        with open(os.path.join(out_dir, f"iran_fill.{data_source.value}.meta.json"), "w") as f:
            json.dump(meta, f, indent=2)
        print(f"  wrote {final_path} (+ sidecar); scales={applied}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--project", default=STATIC_CONFIG["default_project"])
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--finalize", action="store_true", help="Phase 2: apply approved scales + write final artifacts")
    p.add_argument("--scale-spec", default=None, help="Path to approved seam_scale_spec.json (required with --finalize)")
    p.add_argument("--diagnostics-only", action="store_true",
                   help="Recompute seam diagnostics + plots from saved Phase-1 drafts (no re-forecast)")
    p.add_argument("--reseasonalize", action="store_true",
                   help="Restore real DOW amplitude on existing smooth drafts + refresh diagnostics (no re-forecast)")
    return p.parse_args()


def main():
    args = parse_args()
    if args.finalize:
        if not args.scale_spec:
            sys.exit("--finalize requires --scale-spec")
        run_finalize(args.output_dir, args.scale_spec, ALL_DATA_SOURCES)
    elif args.reseasonalize:
        run_reseasonalize(args.project, args.output_dir, ALL_DATA_SOURCES)
    elif args.diagnostics_only:
        run_diagnostics_only(args.project, args.output_dir, ALL_DATA_SOURCES)
    else:
        run_phase1(args.project, args.output_dir, ALL_DATA_SOURCES)


if __name__ == "__main__":
    main()
