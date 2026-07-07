"""Export the canonical forecast curves to CSV.

Two modes:

  (default)        Reproduces the 12 plotted series (6 desktop + 6 mobile) from
                   june_canonical_v2026-05-27.ipynb over the display window
                   2026-01-01 .. 2026-12-31 as 28-day moving averages, writing
                   one tidy ALL-level CSV with the headwind adjustment applied:

                       csv/june_canonical_curves.csv

  --per-country    Writes one CSV per forecasted country (AR, BR, ..., US, and
                   ROW) into csv/per_country/. These are RAW model output — no
                   headwind adjustment (the headwind is an ALL-level total that
                   cannot be meaningfully split per country) and no plus-Iran
                   composition (synthetic Iran exists only at the ALL level).
                   Each file is named *.no-headwinds.csv to make that explicit.

The two "Current (Jun 2026)" columns are forecast-only (blank before the
2026-05-26 forecast start) to match exactly what the notebook plots. The Dec-15
stakeholder marker points (low/baseline/stretch) are intentionally omitted.

Requires BigQuery access for the actuals series. Run from anywhere; paths are
anchored at the git root.

    source .venv/bin/activate && python3 data-official/2026-06/export_canonical_curves.py
    source .venv/bin/activate && python3 data-official/2026-06/export_canonical_curves.py --per-country
"""

import argparse
import glob
import json
import os
import subprocess
from collections import defaultdict

import numpy as np
import pandas as pd
from google.cloud import bigquery

# --- File paths (verbatim from the notebook's [setup] cell) ---
DESKTOP_NO_IRAN_PATH = "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-26.ld-D.raw.parquet"
DESKTOP_PLUS_IRAN_PATH = "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-26.ld-D.raw.plus_iran.parquet"
MOBILE_NO_IRAN_PATH = "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-26.gm-D.adj-m.parquet"
MOBILE_PLUS_IRAN_PATH = "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-26.gm-D.adj-m.plus_iran.parquet"
PREV_FORECAST_DESKTOP_PLUS_IRAN_PATH = "data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.ld-D.raw.plus_iran.parquet"
PREV_FORECAST_DESKTOP_NO_IRAN_PATH = "data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.ld-D.raw.parquet"
PREV_FORECAST_MOBILE_PLUS_IRAN_PATH = "data-official/2026-04/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.gm-D.raw.plus_iran.parquet"
PREV_FORECAST_MOBILE_NO_IRAN_PATH = "data-official/2026-04/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.gm-D.raw.parquet"
ADJUSTMENTS_DIR = "data-official/2026-06/adjustments"
PREV_ADJUSTMENTS_DIR = "data-official/2026-04/adjustments"

CSV_DIR = "data-official/2026-06/csv"
OUTPUT_PATH = f"{CSV_DIR}/june_canonical_curves.csv"
PER_COUNTRY_DIR = f"{CSV_DIR}/per_country"

# --- Date constants (verbatim from the notebook) ---
DISPLAY_START = pd.Timestamp("2026-01-01")
DISPLAY_END = pd.Timestamp("2026-12-31")
FORECAST_START = pd.Timestamp("2026-05-26")
PREV_FORECAST_START = pd.Timestamp("2026-04-01")
BQ_START = "2025-12-04"  # 28 days before DISPLAY_START so the MA is valid at Jan 1

# --- BigQuery actuals sources (one row per platform) ---
DESKTOP_TABLE = "moz-fx-data-shared-prod.telemetry.active_users_aggregates"
DESKTOP_APP_FILTER = 'app_name = "Firefox Desktop"'
MOBILE_TABLE = "moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates"
MOBILE_APP_FILTER = 'app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS")'


def render_adjustment(spec, date_index):
    """Convert a single adjustment spec to net DAU Series for desktop and mobile."""
    idx = pd.DatetimeIndex(date_index)
    desktop = pd.Series(0.0, index=idx)
    mobile = pd.Series(0.0, index=idx)

    if spec["type"] == "linear_ramp":
        start = pd.Timestamp(spec["start_date"])
        anchor = pd.Timestamp(spec["anchor_date"])
        total_days = (anchor - start).days
        elapsed = np.maximum(0, (idx - start).days)
        desktop[:] = spec.get("desktop_dau", 0) * elapsed / total_days
        mobile[:] = spec.get("mobile_dau", 0) * elapsed / total_days

    elif spec["type"] == "step":
        start = pd.Timestamp(spec["start_date"])
        end = pd.Timestamp(spec["end_date"]) if "end_date" in spec else idx[-1]
        mask = (idx >= start) & (idx <= end)
        desktop[mask] = spec.get("desktop_dau", 0)
        mobile[mask] = spec.get("mobile_dau", 0)

    elif spec["type"] == "daily_series":
        for date_str, values in spec["series"].items():
            date = pd.Timestamp(date_str)
            if date in idx:
                loc = idx.get_loc(date)
                desktop.iloc[loc] = values.get("desktop_dau", 0)
                mobile.iloc[loc] = values.get("mobile_dau", 0)

    return {"desktop": desktop, "mobile": mobile}


def load_adjustments(adjustments_dir, date_index):
    """Sum all adjustment components in a directory into net desktop/mobile DAU series."""
    idx = pd.DatetimeIndex(date_index)
    desktop_total = pd.Series(0.0, index=idx)
    mobile_total = pd.Series(0.0, index=idx)
    for path in sorted(glob.glob(f"{adjustments_dir}/*.json")):
        with open(path) as f:
            spec = json.load(f)
        rendered = render_adjustment(spec, idx)
        desktop_total += rendered["desktop"]
        mobile_total += rendered["mobile"]
    return {"desktop": desktop_total, "mobile": mobile_total}


def apply_net_adjustment(ma_series, net_adjustments, platform, forecast_start=FORECAST_START):
    """Apply net adjustment to a 28-day MA series, starting at forecast_start."""
    result = ma_series.copy()
    forecast_mask = result.index >= forecast_start
    adj = net_adjustments[platform].reindex(result.index, fill_value=0.0)
    result[forecast_mask] += adj[forecast_mask]
    return result


def load_segment_dau(path, data_source, segment_filter, app_filter, country="ALL"):
    """Load a forecast parquet, return one country's daily DAU sorted by date."""
    df = pd.read_parquet(path)
    mask = (
        (df["country"] == country)
        & (df["segment"] == segment_filter)
        & (df["data_source"] == data_source)
        & (df["app_name"] == app_filter)
    )
    result = df.loc[mask, ["target_date", "dau"]].copy()
    result["target_date"] = pd.to_datetime(result["target_date"])
    return result.sort_values("target_date").reset_index(drop=True)


def daily_to_28ma(dates, values):
    """Compute 28-day moving average. Returns a date-indexed Series."""
    s = pd.Series(values.values, index=pd.to_datetime(dates.values))
    return s.sort_index().rolling(28).mean()


def reconstruct_matched_daily(pre, fc, forecast_start, window=28):
    """Reconstruct the forecast's daily values to carry the recent actuals' weekly shape.

    The seam wobble (see research/ma-seam-turbulence/) is caused by an *amplitude
    discontinuity* in the weekly (day-of-week) cycle at the actuals->forecast seam: the
    forecast's weekday/weekend swing is damped relative to recent actuals, so a trailing
    28-day window straddling the seam cannot cancel the weekly cycle and the MA oscillates.

    This rebuilds the forecast daily series so its weekly amplitude *matches the recent
    actuals'*, while preserving the forecast's deseasonalized trend (level + curvature).
    Spliced before the forecast, both sides of the seam then carry the same weekly
    amplitude, the 28-day window cancels it cleanly, and the transition MA rides the true
    forecast trend instead of a straight line.

    Args:
      pre: actuals daily series (date-indexed), dates < forecast_start.
      fc:  forecast daily series (date-indexed), dates >= forecast_start.

    Returns the rebuilt forecast daily series over ``fc.index``.

    Level preservation: with the day-of-week profile normalized to mean 1, over an aligned
    28-day window (4 of each weekday) the reconstructed values average back to the trend
    (Σ trend·dow = trend·28·mean(dow) = trend·28). The mean-1 normalization is load-bearing.
    """
    # Deseasonalized forecast trend via a 7-day centered MA. center=True is safe at the
    # seam's left edge: the forecast extends to the right for months, so the [seam, seam+26]
    # transition window always has >= min_periods=4 forward points to average.
    trend_fc = fc.rolling(7, center=True, min_periods=4).mean()

    # Recent actuals' multiplicative day-of-week profile over the last 13 weeks (aligned
    # with mozaic's recency window). Mirrors the detrend+groupby(dayofweek) pattern in
    # research/ma-seam-turbulence/weekly_amplitude.py::weekly_amplitude_pct, but
    # multiplicative (ratio) rather than additive (residual).
    recent = pre[pre.index >= forecast_start - pd.Timedelta(weeks=13)]
    recent_trend = recent.rolling(7, center=True, min_periods=4).mean()
    ratio = (recent / recent_trend).replace([np.inf, -np.inf], np.nan).dropna()
    dow_act = ratio.groupby(ratio.index.dayofweek).mean()
    # Normalize present weekday buckets to mean 1, THEN fill any weekday missing from a
    # short/holiday-laden window with 1.0 (neutral). Doing it in this order keeps the mean
    # over all 7 weekdays exactly 1 (present buckets sum to n_present, fills add the rest).
    # v1 has no winsorization (noted as a limitation in the LOG).
    dow_act = dow_act / dow_act.mean()
    dow_act = dow_act.reindex(range(7)).fillna(1.0)

    return trend_fc * dow_act.reindex(fc.index.dayofweek).to_numpy()


def display_ma(dates, values, forecast_start, window=28, continuous_splice=True, slope_match=0.4):
    """28-day MA for display, with a variance-matched transition across the actuals->forecast seam.

    ``continuous_splice`` (default True) applies a cubic correction across the transition that
    matches the level and (a fraction of) the slope of the forecast-only MA at the day-(window-1)
    handoff, so the variance-matched transition lands on it with a minimal corner, while keeping
    the matched curvature. Set False to reproduce the pre-fix cliff; used only for before/after
    comparison.

    ``slope_match`` in [0, 1] (default 0.4) is the fraction of the splice slope residual the
    correction matches. Matching the FULL slope (1.0) drives the handoff 2nd-difference to ~0 but
    forces the correction to overshoot the level gap (larger deviation from the uncorrected
    curve); 0.0 matches level only (smoothstep — leaves a visible slope kink). 0.4 removes the
    kink (whole corner test under ~400 ppm on real curves — bounded by the transition's inherent
    roughness, not the handoff) at the minimal deviation that clears that target. Only the
    mobile-scale overshoot is sensitive to this; where the level gap dominates (e.g. desktop) the
    deviation is set by the gap, not this fraction.

    The naive 28dMA over the concatenated actuals+forecast daily series blends raw
    actuals into the trailing window for the first ``window-1`` forecast days. When the
    forecast's weekly-seasonality amplitude differs from the recent actuals' amplitude
    (true for high day-of-week-swing countries like AR), that partial-window blend fails
    to cancel the weekly cycle, so the MA oscillates for ~``window`` days and then settles.
    See research/ma-seam-turbulence/ for the full diagnosis.

    Returns a date-indexed Series where:
      - dates < forecast_start: plain 28dMA (actuals region; unchanged).
      - dates >= forecast_start + (window-1): the forecast-ONLY 28dMA (no actuals in the
        window). This is byte-identical to the naive blend there, since the window is
        entirely forecast — so far-horizon values (e.g. Dec-15) are unchanged. The
        reconstructed daily values are NEVER used in this region.
      - dates in [forecast_start, forecast_start + (window-1)): a *variance-matched
        transition* — the forecast's first ``window-1`` daily values are rebuilt (via
        reconstruct_matched_daily) to carry the recent actuals' weekly amplitude, then a
        trailing 28dMA over [actuals + rebuilt forecast] is taken. Because both sides of
        the seam now share the same weekly amplitude, the window cancels it and the
        transition MA rides the true forecast trend (curvature and all), smoothly. It is
        continuous with the day-28 forecast-only MA: both equal the smoothed forecast
        trend at the splice.
    """
    s = pd.Series(values.values, index=pd.to_datetime(dates.values)).sort_index()
    first_clean_date = forecast_start + pd.Timedelta(days=window - 1)

    pre = s[s.index < forecast_start]
    fc = s[s.index >= forecast_start]
    forecast_only_ma = fc.rolling(window).mean()

    # Plain blend everywhere (correct for the actuals region; overwritten in the forecast region).
    result = s.rolling(window).mean()

    # Existence guard: short series degrade to the plain blend without throwing.
    if forecast_start in s.index and first_clean_date in forecast_only_ma.index:
        matched = reconstruct_matched_daily(pre, fc, forecast_start, window)
        transition_ma = pd.concat([pre, matched]).sort_index().rolling(window).mean()

        # Continuous splice: the variance-matched transition is anchored to the actuals MA at
        # the seam, but on real data it can land slightly OFF the forecast-only MA at the
        # day-(window-1) splice, leaving a step — a curvature "corner"/cliff at the handoff.
        # Ramp an affine correction of that splice residual to zero across the transition: 0 at
        # the seam (preserve continuity with the trailing actuals MA), full residual at the
        # splice (land exactly on forecast_only_ma). This removes the corner while keeping the
        # matched transition's curvature/weekly-amplitude cancellation intact.
        transition_dates = pd.date_range(forecast_start, first_clean_date - pd.Timedelta(days=1), freq="D")
        if continuous_splice:
            # Match BOTH level and slope at the splice (C1 handoff) so the variance-matched
            # transition meets the forecast-only MA with no corner. A cubic correction
            # c(f) = a·f³ + b·f² over the transition (f: 0 at the seam → 1 at the splice) has
            # c(0)=c'(0)=0 (preserve the actuals-side continuity) and is solved so the corrected
            # transition meets forecast_only_ma in level (residual) AND slope at the splice —
            # which removes the slope kink a level-only (affine) correction leaves behind. When
            # the slope neighbours are unavailable (short series) r_slope=0, degrading to a
            # smoothstep that still zeroes the level step.
            span = window - 1
            r_level = transition_ma.loc[first_clean_date] - forecast_only_ma.loc[first_clean_date]
            prev_day = first_clean_date - pd.Timedelta(days=1)
            next_day = first_clean_date + pd.Timedelta(days=1)
            if prev_day in transition_ma.index and next_day in forecast_only_ma.index:
                transition_slope = transition_ma.loc[first_clean_date] - transition_ma.loc[prev_day]
                forecast_slope = forecast_only_ma.loc[next_day] - forecast_only_ma.loc[first_clean_date]
                r_slope = slope_match * (transition_slope - forecast_slope)
            else:
                r_slope = 0.0
            f = (transition_dates - forecast_start).days.to_numpy(dtype=float) / span
            a = r_slope * span - 2 * r_level
            b = 3 * r_level - r_slope * span
            correction = a * f**3 + b * f**2
            transition_ma.loc[transition_dates] = transition_ma.loc[transition_dates].to_numpy() - correction

        # Splice (HARD CONSTRAINT): day (window-1) onward (>= first_clean_date) is the clean
        # forecast-only MA — byte-identical to the naive blend, so Dec-15 is exact. The earlier
        # transition days use the (now splice-continuous) variance-matched transition.
        result.loc[forecast_only_ma.index] = forecast_only_ma
        transition_mask = (result.index >= forecast_start) & (result.index < first_clean_date)
        result.loc[transition_mask] = transition_ma.reindex(result.index)[transition_mask]
    return result


def fetch_actuals_28ma():
    """Query BigQuery for the four ALL-level actuals series and return their 28-day MAs."""
    client = bigquery.Client(project="moz-fx-data-bq-data-science")
    cutoff = 'CURRENT_DATE("America/Los_Angeles") - 2'
    queries = {
        "desktop_with_iran": f"""
            SELECT submission_date AS date, SUM(dau) AS dau
            FROM `{DESKTOP_TABLE}`
            WHERE {DESKTOP_APP_FILTER}
              AND submission_date BETWEEN '{BQ_START}' AND {cutoff}
            GROUP BY submission_date ORDER BY 1
        """,
        "desktop_no_iran": f"""
            SELECT submission_date AS date, SUM(dau) AS dau
            FROM `{DESKTOP_TABLE}`
            WHERE {DESKTOP_APP_FILTER}
              AND submission_date BETWEEN '{BQ_START}' AND {cutoff}
              AND country != 'IR'
            GROUP BY submission_date ORDER BY 1
        """,
        "mobile_with_iran": f"""
            SELECT submission_date AS date, SUM(dau) AS dau
            FROM `{MOBILE_TABLE}`
            WHERE {MOBILE_APP_FILTER}
              AND submission_date BETWEEN '{BQ_START}' AND {cutoff}
            GROUP BY submission_date ORDER BY 1
        """,
        "mobile_no_iran": f"""
            SELECT submission_date AS date, SUM(dau) AS dau
            FROM `{MOBILE_TABLE}`
            WHERE {MOBILE_APP_FILTER}
              AND submission_date BETWEEN '{BQ_START}' AND {cutoff}
              AND country != 'IR'
            GROUP BY submission_date ORDER BY 1
        """,
    }
    out = {}
    for name, sql in queries.items():
        df = client.query(sql).to_dataframe()
        df["date"] = pd.to_datetime(df["date"])
        out[name] = daily_to_28ma(df["date"], df["dau"])
    return out


def fetch_actuals_per_country_28ma(named_countries):
    """Query BigQuery for per-country actuals (28-day MAs), bucketing every country
    outside `named_countries` into 'ROW' and excluding IR — matching how the no-Iran
    forecast partitions countries. Returns {country: {"desktop": ma, "mobile": ma}}.
    """
    client = bigquery.Client(project="moz-fx-data-bq-data-science")
    cutoff = 'CURRENT_DATE("America/Los_Angeles") - 2'
    named_sql = ", ".join(f"'{c}'" for c in named_countries)
    platform_sources = {
        "desktop": (DESKTOP_TABLE, DESKTOP_APP_FILTER),
        "mobile": (MOBILE_TABLE, MOBILE_APP_FILTER),
    }
    result = defaultdict(dict)
    for platform, (table, app_filter) in platform_sources.items():
        sql = f"""
            SELECT submission_date AS date,
                   CASE WHEN country IN ({named_sql}) THEN country ELSE 'ROW' END AS bucket,
                   SUM(dau) AS dau
            FROM `{table}`
            WHERE {app_filter}
              AND submission_date BETWEEN '{BQ_START}' AND {cutoff}
              AND country != 'IR'
            GROUP BY date, bucket ORDER BY date
        """
        df = client.query(sql).to_dataframe()
        df["date"] = pd.to_datetime(df["date"])
        for bucket, group in df.groupby("bucket"):
            result[bucket][platform] = daily_to_28ma(group["date"], group["dau"])
    return result


def make_window_helpers(date_index):
    """Return (on_window, forecast_only) reindexers onto the display window."""

    def on_window(series):
        """Reindex a date-indexed series onto the display window."""
        return series.reindex(date_index)

    def forecast_only(series):
        """Reindex onto the window but blank everything before the forecast start
        (matches the notebook's clip_forecast_only for the current-forecast curves)."""
        clipped = series.where(series.index >= FORECAST_START)
        return clipped.reindex(date_index)

    return on_window, forecast_only


def round_dau_columns(frame):
    """Round every non-date column to whole users; keep blanks blank."""
    for col in frame.columns:
        if col != "date":
            frame[col] = frame[col].round(0)
    return frame


def build_all_curves():
    """Build the ALL-level canonical curves CSV (headwind applied, plus-Iran columns)."""
    # Raw forecast 28d MAs
    desktop_no_iran = load_segment_dau(DESKTOP_NO_IRAN_PATH, "legacy_desktop", '{"os": "ALL"}', "desktop")
    desktop_plus_iran = load_segment_dau(DESKTOP_PLUS_IRAN_PATH, "legacy_desktop", '{"os": "ALL"}', "desktop")
    mobile_no_iran = load_segment_dau(MOBILE_NO_IRAN_PATH, "glean_mobile", "{}", "ALL MOBILE")
    mobile_plus_iran = load_segment_dau(MOBILE_PLUS_IRAN_PATH, "glean_mobile", "{}", "ALL MOBILE")

    prev_desktop_plus_iran = load_segment_dau(PREV_FORECAST_DESKTOP_PLUS_IRAN_PATH, "legacy_desktop", '{"os": "ALL"}', "desktop")
    prev_desktop_no_iran = load_segment_dau(PREV_FORECAST_DESKTOP_NO_IRAN_PATH, "legacy_desktop", '{"os": "ALL"}', "desktop")
    prev_mobile_plus_iran = load_segment_dau(PREV_FORECAST_MOBILE_PLUS_IRAN_PATH, "glean_mobile", "{}", "ALL MOBILE")
    prev_mobile_no_iran = load_segment_dau(PREV_FORECAST_MOBILE_NO_IRAN_PATH, "glean_mobile", "{}", "ALL MOBILE")

    desktop_no_iran_ma_raw = display_ma(desktop_no_iran["target_date"], desktop_no_iran["dau"], FORECAST_START)
    desktop_plus_iran_ma_raw = display_ma(desktop_plus_iran["target_date"], desktop_plus_iran["dau"], FORECAST_START)
    mobile_no_iran_ma_raw = display_ma(mobile_no_iran["target_date"], mobile_no_iran["dau"], FORECAST_START)
    mobile_plus_iran_ma_raw = display_ma(mobile_plus_iran["target_date"], mobile_plus_iran["dau"], FORECAST_START)

    # Adjustments (headwind): June set on current, April set on prior.
    net_adjustments = load_adjustments(ADJUSTMENTS_DIR, desktop_no_iran_ma_raw.index)
    prev_net_adjustments = load_adjustments(PREV_ADJUSTMENTS_DIR, desktop_no_iran_ma_raw.index)

    desktop_no_iran_ma = apply_net_adjustment(desktop_no_iran_ma_raw, net_adjustments, "desktop")
    desktop_plus_iran_ma = apply_net_adjustment(desktop_plus_iran_ma_raw, net_adjustments, "desktop")
    mobile_no_iran_ma = apply_net_adjustment(mobile_no_iran_ma_raw, net_adjustments, "mobile")
    mobile_plus_iran_ma = apply_net_adjustment(mobile_plus_iran_ma_raw, net_adjustments, "mobile")

    prev_desktop_plus_iran_ma = apply_net_adjustment(
        display_ma(prev_desktop_plus_iran["target_date"], prev_desktop_plus_iran["dau"], PREV_FORECAST_START),
        prev_net_adjustments, "desktop", forecast_start=PREV_FORECAST_START)
    prev_desktop_no_iran_ma = apply_net_adjustment(
        display_ma(prev_desktop_no_iran["target_date"], prev_desktop_no_iran["dau"], PREV_FORECAST_START),
        prev_net_adjustments, "desktop", forecast_start=PREV_FORECAST_START)
    prev_mobile_plus_iran_ma = apply_net_adjustment(
        display_ma(prev_mobile_plus_iran["target_date"], prev_mobile_plus_iran["dau"], PREV_FORECAST_START),
        prev_net_adjustments, "mobile", forecast_start=PREV_FORECAST_START)
    prev_mobile_no_iran_ma = apply_net_adjustment(
        display_ma(prev_mobile_no_iran["target_date"], prev_mobile_no_iran["dau"], PREV_FORECAST_START),
        prev_net_adjustments, "mobile", forecast_start=PREV_FORECAST_START)

    actuals = fetch_actuals_28ma()

    date_index = pd.date_range(DISPLAY_START, DISPLAY_END, freq="D")
    on_window, forecast_only = make_window_helpers(date_index)

    frame = pd.DataFrame({
        "date": date_index.strftime("%Y-%m-%d"),
        # --- Desktop ---
        "desktop_actuals_all_countries": on_window(actuals["desktop_with_iran"]).values,
        "desktop_actuals_excl_ir": on_window(actuals["desktop_no_iran"]).values,
        "desktop_prior_april_plus_iran": on_window(prev_desktop_plus_iran_ma).values,
        "desktop_prior_april_no_iran": on_window(prev_desktop_no_iran_ma).values,
        "desktop_current_june_plus_iran": forecast_only(desktop_plus_iran_ma).values,
        "desktop_current_june_no_iran": forecast_only(desktop_no_iran_ma).values,
        # --- Mobile ---
        "mobile_actuals_all_countries": on_window(actuals["mobile_with_iran"]).values,
        "mobile_actuals_excl_ir": on_window(actuals["mobile_no_iran"]).values,
        "mobile_prior_april_plus_iran": on_window(prev_mobile_plus_iran_ma).values,
        "mobile_prior_april_no_iran": on_window(prev_mobile_no_iran_ma).values,
        "mobile_current_june_plus_iran": forecast_only(mobile_plus_iran_ma).values,
        "mobile_current_june_no_iran": forecast_only(mobile_no_iran_ma).values,
    })
    frame = round_dau_columns(frame)

    os.makedirs(CSV_DIR, exist_ok=True)
    frame.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {OUTPUT_PATH}: {len(frame)} rows x {len(frame.columns)} cols")
    print(f"Date range: {frame['date'].iloc[0]} .. {frame['date'].iloc[-1]}")
    print("\nNon-null counts per column:")
    print(frame.drop(columns="date").notna().sum().to_string())


def build_per_country_curves():
    """Build one raw (no-headwind) curves CSV per forecasted country into PER_COUNTRY_DIR."""
    # Forecasted countries come straight from the parquet (ALL is the aggregate, excluded).
    countries = sorted(c for c in pd.read_parquet(DESKTOP_NO_IRAN_PATH)["country"].unique() if c != "ALL")
    named_countries = [c for c in countries if c != "ROW"]

    actuals = fetch_actuals_per_country_28ma(named_countries)

    date_index = pd.date_range(DISPLAY_START, DISPLAY_END, freq="D")
    on_window, forecast_only = make_window_helpers(date_index)

    os.makedirs(PER_COUNTRY_DIR, exist_ok=True)
    written = []
    for country in countries:
        # Current (June) and prior (April) forecasts — raw, no headwind, no plus-Iran.
        desktop_june = load_segment_dau(DESKTOP_NO_IRAN_PATH, "legacy_desktop", '{"os": "ALL"}', "desktop", country=country)
        mobile_june = load_segment_dau(MOBILE_NO_IRAN_PATH, "glean_mobile", "{}", "ALL MOBILE", country=country)
        desktop_april = load_segment_dau(PREV_FORECAST_DESKTOP_NO_IRAN_PATH, "legacy_desktop", '{"os": "ALL"}', "desktop", country=country)
        mobile_april = load_segment_dau(PREV_FORECAST_MOBILE_NO_IRAN_PATH, "glean_mobile", "{}", "ALL MOBILE", country=country)

        desktop_june_ma = display_ma(desktop_june["target_date"], desktop_june["dau"], FORECAST_START)
        mobile_june_ma = display_ma(mobile_june["target_date"], mobile_june["dau"], FORECAST_START)
        desktop_april_ma = display_ma(desktop_april["target_date"], desktop_april["dau"], PREV_FORECAST_START)
        mobile_april_ma = display_ma(mobile_april["target_date"], mobile_april["dau"], PREV_FORECAST_START)

        country_actuals = actuals.get(country, {})

        frame = pd.DataFrame({
            "date": date_index.strftime("%Y-%m-%d"),
            # --- Desktop ---
            "desktop_actuals": on_window(country_actuals.get("desktop")).values,
            "desktop_prior_april": on_window(desktop_april_ma).values,
            "desktop_current_june": forecast_only(desktop_june_ma).values,
            # --- Mobile ---
            "mobile_actuals": on_window(country_actuals.get("mobile")).values,
            "mobile_prior_april": on_window(mobile_april_ma).values,
            "mobile_current_june": forecast_only(mobile_june_ma).values,
        })
        frame = round_dau_columns(frame)

        path = f"{PER_COUNTRY_DIR}/june_canonical_curves.{country}.no-headwinds.csv"
        frame.to_csv(path, index=False)
        written.append((country, path, frame))

    print(f"Wrote {len(written)} per-country CSVs to {PER_COUNTRY_DIR}/")
    print(f"Countries: {', '.join(c for c, _, _ in written)}")
    print(f"Each file: {len(date_index)} rows x 7 cols, range {date_index[0].date()} .. {date_index[-1].date()}")
    print("\nForecast-only non-null counts (desktop_current_june) per country:")
    for country, _, frame in written:
        print(f"  {country:4s} {int(frame['desktop_current_june'].notna().sum()):>4d}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--per-country",
        action="store_true",
        help="Export one raw (no-headwind) CSV per forecasted country into csv/per_country/ "
             "instead of the single ALL-level canonical CSV.",
    )
    args = parser.parse_args()

    git_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    ).stdout.strip()
    os.chdir(git_root)

    if args.per_country:
        build_per_country_curves()
    else:
        build_all_curves()


if __name__ == "__main__":
    main()
