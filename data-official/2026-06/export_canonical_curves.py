"""Export the curves from june_canonical_v2026-05-27.ipynb to a single CSV.

Reproduces the 12 plotted series (6 desktop + 6 mobile) over the display window
2026-01-01 .. 2026-12-31 as 28-day moving averages, and writes one tidy CSV:

    date, <6 desktop columns>, <6 mobile columns>

The two "Current (Jun 2026)" columns are forecast-only (blank before the
2026-05-26 forecast start) to match exactly what the notebook plots. The Dec-15
stakeholder marker points (low/baseline/stretch) are intentionally omitted.

Requires BigQuery access for the actuals series (same queries as the notebook).
Run from anywhere; paths are anchored at the git root.

    source .venv/bin/activate && python3 data-official/2026-06/export_canonical_curves.py
"""

import glob
import json
import os
import subprocess

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

OUTPUT_PATH = "data-official/2026-06/june_canonical_curves.csv"

# --- Date constants (verbatim from the notebook) ---
DISPLAY_START = pd.Timestamp("2026-01-01")
DISPLAY_END = pd.Timestamp("2026-12-31")
FORECAST_START = pd.Timestamp("2026-05-26")
PREV_FORECAST_START = pd.Timestamp("2026-04-01")
BQ_START = "2025-12-04"  # 28 days before DISPLAY_START so the MA is valid at Jan 1


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


def load_all_level_dau(path, data_source, segment_filter, app_filter):
    """Load a forecast parquet, return ALL-level daily DAU sorted by date."""
    df = pd.read_parquet(path)
    mask = (
        (df["country"] == "ALL")
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


def fetch_actuals_28ma():
    """Query BigQuery for the four actuals series and return their 28-day MAs."""
    client = bigquery.Client(project="moz-fx-data-bq-data-science")
    cutoff = 'CURRENT_DATE("America/Los_Angeles") - 2'
    queries = {
        "desktop_with_iran": f"""
            SELECT submission_date AS date, SUM(dau) AS dau
            FROM `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
            WHERE app_name = "Firefox Desktop"
              AND submission_date BETWEEN '{BQ_START}' AND {cutoff}
            GROUP BY submission_date ORDER BY 1
        """,
        "desktop_no_iran": f"""
            SELECT submission_date AS date, SUM(dau) AS dau
            FROM `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
            WHERE app_name = "Firefox Desktop"
              AND submission_date BETWEEN '{BQ_START}' AND {cutoff}
              AND country != 'IR'
            GROUP BY submission_date ORDER BY 1
        """,
        "mobile_with_iran": f"""
            SELECT submission_date AS date, SUM(dau) AS dau
            FROM `moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates`
            WHERE app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS")
              AND submission_date BETWEEN '{BQ_START}' AND {cutoff}
            GROUP BY submission_date ORDER BY 1
        """,
        "mobile_no_iran": f"""
            SELECT submission_date AS date, SUM(dau) AS dau
            FROM `moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates`
            WHERE app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS")
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


def main():
    git_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    ).stdout.strip()
    os.chdir(git_root)

    # Raw forecast 28d MAs
    desktop_no_iran = load_all_level_dau(DESKTOP_NO_IRAN_PATH, "legacy_desktop", '{"os": "ALL"}', "desktop")
    desktop_plus_iran = load_all_level_dau(DESKTOP_PLUS_IRAN_PATH, "legacy_desktop", '{"os": "ALL"}', "desktop")
    mobile_no_iran = load_all_level_dau(MOBILE_NO_IRAN_PATH, "glean_mobile", "{}", "ALL MOBILE")
    mobile_plus_iran = load_all_level_dau(MOBILE_PLUS_IRAN_PATH, "glean_mobile", "{}", "ALL MOBILE")

    prev_desktop_plus_iran = load_all_level_dau(PREV_FORECAST_DESKTOP_PLUS_IRAN_PATH, "legacy_desktop", '{"os": "ALL"}', "desktop")
    prev_desktop_no_iran = load_all_level_dau(PREV_FORECAST_DESKTOP_NO_IRAN_PATH, "legacy_desktop", '{"os": "ALL"}', "desktop")
    prev_mobile_plus_iran = load_all_level_dau(PREV_FORECAST_MOBILE_PLUS_IRAN_PATH, "glean_mobile", "{}", "ALL MOBILE")
    prev_mobile_no_iran = load_all_level_dau(PREV_FORECAST_MOBILE_NO_IRAN_PATH, "glean_mobile", "{}", "ALL MOBILE")

    desktop_no_iran_ma_raw = daily_to_28ma(desktop_no_iran["target_date"], desktop_no_iran["dau"])
    desktop_plus_iran_ma_raw = daily_to_28ma(desktop_plus_iran["target_date"], desktop_plus_iran["dau"])
    mobile_no_iran_ma_raw = daily_to_28ma(mobile_no_iran["target_date"], mobile_no_iran["dau"])
    mobile_plus_iran_ma_raw = daily_to_28ma(mobile_plus_iran["target_date"], mobile_plus_iran["dau"])

    # Adjustments (headwind): June set on current, April set on prior.
    net_adjustments = load_adjustments(ADJUSTMENTS_DIR, desktop_no_iran_ma_raw.index)
    prev_net_adjustments = load_adjustments(PREV_ADJUSTMENTS_DIR, desktop_no_iran_ma_raw.index)

    desktop_no_iran_ma = apply_net_adjustment(desktop_no_iran_ma_raw, net_adjustments, "desktop")
    desktop_plus_iran_ma = apply_net_adjustment(desktop_plus_iran_ma_raw, net_adjustments, "desktop")
    mobile_no_iran_ma = apply_net_adjustment(mobile_no_iran_ma_raw, net_adjustments, "mobile")
    mobile_plus_iran_ma = apply_net_adjustment(mobile_plus_iran_ma_raw, net_adjustments, "mobile")

    prev_desktop_plus_iran_ma = apply_net_adjustment(
        daily_to_28ma(prev_desktop_plus_iran["target_date"], prev_desktop_plus_iran["dau"]),
        prev_net_adjustments, "desktop", forecast_start=PREV_FORECAST_START)
    prev_desktop_no_iran_ma = apply_net_adjustment(
        daily_to_28ma(prev_desktop_no_iran["target_date"], prev_desktop_no_iran["dau"]),
        prev_net_adjustments, "desktop", forecast_start=PREV_FORECAST_START)
    prev_mobile_plus_iran_ma = apply_net_adjustment(
        daily_to_28ma(prev_mobile_plus_iran["target_date"], prev_mobile_plus_iran["dau"]),
        prev_net_adjustments, "mobile", forecast_start=PREV_FORECAST_START)
    prev_mobile_no_iran_ma = apply_net_adjustment(
        daily_to_28ma(prev_mobile_no_iran["target_date"], prev_mobile_no_iran["dau"]),
        prev_net_adjustments, "mobile", forecast_start=PREV_FORECAST_START)

    actuals = fetch_actuals_28ma()

    # Daily date index over the full display window.
    date_index = pd.date_range(DISPLAY_START, DISPLAY_END, freq="D")

    def on_window(series):
        """Reindex a date-indexed series onto the display window."""
        return series.reindex(date_index)

    def forecast_only(series):
        """Reindex onto the window but blank everything before the forecast start
        (matches the notebook's clip_forecast_only for the current-forecast curves)."""
        clipped = series.where(series.index >= FORECAST_START)
        return clipped.reindex(date_index)

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

    # Round DAU values to whole users; keep blanks blank.
    for col in frame.columns:
        if col != "date":
            frame[col] = frame[col].round(0)

    frame.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {OUTPUT_PATH}: {len(frame)} rows x {len(frame.columns)} cols")
    print(f"Date range: {frame['date'].iloc[0]} .. {frame['date'].iloc[-1]}")
    print("\nNon-null counts per column:")
    print(frame.drop(columns="date").notna().sum().to_string())


if __name__ == "__main__":
    main()
