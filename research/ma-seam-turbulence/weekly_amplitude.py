"""Clinching measurement for the seam-turbulence root cause.

Hypothesis: the 28-day MA (=4 weeks) cancels a *stationary* weekly cycle exactly, so
at steady state every country's forecast MA is smooth. The early-horizon wobble appears
only while the MA window straddles the actuals->forecast seam, and only when the
forecast's weekly-seasonality amplitude differs from the recent actuals' weekly
amplitude (an amplitude discontinuity the partial-week MA cannot cancel).

This script measures, per country: the weekly peak-to-trough amplitude (% of mean) of
the last 90 days of actuals vs the first 90 days of forecast, and the mismatch between
them. If the mismatch correlates with the blend-MA turbulence (and US/CA ~ no mismatch),
the mechanism is confirmed. Also dumps the seam daily values to explain the seam-step %.

    source .venv/bin/activate && python3 research/ma-seam-turbulence/weekly_amplitude.py
"""

import os
import subprocess

import numpy as np
import pandas as pd

PARQUET = (
    "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/"
    "mozaic_daily_forecast.2026-05-26.ld-D.raw.parquet"
)
FORECAST_START = pd.Timestamp("2026-05-26")
WINDOW_DAYS = 90


def load_country_daily(df, country):
    mask = (
        (df["country"] == country)
        & (df["segment"] == '{"os": "ALL"}')
        & (df["data_source"] == "legacy_desktop")
        & (df["app_name"] == "desktop")
    )
    sub = df.loc[mask, ["target_date", "dau", "data_type"]].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    return sub.sort_values("target_date").set_index("target_date")


def weekly_amplitude_pct(daily_series):
    """Weekly peak-to-trough amplitude as % of mean: detrend by 7d centered MA, then
    average the day-of-week residual, take (max-min)/mean over the 7 weekday means."""
    s = daily_series.dropna()
    if len(s) < 21:
        return np.nan
    trend = s.rolling(7, center=True, min_periods=4).mean()
    resid = s - trend
    dow_mean = resid.groupby(resid.index.dayofweek).mean()
    return 100 * (dow_mean.max() - dow_mean.min()) / s.mean()


def main():
    git_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    ).stdout.strip()
    os.chdir(git_root)
    df = pd.read_parquet(PARQUET)

    countries = sorted(c for c in df["country"].unique() if c != "ALL")
    rows = []
    for country in countries:
        daily = load_country_daily(df, country)
        actuals = daily.loc[daily["data_type"] == "training", "dau"]
        forecast = daily.loc[daily["data_type"] == "forecast", "dau"]
        recent_actuals = actuals[actuals.index >= FORECAST_START - pd.Timedelta(days=WINDOW_DAYS)]
        early_forecast = forecast[forecast.index < FORECAST_START + pd.Timedelta(days=WINDOW_DAYS)]
        amp_act = weekly_amplitude_pct(recent_actuals)
        amp_fc = weekly_amplitude_pct(early_forecast)
        rows.append({
            "country": country,
            "weekly_amp_actuals_pct": amp_act,
            "weekly_amp_forecast_pct": amp_fc,
            "amp_ratio_fc_over_act": amp_fc / amp_act if amp_act else np.nan,
            "amp_mismatch_pct_pts": amp_fc - amp_act,
        })
    table = pd.DataFrame(rows).sort_values("amp_mismatch_pct_pts", ascending=False)
    with pd.option_context("display.float_format", lambda v: f"{v:,.2f}"):
        print("=== Weekly seasonality amplitude: recent actuals vs early forecast ===")
        print(table.to_string(index=False))
    table.to_csv("research/ma-seam-turbulence/plots/weekly_amplitude.csv", index=False)

    # Explain the seam-step metric: dump last training + first forecast daily values.
    print("\n=== Seam daily values (last 5 training, first 5 forecast) ===")
    for country in ["AR", "US"]:
        daily = load_country_daily(df, country)
        tail = daily[daily["data_type"] == "training"].tail(5)
        head = daily[daily["data_type"] == "forecast"].head(5)
        print(f"\n{country} last training days:")
        for d, r in tail.iterrows():
            print(f"  {d.date()} ({d.day_name()[:3]})  dau={r['dau']:,.0f}")
        print(f"{country} first forecast days:")
        for d, r in head.iterrows():
            print(f"  {d.date()} ({d.day_name()[:3]})  dau={r['dau']:,.0f}")


if __name__ == "__main__":
    main()
