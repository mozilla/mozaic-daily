"""Phase-1 diagnostic for the per-country 28dMA early-horizon turbulence.

Reads ONLY the canonical desktop forecast parquet (daily per-country `dau`, with
`data_type` in {training, forecast}). No model re-run, no BigQuery.

Goal: distinguish
  (a) MA-seam artifact  — the daily forecast median is smooth; the wobble lives only
      in the 28-day MA window while it straddles the raw-actuals -> forecast seam.
  (b) real daily transient — the daily forecast median itself is turbulent early on,
      so the forecast-only MA (no actuals in the window) also wobbles.

Outputs decisive AR (turbulent) vs US (control) figures and a per-country metrics
table under research/ma-seam-turbulence/plots/.

    source .venv/bin/activate && python3 research/ma-seam-turbulence/diagnose_seam.py
"""

import os
import subprocess

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PARQUET = (
    "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/"
    "mozaic_daily_forecast.2026-05-26.ld-D.raw.parquet"
)
OUT_DIR = "research/ma-seam-turbulence/plots"
FORECAST_START = pd.Timestamp("2026-05-26")
ZOOM_START = pd.Timestamp("2026-04-15")
ZOOM_END = pd.Timestamp("2026-08-01")
MA_WINDOW = 28
EARLY_DAYS = 28  # forecast weeks 0-3, the window-straddles-seam zone


def load_country_daily(df, country):
    """Return a date-indexed frame with daily dau and data_type for one desktop country."""
    mask = (
        (df["country"] == country)
        & (df["segment"] == '{"os": "ALL"}')
        & (df["data_source"] == "legacy_desktop")
        & (df["app_name"] == "desktop")
    )
    sub = df.loc[mask, ["target_date", "dau", "data_type"]].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    sub = sub.sort_values("target_date").set_index("target_date")
    return sub


def ma_blend(daily):
    """28d MA over the full actuals+forecast daily series (current export behavior)."""
    return daily["dau"].rolling(MA_WINDOW).mean()


def ma_forecast_only(daily):
    """28d MA over forecast rows only — no actuals ever enter the window.

    First valid point lands at FORECAST_START + (MA_WINDOW-1) days.
    """
    fc = daily.loc[daily["data_type"] == "forecast", "dau"]
    return fc.rolling(MA_WINDOW).mean()


def rel_2nd_diff_ppm(series, start, end):
    """Mean |2nd difference| over [start, end], normalized by mean level, in ppm.

    A smooth curve -> near 0; a wobbly curve -> large. Scale-free so AR and US compare.
    """
    window = series[(series.index >= start) & (series.index <= end)].dropna()
    if len(window) < 3:
        return np.nan
    second_diff = window.diff().diff().abs()
    return 1e6 * second_diff.mean() / window.mean()


def plot_decisive(daily, country, path):
    """One figure: daily actuals tail, daily forecast median, MA-blend, MA forecast-only."""
    fig, ax = plt.subplots(figsize=(15, 7))

    actuals = daily.loc[daily["data_type"] == "training", "dau"]
    forecast = daily.loc[daily["data_type"] == "forecast", "dau"]
    blend = ma_blend(daily)
    fc_only = ma_forecast_only(daily)

    # Daily series (thin, to show what the MA is built from).
    ax.plot(actuals.index, actuals.values, color="black", lw=0.8, alpha=0.6,
            label="Daily actuals (raw)")
    ax.plot(forecast.index, forecast.values, color="orange", lw=0.8, alpha=0.7,
            label="Daily forecast median")
    # Moving averages (bold, the thing we actually plot for stakeholders).
    ax.plot(blend.index, blend.values, color="crimson", lw=2.2,
            label="28dMA — blend (current behavior)")
    ax.plot(fc_only.index, fc_only.values, color="green", lw=2.2, ls="--",
            label="28dMA — forecast-only (no actuals in window)")

    ax.axvline(FORECAST_START, color="blue", ls=":", alpha=0.7, label="Forecast start")
    ax.axvline(FORECAST_START + pd.Timedelta(days=MA_WINDOW - 1), color="gray", ls=":",
               alpha=0.7, label="Seam clears window (start+27d)")

    blend_ppm = rel_2nd_diff_ppm(blend, FORECAST_START, FORECAST_START + pd.Timedelta(days=EARLY_DAYS))
    fc_ppm = rel_2nd_diff_ppm(fc_only, FORECAST_START, FORECAST_START + pd.Timedelta(days=90))
    ax.set_title(
        f"{country} desktop — seam diagnostic  "
        f"(blend wk0-3 |2nd diff| = {blend_ppm:,.0f} ppm,  "
        f"forecast-only = {fc_ppm:,.0f} ppm)",
        fontsize=13,
    )
    ax.set_xlim(ZOOM_START, ZOOM_END)
    # y-limit to the zoomed window so the wobble is visible, not the full-year range.
    zoom_vals = pd.concat([
        blend[(blend.index >= ZOOM_START) & (blend.index <= ZOOM_END)],
        fc_only[(fc_only.index >= ZOOM_START) & (fc_only.index <= ZOOM_END)],
    ]).dropna()
    if not zoom_vals.empty:
        pad = (zoom_vals.max() - zoom_vals.min()) * 0.15 + 1
        ax.set_ylim(zoom_vals.min() - pad, zoom_vals.max() + pad)
    ax.set_ylabel("DAU")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=9)
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)
    return blend_ppm, fc_ppm


def per_country_table(df):
    """Tabulate seam metrics for every forecasted country."""
    countries = sorted(c for c in df["country"].unique() if c != "ALL")
    rows = []
    for country in countries:
        daily = load_country_daily(df, country)
        actuals = daily.loc[daily["data_type"] == "training", "dau"]
        forecast = daily.loc[daily["data_type"] == "forecast", "dau"]
        recent_actuals = actuals[actuals.index >= FORECAST_START - pd.Timedelta(days=90)]
        last_actual = actuals.iloc[-1]
        first_forecast = forecast.iloc[0]
        blend = ma_blend(daily)
        fc_only = ma_forecast_only(daily)
        blend_ppm = rel_2nd_diff_ppm(blend, FORECAST_START, FORECAST_START + pd.Timedelta(days=EARLY_DAYS))
        fc_ppm = rel_2nd_diff_ppm(fc_only, FORECAST_START, FORECAST_START + pd.Timedelta(days=90))
        rows.append({
            "country": country,
            "recent_actuals_cv_pct": 100 * recent_actuals.std() / recent_actuals.mean(),
            "seam_step_pct": 100 * (first_forecast - last_actual) / last_actual,
            "blend_wk0_3_ppm": blend_ppm,
            "forecast_only_ppm": fc_ppm,
            "ratio_blend_over_fconly": blend_ppm / fc_ppm if fc_ppm else np.nan,
        })
    return pd.DataFrame(rows).sort_values("blend_wk0_3_ppm", ascending=False)


def main():
    git_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    ).stdout.strip()
    os.chdir(git_root)
    os.makedirs(OUT_DIR, exist_ok=True)

    df = pd.read_parquet(PARQUET)

    print("=== Decisive AR vs US figures ===")
    for country in ["AR", "US"]:
        daily = load_country_daily(df, country)
        path = f"{OUT_DIR}/decisive_{country}.png"
        blend_ppm, fc_ppm = plot_decisive(daily, country, path)
        print(f"{country}: blend wk0-3 = {blend_ppm:,.0f} ppm | "
              f"forecast-only = {fc_ppm:,.0f} ppm | ratio = {blend_ppm / fc_ppm:.1f}x  -> {path}")

    print("\n=== Per-country seam metrics (sorted by blend wk0-3 turbulence) ===")
    table = per_country_table(df)
    table_path = f"{OUT_DIR}/per_country_metrics.csv"
    table.to_csv(table_path, index=False)
    with pd.option_context("display.float_format", lambda v: f"{v:,.1f}"):
        print(table.to_string(index=False))
    print(f"\nWrote {table_path}")

    # Headline read for the log: does blend turbulence track forecast-only (model) or not?
    median_ratio = table["ratio_blend_over_fconly"].median()
    print(f"\nMedian ratio (blend wk0-3 / forecast-only): {median_ratio:.1f}x")
    print("Interpretation: ratio >> 1 across countries => wobble is in the MA-seam blend,")
    print("not the daily forecast (supports hypothesis (a)). ratio ~ 1 => real daily transient (b).")


if __name__ == "__main__":
    main()
