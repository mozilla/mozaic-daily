"""Desktop DAU: actual-vs-forecast disconnect + regional "pancake" decomposition.

Forecast = the canonical **June 2026** cycle forecast (forecast_start_date
2026-05-26), read from data-official/2026-06/csv/june_canonical_curves.csv
(desktop ALL-level, 28d-MA, headwind applied, synthetic Iran included). Because
that forecast starts 2026-05-26 it overlaps a month of actuals, so the
actual-vs-forecast divergence is visible in-sample (not just as a forward stub).

Actuals = fresh legacy_desktop (all countries, incl. Iran) pulled from the mart
into region_daily.csv; they match the canonical CSV's actuals to the dollar over
their overlap, so the +iran forecast and these actuals are directly comparable.

Top panel : the disconnect (zoomed) — 2026 actuals, June forecast 2026-05-26 →
            year-end, April prior forecast, and the 2025 calendar-aligned actuals.
Bottom    : regional stack (CN / US / EU / ROW) of actual DAU + the June forecast
            total, with a Jan-1 → now rise attribution.

Inputs : region_daily.csv, prior_year_all.csv,
         ../../data-official/2026-06/csv/june_canonical_curves.csv
Outputs: pancake_desktop_dau.png, region_ma28.csv
"""
from __future__ import annotations

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from pathlib import Path

HERE = Path(__file__).resolve().parent
JUNE_CURVES = HERE / "../../data-official/2026-06/csv/june_canonical_curves.csv"

MA_WINDOW = 28
PLOT_START = pd.Timestamp("2026-01-01")
ACTUALS_END = pd.Timestamp("2026-06-27")    # last complete training day in the pull
FORECAST_START = pd.Timestamp("2026-05-26")  # June canonical forecast_start_date
FORECAST_END = pd.Timestamp("2026-12-31")
REGIONS = ["CN", "US", "EU", "ROW"]
REGION_LABELS = {
    "CN": "China (CN)",
    "US": "United States (US)",
    "EU": "EU (DE+FR+IT+PL)",
    "ROW": "Rest of World",
}
REGION_COLORS = {"CN": "#d6322e", "US": "#2b6cb0", "EU": "#2f9e44", "ROW": "#9aa0a6"}


def load_actual_region_ma() -> pd.DataFrame:
    """Regional actual 28d-MA over the plot window (CN/US/EU/ROW)."""
    df = pd.read_csv(HERE / "region_daily.csv", parse_dates=["target_date"])
    daily = df[df.data_type == "training"].set_index("target_date")[REGIONS].sort_index()
    ma = daily.rolling(MA_WINDOW).mean()
    return ma.loc[(ma.index >= PLOT_START) & (ma.index <= ACTUALS_END)]


def load_june_forecast() -> tuple[pd.Series, pd.Series]:
    """(June canonical, April prior) desktop forecast 28d-MA, +iran, from the CSV."""
    df = pd.read_csv(JUNE_CURVES, parse_dates=["date"]).set_index("date")
    june = df["desktop_current_june_plus_iran"].dropna()
    april = df["desktop_prior_april_plus_iran"].dropna()
    clip = lambda s: s.loc[(s.index >= PLOT_START) & (s.index <= FORECAST_END)]
    return clip(june), clip(april)


def load_prior_year_ma() -> pd.Series:
    """2025 total actual 28d-MA, calendar-shifted +1yr to align with the 2026 axis."""
    py = pd.read_csv(HERE / "prior_year_all.csv", parse_dates=["target_date"]).sort_values("target_date")
    ma = py.set_index("target_date")["ALL"].rolling(MA_WINDOW).mean()
    ma.index = ma.index + pd.DateOffset(years=1)
    return ma.loc[(ma.index >= PLOT_START) & (ma.index <= FORECAST_END)]


def millions(value: float, _pos) -> str:
    return f"{value / 1e6:.1f}M"


def _format_time_axis(ax) -> None:
    ax.set_xlim(PLOT_START, FORECAST_END)
    ax.yaxis.set_major_formatter(FuncFormatter(millions))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.grid(axis="y", alpha=0.25)


def _draw_disconnect(ax, total_actual, june_fc, april_fc, prior_year_ma) -> None:
    """Top panel: the actual-vs-forecast divergence, zoomed like the dashboard."""
    ax.plot(prior_year_ma.index, prior_year_ma.values, color="#9aa0a6", lw=2.0,
            label="2025 actuals (calendar-aligned)")
    ax.plot(april_fc.index, april_fc.values, color="#b197fc", lw=1.8, ls=":",
            label="April forecast (prior cycle)")
    ax.plot(total_actual.index, total_actual.values, color="#e8590c", lw=2.8,
            label="2026 actuals")
    ax.plot(june_fc.index, june_fc.values, color="#6741d9", lw=2.8, ls="--",
            label="June forecast (2026-05-26 canonical)")

    # Shade the in-sample gap: actuals running above the June forecast since 05-26.
    overlap = total_actual.index[(total_actual.index >= FORECAST_START)]
    fc_on_overlap = june_fc.reindex(overlap)
    act_on_overlap = total_actual.reindex(overlap)
    ax.fill_between(overlap, fc_on_overlap, act_on_overlap,
                    where=act_on_overlap.notna() & fc_on_overlap.notna(),
                    color="#e8590c", alpha=0.18, zorder=0)

    last_gap = total_actual.iloc[-1] - june_fc.reindex([ACTUALS_END]).iloc[0]
    ax.annotate(
        f"by {ACTUALS_END.date()}: actuals {total_actual.iloc[-1]/1e6:.1f}M\n"
        f"forecast {june_fc.reindex([ACTUALS_END]).iloc[0]/1e6:.1f}M  (+{last_gap/1e6:.1f}M)",
        xy=(ACTUALS_END, total_actual.iloc[-1]), xytext=(-150, 18),
        textcoords="offset points", fontsize=9.5, fontweight="bold",
        arrowprops=dict(arrowstyle="->", color="#e8590c"))

    trough_date, trough = june_fc.idxmin(), june_fc.min()
    py_at = prior_year_ma.reindex([trough_date]).iloc[0]
    ax.annotate(
        f"June fc trough {trough/1e6:.1f}M\nvs 2025 {py_at/1e6:.1f}M  (−{(py_at-trough)/1e6:.1f}M)",
        xy=(trough_date, trough), xytext=(10, -40), textcoords="offset points",
        fontsize=9.5, fontweight="bold", arrowprops=dict(arrowstyle="->", color="#6741d9"))

    ax.axvline(FORECAST_START, color="#6741d9", lw=1.0, alpha=0.5)
    ax.set_title("Desktop (legacy) DAU 28-day MA — actual vs June forecast disconnect",
                 fontsize=14, fontweight="bold")
    ax.set_ylabel("DAU (28d-MA)")
    lo = min(june_fc.min(), total_actual.min(), prior_year_ma.min(), april_fc.min())
    hi = max(total_actual.max(), prior_year_ma.max(), april_fc.max())
    ax.set_ylim(lo - 0.6e6, hi + 0.6e6)
    _format_time_axis(ax)
    ax.legend(loc="lower left", ncol=2, framealpha=0.9, fontsize=8.5)


def _draw_pancake(ax, actual_ma, june_fc, prior_year_ma) -> None:
    """Bottom panel: regional stack (0-based) showing what carries the level."""
    ax.stackplot(
        actual_ma.index,
        [actual_ma[r].values for r in REGIONS],
        labels=[REGION_LABELS[r] for r in REGIONS],
        colors=[REGION_COLORS[r] for r in REGIONS],
        alpha=0.85,
    )
    ax.plot(june_fc.index, june_fc.values, color="#6741d9", lw=2.4, ls="--",
            label="June forecast total", zorder=5)
    ax.plot(prior_year_ma.index, prior_year_ma.values, color="#3b3b3b", lw=1.3,
            ls=":", label="2025 actual total", zorder=5)
    ax.axvline(FORECAST_START, color="#6741d9", lw=1.0, alpha=0.5)

    start, end = actual_ma.iloc[0], actual_ma.iloc[-1]
    total_delta = (end - start).sum()
    lines = [f"28d-MA rise {actual_ma.index[0].date()} → {actual_ma.index[-1].date()}:"
             f"  +{total_delta/1e6:.2f}M"]
    for region in REGIONS:
        delta = end[region] - start[region]
        share = 100 * delta / total_delta if total_delta else float("nan")
        lines.append(f"  {REGION_LABELS[region]:<18} {delta/1e6:+.2f}M ({share:+4.0f}%)")
    ax.text(0.5, 0.04, "\n".join(lines), transform=ax.transAxes, ha="center", va="bottom",
            fontsize=9, family="monospace",
            bbox=dict(boxstyle="round", fc="white", ec="0.6", alpha=0.92))

    ax.set_title("Regional composition of actual DAU (stacked)", fontsize=14, fontweight="bold")
    ax.set_ylabel("DAU (28d-MA)")
    ax.set_xlabel("Submission day date")
    ax.set_ylim(0, None)
    _format_time_axis(ax)
    ax.legend(loc="upper left", ncol=3, framealpha=0.9, fontsize=9)


def make_plot(actual_ma, june_fc, april_fc, prior_year_ma) -> Path:
    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(15, 11), sharex=True,
        gridspec_kw={"height_ratios": [1, 1.25], "hspace": 0.18})
    total_actual = actual_ma.sum(axis=1)
    _draw_disconnect(ax_top, total_actual, june_fc, april_fc, prior_year_ma)
    _draw_pancake(ax_bot, actual_ma, june_fc, prior_year_ma)
    fig.autofmt_xdate()
    out = HERE / "pancake_desktop_dau.png"
    fig.savefig(out, dpi=130, bbox_inches="tight")
    plt.close(fig)
    return out


def print_summary(actual_ma, june_fc) -> None:
    total = actual_ma.sum(axis=1)
    print(f"Actuals {ACTUALS_END.date()}: {total.iloc[-1]/1e6:.2f}M  |  "
          f"June fc same day: {june_fc.reindex([ACTUALS_END]).iloc[0]/1e6:.2f}M  |  "
          f"gap +{(total.iloc[-1]-june_fc.reindex([ACTUALS_END]).iloc[0])/1e6:.2f}M")
    print(f"June fc trough: {june_fc.min()/1e6:.2f}M on {june_fc.idxmin().date()}; "
          f"Dec-15: {june_fc.reindex([pd.Timestamp('2026-12-15')]).iloc[0]/1e6:.2f}M")
    start, end = actual_ma.iloc[0], actual_ma.iloc[-1]
    td = (end - start).sum()
    print(f"\nRegional 28d-MA rise {actual_ma.index[0].date()} -> {actual_ma.index[-1].date()} (+{td/1e6:.2f}M):")
    for r in REGIONS:
        d = end[r] - start[r]
        print(f"  {r:>3}: {d/1e6:+.2f}M ({100*d/td:+5.1f}%)")


def main() -> None:
    actual_ma = load_actual_region_ma()
    june_fc, april_fc = load_june_forecast()
    prior_year_ma = load_prior_year_ma()
    actual_ma.assign(total=actual_ma.sum(axis=1)).to_csv(HERE / "region_ma28.csv")
    out = make_plot(actual_ma, june_fc, april_fc, prior_year_ma)
    print_summary(actual_ma, june_fc)
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
