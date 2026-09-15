"""Demo: two ways to turn marketing's weekly Monday values into a daily paid-DAU level.

Marketing delivers one value per Monday = the average paid DAU for the week starting that Monday.
Two daily reconstructions are compared:

  * interpolated -- linear between consecutive Mondays (what paid_curve.py does today)
  * step         -- hold each Monday's value flat for its seven days

Both are smoothed with a trailing 28-day mean and compared at Dec-15. For a rising trend the
interpolated week averages Monday + 3/7 of the week's rise, so it sits above the delivered
weekly mean; the step reproduces the weekly mean exactly.

Usage: python research/marketing-lift/weekly_to_daily_demo.py
Writes research/marketing-lift/plots/weekly_to_daily_interp_vs_step.png
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
CURVE = REPO / "data-official/2026-09/marketing/marketing_lift_model.gmio_uac_meta_total_low.2026-09-09.pull2026-09-10.parquet"
PLOT = Path(__file__).resolve().parent / "plots/weekly_to_daily_interp_vs_step.png"
SEAM = pd.Timestamp("2026-09-09")
DEC15 = pd.Timestamp("2026-12-15")
MA_DAYS = 28


def monday_values(daily: pd.Series) -> pd.Series:
    """Recover the delivered weekly series: the interpolated curve passes through every Monday."""
    return daily[daily.index.dayofweek == 0]


def interpolated_daily(weekly: pd.Series, end: pd.Timestamp) -> pd.Series:
    idx = pd.date_range(weekly.index.min(), end, freq="D")
    return weekly.reindex(idx).interpolate(method="linear", limit_area="inside").ffill()


def step_daily(weekly: pd.Series, end: pd.Timestamp) -> pd.Series:
    idx = pd.date_range(weekly.index.min(), end, freq="D")
    return weekly.reindex(idx).ffill()


def trailing_ma(series: pd.Series) -> pd.Series:
    return series.rolling(MA_DAYS).mean()


def fmt_m(value: float, _pos=None) -> str:
    return f"{value / 1e6:.2f}M"


def main() -> None:
    delivered = pd.read_parquet(CURVE)["paid_dau_level_daily"]
    weekly = monday_values(delivered)
    end = delivered.index.max()

    interp = interpolated_daily(weekly, end)
    step = step_daily(weekly, end)
    assert (interp - delivered).abs().max() < 1e-6, "interpolation does not reproduce the on-disk curve"

    interp_ma, step_ma = trailing_ma(interp), trailing_ma(step)

    # Weekly means: the step reproduces the delivered Monday value; the interpolation does not.
    week_id = pd.Series(interp.index.to_period("W-SUN"), index=interp.index)
    interp_week_mean = interp.groupby(week_id).mean()
    step_week_mean = step.groupby(week_id).mean()
    delivered_by_week = pd.Series(weekly.to_numpy(), index=weekly.index.to_period("W-SUN"))
    common = delivered_by_week.index.intersection(interp_week_mean.index)
    interp_bias = (interp_week_mean[common] - delivered_by_week[common])
    step_bias = (step_week_mean[common] - delivered_by_week[common])

    weekly_rise = weekly.diff()
    print(f"weeks: {len(weekly)}  first {weekly.index.min().date()}  last {weekly.index.max().date()}")
    print(f"median week-over-week rise (forecast weeks after seam): {weekly_rise[weekly_rise.index > SEAM].median():,.0f}")
    print(f"step   weekly-mean bias vs delivered: max |{step_bias.abs().max():,.3f}|  (exact)")
    print(f"interp weekly-mean bias vs delivered: median {interp_bias.median():,.0f}, "
          f"min {interp_bias.min():,.0f}, max {interp_bias.max():,.0f}  (= 3/7 of each week's rise)")
    print()
    rows = {
        "Dec-15 daily": (interp[DEC15], step[DEC15]),
        "Dec-15 28d MA": (interp_ma[DEC15], step_ma[DEC15]),
        "seam 28d MA": (interp_ma[SEAM], step_ma[SEAM]),
        "Sep-09..Dec-15 mean daily": (interp[SEAM:DEC15].mean(), step[SEAM:DEC15].mean()),
    }
    print(f"{'':28s}{'interpolated':>16s}{'step':>16s}{'interp - step':>16s}")
    for name, (a, b) in rows.items():
        print(f"{name:28s}{a:>16,.0f}{b:>16,.0f}{a - b:>16,.0f}")

    fig, (ax_daily, ax_ma) = plt.subplots(2, 1, figsize=(12, 9), sharex=True)
    start = pd.Timestamp("2026-07-01")
    for ax, series_pair, title in (
        (ax_daily, (interp, step), "Daily level reconstructed from weekly Monday values"),
        (ax_ma, (interp_ma, step_ma), f"Trailing {MA_DAYS}-day mean of each reconstruction"),
    ):
        a, b = series_pair
        ax.plot(a[start:], color="#1f77b4", lw=1.8, label="interpolated between Mondays (current)")
        ax.plot(b[start:], color="#d62728", lw=1.8, label="step: hold Monday value for the week")
        ax.axvline(SEAM, color="grey", ls="--", lw=1)
        ax.axvline(DEC15, color="grey", ls=":", lw=1)
        ax.set_title(title)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_m))
        ax.grid(alpha=0.3)
    ax_daily.scatter(weekly[start:].index, weekly[start:], color="black", s=14, zorder=5, label="delivered Monday values")
    ax_daily.legend(loc="upper left")
    ax_ma.legend(loc="upper left")
    d15 = interp_ma[DEC15] - step_ma[DEC15]
    ax_ma.annotate(f"Dec-15 28d MA\ninterp {interp_ma[DEC15]:,.0f}\nstep   {step_ma[DEC15]:,.0f}\ndiff   {d15:+,.0f}",
                   xy=(DEC15, step_ma[DEC15]), xytext=(-170, -90), textcoords="offset points",
                   fontsize=9, family="monospace", arrowprops=dict(arrowstyle="->", color="grey"))
    ax_ma.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax_ma.xaxis.set_major_locator(mdates.MonthLocator())
    fig.suptitle("September 2026 paid-DAU 'Low' scenario: interpolated vs step weekly-to-daily", fontsize=13)
    fig.tight_layout()
    fig.savefig(PLOT, dpi=130)
    print(f"\nplot: {PLOT.relative_to(REPO)}")


if __name__ == "__main__":
    main()
