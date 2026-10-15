"""Month-by-month YoY growth of historical world mobile DAU (actuals only).

Reads the `training` rows of a canonical mobile parquet — byte-identical to raw BigQuery actuals
(`scripts/verify_training_rows_are_actuals.py`) — for world mobile (`ALL MOBILE`, country `ALL`).
Monthly value = mean daily DAU over the calendar month; YoY = month / same month a year earlier − 1.
Months the actuals do not fully cover are dropped (the trailing partial month would compare a few
days against a whole month).

Usage:
  python scripts/mobile_monthly_yoy.py --parquet <gm-D parquet> [--output-csv PATH]
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from mozaic_daily.adjustments import load_forecast  # noqa: E402


def load_world_actuals(parquet_path: str) -> pd.Series:
    """Daily world mobile DAU actuals indexed by date."""
    df, _ = load_forecast(parquet_path)
    world = df[(df.app_name == "ALL MOBILE") & (df.country == "ALL") & (df.data_type == "training")]
    daily = world.set_index(pd.to_datetime(world["target_date"]))["dau"].astype(float)
    return daily.sort_index()


def monthly_yoy(daily: pd.Series) -> pd.DataFrame:
    """Mean DAU per fully covered calendar month and its YoY growth."""
    days_present = daily.resample("MS").count()
    days_in_month = days_present.index.days_in_month
    monthly = daily.resample("MS").mean()
    monthly = monthly[days_present.values == days_in_month]
    previous_year = monthly.reindex(monthly.index - pd.DateOffset(years=1)).to_numpy()
    return pd.DataFrame({"mean_dau": monthly.round(0), "yoy_pct": (100 * (monthly / previous_year - 1)).round(2)})


def save_plot(daily: pd.Series, table: pd.DataFrame, plot_path: str) -> None:
    """Two panels sharing a month axis: monthly mean DAU over daily actuals, and YoY growth bars."""
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    first_month = table.index.min() - pd.DateOffset(years=1)
    daily = daily[daily.index >= first_month]
    month_mid = table.index + pd.Timedelta(days=14)

    fig, (level_ax, yoy_ax) = plt.subplots(2, 1, figsize=(13, 8.5), sharex=True,
                                           gridspec_kw={"height_ratios": [1, 1.1]})
    level_ax.plot(daily.index, daily / 1e6, color="#b9c4d0", linewidth=0.7, label="Daily DAU")
    full_months = monthly_yoy(daily).mean_dau
    step_x = full_months.index.append(pd.DatetimeIndex([full_months.index[-1] + pd.DateOffset(months=1)]))
    step_y = list(full_months / 1e6) + [full_months.iloc[-1] / 1e6]
    level_ax.step(step_x, step_y, where="post", color="#1f4e79", linewidth=1.8,
                  label="Monthly mean DAU (full months)")
    level_ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.1f}M"))
    level_ax.set_ylabel("World mobile DAU")
    level_ax.legend(loc="upper left", frameon=False)
    level_ax.grid(axis="y", alpha=0.3)

    colors = ["#2e7d32" if v >= 0 else "#c62828" for v in table.yoy_pct]
    yoy_ax.bar(month_mid, table.yoy_pct, width=24, color=colors)
    for x, v in zip(month_mid, table.yoy_pct):
        yoy_ax.annotate(f"{v:.1f}", (x, v), xytext=(0, 2), textcoords="offset points",
                        ha="center", va="bottom", fontsize=6.5, rotation=90)
    yoy_ax.axhline(0, color="black", linewidth=0.8)
    yoy_ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    yoy_ax.set_ylabel("YoY growth (monthly mean vs same month prior year)")
    yoy_ax.set_ylim(0, table.yoy_pct.max() * 1.15)
    yoy_ax.grid(axis="y", alpha=0.3)
    yoy_ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))
    yoy_ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.setp(yoy_ax.get_xticklabels(), rotation=45, ha="right")

    last_day = daily.index.max().strftime("%Y-%m-%d")
    fig.suptitle(f"Firefox mobile (Fenix, Firefox iOS, Focus Android, Focus iOS), world — actuals through {last_day}")
    fig.tight_layout()
    fig.savefig(plot_path, dpi=150)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--parquet", required=True)
    parser.add_argument("--output-csv")
    parser.add_argument("--plot", help="PNG path for the level + YoY figure")
    args = parser.parse_args()

    daily = load_world_actuals(args.parquet)
    table = monthly_yoy(daily).dropna()
    if args.plot:
        save_plot(daily, table, args.plot)
    table.index = table.index.strftime("%Y-%m")
    print(table.iloc[::-1].to_string())
    if args.output_csv:
        table.iloc[::-1].to_csv(args.output_csv, index_label="month")


if __name__ == "__main__":
    main()
