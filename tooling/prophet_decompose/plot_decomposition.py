"""Plot the top-level Prophet decomposition of a Mozaic forecast pkl as one stacked figure.

The pkl from `save_mozaic_objects()` holds dict[metric -> Mozaic]; the top-level Mozaic carries
the single aggregate Prophet fit that top-down reconciliation is anchored on. This script
re-predicts that model over history + horizon and draws six panels:

    1. training actuals + Prophet fit/forecast
    2. trend with changepoints
    3. weekly seasonality, full series (conditional: historical vs recent regime)
    4. weekly seasonality PATTERN: one example Mon..Sun week from each regime, as % of level
    5. yearly seasonality
    6. residual over the training window: holiday-detrended actuals minus Prophet fit

Space handling (the part that differs by platform):
- Desktop fits log(y + 1) with multiplicative seasonality, so in Prophet's own units
  yhat = trend * (1 + weekly + yearly). Every component is converted to a DAU offset by
  back-transforming with and without that term: e.g. weekly_dau = exp(trend*(1+w)) - exp(trend).
- Mobile fits raw DAU with additive seasonality, so components are DAU offsets already.
The space is detected from the trend's magnitude (a log trend of a DAU series sits under ~30).

Mozaic strips holidays before Prophet sees the series, so the residual is taken against
`holiday_detrended_historical_data`, not raw actuals; raw actuals are what panel 1 shows.
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path

import cloudpickle
import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from decompose import _build_future_df  # noqa: E402

LOG_SPACE_TREND_CEILING = 30.0  # a log(DAU) trend is ~17; a level-space trend is millions
DAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _is_log_space(prophet_forecast: pd.DataFrame) -> bool:
    return float(prophet_forecast["trend"].abs().max()) < LOG_SPACE_TREND_CEILING


def _components_in_dau(pf: pd.DataFrame, seasonality_mode: str, log_space: bool) -> pd.DataFrame:
    """Return ds, trend, weekly, yearly, yhat, weekly_pct, regime — all offsets in DAU."""
    trend = pf["trend"].to_numpy(dtype=float)
    weekly = (pf["weekly_historical"].fillna(0) + pf["weekly_recent"].fillna(0)).to_numpy(dtype=float)
    yearly = pf["yearly"].fillna(0).to_numpy(dtype=float)
    regime = np.where(pf["weekly_recent"].notna() & (pf["weekly_recent"] != 0), "recent", "historical")

    def compose(t: np.ndarray, s: np.ndarray) -> np.ndarray:
        return t * (1.0 + s) if seasonality_mode == "multiplicative" else t + s

    def to_dau(x: np.ndarray) -> np.ndarray:
        return np.exp(x) - 1.0 if log_space else x

    level = to_dau(trend)
    out = pd.DataFrame({
        "ds": pd.to_datetime(pf["ds"]),
        "trend": level,
        "weekly": to_dau(compose(trend, weekly)) - level,
        "yearly": to_dau(compose(trend, yearly)) - level,
        "yhat": to_dau(pf["yhat"].to_numpy(dtype=float)),
        "regime": regime,
    })
    out["weekly_pct"] = 100.0 * out["weekly"] / out["trend"]
    return out


def _example_week(comps: pd.DataFrame, regime: str, before: pd.Timestamp) -> pd.DataFrame:
    """The last full Mon..Sun week in `regime` ending before `before`."""
    rows = comps[(comps["regime"] == regime) & (comps["ds"] < before)]
    sundays = rows[rows["ds"].dt.weekday == 6]["ds"]
    if sundays.empty:
        raise ValueError(f"no full week found for regime={regime!r} before {before.date()}")
    end = sundays.max()
    week = comps[(comps["ds"] > end - pd.Timedelta(days=7)) & (comps["ds"] <= end)].copy()
    if len(week) != 7:
        raise ValueError(f"example week for {regime!r} has {len(week)} rows, expected 7")
    return week


def _fmt_millions(v: float, _pos) -> str:
    return f"{v / 1e6:.2f}M" if abs(v) < 1e7 else f"{v / 1e6:.1f}M"


def _fmt_signed_millions(v: float, _pos) -> str:
    return f"{v / 1e6:+.1f}M"


def _robust_ylim(values: pd.Series, pad: float = 1.5) -> tuple[float, float]:
    """Axis limits from the 0.5–99.5 percentile band, so a few outliers do not flatten the series."""
    lo, hi = np.nanpercentile(values, [0.5, 99.5])
    span = max(hi - lo, 1.0)
    return lo - (pad - 1) * span, hi + (pad - 1) * span


def _recent_weeks_from_parameters(pkl_path: Path) -> int:
    """`prophet_recent_weeks` from the parameters.json beside the pkl; the stored forecast does not record it."""
    params = pkl_path.parent / "parameters.json"
    if not params.exists():
        raise FileNotFoundError(f"{params} not found; pass --recent-weeks explicitly")
    config = json.loads(params.read_text()).get("config", {})
    if "prophet_recent_weeks" not in config:
        raise KeyError(f"{params} has no config.prophet_recent_weeks; pass --recent-weeks explicitly")
    return int(config["prophet_recent_weeks"])


def plot_decomposition(pkl_path: Path, out_path: Path, metric: str = "DAU", title: str | None = None,
                       recent_weeks: int | None = None) -> dict:
    mozaic = cloudpickle.load(open(pkl_path, "rb"))[metric]
    pm = mozaic._prophet_model
    forecast_start = pd.Timestamp(mozaic.forecast_start_date)

    if recent_weeks is None:
        recent_weeks = _recent_weeks_from_parameters(pkl_path)
    recent_start = forecast_start - pd.Timedelta(weeks=recent_weeks)

    future = _build_future_df(mozaic, pm, recent_weeks=recent_weeks)
    with warnings.catch_warnings():
        # Prophet's multiplicative-terms matmul emits overflow/divide warnings on the rows a
        # condition zeroes out; the result is finite and reproduces the stored forecast to 1e-14.
        warnings.simplefilter("ignore", RuntimeWarning)
        pf = pm.predict(future)
    if not np.isfinite(pf["yhat"]).all():
        raise ValueError(f"Prophet re-prediction has non-finite yhat rows: {(~np.isfinite(pf['yhat'])).sum()}")
    log_space = _is_log_space(pf)
    comps = _components_in_dau(pf, pm.seasonality_mode, log_space)

    hist = pd.DataFrame({
        "ds": pd.to_datetime(mozaic.historical_dates.values),
        "raw": np.asarray(mozaic.raw_historical_data.values, dtype=float),
        "detrended": np.asarray(mozaic.holiday_detrended_historical_data.values, dtype=float),
    })
    train = comps.merge(hist, on="ds", how="inner")
    train["residual"] = train["detrended"] - train["yhat"]

    week_hist = _example_week(comps, "historical", recent_start)
    week_recent = _example_week(comps, "recent", forecast_start)

    label = title or f"{pkl_path.parent.parent.name} — top-level {metric} Prophet (pre-reconciliation)"
    fig = plt.figure(figsize=(13, 20))
    gs = fig.add_gridspec(6, 1, hspace=0.45)
    axes = [fig.add_subplot(gs[i]) for i in range(6)]
    time_axes = [axes[0], axes[1], axes[2], axes[4], axes[5]]

    ax = axes[0]
    ax.plot(hist["ds"], hist["raw"], color="#999999", lw=0.6, label="training actuals (raw)")
    ax.plot(comps["ds"], comps["yhat"], color="#d62728", lw=1.0, label="Prophet yhat (fit + forecast)")
    ax.set_title(f"{label}\n1. Training actuals + Prophet fit/forecast")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(_fmt_millions))

    ax = axes[1]
    ax.plot(comps["ds"], comps["trend"], color="#1f77b4", lw=1.5, label="trend")
    for i, cp in enumerate(pm.changepoints):
        ax.axvline(pd.Timestamp(cp), color="orange", alpha=0.35, lw=0.8, label="changepoints" if i == 0 else None)
    ax.set_title(f"2. Trend + {len(pm.changepoints)} changepoints (orange)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(_fmt_millions))

    ax = axes[2]
    for regime, color in [("historical", "#1f77b4"), ("recent", "#9467bd")]:
        sel = comps["regime"] == regime
        ax.plot(comps.loc[sel, "ds"], comps.loc[sel, "weekly"], lw=0.8, color=color, label=f"weekly_{regime}")
    ax.set_title(f"3. Weekly seasonality — full series, conditional (recent = last {recent_weeks} weeks of training + horizon), DAU offset")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(_fmt_signed_millions))

    ax = axes[3]
    x = np.arange(7)
    for week, color, name in [(week_hist, "#1f77b4", "historical"), (week_recent, "#9467bd", "recent")]:
        week = week.sort_values("ds")
        order = week["ds"].dt.weekday.to_numpy()
        ax.plot(order, week["weekly_pct"].to_numpy(), marker="o", lw=1.8, color=color,
                ls="-" if name == "historical" else "--",
                label=f"{name}: week of {week['ds'].min().date()}")
    ax.axhline(0, color="black", lw=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(DAY_LABELS)
    ax.set_ylabel("% of trend level")
    ax.set_title("4. Weekly seasonality — PATTERN, one example Mon..Sun week per regime (% of trend level)")

    ax = axes[4]
    ax.plot(comps["ds"], comps["yearly"], color="#2ca02c", lw=0.9, label="yearly")
    ax.set_title("5. Yearly seasonality — DAU offset")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(_fmt_signed_millions))

    ax = axes[5]
    ax.plot(train["ds"], train["residual"], color="#7f7f7f", lw=0.6, label="holiday-detrended actuals − yhat")
    ax.axhline(0, color="black", lw=0.5)
    lo, hi = _robust_ylim(train["residual"])
    n_clipped = int(((train["residual"] < lo) | (train["residual"] > hi)).sum())
    ax.set_ylim(lo, hi)
    ax.set_title("6. Residual over training — holiday-detrended actuals minus Prophet fit "
                 f"(holidays stripped before Prophet; std {train['residual'].std() / 1e6:.2f}M, {n_clipped} outliers off-axis)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(_fmt_signed_millions))

    for ax in time_axes:
        ax.axvline(forecast_start, color="k", ls=":", lw=0.8)
        ax.set_xlim(comps["ds"].min(), comps["ds"].max())
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    for ax in axes:
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8, loc="lower left" if ax is axes[3] else "upper left")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)

    return {
        "growth": pm.growth,
        "seasonality_mode": pm.seasonality_mode,
        "log_space": log_space,
        "n_changepoints": len(pm.changepoints),
        "recent_weeks": recent_weeks,
        "recent_start": str(recent_start.date()),
        "forecast_start": str(forecast_start.date()),
        "example_week_historical": str(week_hist["ds"].min().date()),
        "example_week_recent": str(week_recent["ds"].min().date()),
        "residual_std": float(train["residual"].std()),
        "residual_mean": float(train["residual"].mean()),
        "fit_vs_detrended_max_abs_pct": float((train["residual"].abs() / train["detrended"]).max() * 100),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pkl", type=Path, help="mozaic_objects.<source>.<date>.pkl")
    parser.add_argument("--out", type=Path, required=True, help="output PNG path")
    parser.add_argument("--metric", default="DAU")
    parser.add_argument("--title", default=None)
    parser.add_argument("--recent-weeks", type=int, default=None,
                        help="conditional-weekly window; default reads prophet_recent_weeks from parameters.json beside the pkl")
    args = parser.parse_args()
    info = plot_decomposition(args.pkl, args.out, metric=args.metric, title=args.title, recent_weeks=args.recent_weeks)
    for k, v in info.items():
        print(f"{k}: {v}")
    print("saved", args.out)


if __name__ == "__main__":
    main()
