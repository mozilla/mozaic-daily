"""Is a shallower summer trough historically consistent with an unchanged autumn?

The peer concern is that August's higher summer "drags" October and November up more than is
warranted. That is only a defect if history says summer depth and autumn level are decoupled.
If instead every year recovers from its trough to roughly the same multiple of its pre-summer
baseline, then a higher trough *should* come with a higher autumn, and the two curves cannot be
mixed and matched.

Method: normalize each year by its own Jun-15 28d-MA — a pre-summer baseline that, for 2026, is
settled actuals shared identically by the July and August forecasts. Then compare each year's
trough / Oct-15 / Nov-15 as a ratio to that baseline.

Curves are compared PRE-headwind. The Win10 headwind is an exogenous forward judgement, not part
of the seasonal shape, and it uses different ramp conventions in the two cycles; including it
would contaminate a question about seasonality. Post-headwind ratios are printed alongside for
reference against the published chart.

Run: python research/autumn-decoupling/seasonal_shape.py
"""

from __future__ import annotations

import pandas as pd

from curves import AUGUST_HEADWIND, BUILDS, JULY_HEADWIND, build_ma
from fetch_actuals import load_actuals

# Iran's shutdown craters 2026 telemetry from 2026-03-01 to 2026-05-25, but recovery completes
# 05-26, so a Jun-15 28d-MA baseline is clean. Earlier baselines in 2026 would not be.
BASELINE_MMDD = (6, 15)
TROUGH_WINDOW = ("07-20", "09-20")


def ma28(df: pd.DataFrame) -> pd.Series:
    s = pd.Series(df["dau"].to_numpy(), index=pd.DatetimeIndex(df["date"]))
    return s.sort_index().rolling(28).mean()


def year_profile(series: pd.Series, year: int) -> dict[str, float] | None:
    """Baseline-normalized trough / Oct / Nov profile for one calendar year."""
    baseline_date = pd.Timestamp(year=year, month=BASELINE_MMDD[0], day=BASELINE_MMDD[1])
    if baseline_date not in series.index or pd.isna(series.get(baseline_date)):
        return None

    baseline = float(series[baseline_date])
    window = series[f"{year}-{TROUGH_WINDOW[0]}":f"{year}-{TROUGH_WINDOW[1]}"].dropna()
    if window.empty:
        return None

    oct15 = series.get(pd.Timestamp(year=year, month=10, day=15), float("nan"))
    nov15 = series.get(pd.Timestamp(year=year, month=11, day=15), float("nan"))

    return {
        "baseline": baseline,
        "trough": float(window.min()),
        "trough_date": window.idxmin(),
        "trough_ratio": float(window.min()) / baseline,
        "oct_ratio": float(oct15) / baseline,
        "nov_ratio": float(nov15) / baseline,
    }


def print_history(profiles: dict[int, dict]) -> None:
    print("\nACTUALS — desktop DAU 28d-MA, each year normalized to its own Jun-15")
    print("-" * 92)
    print(f"{'year':<6}{'Jun-15 base':>14}{'trough':>13}{'on':>8}"
          f"{'trough/base':>13}{'Oct/base':>11}{'Nov/base':>11}")
    for year, p in sorted(profiles.items()):
        print(f"{year:<6}{p['baseline']:>14,.0f}{p['trough']:>13,.0f}"
              f"{p['trough_date'].strftime('%b-%d'):>8}"
              f"{p['trough_ratio']:>13.4f}{p['oct_ratio']:>11.4f}{p['nov_ratio']:>11.4f}")

    complete = [p for p in profiles.values() if pd.notna(p["oct_ratio"])]
    if complete:
        n = len(complete)
        print("-" * 92)
        print(f"{'mean':<6}{'':>14}{'':>13}{'':>8}"
              f"{sum(p['trough_ratio'] for p in complete) / n:>13.4f}"
              f"{sum(p['oct_ratio'] for p in complete) / n:>11.4f}"
              f"{sum(p['nov_ratio'] for p in complete) / n:>11.4f}")


def main() -> None:
    actual_ma = ma28(load_actuals())

    history = {}
    for year in range(2021, 2026):
        profile = year_profile(actual_ma, year)
        if profile:
            history[year] = profile
    print_history(history)

    # 2026's Jun-15 baseline comes from actuals and is therefore identical for both forecasts.
    baseline_2026 = float(actual_ma[pd.Timestamp("2026-06-15")])
    print(f"\n2026 Jun-15 actual baseline: {baseline_2026:,.0f}")

    print("\nFORECASTS — 2026, same Jun-15 actual baseline")
    print("-" * 92)
    print(f"{'curve':<40}{'trough':>13}{'trough/base':>13}{'Oct/base':>11}{'Nov/base':>11}")

    variants = [
        ("July delivered  (pre-headwind)", BUILDS["july_delivered"], None),
        ("August LOCKED   (pre-headwind)", BUILDS["s01_200k_locked"], None),
        ("July delivered  (post-headwind)", BUILDS["july_delivered"], JULY_HEADWIND),
        ("August LOCKED   (post-headwind)", BUILDS["s01_200k_locked"], AUGUST_HEADWIND),
    ]
    for label, build, convention in variants:
        curve = build_ma(build, convention)
        window = curve["2026-07-20":"2026-09-20"].dropna()
        trough = float(window.min())
        oct15 = float(curve[pd.Timestamp("2026-10-15")])
        nov15 = float(curve[pd.Timestamp("2026-11-15")])
        print(f"{label:<40}{trough:>13,.0f}{trough / baseline_2026:>13.4f}"
              f"{oct15 / baseline_2026:>11.4f}{nov15 / baseline_2026:>11.4f}")

    print_passthrough(history, baseline_2026)


def print_passthrough(history: dict[int, dict], baseline_2026: float) -> None:
    """Regress autumn level on trough depth across years, and compare the model's response.

    The peer concern presumes a passthrough near zero: that summer can be raised while autumn
    stays put. The historical slope is the empirical answer to what passthrough looks like.
    """
    years = sorted(history)
    trough = [history[y]["trough_ratio"] for y in years]

    print("\nHISTORICAL PASSTHROUGH — regress autumn/base on trough/base across years")
    print("-" * 92)
    print(f"{'target':<10}{'slope':>10}{'r':>10}{'n':>5}   interpretation")

    slopes = {}
    for name, key in (("Oct-15", "oct_ratio"), ("Nov-15", "nov_ratio")):
        autumn = [history[y][key] for y in years]
        slope, r = _ols(trough, autumn)
        slopes[name] = slope
        print(f"{name:<10}{slope:>10.3f}{r:>10.3f}{len(years):>5}   "
              f"+1.00 of trough lift historically brings {slope:+.2f} of autumn lift")

    # What the model actually did, July -> August, pre-headwind (seasonal shape only).
    july = build_ma(BUILDS["july_delivered"], None)
    august = build_ma(BUILDS["s01_200k_locked"], None)
    d_trough = (float(august["2026-07-20":"2026-09-20"].min())
                - float(july["2026-07-20":"2026-09-20"].min())) / baseline_2026

    print("\nMODEL PASSTHROUGH — July -> August LOCKED, pre-headwind")
    print("-" * 92)
    print(f"{'target':<10}{'model':>10}{'history':>10}   verdict")
    for name, date in (("Oct-15", "2026-10-15"), ("Nov-15", "2026-11-15")):
        d_autumn = (float(august[date]) - float(july[date])) / baseline_2026
        model_slope = d_autumn / d_trough
        verdict = ("model passes LESS into autumn than history"
                   if model_slope < slopes[name]
                   else "model passes MORE into autumn than history")
        print(f"{name:<10}{model_slope:>10.3f}{slopes[name]:>10.3f}   {verdict}")


def _ols(x: list[float], y: list[float]) -> tuple[float, float]:
    """Least-squares slope and Pearson r. Small-n by construction — read r, not just slope."""
    n = len(x)
    mx, my = sum(x) / n, sum(y) / n
    sxy = sum((a - mx) * (b - my) for a, b in zip(x, y))
    sxx = sum((a - mx) ** 2 for a in x)
    syy = sum((b - my) ** 2 for b in y)
    return sxy / sxx, sxy / (sxx * syy) ** 0.5


if __name__ == "__main__":
    main()
