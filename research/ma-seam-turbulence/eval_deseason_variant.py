"""Score a fourth trend estimator: deseasonalize-then-smooth.

Context: `LOG.md` H5. `forward7` fixes the day-of-week-incomplete window by *widening* it
forward, which is DoW-balanced but shifts the trend estimate's effective centre forward by up
to 3 days. This variant removes the bias a different way: divide the forecast by its OWN
day-of-week profile FIRST, then smooth. Once the weekly cycle is gone, a partial window is no
longer weekday-biased, so the estimate stays centred and no window widening is needed.

    current    trend = mean( y_d )                        over a short, weekday-only window
    forward7   trend = mean( y_d )                        over a forward, DoW-complete window
    deseason   trend = mean( y_d / dow_fc(d) )            over the short window -- unbiased,
                                                          because each term is already
                                                          deseasonalized

`reconstruct_matched_daily` then re-seasonalizes with the ACTUALS' profile, so the operation
becomes exactly "swap the forecast's weekly amplitude for the actuals'" -- which is what the
function claims to do in its docstring.

Also introduces the scoring rule this directory has been missing (`identity_backtest`):
on an all-actuals series the variance-matched transition SHOULD BE A NO-OP, because both sides
of the seam carry identical weekly amplitude and there is nothing to match. Any deviation from
the plain rolling mean is therefore pure estimator error, measured against ground truth rather
than against another cancellation. Read-only; no BQ, no model re-run.
"""

from __future__ import annotations

import os
import subprocess
import sys

import numpy as np
import pandas as pd

os.chdir(subprocess.run(["git", "rev-parse", "--show-toplevel"],
                        capture_output=True, text=True).stdout.strip())
sys.path.insert(0, "src")
sys.path.insert(0, "data-official/2026-06")
sys.path.insert(0, "research/ma-seam-turbulence")

import export_canonical_curves as export  # noqa: E402
from mozaic_daily.adjustments import load_forecast  # noqa: E402
from recon_variants import (  # noqa: E402
    DOW_WEEKS, TREND_WINDOW, _centered_min4, _dow_complete, _dow_profile, make_reconstructor,
    patched_reconstructor,
)

WINDOW = 28
DOW_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

BUILDS = {
    "aug_s01_desktop": (
        "data-official/2026-08/desktop_locked/"
        "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet",
        pd.Timestamp("2026-07-28"), "legacy_desktop", '{"os": "ALL"}', "desktop", ["l", "o"]),
    "aug_mobile": (
        "data-official/2026-08/mobile_baseline_2026-07-28/"
        "cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/"
        "mozaic_daily_forecast.2026-07-28.gm-D.adj-m.parquet",
        pd.Timestamp("2026-07-28"), "glean_mobile", "{}", "ALL MOBILE", ["m"]),
}


def load_all_level(path, data_source, segment, app_name, require_state):
    df, _ = load_forecast(path, require_state=require_state)
    mask = ((df["country"] == "ALL") & (df["segment"] == segment)
            & (df["data_source"] == data_source) & (df["app_name"] == app_name))
    sub = df.loc[mask, ["target_date", "dau", "data_type"]].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    sub = sub.sort_values("target_date")
    series = pd.Series(sub["dau"].values, index=pd.DatetimeIndex(sub["target_date"]))
    training = pd.Series(sub["data_type"].values == "training", index=series.index)
    return series, training


def forecast_own_dow_profile(fc: pd.Series) -> pd.Series:
    """Multiplicative day-of-week profile of the FORECAST itself, from complete windows only.

    Estimated over the first DOW_WEEKS of the forecast. min_periods=TREND_WINDOW (not 4) so
    only genuinely complete 7-day windows contribute -- the forecast runs for months, so
    dropping the 3 incomplete rows at each edge costs nothing here.
    """
    head = fc[fc.index < fc.index.min() + pd.Timedelta(weeks=DOW_WEEKS)]
    trend = head.rolling(TREND_WINDOW, center=True, min_periods=TREND_WINDOW).mean()
    ratio = (head / trend).replace([np.inf, -np.inf], np.nan).dropna()
    dow = ratio.groupby(ratio.index.dayofweek).mean()
    dow = dow / dow.mean()
    return dow.reindex(range(7)).fillna(1.0)


def make_deseason_reconstructor(dow_complete_windows: bool = False):
    """trend = centered mean of (forecast / forecast's own DoW profile). Centre never shifts."""

    def reconstruct(pre, fc, forecast_start, window=WINDOW):
        dow_fc = forecast_own_dow_profile(fc)
        deseasonalized = fc / dow_fc.reindex(fc.index.dayofweek).to_numpy()
        # min_periods=1: a short window is now harmless, because every term it averages has
        # already had its weekday effect divided out. This is the whole point of the variant.
        trend_fc = deseasonalized.rolling(TREND_WINDOW, center=True, min_periods=1).mean()
        dow_act = _dow_profile(pre, forecast_start, dow_complete_windows)
        return trend_fc * dow_act.reindex(fc.index.dayofweek).to_numpy()

    reconstruct.__name__ = "reconstruct_deseason" + ("_dowfix" if dow_complete_windows else "")
    return reconstruct


VARIANTS = {
    "current": make_reconstructor("current"),
    "forward7": make_reconstructor("forward7"),
    "forward7+dowfix": make_reconstructor("forward7", dow_complete_windows=True),
    "deseason": make_deseason_reconstructor(),
    "deseason+dowfix": make_deseason_reconstructor(dow_complete_windows=True),
}


def seam_metrics(series: pd.Series, seam: pd.Timestamp, reconstruct) -> dict:
    """Day-1 reconstruction error, seam step, and the day-27 landing/visible decomposition."""
    pre, fc = series[series.index < seam], series[series.index >= seam]
    plain = series.rolling(WINDOW).mean()
    first_clean = seam + pd.Timedelta(days=WINDOW - 1)

    matched = reconstruct(pre, fc, seam, WINDOW)
    transition = pd.concat([pre, matched]).sort_index().rolling(WINDOW).mean()
    forecast_only = fc.rolling(WINDOW).mean()

    with patched_reconstructor(export, reconstruct):
        disp = export.display_ma(pd.Series(series.index), pd.Series(series.values), seam,
                                 window=WINDOW)

    level = forecast_only.loc[first_clean]
    landing = transition.loc[first_clean] - forecast_only.loc[first_clean]
    slope = transition.loc[first_clean] - transition.loc[first_clean - pd.Timedelta(days=1)]
    return {
        "trend_at_seam": None,
        "day1_error": matched.loc[seam] - fc.loc[seam],
        "seam_step": disp.loc[seam] - plain.loc[seam - pd.Timedelta(days=1)],
        "plain_step": plain.loc[seam] - plain.loc[seam - pd.Timedelta(days=1)],
        "landing_pct": 100 * landing / level,
        "visible_pct": 100 * (forecast_only.loc[first_clean]
                              - transition.loc[first_clean - pd.Timedelta(days=1)]) / level,
        "slope_pct": 100 * slope / level,
    }


def identity_backtest(actuals: pd.Series, reconstruct, iran_crater, first_seam="2025-07-01"):
    """THE GROUND-TRUTHED TEST.

    On a series that is actuals on both sides of the seam there is no amplitude mismatch, so
    the variance-matched transition should reproduce the plain rolling mean exactly. Whatever
    it does instead is estimator error, with a known correct answer -- unlike `visible`, which
    mixes a landing residual against a genuine slope, or |seam step|, which asks a bug fix to
    shrink a quantity that is partly real.
    """
    truth = actuals.rolling(WINDOW).mean()
    dates, values = pd.Series(actuals.index), pd.Series(actuals.values)
    seams = [d for d in pd.date_range(first_seam, actuals.index.max() - pd.Timedelta(days=WINDOW))
             if not (iran_crater[0] <= d <= iran_crater[1])]

    rows = []
    with patched_reconstructor(export, reconstruct):
        for seam in seams:
            got = export.display_ma(dates, values, seam, window=WINDOW)
            if pd.isna(got.get(seam)) or pd.isna(truth.get(seam)):
                continue
            rows.append({"seam": seam, "dow": seam.dayofweek,
                         "error": got.loc[seam] - truth.loc[seam]})
    return pd.DataFrame(rows)


def main() -> int:
    iran_crater = (pd.Timestamp("2026-02-15"), pd.Timestamp("2026-06-05"))

    for build, (path, seam, source, segment, app, state) in BUILDS.items():
        series, training = load_all_level(path, source, segment, app, state)
        print("=" * 100)
        print(f"{build}   seam {seam.date()} ({seam.day_name()})")
        print("=" * 100)

        print(f"\n{'variant':18s} {'day-1 error':>14s} {'seam step':>12s} {'landing%':>10s} "
              f"{'visible%':>10s}")
        plain_step = None
        for name, reconstruct in VARIANTS.items():
            m = seam_metrics(series, seam, reconstruct)
            plain_step = m["plain_step"]
            print(f"{name:18s} {m['day1_error']:>+14,.0f} {m['seam_step']:>+12,.0f} "
                  f"{m['landing_pct']:>+10.3f} {m['visible_pct']:>+10.3f}")
        print(f"{'(plain 28d MA)':18s} {'—':>14s} {plain_step:>+12,.0f}"
              f"   <- the honest reference: what the model itself implies")

        # Ground-truthed estimator test, on this build's own training rows.
        actuals = series[training]
        print(f"\nIdentity backtest — on all-actuals data the transition should be a NO-OP.")
        print(f"{'variant':18s} {'RMSE':>12s} {'mean |err|':>12s} {'max |err|':>12s} "
              f"{'weekday spread':>15s}   per-weekday mean")
        for name, reconstruct in VARIANTS.items():
            bt = identity_backtest(actuals, reconstruct, iran_crater)
            by_dow = bt.groupby("dow")["error"].mean().reindex(range(7))
            spread = by_dow.max() - by_dow.min()
            rmse = np.sqrt((bt["error"] ** 2).mean())
            profile = " ".join(f"{DOW_NAMES[i]}{by_dow[i]/1e3:+.0f}K" for i in range(7))
            print(f"{name:18s} {rmse:>12,.0f} {bt['error'].abs().mean():>12,.0f} "
                  f"{bt['error'].abs().max():>12,.0f} {spread:>15,.0f}   {profile}")
        print(f"({len(bt)} seam dates scored, Iran crater excluded)\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
