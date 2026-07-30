"""Planning probe for Fix A (deseasonalize-before-averaging). Measures only — changes nothing.

Answers the four questions the implementation plan depends on:

  M2b  Which of the 9 existing tests in tests/test_export_canonical_curves.py would break?
       Evaluated by recomputing each test's asserted quantity against an in-memory patched
       reconstructor, so the shipped file is never touched.
  M3   A1 (deseasonalize by the FORECAST's own DoW profile) vs A2 (reuse the ACTUALS' profile).
       A2 is a much smaller diff; this decides whether it is good enough to prefer.
  M4   Does any published far-horizon number move? (Dec-15 and the Aug-25 trough, all cycles.)
  M5   By how much do the prior cycles' TRANSITION-window curves shift?
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys

import numpy as np
import pandas as pd

os.chdir(subprocess.run(["git", "rev-parse", "--show-toplevel"],
                        capture_output=True, text=True).stdout.strip())
sys.path.insert(0, "src")
sys.path.insert(0, "research/ma-seam-turbulence")

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from recon_variants import (  # noqa: E402
    DOW_WEEKS, TREND_WINDOW, _dow_profile, make_reconstructor, patched_reconstructor,
)

WINDOW = 28

# Load the shipped module the same way the test file does, so the quantities are comparable.
_spec = importlib.util.spec_from_file_location(
    "export_canonical_curves", "data-official/2026-06/export_canonical_curves.py")
export = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export)


# --------------------------------------------------------------------------- Fix A variants
def _own_dow_profile(fc: pd.Series) -> pd.Series:
    """DoW profile of the forecast itself, from complete 7-day windows only."""
    head = fc[fc.index < fc.index.min() + pd.Timedelta(weeks=DOW_WEEKS)]
    trend = head.rolling(TREND_WINDOW, center=True, min_periods=TREND_WINDOW).mean()
    ratio = (head / trend).replace([np.inf, -np.inf], np.nan).dropna()
    dow = ratio.groupby(ratio.index.dayofweek).mean()
    return (dow / dow.mean()).reindex(range(7)).fillna(1.0)


def make_fix_a(profile: str):
    """profile='own' -> A1 (forecast's own DoW profile); 'actuals' -> A2 (reuse dow_act)."""

    def reconstruct(pre, fc, forecast_start, window=WINDOW):
        dow_act = _dow_profile(pre, forecast_start, complete_windows=False)
        divisor = _own_dow_profile(fc) if profile == "own" else dow_act
        deseasonalized = fc / divisor.reindex(fc.index.dayofweek).to_numpy()
        trend_fc = deseasonalized.rolling(TREND_WINDOW, center=True, min_periods=1).mean()
        return trend_fc * dow_act.reindex(fc.index.dayofweek).to_numpy()

    reconstruct.__name__ = f"fix_a_{profile}"
    return reconstruct


def make_fix_a3():
    """A3 — deseasonalize BOTH sides, then take a genuinely centred window across the seam.

    Each side is divided by its own day-of-week profile (actuals by the actuals' profile,
    forecast by the forecast's), which puts both on a common deseasonalized level scale. A
    centred 7-day window at the seam then draws its left half from real actuals and its right
    half from the forecast: complete AND centred, so neither the weekday bias (A1 and A3 both
    remove this) nor A1's residual ~1.5-day forward lean remains.

    The risk this carries and A1 does not: any genuine LEVEL offset across the seam now enters
    the forecast's trend estimate. Deseasonalizing does not address that — it is a level effect,
    not a seasonal one. That is what the numbers below have to settle.
    """

    def reconstruct(pre, fc, forecast_start, window=WINDOW):
        dow_act = _dow_profile(pre, forecast_start, complete_windows=False)
        dow_fc = _own_dow_profile(fc)
        pre_des = pre / dow_act.reindex(pre.index.dayofweek).to_numpy()
        fc_des = fc / dow_fc.reindex(fc.index.dayofweek).to_numpy()
        joined = pd.concat([pre_des, fc_des]).sort_index()
        trend_fc = (joined.rolling(TREND_WINDOW, center=True, min_periods=1).mean()
                    .reindex(fc.index))
        return trend_fc * dow_act.reindex(fc.index.dayofweek).to_numpy()

    reconstruct.__name__ = "fix_a3"
    return reconstruct


VARIANTS = {
    "current (shipped)": make_reconstructor("current"),
    "A1 own-profile": make_fix_a("own"),
    "A2 actuals-profile": make_fix_a("actuals"),
    "A3 both-sides": make_fix_a3(),
}


# --------------------------------------------------------------------------- M2b: test impact
FORECAST_START = pd.Timestamp("2026-06-01")
SEAM_CLEAR = FORECAST_START + pd.Timedelta(days=WINDOW - 1)
LAST_TRANSITION = FORECAST_START + pd.Timedelta(days=WINDOW - 2)


def _rel_2nd_diff_ppm(series, start, end):
    w = series[(series.index >= start) & (series.index <= end)].dropna()
    return 1e6 * w.diff().diff().abs().mean() / w.mean()


def seam_series():
    dates = pd.date_range("2026-01-01", "2026-12-31", freq="D")
    is_forecast, is_weekend = dates >= FORECAST_START, dates.dayofweek >= 5
    dow_factor = np.where(is_forecast, np.where(is_weekend, 0.893, 1.043),
                          np.where(is_weekend, 0.625, 1.15))
    values = pd.Series(400_000.0 * dow_factor, index=dates)
    return dates.to_series(name="target_date").reset_index(drop=True), values.reset_index(drop=True)


def curved_seam_series():
    dates = pd.date_range("2026-01-01", "2026-12-31", freq="D")
    idx = np.arange(len(dates))
    trend = 400_000 + 50_000 * np.sin(2 * np.pi * idx / 90.0)
    is_weekend = dates.dayofweek.values >= 5
    actuals_dow = np.where(is_weekend, 0.5, 1.2)
    forecast_dow = np.where(is_weekend, 0.85, 1.06)
    is_forecast = dates >= FORECAST_START
    daily = np.where(is_forecast, trend * forecast_dow, trend * actuals_dow)
    return (dates.to_series(name="target_date").reset_index(drop=True),
            pd.Series(daily).reset_index(drop=True),
            pd.Series(trend * actuals_dow).reset_index(drop=True))


def evaluate_tests(reconstruct) -> dict:
    """Recompute the quantity each existing test asserts on. Returns {test_name: (value, ok)}."""
    out = {}
    with patched_reconstructor(export, reconstruct):
        dates, values = seam_series()
        display = export.display_ma(dates, values, FORECAST_START, window=WINDOW)
        s = pd.Series(values.values, index=pd.to_datetime(dates.values))
        blend = s.rolling(WINDOW).mean()

        blend_ppm = _rel_2nd_diff_ppm(blend, FORECAST_START, SEAM_CLEAR)
        display_ppm = _rel_2nd_diff_ppm(display, FORECAST_START, SEAM_CLEAR)
        out["display_ma_smooths_the_seam"] = (display_ppm, display_ppm < blend_ppm / 3)

        far = display[display.index >= SEAM_CLEAR]
        out["far_horizon_byte_identical"] = (
            (far - blend.reindex(far.index)).abs().max(),
            (far - blend.reindex(far.index)).abs().max() < 1e-9)

        prev = FORECAST_START - pd.Timedelta(days=1)
        step = abs(display.loc[FORECAST_START] - display.loc[prev])
        day1 = display.loc[FORECAST_START]
        out["seam_anchor_is_continuous_with_actuals"] = (step / day1, step / day1 < 0.02)

        pre_region = display[display.index < FORECAST_START]
        delta = (pre_region - blend.reindex(pre_region.index)).abs().max()
        out["actuals_region_unchanged"] = (delta, delta < 1e-9)

        level = display.loc[SEAM_CLEAR]
        splice_step = abs(display.loc[SEAM_CLEAR] - display.loc[LAST_TRANSITION])
        out["splice_smooth_day27_to_28"] = (splice_step / level, splice_step / level < 0.01)
        window4 = display.loc[LAST_TRANSITION - pd.Timedelta(days=1):
                              SEAM_CLEAR + pd.Timedelta(days=1)]
        second = window4.diff().diff().abs().max()
        out["splice_smooth_2nd_diff"] = (second / level, second / level < 0.02)

        cdates, cvalues, ctruth = curved_seam_series()
        on = export.display_ma(cdates, cvalues, FORECAST_START, window=WINDOW)
        off = export.display_ma(cdates, cvalues, FORECAST_START, window=WINDOW,
                               continuous_splice=False)
        out["continuous_splice_anchor_equal"] = (
            abs(on.loc[FORECAST_START] - off.loc[FORECAST_START]),
            abs(on.loc[FORECAST_START] - off.loc[FORECAST_START]) < 1e-6)

        truth_s = pd.Series(ctruth.values, index=pd.to_datetime(cdates.values))
        truth_t = truth_s.rolling(WINDOW).mean()
        seg = on.loc[FORECAST_START:LAST_TRANSITION]
        mae_new = (seg - truth_t.reindex(seg.index)).abs().mean()
        line = pd.Series(np.linspace(seg.iloc[0], seg.iloc[-1], len(seg)), index=seg.index)
        mae_line = (line - truth_t.reindex(seg.index)).abs().mean()
        out["curved_beats_straight_line"] = (mae_new / max(mae_line, 1e-9),
                                             mae_new < mae_line / 3)
    return out


# --------------------------------------------------------------------------- real builds
BUILDS = {
    "Jun desktop": ("data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/"
                    "mozaic_daily_forecast.2026-05-26.ld-D.raw.parquet",
                    pd.Timestamp("2026-05-26"), "legacy_desktop", '{"os": "ALL"}', "desktop", []),
    "Jun mobile": ("data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/"
                   "mozaic_daily_forecast.2026-05-26.gm-D.adj-m.parquet",
                   pd.Timestamp("2026-05-26"), "glean_mobile", "{}", "ALL MOBILE", ["m"]),
    "Jul desktop": ("data-official/2026-07/desktop_locked/"
                    "mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet",
                    pd.Timestamp("2026-07-06"), "legacy_desktop", '{"os": "ALL"}', "desktop",
                    ["l", "o"]),
    "Jul mobile": ("data-official/2026-07/mobile_refresh_2026-07-06/"
                   "cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6/"
                   "mozaic_daily_forecast.2026-07-06.gm-D.adj-m.parquet",
                   pd.Timestamp("2026-07-06"), "glean_mobile", "{}", "ALL MOBILE", ["m"]),
    "Aug desktop": ("data-official/2026-08/desktop_locked/"
                    "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet",
                    pd.Timestamp("2026-07-28"), "legacy_desktop", '{"os": "ALL"}', "desktop",
                    ["l", "o"]),
    "Aug mobile": ("data-official/2026-08/mobile_baseline_2026-07-28/"
                   "cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/"
                   "mozaic_daily_forecast.2026-07-28.gm-D.adj-m.parquet",
                   pd.Timestamp("2026-07-28"), "glean_mobile", "{}", "ALL MOBILE", ["m"]),
}


def load_series(path, source, segment, app, state):
    df, _ = load_forecast(path, require_state=state)
    mask = ((df["country"] == "ALL") & (df["segment"] == segment)
            & (df["data_source"] == source) & (df["app_name"] == app))
    sub = df.loc[mask, ["target_date", "dau"]].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    sub = sub.sort_values("target_date")
    return pd.Series(sub["dau"].values, index=pd.DatetimeIndex(sub["target_date"]))


def main() -> int:
    print("=" * 96)
    print("M2b — would Fix A break any existing test? (thresholds from tests/, fixtures rebuilt)")
    print("=" * 96)
    results = {name: evaluate_tests(r) for name, r in VARIANTS.items()}
    test_names = list(next(iter(results.values())).keys())
    print(f"\n{'asserted quantity':38s} " + "".join(f"{n:>24s}" for n in VARIANTS))
    for t in test_names:
        row = f"{t:38s} "
        for name in VARIANTS:
            value, ok = results[name][t]
            row += f"{value:>16.6g} {'PASS' if ok else 'FAIL':>7s}"
        print(row)

    print("\n" + "=" * 96)
    print("M3 / M4 / M5 — real builds")
    print("=" * 96)
    for label, (path, seam, source, segment, app, state) in BUILDS.items():
        if not os.path.exists(path):
            print(f"\n{label}: parquet missing, skipped ({path})")
            continue
        series = load_series(path, source, segment, app, state)
        plain = series.rolling(WINDOW).mean()
        first_clean = seam + pd.Timedelta(days=WINDOW - 1)
        dec15 = pd.Timestamp(f"{seam.year}-12-15")
        dates, values = pd.Series(series.index), pd.Series(series.values)

        print(f"\n{label}  seam {seam.date()} ({seam.day_name()})")
        print(f"  {'variant':20s} {'day-1 err':>14s} {'seam step':>12s} {'vs plain':>10s} "
              f"{'Dec-15 delta':>13s} {'max shift in transition':>24s}")
        base_disp = None
        for name, reconstruct in VARIANTS.items():
            with patched_reconstructor(export, reconstruct):
                disp = export.display_ma(dates, values, seam, window=WINDOW)
                pre, fc = series[series.index < seam], series[series.index >= seam]
                matched = reconstruct(pre, fc, seam, WINDOW)
            if base_disp is None:
                base_disp = disp
            step = disp.loc[seam] - plain.loc[seam - pd.Timedelta(days=1)]
            plain_step = plain.loc[seam] - plain.loc[seam - pd.Timedelta(days=1)]
            trans = (disp.loc[seam:first_clean - pd.Timedelta(days=1)]
                     - base_disp.loc[seam:first_clean - pd.Timedelta(days=1)])
            far = disp[disp.index >= first_clean]
            far_delta = (far - base_disp.reindex(far.index)).abs().max()
            d15 = (disp.loc[dec15] - base_disp.loc[dec15]) if dec15 in disp.index else float("nan")
            print(f"  {name:20s} {matched.loc[seam] - fc.loc[seam]:>+14,.0f} {step:>+12,.0f} "
                  f"{step - plain_step:>+10,.0f} {d15:>+13,.2f} {trans.abs().max():>+24,.0f}"
                  f"{'' if far_delta < 1e-6 else f'  FAR-HORIZON MOVED {far_delta:,.2f}'}")
        print(f"  {'(plain 28d MA)':20s} {'—':>14s} {plain_step:>+12,.0f} {0:>+10,.0f}")

    print("\n" + "=" * 96)
    print("Ground-truthed estimator test — on all-actuals input the transition must be a NO-OP,")
    print("so every deviation from the plain rolling mean is pure estimator error.")
    print("=" * 96)
    crater = (pd.Timestamp("2026-02-15"), pd.Timestamp("2026-06-05"))
    for label in ["Aug desktop", "Aug mobile"]:
        path, seam, source, segment, app, state = BUILDS[label]
        df, _ = load_forecast(path, require_state=state)
        mask = ((df["country"] == "ALL") & (df["segment"] == segment)
                & (df["data_source"] == source) & (df["app_name"] == app)
                & (df["data_type"] == "training"))
        sub = df.loc[mask, ["target_date", "dau"]].copy()
        sub["target_date"] = pd.to_datetime(sub["target_date"])
        sub = sub.sort_values("target_date")
        actuals = pd.Series(sub["dau"].values, index=pd.DatetimeIndex(sub["target_date"]))
        truth = actuals.rolling(WINDOW).mean()
        dates, values = pd.Series(actuals.index), pd.Series(actuals.values)
        seams = [d for d in pd.date_range("2025-07-01",
                                          actuals.index.max() - pd.Timedelta(days=WINDOW))
                 if not (crater[0] <= d <= crater[1])]

        print(f"\n{label}  ({len(seams)} seam dates, Iran crater excluded)")
        print(f"  {'variant':20s} {'RMSE':>12s} {'mean |err|':>12s} {'max |err|':>12s} "
              f"{'weekday spread':>15s}")
        for name, reconstruct in VARIANTS.items():
            errs = []
            with patched_reconstructor(export, reconstruct):
                for s in seams:
                    got = export.display_ma(dates, values, s, window=WINDOW)
                    if pd.notna(got.get(s)) and pd.notna(truth.get(s)):
                        errs.append({"dow": s.dayofweek, "error": got.loc[s] - truth.loc[s]})
            e = pd.DataFrame(errs)
            by_dow = e.groupby("dow")["error"].mean()
            print(f"  {name:20s} {np.sqrt((e['error'] ** 2).mean()):>12,.0f} "
                  f"{e['error'].abs().mean():>12,.0f} {e['error'].abs().max():>12,.0f} "
                  f"{by_dow.max() - by_dow.min():>15,.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
