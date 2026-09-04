#!/usr/bin/env python3
"""Score a desktop forecast parquet at a near-horizon trough date (e.g. Aug-22).

The Aug-2026 desktop parameter search targets the 28-day trailing MA of the
world-headline ``legacy_desktop`` DAU at the summer trough, measured
post-headwind (display). This module computes that KPI (and the ex-CN/IR
variant, plus the Dec-15 side-effect) from a forecast parquet.

Two adjustment bases are reported for every metric:
- ``pre``  = adj-lo value as stored in the parquet (l/o overlays, no headwind).
- ``post`` = display value = ``pre`` + the linear Win10 headwind ramp at the date
  (the ramp is a level shift, applied to the 28d-MA the same way it is at the
  display layer in the canonical curves).

Two population scopes:
- ``global``   = country=ALL, os=ALL.
- ``ex_cn_ir`` = global minus CN minus IR (os=ALL) — checks the trough lift is
  not purely China (overlay ``o``) / Iran (fill) driven.

CLI
---
    source .venv/bin/activate
    python scripts/score_near_horizon.py \\
        data-official/2026-08/desktop_baseline_2026-07-28/\\
cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/\\
mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet

``DEFAULT_HEADWIND`` is **cycle-scoped** and must be repointed each forecast
cycle: both the amplitude and the ramp ``start_date`` change, and a stale spec
mis-scores silently (no error, just wrong numbers). Pass ``--headwind`` to score
a probe against a spec other than the current cycle's.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from mozaic_daily.seam_ma import display_ma  # noqa: E402

# NOTE ON COMPARABILITY (2026-07-29): this scorer previously imported display_ma from the June
# cycle dir. It now uses the package copy, which carries the A1 trend-estimator fix -- the old
# estimator inflated the first forecast day by up to ~10% of the weekday/weekend swing, which fed
# the near-horizon window. Scores produced before this change are therefore NOT comparable with
# scores produced after it. Aug-25-style targets sit a full MA_WINDOW past the seam and are
# unaffected, but any metric drawing on the 27-day transition has moved. Re-baseline before
# comparing across that boundary. See research/ma-seam-turbulence/LOG.md, Fix A.

# The trough MINIMUM, not July's Aug-22. Aug-25 is exactly MA_WINDOW days past the
# 2026-07-28 seam, so its window is entirely forecast and the value is independent of
# the display_ma splice convention -- unlike Aug-22, which sits inside the transition
# zone and reads ~41K apart under the two conventions.
DEFAULT_TARGET_DATE = "2026-08-25"
DEFAULT_DEC15 = "2026-12-15"
DEFAULT_HEADWIND = REPO_ROOT / "data-official/2026-09/adjustments/headwind.json"  # repointed 2026-09-04; now a clamped ramp, desktop-only
# August target is a BAND, not a bullseye: "around 45M to 46M, exact target depends on
# what's possible" (2026-07-29). July's 45.06M bullseye is retired -- it was the most
# achievable value under July data, not an external benchmark.
TARGET_BAND = (45_000_000, 46_000_000)
OS_ALL = '{"os": "ALL"}'
MA_WINDOW = 28
# Days either side of the seam used to fit the reported slope match.
SEAM_SLOPE_WINDOW = 14


def _daily_series(df: pd.DataFrame, country: str) -> pd.Series:
    """Continuous daily DAU series for (country, os=ALL), training+forecast merged."""
    sub = df[(df["country"] == country) & (df["segment"] == OS_ALL)].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    sub = sub.sort_values("target_date").set_index("target_date")["dau"].astype(float)
    return sub[~sub.index.duplicated(keep="last")]


def _headwind_ramp(date: pd.Timestamp, spec: dict) -> float:
    """Linear ramp: 0 at start_date, spec['desktop_dau'] at anchor_date (clamped)."""
    start = pd.Timestamp(spec["start_date"])
    anchor = pd.Timestamp(spec["anchor_date"])
    full = float(spec["desktop_dau"])
    if date <= start:
        return 0.0
    if date >= anchor:
        return full
    return full * (date - start).days / (anchor - start).days


def score_dataframe(
    df: pd.DataFrame,
    target_date: str = DEFAULT_TARGET_DATE,
    headwind_spec: dict | None = None,
    forecast_start: str | pd.Timestamp | None = None,
) -> dict:
    """Pure scorer over a forecast dataframe (no file I/O).

    ``df`` must have the pipeline output columns (country, segment, target_date,
    dau). ``headwind_spec`` is the parsed headwind.json dict (linear_ramp).
    Reports global and ex-CN/IR scopes, each in pre-/post-headwind bases, at the
    target trough date and Dec-15.

    The MA is the variance-matched ``display_ma``, matching the canonical
    notebook. This matters only inside the 27 days after the seam, where the
    window straddles actuals and forecast -- and the Aug-22 target date sits in
    that zone (a plain ``rolling(28)`` reads ~41K low there). ``forecast_start``
    defaults to the parquet's own ``forecast_start_date`` column.
    """
    spec = headwind_spec or {}
    if forecast_start is None:
        forecast_start = pd.Timestamp(df["forecast_start_date"].iloc[0])

    global_series = _daily_series(df, "ALL")
    cn = _daily_series(df, "CN").reindex(global_series.index).fillna(0.0)
    ir = _daily_series(df, "IR").reindex(global_series.index).fillna(0.0)
    ex_series = global_series - cn - ir

    forecast_start = pd.Timestamp(forecast_start)
    ma = {
        scope: display_ma(series.index.to_series(), series,
                          forecast_start, window=MA_WINDOW)
        for scope, series in [("global", global_series), ("ex_cn_ir", ex_series)]
    }

    out: dict = {"target_date": target_date}
    for label, date in [("target", target_date), ("dec15", DEFAULT_DEC15)]:
        d = pd.Timestamp(date)
        hw = _headwind_ramp(d, spec) if spec else 0.0
        for scope, series_ma in ma.items():
            pre = float(series_ma.loc[d])
            out[f"{scope}_{label}_pre"] = pre
            out[f"{scope}_{label}_post"] = pre + hw
        out[f"headwind_{label}"] = hw

    # Where the post-headwind trough actually bottoms out. A shape change can move the
    # argmin off the scored date, so report it rather than assuming it stays put.
    summer = ma["global"].loc[forecast_start:pd.Timestamp("2026-10-15")].dropna()
    summer_post = summer + pd.Series(
        [_headwind_ramp(d, spec) if spec else 0.0 for d in summer.index], index=summer.index)
    out["trough_min_post"] = float(summer_post.min())
    out["trough_min_date"] = str(summer_post.idxmin().date())

    lo, hi = TARGET_BAND
    gt = out["global_target_post"]
    out["in_band"] = lo <= gt <= hi
    out["gap_to_band_low"] = gt - lo

    out.update(seam_derivatives(ma["global"], forecast_start, spec))
    return out


def seam_derivatives(
    series_ma: pd.Series,
    forecast_start: pd.Timestamp,
    spec: dict | None = None,
    window: int = SEAM_SLOPE_WINDOW,
) -> dict:
    """Slope of the display 28d-MA either side of the seam, in DAU/day (reported, not scored).

    A well-behaved forecast hands off to the actuals with a matching first
    derivative. Two distinct contributions to any mismatch, reported separately:

    - ``model``: the slope kink in the forecast itself (pre-headwind). This is
      what parameters can move.
    - ``headwind``: the ramp contributes 0 slope before the seam and a constant
      ``desktop_dau / (anchor - start)`` after it, so the display curve carries a
      slope kink of exactly that size even though the re-anchored ramp removed
      the level step. Not addressable by parameters.

    Slopes are OLS fits over ``window`` days each side. The post-seam side sits
    inside the splice transition zone, so the ``model`` figure reflects the
    spliced curve the reader actually sees, not the raw forecast-only MA.
    """
    def slope(start: pd.Timestamp, end: pd.Timestamp) -> float:
        seg = series_ma.loc[start:end].dropna()
        if len(seg) < 3:
            return float("nan")
        days = (seg.index - seg.index[0]).days.to_numpy(dtype=float)
        return float(np.polyfit(days, seg.to_numpy(dtype=float), 1)[0])

    before = slope(forecast_start - pd.Timedelta(days=window), forecast_start - pd.Timedelta(days=1))
    after = slope(forecast_start, forecast_start + pd.Timedelta(days=window - 1))

    hw_slope = 0.0
    if spec:
        span = (pd.Timestamp(spec["anchor_date"]) - pd.Timestamp(spec["start_date"])).days
        hw_slope = float(spec["desktop_dau"]) / span

    return {
        "seam_slope_before": before,
        "seam_slope_after_model": after,
        "seam_slope_after_display": after + hw_slope,
        "seam_slope_kink_model": after - before,
        "seam_slope_kink_display": after + hw_slope - before,
        "seam_slope_headwind": hw_slope,
    }


def score_parquet(
    parquet_path: str | Path,
    target_date: str = DEFAULT_TARGET_DATE,
    headwind_spec_path: str | Path = DEFAULT_HEADWIND,
) -> dict:
    """Score a forecast parquet on disk (loads via ``load_forecast`` + headwind spec)."""
    df, _meta = load_forecast(str(parquet_path))
    spec = json.loads(Path(headwind_spec_path).read_text())
    out = score_dataframe(df, target_date=target_date, headwind_spec=spec)
    out["parquet"] = str(parquet_path)
    return out


def _fmt(v: float) -> str:
    return f"{v:,.0f}"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("parquet", type=Path)
    p.add_argument("--target-date", default=DEFAULT_TARGET_DATE)
    p.add_argument("--headwind", type=Path, default=DEFAULT_HEADWIND)
    p.add_argument("--json", action="store_true", help="Emit raw JSON instead of a table.")
    args = p.parse_args()

    r = score_parquet(args.parquet, args.target_date, args.headwind)
    if args.json:
        print(json.dumps(r, indent=2))
        return 0

    print(f"parquet     : {r['parquet']}")
    print(f"target date : {r['target_date']}   (headwind {_fmt(r['headwind_target'])})")
    print(f"{'scope':10s} {'trough pre':>16s} {'trough post':>16s} {'dec15 pre':>16s} {'dec15 post':>16s}")
    for scope in ("global", "ex_cn_ir"):
        print(f"{scope:10s} "
              f"{_fmt(r[f'{scope}_target_pre']):>16s} "
              f"{_fmt(r[f'{scope}_target_post']):>16s} "
              f"{_fmt(r[f'{scope}_dec15_pre']):>16s} "
              f"{_fmt(r[f'{scope}_dec15_post']):>16s}")
    lo, hi = TARGET_BAND
    band = "IN BAND" if r["in_band"] else "out of band"
    print(f"\ntrough minimum : {_fmt(r['trough_min_post'])} on {r['trough_min_date']}")
    print(f"target band    : {lo/1e6:.0f}M-{hi/1e6:.0f}M  [{band}, "
          f"{r['gap_to_band_low']:+,.0f} vs the {lo/1e6:.0f}M floor]")

    # Reported, not scored: we want the handoff derivative to match where it can.
    print("\nseam slope (DAU/day, 14d OLS each side of the seam) -- reported, not scored")
    print(f"  before (actuals)      : {r['seam_slope_before']:+,.0f}")
    print(f"  after, model only     : {r['seam_slope_after_model']:+,.0f}"
          f"   kink {r['seam_slope_kink_model']:+,.0f}")
    print(f"  after, with headwind  : {r['seam_slope_after_display']:+,.0f}"
          f"   kink {r['seam_slope_kink_display']:+,.0f}")
    print(f"  headwind contribution : {r['seam_slope_headwind']:+,.0f}  (not parameter-addressable)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
