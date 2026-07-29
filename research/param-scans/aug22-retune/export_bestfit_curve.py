#!/usr/bin/env python3
"""Export the Aug-trough BEST-FIT desktop curve's 28d-MA to CSV (for the holiday-corrected module).

Best-fit = aug22-retune LHS sample s01 (multiplicative, cps=0.1849, cpr=0.734, recent=17, ncp=35),
with its corresponding Win10-headwind change (anchor -1,284,968; τ*=+85,032 on base -1,370,000, which
pins Dec-15 to the July forecast's 48,585,483). Same anchor 2026-07-06, legacy_desktop world DAU.

The forecast 28d-MA uses the variance-matched seam-smoothed `display_ma` (matches the canonical/orange
plot); the headwind linear ramp is added from the 2026-07-06 seam. Training rows (< 2026-07-06) are real
actuals — identical to any July forecast's training — so the full series spans 2020→2027 and is directly
sliceable by year for a year-over-year comparison.

Two population scopes, each in post-/pre-headwind:
  - global   = country=ALL (os=ALL)
  - ex_cn_ir = ALL − CN − IR (os=ALL); the scope the mean_shape best-fit was derived on.
The SAME desktop headwind ramp is added to both scopes (matches the shared DAU chart, where ex-CN/IR
post-headwind Aug-22 was 40.78M vs 41.53M pre).

Output columns: date, dau_28tma_global, dau_28tma_global_no_headwind, dau_28tma_ex_cn_ir,
dau_28tma_ex_cn_ir_no_headwind, point_type (actual before the seam / forecast on-and-after).
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

os.chdir(subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True).stdout.strip())
sys.path.insert(0, "data-official/2026-06")
from export_canonical_curves import display_ma  # noqa: E402  (seam-smoothed MA; matches canonical)

BESTFIT_PARQUET = ("research/param-scans/aug22-retune/sampling/s01/"
                   "cps0.1849_thresh032_recent17_cpr0.734_ncp35_clip0.6_sps0.00825_regimemultiplicative/"
                   "mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet")
FORECAST_START = pd.Timestamp("2026-07-06")
HW_START, HW_ANCHOR = pd.Timestamp("2026-04-01"), pd.Timestamp("2026-12-15")
HW_DESKTOP_ANCHOR = -1_284_968       # best-fit τ* headwind anchor
OUT_CSV = Path("research/param-scans/aug22-retune/bestfit_28tma_curve.csv")


def headwind_ramp(index: pd.DatetimeIndex) -> pd.Series:
    """Linear ramp: 0 at HW_START, HW_DESKTOP_ANCHOR at HW_ANCHOR (applied from FORECAST_START)."""
    total = (HW_ANCHOR - HW_START).days
    elapsed = np.maximum(0, (index - HW_START).days)
    ramp = pd.Series(HW_DESKTOP_ANCHOR * elapsed / total, index=index)
    ramp[index < FORECAST_START] = 0.0
    return ramp


def _daily(df: pd.DataFrame, country: str) -> pd.Series:
    """Daily DAU for (country, os=ALL), date-indexed."""
    m = ((df["country"] == country) & (df["segment"] == '{"os": "ALL"}')
         & (df["data_source"] == "legacy_desktop") & (df["app_name"] == "desktop"))
    s = df.loc[m, ["target_date", "dau"]].copy()
    s["target_date"] = pd.to_datetime(s["target_date"])
    return s.sort_values("target_date").set_index("target_date")["dau"].astype(float)


def _scope_ma(daily: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Return (pre-headwind, post-headwind) seam-smoothed 28d-MA for a daily series."""
    pre = display_ma(pd.Series(daily.index), pd.Series(daily.values), FORECAST_START)
    return pre, pre + headwind_ramp(pre.index)


def main() -> int:
    df = pd.read_parquet(BESTFIT_PARQUET)
    all_d = _daily(df, "ALL")
    cn = _daily(df, "CN").reindex(all_d.index).fillna(0.0)
    ir = _daily(df, "IR").reindex(all_d.index).fillna(0.0)
    ex_d = all_d - cn - ir

    g_pre, g_post = _scope_ma(all_d)
    e_pre, e_post = _scope_ma(ex_d)

    out = pd.DataFrame({
        "date": g_post.index.strftime("%Y-%m-%d"),
        "dau_28tma_global": g_post.round(0).astype("Int64"),
        "dau_28tma_global_no_headwind": g_pre.round(0).astype("Int64"),
        "dau_28tma_ex_cn_ir": e_post.round(0).astype("Int64"),
        "dau_28tma_ex_cn_ir_no_headwind": e_pre.round(0).astype("Int64"),
    })
    out["point_type"] = np.where(g_post.index < FORECAST_START, "actual", "forecast")
    out = out.dropna(subset=["dau_28tma_global"]).reset_index(drop=True)
    OUT_CSV.write_text(out.to_csv(index=False))

    at = pd.Timestamp("2026-08-22")
    dec = pd.Timestamp("2026-12-15")
    print(f"Wrote {OUT_CSV} ({len(out)} rows, {out['date'].iloc[0]}→{out['date'].iloc[-1]})")
    print(f"  global  Aug-22 {g_post.get(at):,.0f} / Dec-15 {g_post.get(dec):,.0f}")
    print(f"  ex_cn_ir Aug-22 {e_post.get(at):,.0f} / Dec-15 {e_post.get(dec):,.0f}  "
          f"(pre-hw Aug-22 {e_pre.get(at):,.0f})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
