"""What does reconstruct_matched_daily actually produce at the seam, and why?

`trend_fc = fc.rolling(7, center=True, min_periods=4).mean()` is computed on the FORECAST
ONLY. At the seam that "centered" window has no left half inside fc, so with min_periods=4
it degenerates to a forward mean over [seam, seam+3]. Check whether that edge behaviour,
combined with the day-of-week profile, is what shifts the seam value.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "data-official/2026-06"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from export_canonical_curves import reconstruct_matched_daily  # noqa: E402

SEAM = pd.Timestamp("2026-07-28")
BUILDS = {
    "canonical": REPO / "data-official/2026-08/desktop_baseline_2026-07-28"
    / "cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825"
    / "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet",
    "s01": REPO / "research/param-scans/summer-trough-v2/s01_gradient"
    / "cps0.1849_thresh032_recent17_cpr0.734_ncp35_clip0.6_sps0.00825_regimemultiplicative"
    / "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet",
}

for name, path in BUILDS.items():
    df, _ = load_forecast(str(path))
    sub = df[(df["country"] == "ALL") & (df["segment"] == '{"os": "ALL"}')].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    daily = sub.sort_values("target_date").set_index("target_date")["dau"].astype(float)
    daily = daily[~daily.index.duplicated(keep="last")]

    pre = daily[daily.index < SEAM]
    fc = daily[daily.index >= SEAM]
    matched = reconstruct_matched_daily(pre, fc, SEAM)

    print("=" * 74)
    print(f"{name}")
    print(f"{'date':12s} {'dow':4s} {'raw fc':>14s} {'reconstructed':>15s} {'diff':>13s}")
    for d in pd.date_range(SEAM, SEAM + pd.Timedelta(days=6)):
        print(f"{str(d.date()):12s} {d.day_name()[:3]:4s} {fc.loc[d]:>14,.0f} "
              f"{matched.loc[d]:>15,.0f} {matched.loc[d]-fc.loc[d]:>+13,.0f}")

    # Cumulative effect over the first 28 days -- what actually lands in the MA window.
    win = pd.date_range(SEAM, SEAM + pd.Timedelta(days=27))
    tot = (matched.reindex(win) - fc.reindex(win)).sum()
    print(f"  sum(reconstructed - raw) over the first 28 days: {tot:+,.0f} "
          f"(= {tot/28:+,.0f} on a 28d MA)")

    # The edge effect: trend_fc at the seam is a FORWARD 4-day mean, not centered.
    trend = fc.rolling(7, center=True, min_periods=4).mean()
    print(f"  trend_fc[seam]                 = {trend.loc[SEAM]:,.0f}  "
          f"(forward mean of first 4 days = {fc.iloc[:4].mean():,.0f})")
    print(f"  true centered 7d at seam would need 3 pre-seam days; "
          f"actuals mean {pre.iloc[-3:].mean():,.0f}")
    centered_true = pd.concat([pre.iloc[-3:], fc.iloc[:4]]).mean()
    print(f"  centered-with-actuals trend    = {centered_true:,.0f}  "
          f"(vs forecast-only {trend.loc[SEAM]:,.0f}: "
          f"{trend.loc[SEAM] - centered_true:+,.0f})")
