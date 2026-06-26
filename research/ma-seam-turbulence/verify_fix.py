"""Verify the display_ma seam fix: AR seam smoothed, US ~unchanged, Dec-15 byte-identical.

Imports the real `display_ma` from export_canonical_curves.py and compares it against the
old blended 28dMA, straight from the parquet (no BigQuery, no regeneration needed).

    source .venv/bin/activate && python3 research/ma-seam-turbulence/verify_fix.py
"""

import importlib.util
import os
import subprocess

import numpy as np
import pandas as pd

GIT_ROOT = subprocess.run(
    ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
).stdout.strip()
os.chdir(GIT_ROOT)

EXPORT_PY = "data-official/2026-06/export_canonical_curves.py"
spec = importlib.util.spec_from_file_location("export_canonical_curves", EXPORT_PY)
export = importlib.util.module_from_spec(spec)
spec.loader.exec_module(export)

PARQUET = (
    "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/"
    "mozaic_daily_forecast.2026-05-26.ld-D.raw.parquet"
)
FORECAST_START = pd.Timestamp("2026-05-26")
DEC15 = pd.Timestamp("2026-12-15")
SEAM_CLEAR = FORECAST_START + pd.Timedelta(days=27)


def load_country(df, country):
    mask = (
        (df["country"] == country)
        & (df["segment"] == '{"os": "ALL"}')
        & (df["data_source"] == "legacy_desktop")
        & (df["app_name"] == "desktop")
    )
    sub = df.loc[mask, ["target_date", "dau"]].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    return sub.sort_values("target_date").reset_index(drop=True)


def rel_2nd_diff_ppm(series, start, end):
    w = series[(series.index >= start) & (series.index <= end)].dropna()
    return 1e6 * w.diff().diff().abs().mean() / w.mean()


def main():
    df = pd.read_parquet(PARQUET)
    print(f"{'country':>8} | {'old wk0-3 ppm':>13} | {'new wk0-3 ppm':>13} | "
          f"{'Dec-15 old':>13} | {'Dec-15 new':>13} | {'Dec-15 delta':>12}")
    print("-" * 92)
    all_ok = True
    for country in ["AR", "US", "BR", "IN", "CA"]:
        daily = load_country(df, country)
        old = export.daily_to_28ma(daily["target_date"], daily["dau"])         # blend (pre-fix)
        new = export.display_ma(daily["target_date"], daily["dau"], FORECAST_START)  # post-fix

        old_wk = rel_2nd_diff_ppm(old, FORECAST_START, SEAM_CLEAR)
        new_wk = rel_2nd_diff_ppm(new, FORECAST_START, SEAM_CLEAR)
        dec_old = old.loc[DEC15]
        dec_new = new.loc[DEC15]
        delta = dec_new - dec_old

        # Invariant: every date >= seam+27 must be byte-identical (the fix only touches
        # the first 27 forecast days).
        post = new.index >= SEAM_CLEAR
        max_post_delta = (new[post] - old.reindex(new.index)[post]).abs().max()
        ok = abs(delta) < 1e-6 and max_post_delta < 1e-6
        all_ok = all_ok and ok
        flag = "OK" if ok else "FAIL"
        print(f"{country:>8} | {old_wk:>13,.0f} | {new_wk:>13,.0f} | "
              f"{dec_old:>13,.1f} | {dec_new:>13,.1f} | {delta:>12,.4f}  {flag}")

    print("-" * 92)
    print(f"All dates >= seam+27 byte-identical AND Dec-15 unchanged: {all_ok}")
    print("(Expect AR/BR/IN wk0-3 ppm to drop sharply; US/CA ~unchanged; deltas == 0.)")


if __name__ == "__main__":
    main()
