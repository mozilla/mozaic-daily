"""Criteria 2 (mobile) and 4: does a trend-estimator change move anything delivered?

`display_ma` is shared by all three cycles' canonical notebooks, so a change to it moves
June's and July's published curves as well as August's. Criterion 4 requires every
delivered Dec-15 number to still reproduce exactly.

The transition zone is only the 27 days after each seam, and every delivered number is
Dec-15, so byte-identity is expected — but "expected" is how the original bug survived a
docstring assertion, so it is measured. Also completes criterion 2 by reporting the mobile
seam step, which the desktop-focused harness skipped.

    source .venv/bin/activate && python3 research/ma-seam-turbulence/check_delivered_numbers.py
"""

from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

GIT_ROOT = Path(subprocess.run(["git", "rev-parse", "--show-toplevel"],
                               capture_output=True, text=True).stdout.strip())
os.chdir(GIT_ROOT)
sys.path.insert(0, str(GIT_ROOT / "research/ma-seam-turbulence"))

from eval_recon_edge_fix import (DESKTOP_KEY, JUNE_MOBILE, JUNE_SEAM, MOBILE_KEY,  # noqa: E402
                                VARIANTS, WINDOW, daily_series, display, export,
                                headwind_ramp, seam_step)

DEC15 = pd.Timestamp("2026-12-15")

# Delivered Dec-15 figures, from each cycle's canonical notebook (LOG.md / handoff §7.4).
DELIVERED = {
    "june desktop no-Iran": (
        46_893_112,
        "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/"
        "mozaic_daily_forecast.2026-05-26.ld-D.raw.parquet",
        DESKTOP_KEY, JUNE_SEAM, "data-official/2026-06/adjustments", "desktop"),
    "june desktop plus-Iran": (
        47_834_362,
        "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/"
        "mozaic_daily_forecast.2026-05-26.ld-D.raw.plus_iran.parquet",
        DESKTOP_KEY, JUNE_SEAM, "data-official/2026-06/adjustments", "desktop"),
    "june mobile no-Iran": (
        16_911_773,
        "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/"
        "mozaic_daily_forecast.2026-05-26.gm-D.adj-m.parquet",
        MOBILE_KEY, JUNE_SEAM, "data-official/2026-06/adjustments", "mobile"),
    "june mobile plus-Iran": (
        17_511_100,
        "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/"
        "mozaic_daily_forecast.2026-05-26.gm-D.adj-m.plus_iran.parquet",
        MOBILE_KEY, JUNE_SEAM, "data-official/2026-06/adjustments", "mobile"),
    "july desktop": (
        48_585_483,
        "data-official/2026-07/desktop_locked/mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet",
        DESKTOP_KEY, pd.Timestamp("2026-07-06"), "data-official/2026-07/adjustments", "desktop"),
    "july mobile": (
        17_923_869,
        "data-official/2026-07/mobile_refresh_2026-07-06/"
        "cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6/"
        "mozaic_daily_forecast.2026-07-06.gm-D.adj-m.parquet",
        MOBILE_KEY, pd.Timestamp("2026-07-06"), "data-official/2026-07/adjustments", "mobile"),
}


def net_adjustment_at(adjustments_dir: str, date: pd.Timestamp, platform: str) -> float:
    """Sum every adjustment spec in a cycle's dir at one date, for one platform."""
    total = 0.0
    for path in sorted(glob.glob(f"{adjustments_dir}/*.json")):
        spec = json.loads(Path(path).read_text())
        if spec["type"] != "linear_ramp":
            raise ValueError(f"{path}: only linear_ramp is handled here, got {spec['type']!r}")
        total += headwind_ramp(date, {**spec, "desktop_dau": spec.get(f"{platform}_dau", 0)})
    return total


def main() -> int:
    print("CRITERION 4 — delivered Dec-15 numbers under each trend estimator\n")
    print(f"{'series':26s} {'delivered':>13s} " + " ".join(f"{n:>20s}" for n in VARIANTS))
    worst = 0.0
    for label, (delivered, path, key, seam, adj_dir, platform) in DELIVERED.items():
        if not Path(path).exists():
            print(f"{label:26s} {'MISSING':>13s}  {path}")
            continue
        series = daily_series(path, key)
        adj = net_adjustment_at(adj_dir, DEC15, platform)
        cells = []
        for name, recon in VARIANTS.items():
            value = display(series, seam, recon).loc[DEC15] + adj
            cells.append(f"{value - delivered:>+20,.0f}")
            worst = max(worst, abs(value - delivered))
        print(f"{label:26s} {delivered:>13,.0f} " + " ".join(cells))
    print(f"\n(cells are delta vs delivered; worst |delta| across all = {worst:,.2f})")

    print("\n\nCRITERION 2, mobile half — day-0 seam step\n")
    print(f"{'series':26s} " + " ".join(f"{n:>20s}" for n in VARIANTS))
    for label, (_, path, key, seam, _, _) in DELIVERED.items():
        if key is not MOBILE_KEY or not Path(path).exists():
            continue
        series = daily_series(path, key)
        cells = " ".join(f"{seam_step(series, seam, r):>+20,.0f}" for r in VARIANTS.values())
        print(f"{label:26s} {cells}")
    aug_mobile = ("data-official/2026-08/mobile_baseline_2026-07-28/"
                  "cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/"
                  "mozaic_daily_forecast.2026-07-28.gm-D.adj-m.parquet")
    if Path(aug_mobile).exists():
        series = daily_series(aug_mobile, MOBILE_KEY)
        cells = " ".join(f"{seam_step(series, pd.Timestamp('2026-07-28'), r):>+20,.0f}"
                         for r in VARIANTS.values())
        print(f"{'aug mobile baseline':26s} {cells}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
