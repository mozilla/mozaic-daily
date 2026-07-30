"""Is June's day-27 splice metric measuring a discontinuity, or a slope?

`eval_recon_edge_fix.py` reproduces June's published splice figures (ALL 0.086%, AR 0.99%,
BR 0.74%, US 0.031%; rejected fix 0.698%) with the *visible* day-27 -> day-28 step:

    visible = forecast_only_ma[day28] - transition_ma[day27]

But that difference is one day apart, so it contains the curve's genuine one-day slope as
well as any discontinuity:

    visible = -landing_residual + transition_one_day_slope

If the transition is rising steeply, a large landing residual of the opposite sign can
cancel the slope and *look* smooth. That is the same coincidental-cancellation trap that
made the superseded build's +5,157 seam step look like continuity.

This decomposes `visible` into its two terms per variant, and reports a slope-invariant
alternative (the 2nd difference across the handoff on the uncorrected curve).

    source .venv/bin/activate && python3 research/ma-seam-turbulence/diagnose_splice_metric.py
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

GIT_ROOT = Path(subprocess.run(["git", "rev-parse", "--show-toplevel"],
                               capture_output=True, text=True).stdout.strip())
os.chdir(GIT_ROOT)
sys.path.insert(0, str(GIT_ROOT / "research/ma-seam-turbulence"))

_spec = importlib.util.spec_from_file_location(
    "export_canonical_curves", GIT_ROOT / "data-official/2026-06/export_canonical_curves.py")
export = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export)

from eval_recon_edge_fix import (DESKTOP_KEY, JUNE_DESKTOP, JUNE_SEAM, VARIANTS,  # noqa: E402
                                WINDOW, daily_series, display)
from recon_variants import patched_reconstructor  # noqa: E402


def decompose(series: pd.Series, seam: pd.Timestamp, reconstruct) -> dict:
    """Split the visible day-27 step into (landing residual, one-day slope)."""
    first_clean = seam + pd.Timedelta(days=WINDOW - 1)
    prev_day = first_clean - pd.Timedelta(days=1)
    pre, fc = series[series.index < seam], series[series.index >= seam]

    with patched_reconstructor(export, reconstruct):
        matched = export.reconstruct_matched_daily(pre, fc, seam, WINDOW)
    transition = pd.concat([pre, matched]).sort_index().rolling(WINDOW).mean()
    forecast_only = fc.rolling(WINDOW).mean()

    uncorrected = display(series, seam, reconstruct, continuous_splice=False)
    level = uncorrected.loc[seam:first_clean].mean()

    landing = transition.loc[first_clean] - forecast_only.loc[first_clean]
    slope = transition.loc[first_clean] - transition.loc[prev_day]
    visible = forecast_only.loc[first_clean] - transition.loc[prev_day]

    # Slope-invariant: the 2nd difference across the handoff on the curve as displayed
    # without the cubic correction. A pure level step shows up here; a smooth slope does not.
    d = first_clean
    corner = abs((uncorrected.loc[d + pd.Timedelta(days=1)] - uncorrected.loc[d])
                 - (uncorrected.loc[d] - uncorrected.loc[d - pd.Timedelta(days=1)]))

    return {"level": level, "landing": landing, "slope": slope, "visible": visible,
            "corner_uncorrected": corner,
            "landing_pct": 100 * landing / level, "slope_pct": 100 * slope / level,
            "visible_pct": 100 * visible / level, "corner_pct": 100 * corner / level}


def main() -> int:
    print("June desktop -- decomposing the day-27 'splice step' (% of level)")
    print("visible = -landing + slope, so a rising transition can mask a landing error.\n")
    for country in ["ALL", "AR", "BR", "US", "CN", "IN"]:
        series = daily_series(JUNE_DESKTOP, DESKTOP_KEY, country)
        if series.empty:
            continue
        print(f"{country}")
        print(f"  {'variant':20s} {'landing':>10s} {'+ slope':>10s} {'= visible':>10s} "
              f"{'| corner':>10s}   (June published)")
        for name, recon in VARIANTS.items():
            r = decompose(series, JUNE_SEAM, recon)
            print(f"  {name:20s} {-r['landing_pct']:>+9.3f}% {r['slope_pct']:>+9.3f}% "
                  f"{r['visible_pct']:>+9.3f}% {r['corner_pct']:>9.3f}%")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
