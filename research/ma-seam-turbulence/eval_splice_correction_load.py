"""How hard does `continuous_splice` have to work under each trend estimator?

June's day-27 splice metric was measured on a curve with NO cubic correction — that
correction (`continuous_splice`, added in July) did not exist when the concatenated-trend
fix was rejected. It forces the transition to land exactly on the forecast-only MA and
matches 40% of the slope residual, so it absorbs whatever landing error the reconstruction
leaves behind.

That reframes the question. The correction is a cosmetic bend applied to the displayed
curve; the bigger the landing error it must absorb, the more the published transition is
bent away from what the reconstruction actually produced. So the relevant quantity is not
only "is the uncorrected handoff smooth" but "how much distortion does shipping require".

Reports, per variant: the landing error the correction must absorb, the resulting maximum
deviation between the corrected and uncorrected curves, and the handoff corner that
actually ships.

    source .venv/bin/activate && python3 research/ma-seam-turbulence/eval_splice_correction_load.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

GIT_ROOT = Path(subprocess.run(["git", "rev-parse", "--show-toplevel"],
                               capture_output=True, text=True).stdout.strip())
os.chdir(GIT_ROOT)
sys.path.insert(0, str(GIT_ROOT / "research/ma-seam-turbulence"))

from eval_recon_edge_fix import (AUG_BUILDS, AUG_SEAM, DESKTOP_KEY, JUNE_DESKTOP,  # noqa: E402
                                JUNE_MOBILE, JUNE_SEAM, MOBILE_KEY, VARIANTS, WINDOW,
                                daily_series, display)

CASES = {
    "june desktop ALL": (JUNE_DESKTOP, DESKTOP_KEY, JUNE_SEAM),
    "june mobile ALL": (JUNE_MOBILE, MOBILE_KEY, JUNE_SEAM),
    "aug desktop s01": (AUG_BUILDS["s01 (locked)"], DESKTOP_KEY, AUG_SEAM),
    "aug desktop superseded": (AUG_BUILDS["superseded"], DESKTOP_KEY, AUG_SEAM),
}


def correction_load(series: pd.Series, seam: pd.Timestamp, reconstruct) -> dict:
    """Deviation the cubic splice correction imposes on the displayed transition."""
    first_clean = seam + pd.Timedelta(days=WINDOW - 1)
    off = display(series, seam, reconstruct, continuous_splice=False)
    on = display(series, seam, reconstruct, continuous_splice=True)
    window = (on.index >= seam) & (on.index < first_clean)
    level = off.loc[seam:first_clean].mean()

    def corner(ma):
        d = first_clean
        return abs((ma.loc[d + pd.Timedelta(days=1)] - ma.loc[d])
                   - (ma.loc[d] - ma.loc[d - pd.Timedelta(days=1)]))

    return {"max_bend": float((on[window] - off[window]).abs().max()),
            "max_bend_pct": 100 * float((on[window] - off[window]).abs().max()) / level,
            "corner_shipped_pct": 100 * corner(on) / level}


def main() -> int:
    print("Distortion the shipped continuous_splice correction must apply (transition days 0..26)\n")
    for label, (path, key, seam) in CASES.items():
        series = daily_series(path, key, "ALL")
        print(f"{label}")
        print(f"  {'variant':20s} {'max bend':>12s} {'as % level':>11s} {'shipped corner':>15s}")
        for name, recon in VARIANTS.items():
            r = correction_load(series, seam, recon)
            print(f"  {name:20s} {r['max_bend']:>12,.0f} {r['max_bend_pct']:>10.3f}% "
                  f"{r['corner_shipped_pct']:>14.4f}%")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
