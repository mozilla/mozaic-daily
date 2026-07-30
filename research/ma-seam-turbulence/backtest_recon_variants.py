"""Score each trend estimator against realized actuals — the empirical arbiter.

The geometric splice metrics disagree with each other (§ `diagnose_splice_metric.py`) and
with the as-shipped configuration, so they cannot settle which estimator is better. A
backtest against what actually happened can.

June used a single April seam for this. April's parquets have since been archived to GCS,
but a **better** test is available on disk: four `.raw.` June-cycle desktop builds with
seams on 2026-05-17 (Sun), 05-21 (Thu), 05-26 (Tue) and 05-28 (Thu), and realized actuals
through 2026-07-27 in the raw August build's training rows. That covers every transition
window in full, and — unlike a single seam — it spans **both** bias regimes: a Mon/Tue seam
makes the current estimator read high (its 4-day window is all weekdays), a Thu/Fri seam
makes it read low (2 of 4 days are weekend). A fix that only works for one regime is not a
fix, and one seam cannot tell the difference.

Metric and gate are June's, unchanged: bias-removed (shape) MAE over transition days 1..27
at the ALL level, compared against the OLD straight linear bridge as the common reference.

    source .venv/bin/activate && python3 research/ma-seam-turbulence/backtest_recon_variants.py
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
    "backtest_seam", GIT_ROOT / "research/ma-seam-turbulence/backtest_seam.py")
backtest = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(backtest)
export = backtest.export

from eval_recon_edge_fix import DESKTOP_KEY, WINDOW, daily_series, display  # noqa: E402
from recon_variants import make_reconstructor  # noqa: E402

JUNE_DIR = "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426"
SEAMS = ["2026-05-17", "2026-05-21", "2026-05-26", "2026-05-28"]

# Realized truth must be RAW actuals. The August canonical build is `.adj-lo.`, whose
# overlays subtract lift from modern_windows *training* rows — so its training rows are not
# actuals. The adjustment-isolation `none` build is the same data with no overlays.
TRUTH = ("data-official/2026-08/adjustment_isolation/none/"
         "cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/"
         "mozaic_daily_forecast.2026-07-28.ld-D.raw.parquet")

VARIANTS = {
    "OLD straight bridge": None,  # June's pre-2B reference
    "current": make_reconstructor("current"),
    "forward7": make_reconstructor("forward7"),
    "forward7 + dow fix": make_reconstructor("forward7", True),
    "concat (rejected)": make_reconstructor("concat"),
}
COUNTRIES = ["ALL", "AR", "BR", "CN", "IN", "US", "ROW"]


def realized_28ma(country: str) -> pd.Series:
    """28dMA of the real daily actuals (raw August build's training rows)."""
    df = pd.read_parquet(TRUTH)
    mask = ((df["country"] == country) & (df["segment"] == DESKTOP_KEY["segment"])
            & (df["data_source"] == DESKTOP_KEY["data_source"])
            & (df["app_name"] == DESKTOP_KEY["app_name"]) & (df["data_type"] == "training"))
    sub = df.loc[mask, ["target_date", "dau"]].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    sub = sub.sort_values("target_date")
    return export.daily_to_28ma(sub["target_date"], sub["dau"])


def score(country: str, seam: pd.Timestamp, truth: pd.Series) -> dict:
    """Shape MAE of each variant's transition vs realized, over days 1..27."""
    path = f"{JUNE_DIR}/mozaic_daily_forecast.{seam.date()}.ld-D.raw.parquet"
    series = daily_series(path, DESKTOP_KEY, country)
    if series.empty:
        return {}
    end = seam + pd.Timedelta(days=WINDOW - 2)

    def window(s):
        return s[(s.index >= seam) & (s.index <= end)]

    realized = window(truth).dropna()
    out = {}
    for name, recon in VARIANTS.items():
        ma = (backtest._display_ma_linear(series.index.to_series(), series, seam, WINDOW)
              if recon is None else display(series, seam, recon))
        est = window(ma)
        common = est.dropna().index.intersection(realized.index)
        if len(common) < WINDOW - 2:
            return {}
        out[name] = backtest.shape_mae(est[common], realized[common])
    return out


def main() -> int:
    print("Realized backtest — desktop transition shape MAE vs actuals (days 1..27).")
    print(f"Truth: raw actuals from {Path(TRUTH).name} training rows.\n")

    rows = []
    for seam_str in SEAMS:
        seam = pd.Timestamp(seam_str)
        for country in COUNTRIES:
            truth = realized_28ma(country)
            if truth.dropna().empty:
                continue
            scored = score(country, seam, truth)
            if scored:
                rows.append({"seam": seam_str, "dow": seam.day_name()[:3],
                             "country": country, **scored})
    table = pd.DataFrame(rows)

    print("=" * 100)
    print("ALL-level (June's gate) — shape MAE, and % improvement vs the straight bridge")
    print("=" * 100)
    alls = table[table["country"] == "ALL"]
    header = f"  {'seam':12s} {'dow':4s} " + " ".join(f"{n:>20s}" for n in VARIANTS)
    print(header)
    for _, r in alls.iterrows():
        cells = " ".join(f"{r[n]:>20,.0f}" for n in VARIANTS)
        print(f"  {r['seam']:12s} {r['dow']:4s} {cells}")
    print(f"\n  {'mean':12s} {'':4s} " + " ".join(f"{alls[n].mean():>20,.0f}" for n in VARIANTS))
    base = alls["OLD straight bridge"].mean()
    print(f"  {'vs bridge':12s} {'':4s} "
          + " ".join(f"{1 - alls[n].mean() / base:>+19.1%}" for n in VARIANTS))

    print("\n" + "=" * 100)
    print("Per-country mean over the four seams (diagnostic, non-gating)")
    print("=" * 100)
    print(f"  {'country':8s} " + " ".join(f"{n:>20s}" for n in VARIANTS))
    for country in COUNTRIES:
        sub = table[table["country"] == country]
        if sub.empty:
            continue
        print(f"  {country:8s} " + " ".join(f"{sub[n].mean():>20,.0f}" for n in VARIANTS))

    out = GIT_ROOT / "research/ma-seam-turbulence/plots/backtest_recon_variants.csv"
    table.to_csv(out, index=False)
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
