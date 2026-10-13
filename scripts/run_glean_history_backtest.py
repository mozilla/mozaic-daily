#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Backtest the Glean-history forecast at historical seams and score forecast error by horizon.

Why: the Prophet predictive band is unusable at a 15-month horizon (PROCEDURE.html build-3 note), so the
deliverable's forecast uncertainty is an empirical one — how far forecasts made with the locked configuration
from past seams missed the Glean-equivalent history that followed (decision of 2026-10-12).

``run``   launches ``run_glean_history_forecast.py`` for each seam (same raw inputs, same config, the Iran fill
          passed through — it only matters for seams before the 2026 crater; the China correction is not passed
          because the June-2026 step is after every backtest seam) with a concurrency limit.
``score`` reads each seam's parquet, takes the world series (MAU daily, DAU 28-day trailing mean), compares the
          forecast with the history at every horizon day, and writes ``errors_by_horizon.csv`` (long), a summary
          at the headline horizon (the seam's own "Dec 15 of the following year", ~431 days) and at 30-day
          horizon steps, and a plot of the error envelope vs horizon with the per-seam paths.

Usage:
    python scripts/run_glean_history_backtest.py run --seams 2022-04-10 2022-10-10 ... --jobs 3
    python scripts/run_glean_history_backtest.py score
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
if str(repo_root / "src") not in sys.path:
    sys.path.insert(0, str(repo_root / "src"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402

DRAFT = repo_root / "data-official/2027-glean-draft"
BACKTEST_DIR = DRAFT / "backtest"
RAW_DIR = DRAFT / "raw_glean_history_2026-10-09"
IRAN_FILL = DRAFT / "iran_fill/iran_fill.glean_history_desktop.parquet"
DEFAULT_SEAMS = ["2022-04-10", "2022-10-10", "2023-04-10", "2023-10-10", "2024-04-10", "2024-10-10", "2025-04-10", "2025-10-10"]
LOCKED_KNOBS = ["--changepoint-range", "0.87"]          # LOCK.md: g01 + cpr 0.87
WORLD_SEG = json.dumps({"os": "ALL"})
HEADLINE_HORIZON_DAYS = (pd.Timestamp("2027-12-15") - pd.Timestamp("2026-10-10")).days   # 431
LEVELS = (0.5, 0.8, 0.9)


def run_one(seam: str) -> int:
    out = BACKTEST_DIR / f"seam_{seam}"
    out.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(repo_root / "scripts/run_glean_history_forecast.py"), "--forecast-start-date", seam,
           "--raw-dir", str(RAW_DIR), "--iran-fill", str(IRAN_FILL), "--results-dir", str(out), *LOCKED_KNOBS]
    with open(out / "run.log", "w") as log:
        return subprocess.call(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=repo_root)


def cmd_run(args):
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for seam, code in zip(args.seams, pool.map(run_one, args.seams)):
            print(f"seam {seam}: exit {code}")


def world_series(parquet: Path):
    df, meta = load_forecast(str(parquet))
    df["target_date"] = pd.to_datetime(df["target_date"])
    w = df[(df["country"] == "ALL") & (df["segment"] == WORLD_SEG)].set_index("target_date").sort_index()
    return w, pd.Timestamp(meta["forecast_start_date"])


def truth_series() -> dict[str, pd.Series]:
    """The Glean-equivalent history as the model saw it (the adapter's raw frames), world totals."""
    out = {}
    for metric in ("DAU", "MAU"):
        raw = pd.read_parquet(RAW_DIR / f"mozaic_parts.raw.glean_history.desktop.{metric}.parquet")
        raw["x"] = pd.to_datetime(raw["x"])
        s = raw.groupby("x")["y"].sum().astype(float).sort_index()
        out[metric] = s.rolling(28).mean() if metric == "DAU" else s
    return out


def cmd_score(args):
    truth = truth_series()
    rows = []
    for parquet in sorted(BACKTEST_DIR.glob("seam_*/cps*/mozaic_daily_forecast.*.parquet")):
        w, seam = world_series(parquet)
        for metric, col in (("DAU", "dau"), ("MAU", "mau")):
            fc = w.loc[w["data_type"] == "forecast", col]
            # DAU: trailing 28-day mean over history + forecast so the first window is well defined
            if metric == "DAU":
                full = pd.concat([w.loc[w["data_type"] == "training", col], fc]).rolling(28).mean()
                fc = full[full.index >= seam]
            common = fc.index.intersection(truth[metric].dropna().index)
            err = (fc.loc[common] / truth[metric].loc[common] - 1.0)
            for d, e in err.items():
                rows.append({"seam": seam.date().isoformat(), "metric": metric, "date": d.date().isoformat(),
                             "horizon_days": (d - seam).days, "forecast": fc.loc[d], "actual": truth[metric].loc[d], "rel_error": e})
    long = pd.DataFrame(rows)
    BACKTEST_DIR.mkdir(exist_ok=True)
    long.to_csv(BACKTEST_DIR / "errors_by_horizon.csv", index=False)

    # Summary at 30-day steps and the headline horizon
    summary = []
    for metric in ("DAU", "MAU"):
        m = long[long["metric"] == metric]
        for h in list(range(30, 451, 30)) + [HEADLINE_HORIZON_DAYS]:
            at = m[(m["horizon_days"] >= h - 3) & (m["horizon_days"] <= h + 3)].groupby("seam")["rel_error"].mean()
            if at.empty:
                continue
            row = {"metric": metric, "horizon_days": h, "n_seams": int(at.size), "mean": at.mean(), "median": at.median(),
                   "rmse": float(np.sqrt((at ** 2).mean())), "min": at.min(), "max": at.max()}
            for lvl in LEVELS:
                row[f"abs_err_p{int(lvl * 100)}"] = float(at.abs().quantile(lvl))
            summary.append(row)
    summary = pd.DataFrame(summary)
    summary.to_csv(BACKTEST_DIR / "errors_summary.csv", index=False)

    # Plot: per-seam relative error vs horizon, with the |error| envelope
    fig, axes = plt.subplots(1, 2, figsize=(18, 6))
    for ax, metric in zip(axes, ("DAU", "MAU")):
        m = long[long["metric"] == metric]
        for seam, g in m.groupby("seam"):
            ax.plot(g["horizon_days"], g["rel_error"] * 100, lw=1, alpha=0.8, label=seam)
        env = m.groupby("horizon_days")["rel_error"].apply(lambda s: s.abs().quantile(0.9)) * 100
        ax.plot(env.index, env.values, color="black", lw=2, label="90th pct of |error|")
        ax.plot(env.index, -env.values, color="black", lw=2)
        ax.axvline(HEADLINE_HORIZON_DAYS, color="red", ls=":", label=f"headline horizon ({HEADLINE_HORIZON_DAYS} d)")
        ax.axhline(0, color="grey", lw=0.8)
        ax.set_title(f"{metric}: forecast vs Glean-equivalent history, % error by horizon (locked config, {m['seam'].nunique()} seams)")
        ax.set_xlabel("days after seam"); ax.set_ylabel("% error"); ax.grid(alpha=0.3); ax.legend(fontsize=8)
        ax.text(0.5, 0.5, "PRE-DRAFT", transform=ax.transAxes, fontsize=60, color="#6a1b9a", alpha=0.12, ha="center", va="center", rotation=28, fontweight="bold")
    fig.tight_layout()
    plot = DRAFT / "plots" / "backtest_error_by_horizon.png"
    fig.savefig(plot, dpi=110)
    plt.close(fig)
    pd.set_option("display.width", 200)
    print(summary[summary["horizon_days"].isin([90, 180, 270, 360, HEADLINE_HORIZON_DAYS])].to_string(index=False, float_format=lambda v: f"{v:+.3%}" if abs(v) < 1 else f"{v:.0f}"))
    print(f"\nwrote {BACKTEST_DIR / 'errors_by_horizon.csv'}, {BACKTEST_DIR / 'errors_summary.csv'}, {plot}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run"); r.add_argument("--seams", nargs="+", default=DEFAULT_SEAMS); r.add_argument("--jobs", type=int, default=3)
    sub.add_parser("score")
    args = p.parse_args()
    {"run": cmd_run, "score": cmd_score}[args.cmd](args)


if __name__ == "__main__":
    main()
