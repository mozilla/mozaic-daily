#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Changepoint probes for the 2027 (organic) mobile MAU build: run a grid of the runner, then score and plot.

Brendan, 2026-10-15: "tune the changepoint location and stiffness ... forecasting organic ... to make it clean". The first
total-MAU build opened 7% below the last actual because changepoint range 0.725 on 5.8 years of history leaves the trend
unable to follow the 2026 run-up. Each probe is one ``run_mobile_mau_forecast.py`` run (organic split on, Iran MAU fill on)
under ``data-official/2027-glean-draft/probes_mobile_mau/<slug>/``; scoring reads the parquets back.

    python scripts/run_mobile_mau_probes.py run --grid round1 [--parallel 3]
    python scripts/run_mobile_mau_probes.py score [--grid round1]

Score per probe: 7-day-mean seam step (first forecast week vs last history week), world MAU 2026-12-15 / 2027-12-15 and
Dec→Dec, Fenix seam step, and the implied Oct-9→Oct-9 slope against the data's own last YoY; ``scores_<grid>.csv`` and
``plots/probes_<grid>_world.png`` (world + Fenix over the organic actuals, one line per probe).
"""

from __future__ import annotations

import argparse
import itertools
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
if str(repo_root / "src") not in sys.path:
    sys.path.insert(0, str(repo_root / "src"))
from mozaic_daily.adjustments import load_forecast  # noqa: E402

DRAFT = repo_root / "data-official/2027-glean-draft"
PROBES = DRAFT / "probes_mobile_mau"
SEAM = "2026-10-10"
RAW_DIR = DRAFT / "raw_glean_mobile_countryxapp_2026-10-09"
IRAN_FILL = DRAFT / "iran_fill_mobile/iran_fill.glean_mobile.MAU.parquet"
ORGANIC_SPLIT_GLOB = "organic_mau/fenix_paid_organic_mau.*.parquet"

# grid name -> list of knob dicts (CLI flag names of the runner)
GRIDS = {
    # location (changepoint_range) x stiffness (changepoint_prior_scale); 0.725 = October lock, 0.87 = desktop's seam fix
    "round1": [{"changepoint_range": cpr, "changepoint_prior_scale": cps}
               for cpr, cps in itertools.product([0.725, 0.87, 0.95], [0.035, 0.10])],
}


def slug_for(knobs: dict) -> str:
    return "_".join(f"{k.replace('changepoint_prior_scale', 'cps').replace('changepoint_range', 'cpr').replace('n_changepoints', 'ncp')}{v}"
                    for k, v in sorted(knobs.items()))


def run_one(knobs: dict, organic_split: Path) -> str:
    out = PROBES / slug_for(knobs)
    out.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(repo_root / "scripts/run_mobile_mau_forecast.py"), "--forecast-start-date", SEAM,
           "--raw-dir", str(RAW_DIR), "--iran-fill", str(IRAN_FILL), "--organic-split", str(organic_split), "--results-dir", str(out)]
    for k, v in knobs.items():
        cmd += [f"--{k.replace('_', '-')}", str(v)]
    log = out / "run.log"
    with log.open("w") as fh:
        rc = subprocess.call(cmd, stdout=fh, stderr=subprocess.STDOUT, cwd=repo_root)
    return f"{slug_for(knobs)}: exit {rc} ({log})"


def probe_parquets(grid: str) -> dict[str, Path]:
    found = {}
    for knobs in GRIDS[grid]:
        slug = slug_for(knobs)
        hits = sorted((PROBES / slug).glob("*/mozaic_daily_forecast.*.gm-M.raw.parquet"))
        if hits:
            found[slug] = hits[0]
    return found


def world(df: pd.DataFrame, app: str = "ALL MOBILE") -> pd.Series:
    w = df[(df["country"] == "ALL") & (df["app_name"] == app)].set_index("target_date").sort_index()
    return w["mau"].astype(float)


def score_one(slug: str, path: Path) -> dict:
    df, meta = load_forecast(str(path))
    df["target_date"] = pd.to_datetime(df["target_date"])
    seam = pd.Timestamp(meta["forecast_start_date"])
    w, fx = world(df), world(df, "fenix_android")
    pre = lambda s: s[seam - pd.Timedelta(days=7):seam - pd.Timedelta(days=1)].mean()
    post = lambda s: s[seam:seam + pd.Timedelta(days=6)].mean()
    last = seam - pd.Timedelta(days=1)
    return {"probe": slug, "config": meta.get("model_config", {}),
            "seam_step_world": post(w) / pre(w) - 1, "seam_step_fenix": post(fx) / pre(fx) - 1,
            "last_actual": w[last], "dec15_2026": w[pd.Timestamp("2026-12-15")], "dec15_2027": w[pd.Timestamp("2027-12-15")],
            "dec_to_dec": w[pd.Timestamp("2027-12-15")] / w[pd.Timestamp("2026-12-15")] - 1,
            "yoy_data_last": w[last] / w[last - pd.DateOffset(years=1)] - 1,
            "yoy_forecast_oct9_2027": w[last + pd.DateOffset(years=1)] / w[last] - 1}


def plot_grid(parquets: dict[str, Path], grid: str) -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(20, 7))
    colors = plt.cm.tab10.colors
    for ax, app, title in zip(axes, ["ALL MOBILE", "fenix_android"], ["Mobile MAU (all apps), organic", "Fenix MAU, organic"]):
        first = True
        for (slug, path), color in zip(parquets.items(), colors):
            df, meta = load_forecast(str(path))
            df["target_date"] = pd.to_datetime(df["target_date"])
            s = world(df, app)
            act = s[df[(df["country"] == "ALL") & (df["app_name"] == app)].set_index("target_date").sort_index()["data_type"] == "training"]
            fc = s[s.index >= pd.Timestamp(SEAM)]
            if first:
                ax.plot(act[pd.Timestamp("2024-01-01"):], color="black", lw=2, label="actuals (organic, real Glean)")
                first = False
            ax.plot(fc, color=color, lw=1.2, label=slug)
        ax.axvline(pd.Timestamp(SEAM), color="grey", ls="--", alpha=0.6)
        ax.axvline(pd.Timestamp("2027-12-15"), color="red", ls=":", alpha=0.6)
        ax.set_title(f"{title} — probes {grid}")
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e6:.1f}M"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3)); ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        ax.grid(True, alpha=0.3); ax.legend(fontsize=8)
        ax.text(0.5, 0.5, "PRE-DRAFT", transform=ax.transAxes, fontsize=60, color="#6a1b9a", alpha=0.14, ha="center", va="center", rotation=28, fontweight="bold")
    fig.tight_layout()
    out = PROBES / "plots" / f"probes_{grid}_world.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=110)
    plt.close(fig)
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run"); r.add_argument("--grid", default="round1", choices=sorted(GRIDS)); r.add_argument("--parallel", type=int, default=3)
    s = sub.add_parser("score"); s.add_argument("--grid", default="round1", choices=sorted(GRIDS))
    args = p.parse_args()
    if args.cmd == "run":
        split = sorted(DRAFT.glob(ORGANIC_SPLIT_GLOB))[-1]
        print(f"organic split: {split}\nprobes: {[slug_for(k) for k in GRIDS[args.grid]]}")
        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            for line in pool.map(lambda k: run_one(k, split), GRIDS[args.grid]):
                print(line)
        return
    parquets = probe_parquets(args.grid)
    rows = [score_one(slug, path) for slug, path in parquets.items()]
    scores = pd.DataFrame(rows)
    out = PROBES / f"scores_{args.grid}.csv"
    scores.drop(columns=["config"]).to_csv(out, index=False)
    pd.set_option("display.width", 200)
    print(scores.drop(columns=["config"]).to_string(index=False, float_format=lambda v: f"{v:,.4f}" if abs(v) < 10 else f"{v:,.0f}"))
    print(f"\ncsv   {out}\nplot  {plot_grid(parquets, args.grid)}")


if __name__ == "__main__":
    main()
