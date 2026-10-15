#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Changepoint probes for the 2027 (organic) mobile MAU build: run a grid of the runner, then score and plot.

Brendan, 2026-10-15: "tune the changepoint location and stiffness ... forecasting organic ... to make it clean". The first
total-MAU build opened 7% below the last actual because changepoint range 0.725 on 5.8 years of history leaves the trend
unable to follow the 2026 run-up. Each probe is one ``run_mobile_mau_forecast.py`` run (organic split on, Iran MAU fill on)
under ``data-official/2027-glean-draft/probes_mobile_mau/<slug>/``; scoring reads the parquets back.

    python scripts/run_mobile_mau_probes.py run --grid round1 [--parallel 3]
    python scripts/run_mobile_mau_probes.py score [--grid round1]
    python scripts/run_mobile_mau_probes.py run --grid round2          # on the iOS-rate-capped organic series
    python scripts/run_mobile_mau_probes.py score --grid round2 --compare-to round1
    python scripts/run_mobile_mau_probes.py run --grid round3          # the 3 x 4 neighbourhood of 0.005 / 0.87 (centre reused from round 2)

Round 2 (Brendan, 2026-10-15, after the correlation check): the organic series with the India + Indonesia iOS-rate cap
(``build_ios_rate_cap.py``, passed through as ``--ios-rate-cap``), changepoint range {0.80, 0.87} x prior scale
{0.005, 0.01, 0.02}; target seam within ±1% and Dec→Dec in the +3%..+8% band. Its probes live under
``probes_mobile_mau/round2_ioscap/`` so the uncorrected round-1 probes stay beside them.

Score per probe: 7-day-mean seam step (first forecast week vs last history week), world MAU 2026-12-15 / 2027-12-15 and
Dec→Dec, Fenix seam step, and the implied Oct-9→Oct-9 slope against the data's own last YoY; ``scores_<grid>.csv`` and
``plots/probes_<grid>_world.png`` (world, Fenix, and the capped countries' Fenix over the organic actuals, one line per probe;
``--compare-to`` adds the other grid's scores to the printout and csv as ``scores_<grid>_vs_<other>.csv``).
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

IOS_RATE_CAP_GLOB = "organic_mau/ios_rate_cap/ios_rate_cap.*.parquet"
TARGET_SEAM_ABS = 0.01            # |seam step| the round-2 brief asks for
TARGET_DEC_TO_DEC = (0.03, 0.08)  # the Dec→Dec slope band the data supports (round-2 brief)

# grid name -> knobs (CLI flag names of the runner), subdirectory under probes_mobile_mau/, whether the iOS-rate cap is on
GRIDS = {
    # location (changepoint_range) x stiffness (changepoint_prior_scale); 0.725 = October lock, 0.87 = desktop's seam fix
    "round1": {"knobs": [{"changepoint_range": cpr, "changepoint_prior_scale": cps}
                         for cpr, cps in itertools.product([0.725, 0.87, 0.95], [0.035, 0.10])],
               "subdir": "", "ios_rate_cap": False},
    # on the IN + ID iOS-rate-capped organic series; stiffer priors so the final segment does not take the high spend period as its slope
    "round2": {"knobs": [{"changepoint_range": cpr, "changepoint_prior_scale": cps}
                         for cpr, cps in itertools.product([0.80, 0.87], [0.005, 0.01, 0.02])],
               "subdir": "round2_ioscap", "ios_rate_cap": True},
    # Brendan 2026-10-15: "0.005/0.87 is the best so far ... walk around that space in both axes"; the centre is reused from round 2
    "round3": {"knobs": [{"changepoint_range": cpr, "changepoint_prior_scale": cps}
                         for cpr, cps in itertools.product([0.84, 0.87, 0.90], [0.004, 0.005, 0.0065, 0.008])
                         if not (cpr == 0.87 and cps == 0.005)],
               "subdir": "round3_ioscap", "ios_rate_cap": True},
}
CAPPED_COUNTRIES = ["IN", "ID"]


def grid_dir(grid: str) -> Path:
    return PROBES / GRIDS[grid]["subdir"] if GRIDS[grid]["subdir"] else PROBES


def slug_for(knobs: dict) -> str:
    return "_".join(f"{k.replace('changepoint_prior_scale', 'cps').replace('changepoint_range', 'cpr').replace('n_changepoints', 'ncp')}{v}"
                    for k, v in sorted(knobs.items()))


def run_one(knobs: dict, organic_split: Path, grid: str, ios_rate_cap: Path | None) -> str:
    out = grid_dir(grid) / slug_for(knobs)
    out.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(repo_root / "scripts/run_mobile_mau_forecast.py"), "--forecast-start-date", SEAM,
           "--raw-dir", str(RAW_DIR), "--iran-fill", str(IRAN_FILL), "--organic-split", str(organic_split), "--results-dir", str(out)]
    if ios_rate_cap:
        cmd += ["--ios-rate-cap", str(ios_rate_cap)]
    for k, v in knobs.items():
        cmd += [f"--{k.replace('_', '-')}", str(v)]
    log = out / "run.log"
    with log.open("w") as fh:
        rc = subprocess.call(cmd, stdout=fh, stderr=subprocess.STDOUT, cwd=repo_root)
    return f"{slug_for(knobs)}: exit {rc} ({log})"


def probe_parquets(grid: str) -> dict[str, Path]:
    found = {}
    for knobs in GRIDS[grid]["knobs"]:
        slug = slug_for(knobs)
        hits = sorted((grid_dir(grid) / slug).glob("*/mozaic_daily_forecast.*.gm-M.raw.parquet"))
        if hits:
            found[slug] = hits[0]
    return found


def world(df: pd.DataFrame, app: str = "ALL MOBILE", country: str = "ALL") -> pd.Series:
    w = df[(df["country"] == country) & (df["app_name"] == app)].set_index("target_date").sort_index()
    return w["mau"].astype(float)


def score_one(slug: str, path: Path) -> dict:
    df, meta = load_forecast(str(path))
    df["target_date"] = pd.to_datetime(df["target_date"])
    seam = pd.Timestamp(meta["forecast_start_date"])
    w, fx = world(df), world(df, "fenix_android")
    pre = lambda s: s[seam - pd.Timedelta(days=7):seam - pd.Timedelta(days=1)].mean()
    post = lambda s: s[seam:seam + pd.Timedelta(days=6)].mean()
    last = seam - pd.Timedelta(days=1)
    cap = meta.get("ios_rate_cap")   # write_meta flattens `extra` into the sidecar's top level
    return {"probe": slug, "config": meta.get("model_config", {}), "ios_rate_cap": bool(cap),
            "seam_step_world": post(w) / pre(w) - 1, "seam_step_fenix": post(fx) / pre(fx) - 1,
            "last_actual": w[last], "dec15_2026": w[pd.Timestamp("2026-12-15")], "dec15_2027": w[pd.Timestamp("2027-12-15")],
            "dec_to_dec": w[pd.Timestamp("2027-12-15")] / w[pd.Timestamp("2026-12-15")] - 1,
            "yoy_data_last": w[last] / w[last - pd.DateOffset(years=1)] - 1,
            "yoy_forecast_oct9_2027": w[last + pd.DateOffset(years=1)] / w[last] - 1,
            "meets_seam": abs(post(w) / pre(w) - 1) <= TARGET_SEAM_ABS,
            "meets_dec_to_dec": TARGET_DEC_TO_DEC[0] <= w[pd.Timestamp("2027-12-15")] / w[pd.Timestamp("2026-12-15")] - 1 <= TARGET_DEC_TO_DEC[1]}


def plot_grid(parquets: dict[str, Path], grid: str, reference: dict[str, Path] | None = None) -> Path:
    """World, Fenix and the capped countries' Fenix: this grid's forecasts over the actuals it trained on.

    When ``reference`` (another grid's parquets) is given, its training rows are drawn too, so a corrected grid shows the
    uncorrected organic actuals in grey beside the series it trained on."""
    panels = [("ALL MOBILE", "ALL", "Mobile MAU (all apps), organic"), ("fenix_android", "ALL", "Fenix MAU, organic")]
    panels += [("fenix_android", c, f"Fenix MAU {c}, organic") for c in CAPPED_COUNTRIES]
    fig, axes = plt.subplots(2, 2, figsize=(20, 13))
    colors = plt.cm.tab10.colors
    frames = {slug: load_forecast(str(path))[0].assign(target_date=lambda d: pd.to_datetime(d["target_date"])) for slug, path in parquets.items()}
    ref = None
    if reference:
        ref = load_forecast(str(next(iter(reference.values()))))[0].assign(target_date=lambda d: pd.to_datetime(d["target_date"]))
    for ax, (app, country, title) in zip(axes.ravel(), panels):
        first = True
        for (slug, df), color in zip(frames.items(), colors):
            s = world(df, app, country)
            rows = df[(df["country"] == country) & (df["app_name"] == app)].set_index("target_date").sort_index()
            act = s[rows["data_type"] == "training"]
            fc = s[s.index >= pd.Timestamp(SEAM)]
            if first:
                if ref is not None:
                    r = world(ref, app, country)
                    r_rows = ref[(ref["country"] == country) & (ref["app_name"] == app)].set_index("target_date").sort_index()
                    ax.plot(r[r_rows["data_type"] == "training"][pd.Timestamp("2024-01-01"):], color="grey", lw=1.2, label="actuals, organic uncorrected")
                ax.plot(act[pd.Timestamp("2024-01-01"):], color="black", lw=2,
                        label="actuals the probes trained on (organic" + (", iOS-rate cap)" if ref is not None else ")"))
                first = False
            ax.plot(fc, color=color, lw=1.2, label=slug)
        ax.axvline(pd.Timestamp(SEAM), color="grey", ls="--", alpha=0.6)
        ax.axvline(pd.Timestamp("2027-12-15"), color="red", ls=":", alpha=0.6)
        ax.set_title(f"{title} — probes {grid}")
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M" if ax.get_ylim()[1] - ax.get_ylim()[0] < 5e6 else f"{v / 1e6:.1f}M"))
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
    r.add_argument("--ios-rate-cap", type=Path, default=None, help="cap parquet for a capped grid (default: the latest under organic_mau/ios_rate_cap/)")
    s = sub.add_parser("score"); s.add_argument("--grid", default="round1", choices=sorted(GRIDS))
    s.add_argument("--compare-to", default=None, choices=sorted(GRIDS), help="another grid whose scores are appended for comparison")
    args = p.parse_args()
    if args.cmd == "run":
        split = sorted(DRAFT.glob(ORGANIC_SPLIT_GLOB))[-1]
        cap = None
        if GRIDS[args.grid]["ios_rate_cap"]:
            cap = args.ios_rate_cap or sorted(DRAFT.glob(IOS_RATE_CAP_GLOB))[-1]
        print(f"organic split: {split}\niOS-rate cap: {cap or 'off'}\nprobes: {[slug_for(k) for k in GRIDS[args.grid]['knobs']]} -> {grid_dir(args.grid)}")
        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            for line in pool.map(lambda k: run_one(k, split, args.grid, cap), GRIDS[args.grid]["knobs"]):
                print(line)
        return
    parquets = probe_parquets(args.grid)
    scores = pd.DataFrame([score_one(slug, path) for slug, path in parquets.items()]).assign(grid=args.grid)
    out = PROBES / f"scores_{args.grid}.csv"
    scores.drop(columns=["config"]).to_csv(out, index=False)
    reference = None
    if args.compare_to:
        reference = probe_parquets(args.compare_to)
        other = pd.DataFrame([score_one(slug, path) for slug, path in reference.items()]).assign(grid=args.compare_to)
        scores = pd.concat([scores, other], ignore_index=True)
        scores.drop(columns=["config"]).to_csv(PROBES / f"scores_{args.grid}_vs_{args.compare_to}.csv", index=False)
    pd.set_option("display.width", 250)
    print(scores.drop(columns=["config"]).to_string(index=False, float_format=lambda v: f"{v:,.4f}" if abs(v) < 10 else f"{v:,.0f}"))
    print(f"\ncsv   {out}\nplot  {plot_grid(parquets, args.grid, reference if GRIDS[args.grid]['ios_rate_cap'] else None)}")


if __name__ == "__main__":
    main()
