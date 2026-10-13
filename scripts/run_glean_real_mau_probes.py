#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Parameter probes for the real-Glean MAU comparison (no simulated history) of the 2027 Glean-history forecast.

Why: the locked configuration is tuned for 5+ years of history; on 17 months of real Glean it reads the one autumn
ramp it saw as trend and sends MAU up and to the right while the data trend is down (COMPARISONS.html §2). Brendan,
2026-10-13: "freely adjust the changepoint parameters and trend parameters to bring that prediction to something
reasonable" — lower than the simulated-history forecast, slope roughly the YoY trend of the data (world ≈ −4%).

``run``   launches ``run_glean_history_forecast.py`` MAU-only on the real-Glean frames for each probe of a named grid
          (outage calendar OFF, Iran fill and China correction as in the comparison build), concurrency-limited.
``score`` reads every probe parquet, writes ``probes_glean_real_mau/scores.csv`` (world + per-OS Dec-15 values, the
          implied Oct→Oct and Dec→Dec slopes, min over the horizon, negative-value flag) and a PRE-DRAFT plot of all
          probe world-MAU paths over the real actuals and the locked forecast.

Usage:
    python scripts/run_glean_real_mau_probes.py run --grid round1 --jobs 3
    python scripts/run_glean_real_mau_probes.py score
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
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "src"))
sys.path.insert(0, str(repo_root / "scripts"))
from mozaic_daily.adjustments import load_forecast  # noqa: E402
from plot_glean_history_canonical import millions_formatter, save  # noqa: E402

DRAFT = repo_root / "data-official/2027-glean-draft"
PROBES_DIR = DRAFT / "probes_glean_real_mau"
RAW_DIR = DRAFT / "raw_glean_real_osxcountry_2026-10-09"
IRAN_FILL = DRAFT / "iran_fill/iran_fill.glean_history_desktop.parquet"
OVERLAY_SPEC = DRAFT / "mozillaonline/mozillaonline.glean_history.json"
LOCKED_CURVES = DRAFT / "csv/glean2027_canonical_curves.csv"
SEAM = "2026-10-10"
OS_SEGMENTS = {"world": '{"os": "ALL"}', "windows": '{"os": "windows"}', "mac": '{"os": "mac"}', "linux": '{"os": "linux"}'}

# Each probe: name → runner knob flags. Round 1 (2026-10-13): is the problem the mask, the changepoint freedom, or both?
GRIDS: dict[str, dict[str, list[str]]] = {
    "round1": {
        "locked_nomask": [],
        **{f"cps{cps}_ncp{ncp}_cpr{cpr}": ["--changepoint-prior-scale", str(cps), "--n-changepoints", str(ncp), "--changepoint-range", str(cpr)]
           for cps in (0.05, 0.01) for ncp in (10, 5) for cpr in (0.8, 0.5)},
        "cps0.05_ncp10_cpr0.8_sps0.1": ["--changepoint-prior-scale", "0.05", "--n-changepoints", "10", "--changepoint-range", "0.8",
                                        "--seasonality-prior-scale", "0.1"],
    },
    # Round 2 (approved 2026-10-13): near-linear trend so each OS gets its mean slope over the 17 months; one additive-regime run
    "round2": {
        **{f"cps{cps}_ncp{ncp}_cpr{cpr}": ["--changepoint-prior-scale", str(cps), "--n-changepoints", str(ncp), "--changepoint-range", str(cpr)]
           for cps in (0.005, 0.001) for ncp in (3, 2) for cpr in (0.8, 0.5)},
        "cps0.005_ncp3_cpr0.8_additive": ["--changepoint-prior-scale", "0.005", "--n-changepoints", "3", "--changepoint-range", "0.8",
                                          "--seasonality-regime", "additive"],
    },
}


def run_one(name_and_flags: tuple[str, list[str]]) -> tuple[str, int]:
    name, flags = name_and_flags
    out = PROBES_DIR / name
    out.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(repo_root / "scripts/run_glean_history_forecast.py"), "--forecast-start-date", SEAM,
           "--raw-dir", str(RAW_DIR), "--results-dir", str(out), "--metrics", "MAU", "--iran-fill", str(IRAN_FILL),
           "--overlay-spec", str(OVERLAY_SPEC), "--no-outage-calendar", "--changepoint-range", "0.87", *flags]
    with open(out / "run.log", "w") as log:
        code = subprocess.call(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=repo_root)
    return name, code


def cmd_run(args):
    grid = GRIDS[args.grid]
    todo = [(name, flags) for name, flags in grid.items() if args.force or not list((PROBES_DIR / name).glob("*/mozaic_daily_forecast.*.parquet"))]
    print(f"grid {args.grid}: {len(todo)} of {len(grid)} probes to run")
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for name, code in pool.map(run_one, todo):
            print(f"  {name}: exit {code}")


def world_series(parquet: Path, segment: str) -> pd.Series:
    df, _ = load_forecast(str(parquet))
    df["target_date"] = pd.to_datetime(df["target_date"])
    w = df[(df["country"] == "ALL") & (df["segment"] == segment)].set_index("target_date").sort_index()
    return w.loc[w["data_type"] == "forecast", "mau"]


def cmd_score(args):
    locked = pd.read_csv(LOCKED_CURVES, parse_dates=["date"]).set_index("date")
    actual = pd.read_parquet(RAW_DIR / "mozaic_parts.raw.glean_history.desktop.MAU.parquet")
    actual["x"] = pd.to_datetime(actual["x"])
    actual_world = actual.groupby("x")["y"].sum().astype(float)
    last_oct = actual_world.loc["2026-09-10":"2026-10-09"].mean()
    actual_os = {os_name: actual[actual[os_name]].groupby("x")["y"].sum().astype(float) for os_name in ("windows", "mac", "linux")}
    actual_os["world"] = actual_world
    rows, paths, os_paths = [], {}, {o: {} for o in OS_SEGMENTS}
    keep = set(GRIDS[args.grid]) if args.grid else None
    suffix = f"_{args.grid}" if args.grid else ""
    for parquet in sorted(PROBES_DIR.glob("*/*/mozaic_daily_forecast.*.parquet")):
        name = parquet.parent.parent.name
        if keep is not None and name not in keep:
            continue
        params = json.loads((parquet.parent / "parameters.json").read_text())["config"]
        row = {"probe": name, "cps": params["prophet_changepoint_prior_scale"], "ncp": params["prophet_n_changepoints"],
               "cpr": params["prophet_changepoint_range"], "sps": params["prophet_seasonality_prior_scale"]}
        for os_name, segment in OS_SEGMENTS.items():
            s = world_series(parquet, segment)
            row[f"{os_name}_2026-12-15"] = s.loc["2026-12-15"]
            row[f"{os_name}_2027-12-15"] = s.loc["2027-12-15"]
            row[f"{os_name}_min"] = s.min()
            os_paths[os_name][name] = s
            if os_name == "world":
                paths[name] = s
                row["world_dec_to_dec"] = s.loc["2027-12-15"] / s.loc["2026-12-15"] - 1
                row["world_oct27_vs_oct26_actual"] = s.loc["2027-09-10":"2027-10-09"].mean() / last_oct - 1
                row["world_vs_locked_2027-12-15"] = s.loc["2027-12-15"] / locked.loc["2027-12-15", "mau_world"] - 1
        row["any_negative"] = any(row[f"{o}_min"] < 0 for o in OS_SEGMENTS)
        rows.append(row)
    scores = pd.DataFrame(rows).sort_values("world_2027-12-15")
    scores.to_csv(PROBES_DIR / f"scores{suffix}.csv", index=False)

    fig, ax = plt.subplots(figsize=(16, 7))
    ax.plot(actual_world.loc["2025-05-22":], color="black", linewidth=2, label="Actuals, real Glean MAU")
    ax.plot(locked.loc[locked["data_type"] == "forecast", "mau_world"], color="green", linewidth=1.6, label="Locked forecast (Glean-equivalent history)")
    cmap = plt.get_cmap("tab20")
    for i, (name, s) in enumerate(sorted(paths.items(), key=lambda kv: kv[1].loc["2027-12-15"])):
        ax.plot(s, linewidth=1, color=cmap(i % 20), label=f"{name}: Dec-15-27 {s.loc['2027-12-15'] / 1e6:.1f}M")
    for d in ("2026-12-15", "2027-12-15"):
        ax.axvline(pd.Timestamp(d), color="red", linestyle=":", alpha=0.5)
    ax.axvline(pd.Timestamp(SEAM), color="grey", linestyle="--", alpha=0.6)
    lo = min(actual_world.loc["2025-05-22":].min(), locked["mau_world"].loc["2026-10-10":].min()) * 0.8
    hi = max(actual_world.loc["2025-05-22":].max(), locked["mau_world"].loc["2026-10-10":].max()) * 1.25
    ax.set_ylim(lo, hi)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2)); ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.set_title("Real Glean only, MAU world: parameter probes (outage mask off) vs actuals and the locked forecast", fontsize=13)
    ax.set_ylabel("MAU"); ax.grid(True, alpha=0.3); ax.legend(fontsize=7, loc="upper left", ncol=2)
    save(fig, PROBES_DIR / f"probes_world_mau{suffix}.png")
    # Per-OS small multiples: the world total can look tame while Windows and Linux errors cancel
    fig, axes = plt.subplots(2, 2, figsize=(16, 9))
    for ax, os_name in zip(axes.ravel(), OS_SEGMENTS):
        ax.plot(actual_os[os_name].loc["2025-05-22":], color="black", linewidth=2, label="Actuals, real Glean MAU")
        ax.plot(locked.loc[locked["data_type"] == "forecast", f"mau_{os_name}"], color="green", linewidth=1.6, label="Locked forecast (Glean-equivalent)")
        for i, (name, s) in enumerate(sorted(os_paths[os_name].items(), key=lambda kv: kv[1].loc["2027-12-15"])):
            ax.plot(s, linewidth=0.9, color=cmap(i % 20), label=f"{name}: {s.loc['2027-12-15'] / 1e6:.1f}M")
        a = actual_os[os_name].loc["2025-05-22":]
        ax.set_ylim(a.min() * 0.6, a.max() * 1.6)
        for d in ("2026-12-15", "2027-12-15"):
            ax.axvline(pd.Timestamp(d), color="red", linestyle=":", alpha=0.5)
        ax.axvline(pd.Timestamp(SEAM), color="grey", linestyle="--", alpha=0.6)
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3)); ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        ax.set_title(f"{os_name} MAU: probes vs actuals and locked (y clipped to 0.6–1.6× actual range)", fontsize=11)
        ax.grid(True, alpha=0.3); ax.legend(fontsize=6, loc="upper left", ncol=2)
    save(fig, PROBES_DIR / f"probes_by_os_mau{suffix}.png")
    pd.set_option("display.width", 250)
    cols = ["probe", "cps", "ncp", "cpr", "sps", "world_2026-12-15", "world_2027-12-15", "world_dec_to_dec", "world_oct27_vs_oct26_actual", "world_vs_locked_2027-12-15", "any_negative"]
    print(scores[cols].to_string(index=False, float_format=lambda v: f"{v:+.1%}" if abs(v) < 5 else f"{v:,.0f}"))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run"); r.add_argument("--grid", default="round1", choices=sorted(GRIDS)); r.add_argument("--jobs", type=int, default=3)
    r.add_argument("--force", action="store_true", help="rerun probes that already have a parquet")
    sc = sub.add_parser("score"); sc.add_argument("--grid", default=None, choices=sorted(GRIDS), help="score only this grid's probes (default: all)")
    args = p.parse_args()
    {"run": cmd_run, "score": cmd_score}[args.cmd](args)


if __name__ == "__main__":
    main()
