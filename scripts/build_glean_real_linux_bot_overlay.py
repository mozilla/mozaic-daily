#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Turn the measured Linux bot-ISP daily curve into the `b` overlay (curve parquets + spec) for the real-Glean comparison.

Input: ``data-official/2027-glean-draft/linux_bot/bot_isp_daily_curve.csv`` (scripts/sql/glean_real_bot_isp_daily_curve.sql:
per day x country x OS, real Glean, 10% sample scaled x10: dau, mau, dau_bot, mau_bot). Linux rows only.

Output (``linux_bot/``):
    linux_bot_dau.<edge>.parquet / linux_bot_mau.<edge>.parquet   daily world series, 7-day centred mean over the history,
                                                                  held flat after the seam at the last 28-day mean, to 2027-12-31
    linux_bot.glean_history.json                                  desktop_overlay spec: flag `linux`, fixed country shares from the
                                                                  last 28 days of the MAU component, population share linux = 1,
                                                                  `metrics` block with both curves, gated to the seam
    linux_bot.<edge>.meta.json                                    provenance + the numbers (shares, held levels, baseline)
    plots/linux_bot_curve.png                                     the two curves, raw and smoothed, with the held path

The hold-flat path is a planning choice (Brendan to confirm; decay and zero are the alternates). Nothing here runs the model.

Usage:
    python scripts/build_glean_real_linux_bot_overlay.py [--seam 2026-10-10] [--hold flat|zero]
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
DRAFT = repo_root / "data-official/2027-glean-draft"
OUT = DRAFT / "linux_bot"
SOURCE = OUT / "bot_isp_daily_curve.csv"
HORIZON_END = pd.Timestamp("2027-12-31")
SMOOTH_DAYS = 7
HOLD_WINDOW_DAYS = 28


def load_linux(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["x"])
    df = df[df["os_bucket"] == "linux"].copy()
    for col in ("dau", "mau", "dau_bot", "mau_bot"):
        df[col] = pd.to_numeric(df[col])
    return df


def world_curve(linux: pd.DataFrame, col: str, seam: pd.Timestamp, hold: str) -> tuple[pd.Series, pd.Series, float]:
    """(raw daily world series, smoothed + held series to the horizon end, held level)."""
    raw = linux.groupby("x")[col].sum().astype(float).sort_index()
    smooth = raw.rolling(SMOOTH_DAYS, center=True, min_periods=1).mean()
    level = float(raw.iloc[-HOLD_WINDOW_DAYS:].mean()) if hold == "flat" else 0.0
    future = pd.Series(level, index=pd.date_range(smooth.index.max() + pd.Timedelta(days=1), HORIZON_END, freq="D"))
    full = pd.concat([smooth, future])
    full.index.name = "target_date"
    return raw, full, level


def country_shares(linux: pd.DataFrame, col: str) -> dict[str, float]:
    edge = linux["x"].max()
    last = linux[linux["x"] > edge - pd.Timedelta(days=HOLD_WINDOW_DAYS)].groupby("country")[col].mean()
    shares = (last / last.sum()).sort_values(ascending=False)
    return {c: round(float(v), 4) for c, v in shares.items() if v > 0}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seam", default="2026-10-10")
    p.add_argument("--hold", default="flat", choices=["flat", "zero"])
    args = p.parse_args()
    seam = pd.Timestamp(args.seam)
    linux = load_linux(SOURCE)
    edge = linux["x"].max().date().isoformat()
    curves, levels, raws = {}, {}, {}
    for metric, col in (("DAU", "dau_bot"), ("MAU", "mau_bot")):
        raw, full, level = world_curve(linux, col, seam, args.hold)
        name = f"linux_bot_{metric.lower()}.{edge}.parquet"
        pd.DataFrame({f"linux_bot_{metric.lower()}_daily": full}).to_parquet(OUT / name)
        curves[metric] = {"data_file": name, "value_column": f"linux_bot_{metric.lower()}_daily"}
        levels[metric] = level
        raws[metric] = (raw, full)
        print(f"{metric}: {name}  history {raw.index.min().date()}..{raw.index.max().date()}, last-28d mean {level:,.0f}, held {args.hold} to {HORIZON_END.date()}")
    shares = country_shares(linux, "mau_bot")
    spec = {
        "type": "desktop_overlay", "platform": "desktop",
        "data_file": curves["DAU"]["data_file"], "value_column": curves["DAU"]["value_column"],
        "allocation": {"key": "fixed_country_shares", "flag_column": "linux", "window_days": HOLD_WINDOW_DAYS, "shares": shares,
                       "population_shares": {"DAU": {"linux": 1.0}, "MAU": {"linux": 1.0}}},
        "scope": {"exclude_countries": []},
        "model_meta_file": f"linux_bot.{edge}.meta.json",
        "applies_to_forecast_start": args.seam, "applies_to_data_source": "glean_history_desktop", "placeholder": False,
        "metrics": curves,
        "notes": (f"Linux bot-like (hosting-ISP) component measured from real Glean alone (code `b`, registry description has the definition). "
                  f"World daily DAU and MAU of flagged clients, {SMOOTH_DAYS}-day centred mean over {linux['x'].min().date()}..{edge}, "
                  f"held {args.hold} after the seam at the last-{HOLD_WINDOW_DAYS}-day mean (DAU {levels['DAU']:,.0f}, MAU {levels['MAU']:,.0f}) — "
                  f"a planning choice. Country shares = last-28-day shares of the MAU component. Applied to `linux` rows only (Windows 0.2% of MAU, Mac ~0). "
                  f"Subtracted from training before mozaic, added back after, so the surge is not read as trend. Gated to the {args.seam} seam."),
    }
    (OUT / "linux_bot.glean_history.json").write_text(json.dumps(spec, indent=2) + "\n")
    meta = {"produced_by": "scripts/build_glean_real_linux_bot_overlay.py", "written_at": datetime.now().isoformat(timespec="seconds"),
            "source_csv": str(SOURCE.relative_to(repo_root)), "source_sha1": hashlib.sha1(SOURCE.read_bytes()).hexdigest(),
            "sql": "scripts/sql/glean_real_bot_isp_daily_curve.sql", "definition": "ISP behavioural share >= 0.8 (see registry code b)",
            "sample": "10% (sample_id 0-9) scaled x10", "smoothing_days": SMOOTH_DAYS, "hold": args.hold, "hold_window_days": HOLD_WINDOW_DAYS,
            "held_levels": levels, "data_edge": edge, "seam": args.seam, "country_shares_mau_last28d": shares,
            "baseline_2025_mean": {m: float(raws[m][0].loc["2025-06-01":"2025-12-31"].mean()) for m in raws},
            "share_of_linux_last28d": {m: float(raws[m][0].iloc[-28:].mean() / linux.groupby("x")[m.lower()].sum().iloc[-28:].mean()) for m in raws}}
    (OUT / f"linux_bot.{edge}.meta.json").write_text(json.dumps(meta, indent=2) + "\n")

    fig, axes = plt.subplots(1, 2, figsize=(16, 5.5))
    for ax, metric in zip(axes, ("MAU", "DAU")):
        raw, full = raws[metric]
        ax.plot(raw, color="grey", linewidth=0.8, alpha=0.7, label="measured, daily (10% sample ×10)")
        ax.plot(full[full.index <= raw.index.max()], color="#c62828", linewidth=1.6, label=f"{SMOOTH_DAYS}-day centred mean (subtracted from training)")
        ax.plot(full[full.index > raw.index.max()], color="#c62828", linewidth=1.6, linestyle="--", label=f"held {args.hold} at {levels[metric]:,.0f} (added back)")
        ax.axvline(seam, color="grey", linestyle="--", alpha=0.6, label=f"seam {seam.date()}")
        ax.set_title(f"Linux world bot-like {metric}: the `b` overlay curve", fontsize=12)
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3)); ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        ax.grid(True, alpha=0.3); ax.legend(fontsize=8, loc="upper left")
        ax.text(0.5, 0.5, "PRE-DRAFT", transform=ax.transAxes, fontsize=60, color="#6a1b9a", alpha=0.14, ha="center", va="center", rotation=28, fontweight="bold")
    fig.tight_layout()
    (OUT / "plots").mkdir(exist_ok=True)
    fig.savefig(OUT / "plots/linux_bot_curve.png", dpi=120)
    print(f"shares (MAU, last 28 d): {shares}")
    print(f"wrote {OUT / 'linux_bot.glean_history.json'}, meta, {OUT / 'plots/linux_bot_curve.png'}")


if __name__ == "__main__":
    main()
