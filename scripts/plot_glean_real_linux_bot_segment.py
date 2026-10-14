#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Linux real Glean: the behavioural bot-like segment (new + ≤2 active days) as a share of MAU and DAU over time, world + DE / US / ROW.

Reads ``data-official/2027-glean-draft/linux_bot/linux_bot_segment_shares.csv`` (10% sample scaled ×10, 1st/15th of each month) and draws
MAU with the bot-like subset stacked, and the share lines, so the ~37% baseline and the surges (Apr–May 2026, Sep–Oct 2026) can be read off.

Usage:
    python scripts/plot_glean_real_linux_bot_segment.py   # writes data-official/2027-glean-draft/plots/linux_bot_segment_shares.png
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "scripts"))
from plot_glean_history_canonical import millions_formatter, save  # noqa: E402

DRAFT = repo_root / "data-official/2027-glean-draft"
SCOPES = ["world", "DE", "US", "ROW"]


def main():
    s = pd.read_csv(DRAFT / "linux_bot/linux_bot_segment_shares.csv", parse_dates=["submission_date"])
    num = ["mau", "mau_new_lowact", "mau_new", "mau_lowact", "dau", "dau_new_lowact"]
    s[num] = s[num].apply(pd.to_numeric)
    world = s.groupby("submission_date")[num].sum().reset_index().assign(country="world")
    s = pd.concat([s, world])
    fig, axes = plt.subplots(2, 4, figsize=(20, 9))
    for j, scope in enumerate(SCOPES):
        x = s[s.country == scope].set_index("submission_date").sort_index()
        ax = axes[0, j]
        ax.plot(x["mau"], color="black", linewidth=1.8, label="Linux MAU (real Glean)")
        ax.fill_between(x.index, 0, x["mau_new_lowact"], color="#c62828", alpha=0.3, label="bot-like: new in window and ≤ 2 active days")
        ax.plot(x["mau_new_lowact"], color="#c62828", linewidth=1)
        ax.set_title(f"Linux {scope} MAU and the behavioural bot-like subset", fontsize=11)
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
        ax.set_ylim(0, None); ax.grid(True, alpha=0.3); ax.legend(fontsize=7, loc="upper left")
        ax = axes[1, j]
        ax.plot(x["mau_new_lowact"] / x["mau"], color="#c62828", linewidth=1.6, label="share of MAU")
        ax.plot(x["dau_new_lowact"] / x["dau"], color="#1f77b4", linewidth=1.2, label="share of DAU")
        ax.set_title(f"Linux {scope}: bot-like share", fontsize=11)
        ax.set_ylim(0, 0.8); ax.grid(True, alpha=0.3); ax.legend(fontsize=7, loc="upper left")
        for a in (axes[0, j], axes[1, j]):
            a.xaxis.set_major_locator(mdates.MonthLocator(interval=3)); a.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
            plt.setp(a.xaxis.get_majorticklabels(), rotation=45)
    save(fig, DRAFT / "plots/linux_bot_segment_shares.png")


if __name__ == "__main__":
    main()
