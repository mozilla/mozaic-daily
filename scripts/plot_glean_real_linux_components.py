#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Linux desktop: real Glean vs Glean-equivalent, world and the three countries carrying the growth (PRE-DRAFT diagnostic).

Why: the real-Glean MAU probes (2026-10-13) extrapolate Linux's 2025-26 surge as trend. Brendan: LLM/bot growth is
happening on all systems, mostly Linux, and those are not trend markers; can short components be separated from long
ones? The Glean-equivalent column already excludes the Linux Glean-only temporary term, so real − Glean-equivalent per
country is a measured daily short component. This plot shows it: MAU and DAU, world and ROW / DE / US.

Usage:
    python scripts/plot_glean_real_linux_components.py   # writes data-official/2027-glean-draft/plots/linux_real_vs_glean_equivalent.png
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
REAL = DRAFT / "raw_glean_real_osxcountry_2026-10-09"
GEQ = DRAFT / "raw_glean_history_2026-10-09"
COUNTRIES = ["ROW", "DE", "US"]


def linux_by_country(raw_dir: Path, metric: str) -> pd.DataFrame:
    df = pd.read_parquet(raw_dir / f"mozaic_parts.raw.glean_history.desktop.{metric}.parquet")
    df["x"] = pd.to_datetime(df["x"])
    return df[df["linux"]].pivot_table(index="x", columns="country", values="y", aggfunc="sum").astype(float)


def main():
    fig, axes = plt.subplots(2, 4, figsize=(20, 9), sharex=True)
    for row, metric in enumerate(("MAU", "DAU")):
        real, geq = linux_by_country(REAL, metric), linux_by_country(GEQ, metric)
        geq = geq.loc[real.index.min():]
        for ax, scope in zip(axes[row], ["world", *COUNTRIES]):
            r = real.sum(axis=1) if scope == "world" else real[scope]
            g = geq.sum(axis=1) if scope == "world" else geq[scope]
            if metric == "DAU":
                r, g = r.rolling(28).mean(), g.rolling(28).mean()
            ax.plot(r, color="black", linewidth=1.8, label="real Glean")
            ax.plot(g, color="dimgrey", linewidth=1.2, label="Glean-equivalent (temporary term excluded)")
            ax.fill_between(r.index, g, r, where=(r > g), color="#c62828", alpha=0.25, label="real − Glean-equivalent (short component)")
            gap_last = (r - g).iloc[-28:].mean()
            ax.set_title(f"Linux {scope} {metric}{' 28d MA' if metric == 'DAU' else ''}: last-28d gap {gap_last / 1e6:+.2f}M ({gap_last / g.iloc[-28:].mean():+.0%})", fontsize=11)
            ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(millions_formatter))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3)); ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            ax.grid(True, alpha=0.3); ax.legend(fontsize=7, loc="upper left")
    save(fig, DRAFT / "plots/linux_real_vs_glean_equivalent.png")


if __name__ == "__main__":
    main()
