#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Quarter-by-quarter version of Section D of the paid-vs-organic sanity check (for the marketing DS conversation).

Brendan, 2026-10-15: "1 quarter charts going back two years ... Q3 2026, Q2 2026, and so on". Section D of
`paid_organic_correlation.html` compares, per country, organic Fenix MAU growth MINUS the same country's Firefox iOS growth
(iOS stands in for local market growth) against the paid Fenix MAU added, as a share of organic Fenix at the window start.
It used two long windows; this script cuts the same quantity into calendar quarters, Q4 2024 (the paid split's first day is
2024-10-01) through the last complete quarter.

Growth is endpoint to endpoint on the rolling-28 MAU: first day of the quarter -> last day of the quarter. Iran is excluded
from every statistic and kept off the axes (2025-26 is its shutdown and recovery). All quarter panels share axes.

`--months 6` (Brendan, 2026-10-15: quarters split India's effect across boundaries) groups consecutive quarters into
six-month periods counted back from the last complete quarter, so the latest is Apr-Sep 2026 and matches section D's high
spend period; periods are labelled by month range ("Apr–Sep 2026").

Writes to <out-dir>, with SUFFIX "quarterly" for 3 months and "<N>mo" otherwise: plots/paid_vs_excess_over_ios_SUFFIX.png,
plots/paid_vs_excess_over_ios_SUFFIX_corr.png, paid_organic_SUFFIX.csv (one row per period x country), paid_organic_SUFFIX.html.

Usage:
    python scripts/analyze_paid_organic_quarterly.py              # quarters
    python scripts/analyze_paid_organic_quarterly.py --months 6   # six-month periods
"""

from __future__ import annotations

import argparse
import base64
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent))
from analyze_paid_organic_correlation import DRAFT, load, organic_panel, repo_root  # noqa: E402

EXCLUDED_COUNTRIES = ("IR",)
HIGHLIGHT_COUNTRIES = {"IN": "#d62728", "ID": "#ff7f0e", "BR": "#2ca02c"}


def complete_periods(first_day: pd.Timestamp, last_day: pd.Timestamp, months: int) -> list[tuple[str, pd.Timestamp, pd.Timestamp]]:
    """(label, start, end) for consecutive whole calendar quarters inside [first_day, last_day], grouped `months // 3` at a time
    counting back from the last complete quarter (a leftover partial group at the start is dropped)."""
    if months % 3:
        raise ValueError(f"--months must be a multiple of 3, got {months}")
    quarters = [q for q in pd.period_range(first_day, last_day, freq="Q")
                if q.start_time >= first_day and q.end_time.normalize() <= last_day]
    per_group = months // 3
    groups = [quarters[i - per_group:i] for i in range(len(quarters), per_group - 1, -per_group)][::-1]
    periods = []
    for group in groups:
        start, end = group[0].start_time, group[-1].end_time.normalize()
        label = str(group[0]) if per_group == 1 else f"{start:%b}–{end:%b %Y}" if start.year == end.year else f"{start:%b %Y}–{end:%b %Y}"
        periods.append((label, start, end))
    return periods


def period_excess(organic: pd.DataFrame, paid: pd.DataFrame, periods: list[tuple[str, pd.Timestamp, pd.Timestamp]]) -> pd.DataFrame:
    """One row per (period, country): organic Fenix growth, iOS growth, their difference, paid added relative to organic Fenix."""
    rows = []
    for label, start, end in periods:
        for country in paid.columns:
            fenix_start, fenix_end = organic.loc[start, (country, "fenix_android")], organic.loc[end, (country, "fenix_android")]
            ios_start, ios_end = organic.loc[start, (country, "firefox_ios")], organic.loc[end, (country, "firefox_ios")]
            if not (fenix_start > 0 and ios_start > 0):
                continue
            fenix_growth, ios_growth = fenix_end / fenix_start - 1, ios_end / ios_start - 1
            rows.append({"period": label, "start": start.date(), "end": end.date(), "country": country,
                         "organic_fenix_growth": fenix_growth, "ios_growth": ios_growth,
                         "excess_over_ios": fenix_growth - ios_growth,
                         "paid_added_rel_organic": (paid.loc[end, country] - paid.loc[start, country]) / fenix_start,
                         "paid_share_end": paid.loc[end, country] / (paid.loc[end, country] + fenix_end),
                         "organic_fenix_mau_end": fenix_end})
    return pd.DataFrame(rows)


def period_correlations(table: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for period, group in table[~table.country.isin(EXCLUDED_COUNTRIES)].groupby("period", sort=False):
        pearson_r, pearson_p = stats.pearsonr(group.paid_added_rel_organic, group.excess_over_ios)
        spearman_rho, spearman_p = stats.spearmanr(group.paid_added_rel_organic, group.excess_over_ios)
        slope = np.polyfit(group.paid_added_rel_organic, group.excess_over_ios, 1)[0]
        rows.append({"period": period, "n": len(group), "pearson_r": pearson_r, "pearson_p": pearson_p,
                     "spearman_rho": spearman_rho, "spearman_p": spearman_p, "slope": slope})
    return pd.DataFrame(rows)


def plot_period_panels(table: pd.DataFrame, correlations: pd.DataFrame, unit: str, out: Path) -> None:
    periods = list(correlations.period)
    ncols = 4
    nrows = int(np.ceil(len(periods) / ncols))
    included = table[~table.country.isin(EXCLUDED_COUNTRIES)]
    pad = lambda lo, hi: (lo - 0.08 * (hi - lo), hi + 0.08 * (hi - lo))
    xlim = pad(min(included.paid_added_rel_organic.min(), 0), included.paid_added_rel_organic.max())
    ylim = pad(included.excess_over_ios.min(), included.excess_over_ios.max())
    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 5 * nrows), sharex=True, sharey=True, squeeze=False)
    for ax, period in zip(axes.flat, periods):
        group = included[included.period == period]
        stat = correlations.set_index("period").loc[period]
        colors = [HIGHLIGHT_COUNTRIES.get(c, "#1f77b4") for c in group.country]
        ax.scatter(group.paid_added_rel_organic, group.excess_over_ios, color=colors, s=32, zorder=3)
        for _, row in group.iterrows():
            ax.annotate(row.country, (row.paid_added_rel_organic, row.excess_over_ios), fontsize=8, xytext=(3, 3), textcoords="offset points",
                        color=HIGHLIGHT_COUNTRIES.get(row.country, "black"), fontweight="bold" if row.country in HIGHLIGHT_COUNTRIES else "normal")
        xs = np.linspace(group.paid_added_rel_organic.min(), group.paid_added_rel_organic.max(), 10)
        intercept = np.polyfit(group.paid_added_rel_organic, group.excess_over_ios, 1)[1]
        ax.plot(xs, intercept + stat.slope * xs, color="#888", lw=1, zorder=2)
        ax.axhline(0, color="#555", lw=0.8); ax.axvline(0, color="#555", lw=0.8)
        start, end = group.start.iloc[0], group.end.iloc[0]
        ax.set_title(f"{period} ({start:%b %d} → {end:%b %d %Y})\nPearson {stat.pearson_r:+.2f} (p={stat.pearson_p:.2f}), "
                     f"Spearman {stat.spearman_rho:+.2f} (p={stat.spearman_p:.2f}), slope {stat.slope:+.2f}", fontsize=10)
        excluded = table[(table.period == period) & table.country.isin(EXCLUDED_COUNTRIES)]
        for _, row in excluded.iterrows():
            ax.text(0.02, 0.02, f"{row.country} excluded: excess {row.excess_over_ios:+.0%}, paid {row.paid_added_rel_organic:+.0%}",
                    transform=ax.transAxes, fontsize=8, color="#777")
        ax.set_xlim(*xlim); ax.set_ylim(*ylim); ax.grid(alpha=0.3)
        ax.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0))
        ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0))
    for ax in axes.flat[len(periods):]:
        ax.set_visible(False)
    for ax in axes[-1]:
        ax.set_xlabel(f"paid Fenix MAU added in the {unit},\nas a share of organic Fenix at {unit} start")
    for ax in axes[:, 0]:
        ax.set_ylabel(f"organic Fenix growth − iOS growth\n(same country, same {unit})")
    fig.suptitle(f"Organic Fenix growth in excess of the same country's Firefox iOS growth vs paid added, by {unit} "
                 "(one point per country; shared axes; IN/ID/BR highlighted; grey = OLS fit)", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(out, dpi=100); plt.close(fig)


def plot_correlation_by_period(correlations: pd.DataFrame, table: pd.DataFrame, unit: str, out: Path) -> None:
    fig, (ax_corr, ax_countries) = plt.subplots(1, 2, figsize=(16, 5))
    x = np.arange(len(correlations))
    ax_corr.plot(x, correlations.pearson_r, marker="o", label="Pearson r")
    ax_corr.plot(x, correlations.spearman_rho, marker="s", label="Spearman ρ")
    critical_r = stats.t.ppf(0.975, correlations.n - 2) / np.sqrt(correlations.n - 2 + stats.t.ppf(0.975, correlations.n - 2) ** 2)
    ax_corr.fill_between(x, -critical_r, critical_r, color="#ddd", alpha=0.6, label=f"|r| below p=0.05 (n={correlations.n.iloc[0]})")
    ax_corr.axhline(0, color="#555", lw=0.8)
    ax_corr.set_xticks(x); ax_corr.set_xticklabels(correlations.period)
    ax_corr.set_ylim(-1, 1); ax_corr.set_ylabel("correlation across countries (IR excluded)")
    ax_corr.set_title(f"Cross-country correlation of excess-over-iOS with paid added, per {unit}"); ax_corr.grid(alpha=0.3); ax_corr.legend(fontsize=9)
    for country, color in HIGHLIGHT_COUNTRIES.items():
        series = table[table.country == country].set_index("period").reindex(correlations.period)
        ax_countries.plot(x, series.excess_over_ios, marker="o", color=color, label=f"{country} excess over iOS")
        ax_countries.plot(x, series.paid_added_rel_organic, marker="o", ls="--", color=color, alpha=0.6, label=f"{country} paid added")
    ax_countries.axhline(0, color="#555", lw=0.8)
    ax_countries.set_xticks(x); ax_countries.set_xticklabels(correlations.period)
    ax_countries.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0))
    ax_countries.set_title(f"IN / ID / BR per {unit}: excess over iOS (solid) and paid added (dashed)"); ax_countries.grid(alpha=0.3)
    ax_countries.legend(fontsize=8, ncol=3)
    fig.tight_layout(); fig.savefig(out, dpi=100); plt.close(fig)


def img(path: Path) -> str:
    return f'<figure><img src="data:image/png;base64,{base64.b64encode(path.read_bytes()).decode()}"><figcaption><code>{path.relative_to(repo_root)}</code></figcaption></figure>'


def render_html(table: pd.DataFrame, correlations: pd.DataFrame, unit: str, data_as_of: pd.Timestamp, panels: Path, summary: Path) -> str:
    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"], text=True).strip()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    fmt = lambda df: df.to_html(index=False, float_format=lambda v: f"{v:+.3f}" if abs(v) < 10 else f"{v:,.0f}", classes="t", border=0)
    wide = table[~table.country.isin(EXCLUDED_COUNTRIES)].pivot(index="country", columns="period", values="excess_over_ios")[list(correlations.period)]
    wide = wide.map(lambda v: f"{v:+.1%}").reset_index()
    paid_wide = table[~table.country.isin(EXCLUDED_COUNTRIES)].pivot(index="country", columns="period", values="paid_added_rel_organic")[list(correlations.period)]
    paid_wide = paid_wide.map(lambda v: f"{v:+.1%}").reset_index()
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Paid vs organic excess over iOS — by {unit}</title>
<style>body{{font:15px/1.5 -apple-system,Helvetica,Arial,sans-serif;max-width:1500px;margin:0 auto;padding:0 24px 60px;color:#1c1c1c}}
.banner{{background:#222;color:#eee;padding:10px 16px;margin:0 -24px 24px;font-size:13px;display:flex;gap:28px;flex-wrap:wrap}} .banner b{{color:#fff}}
table.t{{border-collapse:collapse;font-size:13px;margin:12px 0}} table.t th,table.t td{{border:1px solid #ddd;padding:4px 8px;text-align:right}} table.t th{{background:#f3f3f3}} table.t td:first-child{{text-align:left}}
figure{{margin:18px 0}} img{{max-width:100%;border:1px solid #ddd}} figcaption{{font-size:12px;color:#555}} .callout{{border-left:4px solid #6a1b9a;background:#f5eefa;padding:8px 14px;margin:14px 0}} h2{{margin-top:30px;border-bottom:1px solid #ddd}}</style></head><body>
<div class="banner"><span><b>Generated:</b> {now}</span><span><b>Source:</b> <code>scripts/analyze_paid_organic_quarterly.py</code>, mozaic-daily {commit}</span><span><b>Data as of:</b> {data_as_of.date()} (real Glean mobile MAU; paid split from 2024-10-01)</span><span><b>Status:</b> internal, for the marketing DS conversation</span></div>
<h1>Organic Fenix growth in excess of iOS vs paid added, {unit} by {unit}</h1>
<div class="callout">Section D of <code>paid_organic_correlation.html</code>, cut into {unit}s, {correlations.period.iloc[0]} through {correlations.period.iloc[-1]}.
Per country: organic Fenix MAU growth (total − measured paid) minus the same country's Firefox iOS growth, against the paid Fenix MAU added in the
{unit} as a share of organic Fenix at {unit} start. Endpoints are the first and last day of the {unit} on the rolling-28 MAU. Iran excluded.
With 15 countries a single {unit}'s correlation needs |r| ≳ 0.51 to clear p=0.05, so read the panels for which countries move, not for the r.</div>
<h2>Per-{unit} scatter (shared axes)</h2>{img(panels)}
<h2>Correlation by {unit}, and the three countries in question</h2>{img(summary)}{fmt(correlations)}
<h2>Excess over iOS, country × {unit}</h2>{fmt(wide)}
<h2>Paid added (share of organic Fenix at {unit} start), country × {unit}</h2>{fmt(paid_wide)}
</body></html>"""


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, default=DRAFT / "organic_mau")
    parser.add_argument("--months", type=int, default=3, help="period length in months, a multiple of 3 (default 3 = calendar quarters)")
    args = parser.parse_args()
    raw, split = load()
    organic, paid = organic_panel(raw, split)
    periods = complete_periods(organic.index.min(), organic.index.max(), args.months)
    table = period_excess(organic, paid, periods)
    correlations = period_correlations(table)
    unit = "quarter" if args.months == 3 else f"{args.months}-month period"
    suffix = "quarterly" if args.months == 3 else f"{args.months}mo"
    plots = args.out_dir / "plots"; plots.mkdir(parents=True, exist_ok=True)
    panels, summary = plots / f"paid_vs_excess_over_ios_{suffix}.png", plots / f"paid_vs_excess_over_ios_{suffix}_corr.png"
    plot_period_panels(table, correlations, unit, panels)
    plot_correlation_by_period(correlations, table, unit, summary)
    table.to_csv(args.out_dir / f"paid_organic_{suffix}.csv", index=False)
    out = args.out_dir / f"paid_organic_{suffix}.html"
    out.write_text(render_html(table, correlations, unit, organic.index.max(), panels, summary))
    print(correlations.to_string(index=False, float_format=lambda v: f"{v:+.3f}"))
    print(f"page  {out}")


if __name__ == "__main__":
    main()
