#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Sanity check: does paid Fenix growth correlate with ORGANIC growth, segment by segment (country x app)?

Brendan, 2026-10-15: "a correlation analysis between paid growth and organic growth for all organic segments ... I'm hoping it
comes back negative". Segments: 16 countries x {Fenix organic (= total - measured paid), Firefox iOS, Focus Android, Focus iOS}.
The paid signal is the country's paid Fenix MAU (the split). Everything is sampled every 28 days (the metric is a rolling-28
count, so daily differences overlap).

Three views, all written to <out>/paid_organic_correlation.{csv,html} with the plots embedded:

  A. Cross-section (one point per country, per app): organic growth over a window vs paid growth over the same window, both
     expressed relative to the country's organic level at the window start (so "paid grew by 20% of organic"). Windows:
     the last year (2025-10-09 -> 2026-10-09) and the high spend period (2026-04-01 -> 2026-10-09). Pearson and Spearman per app.
  B. Monthly panel per app: 28-day log-changes of organic vs the 28-day change of paid relative to organic, per country,
     with EACH PERIOD'S CROSS-COUNTRY MEAN REMOVED (kills the seasonality every segment shares) and Iran dropped.
     Pooled correlation per app, at lags 0 and +1 period (paid leading), plus a PLACEBO: the same regression against the
     paid change in all OTHER countries, which is what a spurious value looks like.
  C. Per-segment table: the demeaned panel correlation for each of the 64 segments, with n.

Usage:
    python scripts/analyze_paid_organic_correlation.py
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

repo_root = Path(__file__).resolve().parent.parent
DRAFT = repo_root / "data-official/2027-glean-draft"
APPS = ["fenix_android", "firefox_ios", "focus_android", "focus_ios"]
APP_LABEL = {"fenix_android": "Fenix organic", "firefox_ios": "Firefox iOS", "focus_android": "Focus Android", "focus_ios": "Focus iOS"}
STEP_DAYS = 28
WINDOWS = {"last year (2025-10-09 → 2026-10-09)": ("2025-10-09", "2026-10-09"), "high spend period (2026-04-01 → 2026-10-09)": ("2026-04-01", "2026-10-09")}


def load() -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_parquet(DRAFT / "raw_glean_mobile_countryxapp_2026-10-09/mozaic_parts.raw.glean.mobile.MAU.parquet")
    raw["x"] = pd.to_datetime(raw["x"])
    split = pd.read_parquet(sorted(DRAFT.glob("organic_mau/fenix_paid_organic_mau.*.parquet"))[-1])
    split["submission_date"] = pd.to_datetime(split["submission_date"])
    return raw, split


def organic_panel(raw: pd.DataFrame, split: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(organic MAU wide: index date, columns (country, app)) and (paid MAU wide: index date, columns country), from the split's first day."""
    start = split["submission_date"].min()
    frames = {}
    for app in APPS:
        w = raw[raw[app]].pivot(index="x", columns="country", values="y").astype(float)
        frames[app] = w
    paid = split.pivot(index="submission_date", columns="country", values="paid_mau").astype(float)
    fenix = frames["fenix_android"].loc[start:]
    frames["fenix_android"] = fenix - paid.reindex(fenix.index).reindex(columns=fenix.columns).fillna(0)
    organic = pd.concat({app: f.loc[start:] for app, f in frames.items()}, axis=1).swaplevel(axis=1).sort_index(axis=1)
    organic.columns.names = ["country", "app"]
    return organic, paid.loc[start:]


def cross_section(organic: pd.DataFrame, paid: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for window, (a, b) in WINDOWS.items():
        a, b = pd.Timestamp(a), pd.Timestamp(b)
        for app in APPS:
            for c in paid.columns:
                o0, o1 = organic.loc[a, (c, app)], organic.loc[b, (c, app)]
                if not (o0 > 0):
                    continue
                rows.append({"window": window, "app": app, "country": c, "organic_growth": o1 / o0 - 1,
                             "paid_growth_rel_organic": (paid.loc[b, c] - paid.loc[a, c]) / organic.loc[a, (c, "fenix_android")],
                             "paid_share_end": paid.loc[b, c] / (paid.loc[b, c] + organic.loc[b, (c, "fenix_android")])})
    return pd.DataFrame(rows)


def panel_changes(organic: pd.DataFrame, paid: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """28-day sampled series -> organic log-change per (country, app), paid change relative to the country's organic Fenix level."""
    idx = organic.index[::-1][::STEP_DAYS][::-1]           # every 28 days, anchored on the last day
    o = organic.loc[idx]
    p = paid.loc[idx]
    dlog_o = np.log(o).diff().iloc[1:]
    fenix_org = o.xs("fenix_android", axis=1, level="app")
    dpaid = (p.diff() / fenix_org.shift(1)).iloc[1:]
    return dlog_o, dpaid


def demean_by_period(df: pd.DataFrame) -> pd.DataFrame:
    return df.sub(df.mean(axis=1), axis=0)


def panel_correlations(dlog_o: pd.DataFrame, dpaid: pd.DataFrame, exclude=("IR",)) -> tuple[pd.DataFrame, pd.DataFrame]:
    countries = [c for c in dpaid.columns if c not in exclude]
    dpaid = dpaid[countries]
    rng = np.random.default_rng(20261015)
    pooled, per_segment = [], []
    for app in APPS:
        o = dlog_o.xs(app, axis=1, level="app")[countries].replace([np.inf, -np.inf], np.nan)
        o_dm, p_dm = demean_by_period(o), demean_by_period(dpaid)
        # lag 0: same period; lag +1: paid change in the PREVIOUS period (paid leads); lag -1: paid change in the NEXT period,
        # which cannot have caused this period's organic change -- a falsification test
        for lag in (-1, 0, 1):
            stack = pd.DataFrame({"o": o_dm.stack(), "p": p_dm.shift(lag).stack()}).dropna()
            r, pv = stats.pearsonr(stack["o"], stack["p"])
            rho, _ = stats.spearmanr(stack["o"], stack["p"])
            slope = np.polyfit(stack["p"], stack["o"], 1)[0]
            # permutation null: shuffle which country's paid series each organic series is paired with (period structure kept)
            null = []
            for _ in range(500):
                perm = p_dm.shift(lag).copy(); perm.columns = rng.permutation(perm.columns)
                st = pd.DataFrame({"o": o_dm.stack(), "p": perm[o_dm.columns].stack()}).dropna()
                null.append(stats.pearsonr(st["o"], st["p"])[0])
            null = np.array(null)
            pooled.append({"app": app, "lag_periods": lag, "n": len(stack), "pearson_r": r, "p_value": pv, "spearman_rho": rho,
                           "slope_dlog_organic_per_unit_paid": slope, "permutation_null_r_p95": float(np.quantile(np.abs(null), 0.95)),
                           "permutation_p_value": float((np.abs(null) >= abs(r)).mean())})
        for c in countries:
            s = pd.DataFrame({"o": o_dm[c], "p": p_dm[c]}).dropna()
            if len(s) >= 8:
                r, pv = stats.pearsonr(s["o"], s["p"])
                per_segment.append({"app": app, "country": c, "n": len(s), "pearson_r": r, "p_value": pv})
    return pd.DataFrame(pooled), pd.DataFrame(per_segment)


def plot_cross_section(cs: pd.DataFrame, out: Path) -> None:
    fig, axes = plt.subplots(len(WINDOWS), len(APPS), figsize=(22, 10))
    for i, window in enumerate(WINDOWS):
        for j, app in enumerate(APPS):
            ax = axes[i, j]
            d = cs[(cs.window == window) & (cs.app == app)]
            d_ex = d[d.country != "IR"]
            ax.scatter(d_ex.paid_growth_rel_organic, d_ex.organic_growth, color="#1f77b4", s=30)
            # IR's 2026 is the shutdown recovery (organic +3,500% over the high spend period); it is excluded from the statistics and kept
            # off the axes, which are set from the other countries, so it is listed in the corner instead of drawn
            ir = d[d.country == "IR"]
            if len(ir):
                ax.text(0.02, 0.02, f"IR off-scale: organic {ir.organic_growth.item():+.0%}, paid {ir.paid_growth_rel_organic.item():+.0%} (excluded)",
                        transform=ax.transAxes, fontsize=8, color="#777")
            pad = lambda lo, hi: (lo - 0.08 * (hi - lo), hi + 0.08 * (hi - lo))
            ax.set_xlim(*pad(d_ex.paid_growth_rel_organic.min(), d_ex.paid_growth_rel_organic.max()))
            ax.set_ylim(*pad(d_ex.organic_growth.min(), d_ex.organic_growth.max()))
            for _, r in d_ex.iterrows():
                ax.annotate(r.country, (r.paid_growth_rel_organic, r.organic_growth), fontsize=8, xytext=(3, 3), textcoords="offset points")
            if len(d_ex) > 3:
                pr, pp = stats.pearsonr(d_ex.paid_growth_rel_organic, d_ex.organic_growth)
                sr, sp = stats.spearmanr(d_ex.paid_growth_rel_organic, d_ex.organic_growth)
                b, a = np.polyfit(d_ex.paid_growth_rel_organic, d_ex.organic_growth, 1)
                xs = np.linspace(d_ex.paid_growth_rel_organic.min(), d_ex.paid_growth_rel_organic.max(), 10)
                ax.plot(xs, a + b * xs, color="#d62728", lw=1, alpha=0.7)
                ax.set_title(f"{APP_LABEL[app]} — {window.split(' (')[0]}\nPearson {pr:+.2f} (p={pp:.2f}), Spearman {sr:+.2f} (p={sp:.2f}), slope {b:+.2f}", fontsize=10)
            ax.set_xlabel("paid Fenix MAU growth, as a share of the country's organic Fenix level")
            ax.set_ylabel("organic MAU growth")
            ax.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0)); ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0))
            ax.grid(alpha=0.3)
    fig.suptitle("Cross-section: organic growth vs paid growth, one point per country (IR excluded: shutdown year)", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(out, dpi=100); plt.close(fig)


def plot_panel(dlog_o: pd.DataFrame, dpaid: pd.DataFrame, out: Path) -> None:
    countries = [c for c in dpaid.columns if c != "IR"]
    fig, axes = plt.subplots(1, len(APPS), figsize=(22, 5.5))
    p_dm = demean_by_period(dpaid[countries])
    for ax, app in zip(axes, APPS):
        o_dm = demean_by_period(dlog_o.xs(app, axis=1, level="app")[countries])
        st = pd.DataFrame({"o": o_dm.stack(), "p": p_dm.stack()}).dropna()
        ax.scatter(st["p"], st["o"], s=8, alpha=0.5)
        r, pv = stats.pearsonr(st["o"], st["p"]); b, a = np.polyfit(st["p"], st["o"], 1)
        xs = np.linspace(st["p"].min(), st["p"].max(), 10); ax.plot(xs, a + b * xs, color="#d62728", lw=1)
        ax.set_title(f"{APP_LABEL[app]} — monthly panel, period means removed\nr {r:+.2f} (p={pv:.3f}), slope {b:+.2f}, n={len(st)}", fontsize=10)
        ax.set_xlabel("Δ paid / organic Fenix level, demeaned"); ax.set_ylabel("Δ log organic MAU, demeaned"); ax.grid(alpha=0.3)
        ax.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0)); ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0))
    fig.tight_layout(); fig.savefig(out, dpi=100); plt.close(fig)


def plot_segments(per_segment: pd.DataFrame, out: Path) -> None:
    piv = per_segment.pivot(index="country", columns="app", values="pearson_r")[APPS]
    fig, ax = plt.subplots(figsize=(9, 8))
    im = ax.imshow(piv.values, cmap="RdBu_r", vmin=-1, vmax=1, aspect="auto")
    ax.set_xticks(range(len(APPS))); ax.set_xticklabels([APP_LABEL[a] for a in APPS], rotation=20)
    ax.set_yticks(range(len(piv.index))); ax.set_yticklabels(piv.index)
    for i in range(piv.shape[0]):
        for j in range(piv.shape[1]):
            v = piv.values[i, j]
            if pd.notna(v):
                ax.text(j, i, f"{v:+.2f}", ha="center", va="center", fontsize=9, color="black" if abs(v) < 0.6 else "white")
    fig.colorbar(im, ax=ax, label="Pearson r, demeaned monthly changes")
    ax.set_title("Per segment: corr(Δ log organic, Δ paid / organic Fenix level), same country")
    fig.tight_layout(); fig.savefig(out, dpi=110); plt.close(fig)


def img(path: Path) -> str:
    return f'<figure><img src="data:image/png;base64,{base64.b64encode(path.read_bytes()).decode()}"><figcaption><code>{path.relative_to(repo_root)}</code></figcaption></figure>'


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out-dir", type=Path, default=DRAFT / "organic_mau")
    args = p.parse_args()
    raw, split = load()
    organic, paid = organic_panel(raw, split)
    cs = cross_section(organic, paid)
    dlog_o, dpaid = panel_changes(organic, paid)
    pooled, per_segment = panel_correlations(dlog_o, dpaid)
    plots = args.out_dir / "plots"; plots.mkdir(parents=True, exist_ok=True)
    plot_cross_section(cs, plots / "paid_organic_cross_section.png")
    plot_panel(dlog_o, dpaid, plots / "paid_organic_panel.png")
    plot_segments(per_segment, plots / "paid_organic_segments.png")
    cs.to_csv(args.out_dir / "paid_organic_cross_section.csv", index=False)
    pooled.to_csv(args.out_dir / "paid_organic_panel_pooled.csv", index=False)
    per_segment.to_csv(args.out_dir / "paid_organic_panel_segments.csv", index=False)

    cs_stats = []
    for window in WINDOWS:
        for app in APPS:
            d = cs[(cs.window == window) & (cs.app == app) & (cs.country != "IR")]
            pr, pp = stats.pearsonr(d.paid_growth_rel_organic, d.organic_growth); sr, sp = stats.spearmanr(d.paid_growth_rel_organic, d.organic_growth)
            cs_stats.append({"window": window, "app": app, "n": len(d), "pearson_r": pr, "p": pp, "spearman_rho": sr, "p_s": sp})
    cs_stats = pd.DataFrame(cs_stats)
    fmt = lambda df: df.to_html(index=False, float_format=lambda v: f"{v:+.3f}" if abs(v) < 10 else f"{v:,.0f}", classes="t", border=0)
    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"], text=True).strip()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Paid vs organic growth — correlation sanity check</title>
<style>body{{font:15px/1.5 -apple-system,Helvetica,Arial,sans-serif;max-width:1500px;margin:0 auto;padding:0 24px 60px;color:#1c1c1c}}
.banner{{background:#222;color:#eee;padding:10px 16px;margin:0 -24px 24px;font-size:13px;display:flex;gap:28px;flex-wrap:wrap}} .banner b{{color:#fff}}
table.t{{border-collapse:collapse;font-size:13px;margin:12px 0}} table.t th,table.t td{{border:1px solid #ddd;padding:4px 8px;text-align:right}} table.t th{{background:#f3f3f3}} table.t td:first-child,table.t td:nth-child(2){{text-align:left}}
figure{{margin:18px 0}} img{{max-width:100%;border:1px solid #ddd}} figcaption{{font-size:12px;color:#555}} .callout{{border-left:4px solid #6a1b9a;background:#f5eefa;padding:8px 14px;margin:14px 0}} h2{{margin-top:30px;border-bottom:1px solid #ddd}}</style></head><body>
<div class="banner"><span><b>Generated:</b> {now}</span><span><b>Source:</b> <code>scripts/analyze_paid_organic_correlation.py</code>, mozaic-daily {commit}</span><span><b>Data as of:</b> {organic.index.max().date()} (real Glean mobile MAU; paid split from {organic.index.min().date()})</span><span><b>Status:</b> internal sanity check</span></div>
<h1>Does paid Fenix growth correlate with organic growth? Segment-level sanity check</h1>
<div class="callout">Segments: 16 countries × {{Fenix organic = total − measured paid, Firefox iOS, Focus Android, Focus iOS}}. Paid signal: the country's paid Fenix MAU. Sampled every {STEP_DAYS} days (rolling-28 metric). Iran excluded from every statistic (its 2026 is the shutdown).
Paid growth is expressed relative to the country's organic Fenix level, so a slope of +0.5 reads "organic grew by half of what paid added".</div>
<h2>A. Cross-section by country</h2>{img(plots / "paid_organic_cross_section.png")}{fmt(cs_stats)}
<h2>B. Monthly panel, shared seasonality removed, with lead/lag and a permutation null</h2>
<p>Each period's cross-country mean is subtracted from both sides, so anything every country does together (seasonality, releases, the mobile-wide 2026 lift) cannot create the correlation. Lag +1 = paid change in the previous period (paid leads organic); lag −1 = paid change in the <i>next</i> period, which cannot have caused this period's organic change, so a similar value there means the correlation is not causal. The permutation null re-pairs each organic series with a random other country's paid series 500 times, keeping the period structure: <code>permutation_null_r_p95</code> is the |r| a spurious pairing reaches 5% of the time, <code>permutation_p_value</code> the share of random pairings at least as strong as the real one.</p>
{img(plots / "paid_organic_panel.png")}{fmt(pooled)}
<h2>C. Every segment</h2>{img(plots / "paid_organic_segments.png")}{fmt(per_segment.sort_values(["app", "pearson_r"], ascending=[True, False]))}
</body></html>"""
    out = args.out_dir / "paid_organic_correlation.html"
    out.write_text(html)
    pd.set_option("display.width", 200)
    print(cs_stats.to_string(index=False, float_format=lambda v: f"{v:+.3f}"))
    print(pooled.to_string(index=False, float_format=lambda v: f"{v:+.3f}"))
    print(f"page  {out}")


if __name__ == "__main__":
    main()
