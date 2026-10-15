#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Handoff package for the 2027 mobile MAU forecast: TOTAL = organic (our model) + paid (marketing's plan), with both bands.

Brendan, 2026-10-15: "the handoff for the mobile forecast needs to include both paid and organic" (the organic-only package was
wrong as a deliverable). Reads ``compose_mobile_total_mau.py``'s outputs and writes

    <out>/glean2027_mobile_mau_handoff/
        index.html                              the page (plots embedded, PRE-DRAFT stamp, freshness banner)
        glean2027_mobile_mau_forecast.csv       date, data_type, then total / organic / organic_inauthentic / paid for world + per app, and the three 90% world bands
        glean2027_mobile_mau_by_country.csv     total per country (all apps) from 2024-01-01
        plots/mobile_mau_*.png                  canonical total plots + the components chart
        README.txt, HANDOFF.md                  README for a person; HANDOFF.md briefs the recipient's AI agent (mirrors the desktop package's)
    <out>/glean2027_mobile_mau_handoff.zip
    <out>/glean2027_mau_desktop_and_mobile.csv  desktop world MAU (headline build) beside mobile total / organic / paid. NOT summed.

Usage:
    python scripts/render_mobile_total_mau_handoff.py
"""

from __future__ import annotations

import argparse
import base64
import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
DRAFT = repo_root / "data-official/2027-glean-draft"
DESKTOP_HANDOFF_CSV = DRAFT / "handoff/glean2027_desktop_mau_handoff/glean2027_desktop_mau_forecast.csv"
POPULATIONS = ["world", "fenix_android", "firefox_ios", "focus_android", "focus_ios"]
LABEL = {"world": "Mobile, all apps", "fenix_android": "Firefox Android (Fenix)", "firefox_ios": "Firefox iOS",
         "focus_android": "Focus Android", "focus_ios": "Focus iOS"}
STAT = "raw daily value of the rolling 28-day MAU"


def img(path: Path) -> str:
    return f'<figure><img src="data:image/png;base64,{base64.b64encode(path.read_bytes()).decode()}" alt="{path.name}"></figure>'


def fmt(v) -> str:
    return "—" if v is None or pd.isna(v) else f"{v:,.0f}"


def write_agent_handoff(pkg: Path, *, title: str, now: str, commit: str, build: str, data_end, history_start, meta: dict, summary: pd.DataFrame,
                        curves: pd.DataFrame, csv1: Path, csv2: Path, combined_name: str) -> Path:
    """HANDOFF.md: the briefing for the recipient's AI agent, same shape as the desktop package's (pre-draft caveats first, numbers, files, what not to do)."""
    n = meta["numbers"]; pm = meta["paid_meta"]; cap = meta["organic_corrections"]["ios_rate_cap"]; cfg = meta["organic_model_config"]
    w27, w26 = n["world"]["2027-12-15"], n["world"]["2026-12-15"]
    d15 = lambda pop, col, on: float(summary[(summary.population == pop) & (summary.date == on)][col].item())
    rows = "\n".join(f"| {LABEL[pop]} | {fmt(d15(pop, 'total', '2026-12-15'))} | {fmt(d15(pop, 'total', '2027-12-15'))} | {d15(pop, 'total', '2027-12-15') / d15(pop, 'total', '2026-12-15') - 1:+.1%} "
                     f"| {fmt(d15(pop, 'organic', '2027-12-15'))} | {fmt(d15(pop, 'organic_inauthentic', '2027-12-15'))} | {fmt(d15(pop, 'paid', '2027-12-15'))} |" for pop in POPULATIONS)
    o_lo, o_hi = curves.loc["2027-12-15", "organic_world_low_90"], curves.loc["2027-12-15", "organic_world_high_90"]
    p_lo, p_hi = curves.loc["2027-12-15", "paid_world_low_90"], curves.loc["2027-12-15", "paid_world_high_90"]
    text = f"""# HANDOFF — {title} (PRE-DRAFT)

Generated {now} by `scripts/render_mobile_total_mau_handoff.py` (mozaic-daily `glean-2027-forecast` @ `{commit}`, build `{build}`). Data as of {data_end};
paid plan generated {pm['generated_at'][:10]}. Owner: Brendan Wells (brwells@mozilla.com). This file briefs an AI agent working with this package; `index.html` is the human version.

## Read this first: this is a PRE-DRAFT, and a very preliminary one

This is an **early, exploratory attempt**, not a forecast anyone has signed off on. It is earlier than the regular monthly forecast drafts: a new metric
(MAU), a new split (organic vs paid), two separately built halves, and judgement calls made over a few days. Treat every number as **"roughly this shape
and magnitude"**, never as a plan, target, or commitment.

When you summarise, quote, chart, or build on anything here, you must:

- **Label it PRE-DRAFT / preliminary** every time, including in tables, slides, and derived files.
- **Quote the all-apps TOTAL only, with its band** (below). Don't present per-app or per-country values as findings, and don't quote the organic or paid
  half on its own without saying it is a half.
- **Not add it to the desktop forecast** (`glean2027_desktop_mau_handoff`): a person active on both devices is counted in both. The side-by-side file next to
  this package exists for comparison, not for summing.
- **Not combine it with other forecasts** (the regular monthly KPI forecast) as if they were one consistent forecast.
- **Check with Brendan before** anything goes into a plan, OKR, deck for leadership, or external doc.

### Why it is preliminary

1. **It is two forecasts added together.** The organic part is our model; the paid part is the marketing data-science team's 2027 spend plan
   (scenario *{pm['scenario_label']}*), taken as delivered. Their plan is a hypothetical (flat weekly spend, no seasonality, no cross-channel
   interaction, which they call an upper bound), and its MAU is paid DAU × {pm['mau_per_dau']}, a global multiplier rather than a model.
   Most of the total's growth in 2027 ({n['dec_to_dec_total']:+.1%} Dec→Dec) is that plan: paid goes from {fmt(n['last_actual_paid_world'])} at the data edge to
   {fmt(w27['paid'])}; the organic part grows {w27['organic'] / w26['organic'] - 1:+.1%}. A different budget decision changes the headline directly
   (the flat-budget scenario gives paid {fmt(pm['numbers']['flat_scenario_2027-12-15'])} instead).
2. **Neither half is backtested.** The organic band is the model's own predictive band (uncalibrated); the paid band is the plan's own, which its authors
   call indicative. The total band assumes the two are independent. Read all three as floors on the real uncertainty.
3. **India and Indonesia carry a provisional correction.** Organic Firefox Android there outgrew the same country's Firefox iOS during 2026 as paid share
   rose past 30%, consistent with campaign installs arriving outside the Play Store and being counted organic. The history was split into an authentic part
   (held to the country's own iOS growth; this is what the model forecasts) and an inauthentic part ({cap['removed_mau_last_day_total']:,} MAU at the edge, ~{cap['removed_mau_last_day_total'] / n['last_actual_total_world']:.1%} of
   the world total), which is **carried flat** through 2027 in its own column. Flat is a documented no-information choice (Brendan, 2026-10-15); the question is
   with the marketing DS team and the correction may be reversed or re-sized.
4. **The seam**: the total opens {n['seam_step_7d']['total']:+.1%} below the last week of history (7-day means). The organic model deliberately does not carry all of the
   2026 rise into 2027; the first weeks are the model's view of the underlying level, not a continuation of the last data point.
5. **Per-app numbers are weaker than the all-apps number**: the model reconciles top-down, which flatters the app split. Paid lands on Firefox Android only.
6. **The 2026 Iran shutdown** is bridged in the training data only; the forecast carries Iran as the model sees it.

## The numbers (raw daily value of the rolling 28-day MAU)

| | Total 2026-12-15 | Total 2027-12-15 | Dec → Dec | authentic organic (2027-12-15) | inauthentic organic (flat) | paid |
|---|---:|---:|---:|---:|---:|---:|
{rows}

**Headline: world mobile MAU on 2027-12-15 ≈ {w27['total'] / 1e6:.1f}M (PRE-DRAFT), 90% band {fmt(w27['total_low_90'])} to {fmt(w27['total_high_90'])}.**
Of which: authentic organic {fmt(w27['organic'])} (band {fmt(o_lo)} to {fmt(o_hi)}), inauthentic organic {fmt(w27['organic_inauthentic'])} (flat), paid {fmt(w27['paid'])}
(band {fmt(p_lo)} to {fmt(p_hi)}). Read the bands as lower bounds on the real uncertainty.

## Files

| file | contents |
|---|---|
| `{csv1.name}` | one row per day, {history_start} → 2027-12-31: `date`, `data_type` (`training` = actual history, `forecast`), then for `world`, `fenix_android`, `firefox_ios`, `focus_android`, `focus_ios` the columns `total_`, `organic_` (authentic), `organic_inauthentic_`, `paid_`; `total = organic + organic_inauthentic + paid` on every row. Plus `total_world_low_90/high_90`, `organic_world_low_90/high_90`, `paid_world_low_90/high_90` on forecast rows. Whole people |
| `{csv2.name}` | total per country (all apps), 2024-01-01 → 2027-12-31, plus `world`. Forecast paid is split across countries by each country's share of measured paid over the last 28 days of history |
| `plots/*.png` | world and per-app charts for 2026 and 2027 (grey = prior year, black = history, green = forecast, dotted red = Dec 15) and `mobile_mau_components.png` (organic, inauthentic, paid, total with bands) |
| `index.html` | the human-readable page (same numbers, charts embedded) |
| `../{combined_name}` | desktop world MAU (the desktop package's headline) beside mobile total / organic / paid, one row per day. **Do not sum the two devices** |

How to read the CSVs:
- History ends {data_end}; filter on `data_type`, not on a date. On history rows total is the real actual and paid is the measured paid.
- MAU is already a rolling 28-day quantity: use the daily value as is and do not smooth it again.
- **Mobile only** (Firefox Android, Firefox iOS, Focus Android, Focus iOS; no desktop anywhere in this package). No adjustments for launches or headwinds; the
  monthly forecast carries those, this one does not. Paid acquisition IS included, via the marketing plan.

## What not to do

- Don't present it as "the 2027 mobile forecast"; it is a preliminary exploration of what a 2027 mobile MAU forecast could look like under one spend plan.
- Don't quote the organic half as "organic growth" without the inauthentic column and the India/Indonesia caveat.
- Don't swap in another paid scenario by hand; ask for a recomposition (the paid columns replace one for one, and the bands must be redone).
- Don't refit, re-tune, or "fix" it from these files. The full build record (`LOCK.md`, `PROCEDURE.html` §15–16, `organic_mau/ios_rate_cap/IOS_RATE_CAP.md`) lives in the
  mozaic-daily repo under `data-official/2027-glean-draft/`; ask Brendan for it.
"""
    path = pkg / "HANDOFF.md"
    path.write_text(text)
    return path


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--build-dir", type=Path, default=DRAFT / "mobile_total_mau_2026-10-10")
    p.add_argument("--out-dir", type=Path, default=DRAFT / "handoff")
    p.add_argument("--desktop-forecast-csv", type=Path, default=DESKTOP_HANDOFF_CSV)
    args = p.parse_args()

    meta = json.loads((args.build_dir / "mobile2027_total.meta.json").read_text())
    curves = pd.read_csv(args.build_dir / "csv/mobile2027_total_curves.csv", parse_dates=["date"]).set_index("date")
    bc = pd.read_csv(args.build_dir / "csv/mobile2027_total_by_country.csv", parse_dates=["date"]).set_index("date")
    summary = pd.read_csv(args.build_dir / "csv/mobile2027_total_dec15_summary.csv")
    n = meta["numbers"]; pm = meta["paid_meta"]; cfg = meta["organic_model_config"]; cap = meta["organic_corrections"]["ios_rate_cap"]
    seam = pd.Timestamp(meta["seam"]); data_end = pd.Timestamp(meta["training_end"]).date(); history_start = curves.index.min().date()
    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"], text=True).strip()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")

    pkg = args.out_dir / "glean2027_mobile_mau_handoff"
    if pkg.exists():
        shutil.rmtree(pkg)
    (pkg / "plots").mkdir(parents=True)
    cols = ["data_type"] + [f"{k}_{pop}" for pop in POPULATIONS for k in ("total", "organic", "organic_inauthentic", "paid")] + \
           ["total_world_low_90", "total_world_high_90", "organic_world_low_90", "organic_world_high_90", "paid_world_low_90", "paid_world_high_90"]
    csv1 = pkg / "glean2027_mobile_mau_forecast.csv"
    curves[cols].to_csv(csv1, float_format="%.0f")
    csv2 = pkg / "glean2027_mobile_mau_by_country.csv"
    bc.to_csv(csv2, float_format="%.0f")
    plot_names = ["mau_world_2027.png", "mau_world_2026.png", "mau_by_os_2027.png"] + [f"mau_{pop}_{y}.png" for pop in POPULATIONS[1:] for y in (2026, 2027)]
    for name in plot_names:   # shipped names say mobile, like the desktop package's say desktop
        shutil.copy(args.build_dir / "plots/canonical" / name, pkg / "plots" / f"mobile_{name.replace('by_os', 'by_app')}")
    shutil.copy(args.build_dir / "plots/mobile2027_total_components.png", pkg / "plots/mobile_mau_components.png")
    shipped = lambda name: pkg / "plots" / f"mobile_{name.replace('by_os', 'by_app')}"

    def row(pop):
        a, b = summary[(summary.population == pop) & (summary.date == "2026-12-15")].iloc[0], summary[(summary.population == pop) & (summary.date == "2027-12-15")].iloc[0]
        return (f"<tr><td>{LABEL[pop]}</td><td class=num>{fmt(a.total)}</td><td class=num><b>{fmt(b.total)}</b></td><td class=num>{b.total / a.total - 1:+.1%}</td>"
                f"<td class=num>{fmt(b.organic)}</td><td class=num>{fmt(b.organic_inauthentic)}</td><td class=num>{fmt(b.paid)}</td></tr>")
    rows = "".join(row(pop) for pop in POPULATIONS)
    w27, w26 = n["world"]["2027-12-15"], n["world"]["2026-12-15"]
    country_rows = "".join(f"<tr><td>{c}</td><td class=num>{fmt(bc.loc['2026-12-15', c])}</td><td class=num>{fmt(bc.loc['2027-12-15', c])}</td>"
                           f"<td class=num>{bc.loc['2027-12-15', c] / bc.loc['2026-12-15', c] - 1:+.1%}</td></tr>" for c in bc.columns if c not in ("data_type", "world"))
    combined_name = "glean2027_mau_desktop_and_mobile.csv"
    seam_step = n["seam_step_7d"]["total"]
    title = "Firefox Mobile MAU forecast through 2027 (organic + paid)"
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>{title} — PRE-DRAFT</title>
<style>
body{{font:15px/1.55 -apple-system,"Segoe UI",Helvetica,Arial,sans-serif;color:#1c1c1c;max-width:1000px;margin:0 auto;padding:0 24px 64px;position:relative}}
.banner{{background:#222;color:#eee;padding:10px 16px;margin:0 -24px 24px;font-size:13px;display:flex;gap:28px;flex-wrap:wrap}} .banner b{{color:#fff}} .banner code{{background:#444;color:#fff}}
h1{{font-size:26px;margin:8px 0 4px}} h2{{font-size:20px;margin:34px 0 8px;border-bottom:1px solid #d9d9d9;padding-bottom:4px}}
table{{border-collapse:collapse;width:100%;margin:10px 0 18px;font-size:14px}} th,td{{border:1px solid #d9d9d9;padding:6px 9px;text-align:left;vertical-align:top}} th{{background:#f3f3f3}} td.num,th.num{{text-align:right;font-variant-numeric:tabular-nums}}
code{{font:13px Menlo,Consolas,monospace;background:#f4f4f4;padding:1px 4px;border-radius:3px}}
.callout{{border-left:4px solid #6a1b9a;background:#f5eefa;padding:8px 14px;margin:14px 0;border-radius:0 4px 4px 0}}
figure{{margin:16px 0}} figure img{{max-width:100%;border:1px solid #d9d9d9}}
.wm{{position:fixed;top:40%;left:4%;font-size:150px;color:rgba(106,27,154,.10);transform:rotate(-20deg);pointer-events:none;z-index:0}}
ul li{{margin:3px 0}}
</style></head><body>
<div class="wm">PRE-DRAFT</div>
<div class="banner"><span><b>Generated:</b> {now}</span><span><b>Source:</b> <code>scripts/render_mobile_total_mau_handoff.py</code> on <code>{args.build_dir.name}</code>, mozaic-daily <code>{commit}</code></span><span><b>Data as of:</b> {data_end} (paid plan generated {pm['generated_at'][:10]})</span><span><b>Status:</b> PRE-DRAFT</span></div>

<h1>PRE-DRAFT — {title}</h1>
<p>A forecast of Firefox Mobile <b>monthly active users (MAU)</b> from {data_end} through 2027-12-31, for the four mobile apps together and each on its own,
in Glean telemetry terms. <b>Total = organic + paid, built from two sources:</b> the <b>organic</b> part is our model (the production mobile model on real Glean
history from {history_start}, with Firefox Android's history reduced to its organic share before fitting); the <b>paid</b> part is the marketing data-science
team's 2027 plan for paid Firefox Android MAU (<i>{pm['scenario_label']}</i>, UAC + Meta Android, attributed basis), added on top as a level, with their own
2026 run bridging from our data edge to January. The other three apps carry no paid acquisition. The number to read is the <b>raw daily MAU on 2027-12-15</b>.</p>
<div class="callout"><b>Pre-draft.</b> Neither half is backtested yet. Organic: the model's own 90% band is quoted (Prophet predictive, uncalibrated).
Paid: the plan's own 90% band is quoted (their curve uncertainty only; they call their intervals indicative). The total band treats the two as independent
and says so. Data treatments in the organic half: the 2026 Iran internet shutdown is bridged in training; and in India and Indonesia, where organic Firefox Android
outgrew the same country's Firefox iOS during 2026 in a way consistent with campaign installs arriving outside the Play Store, organic history from {cap['from_date']} is
split into an <b>authentic</b> part (held to the country's own iOS growth; this is what the model forecasts) and an <b>inauthentic</b> part (the excess,
{cap['removed_mau_last_day_total']:,} MAU at the edge, in neither our model nor marketing's plan), which is <b>carried flat</b> through 2027 as its own column — a documented
no-information choice, <b>provisional</b> and under review with the marketing DS team. Use the curves for shape and magnitude; do not quote them as plan numbers.</div>
<div class="callout"><b>Data in this package:</b> <code>{csv1.name}</code> — every day from {history_start} to 2027-12-31, history and forecast, total / organic / paid for all apps
and per app, plus the 90% bands; <code>{csv2.name}</code> — total per country; <code>plots/</code> — the charts. Beside this package, <code>{combined_name}</code> puts the
desktop and mobile world curves side by side. Details in section 4.</div>
{'<div class="callout" style="border-color:#c62828;background:#fdecea"><b>Seam step ' + f'{seam_step:+.1%}' + '.</b> The total opens ' + f'{abs(seam_step):.1%}' + ' below the last week of history (7-day means): organic ' + f"{n['seam_step_7d']['organic']:+.1%}" + ', paid ' + f"{n['seam_step_7d']['paid']:+.1%}" + '. The organic model deliberately does not carry all of the 2026 rise into 2027; read the first weeks as the model&#39;s view of the underlying level.</div>' if abs(seam_step) > 0.01 else ''}

<h2>1. The numbers</h2>
<table><tr><th></th><th class=num>Total 2026-12-15</th><th class=num>Total 2027-12-15</th><th class=num>Dec → Dec</th><th class=num>of which authentic organic (2027-12-15)</th><th class=num>inauthentic organic (held flat)</th><th class=num>paid</th></tr>{rows}</table>
<p><b>World on 2027-12-15: {fmt(w27['total'])}</b>, 90% band {fmt(w27['total_low_90'])} to {fmt(w27['total_high_90'])} (authentic organic {fmt(w27['organic'])}, band {fmt(curves.loc['2027-12-15', 'organic_world_low_90'])} to
{fmt(curves.loc['2027-12-15', 'organic_world_high_90'])}; inauthentic organic {fmt(w27['organic_inauthentic'])}, flat; paid {fmt(w27['paid'])}, band {fmt(curves.loc['2027-12-15', 'paid_world_low_90'])} to {fmt(curves.loc['2027-12-15', 'paid_world_high_90'])}).
Paid grows from {fmt(n['last_actual_paid_world'])} at the data edge to {fmt(w27['paid'])}: most of the total's {n['dec_to_dec_total']:+.1%} Dec→Dec is the marketing plan,
the organic part grows {w27['organic'] / w26['organic'] - 1:+.1%}. Under the flat-budget scenario paid would be {fmt(pm['numbers']['flat_scenario_2027-12-15'])} instead.</p>

<h2>2. What it looks like</h2>
<p>Grey: the previous year on the same calendar. Black: history (real actuals). Green: the total forecast. Dotted red: December 15.</p>
{img(shipped("mau_world_2027.png"))}
{img(shipped("mau_world_2026.png"))}
<p>The components and their bands:</p>
{img(pkg / "plots/mobile_mau_components.png")}
{img(shipped("mau_by_os_2027.png"))}

<h2>3. By country</h2>
<p>Total daily MAU, all apps, 15 named markets plus the rest of the world (ROW). Forecast paid is split across countries by each country's share of measured paid MAU
over the last 28 days of history (India 32%, ROW 28%, Indonesia 8%, US 8%, Brazil 6%; Iran none). Full series in <code>{csv2.name}</code>.</p>
<table><tr><th>country</th><th class=num>2026-12-15</th><th class=num>2027-12-15</th><th class=num>Dec → Dec</th></tr>{country_rows}</table>

<h2>4. How to use the files</h2>
<ul>
<li><code>{csv1.name}</code> — one row per day from {history_start} to 2027-12-31: <code>date</code>, <code>data_type</code> (<code>training</code> = history, <code>forecast</code>), then for
<code>world</code>, <code>fenix_android</code>, <code>firefox_ios</code>, <code>focus_android</code>, <code>focus_ios</code> the columns <code>total_</code>, <code>organic_</code> (authentic),
<code>organic_inauthentic_</code>, <code>paid_</code>, with <code>total = organic + organic_inauthentic + paid</code> on every row; and <code>total_world_low_90 / high_90</code>,
<code>organic_world_low_90 / high_90</code>, <code>paid_world_low_90 / high_90</code> on forecast rows. Values are the <b>{STAT}</b>, whole people. On history rows total is the real actual
and paid is the measured paid; on forecast rows paid is the marketing plan and the inauthentic column is flat.</li>
<li><code>{csv2.name}</code> — total per country (all apps) from 2024-01-01, plus <code>world</code>.</li>
<li><code>{combined_name}</code> (next to this package) — <code>date</code>, <code>desktop_data_type</code>, <code>desktop_mau</code>, <code>mobile_data_type</code>, <code>mobile_mau</code> (total),
<code>mobile_organic_mau</code>, <code>mobile_paid_mau</code>. <b>Do not add desktop and mobile</b>: a person active on both devices is counted in both.</li>
<li>Read the 2027-12-15 row for the headline; <code>data_type</code> says where history ends ({data_end}). MAU is already a 28-day quantity; do not smooth it again.</li>
<li>Quote the all-apps total. The split is there so any piece can be swapped: a new marketing scenario replaces the <code>paid_</code> columns one for one; a different reading of the
India/Indonesia excess replaces <code>organic_inauthentic_</code>.</li>
</ul>

<h2>5. What is and is not in it</h2>
<ul>
<li>Mobile only: Firefox Android (Fenix), Firefox iOS, Focus Android, Focus iOS; no other mobile product, matching the regular monthly forecast's universe.</li>
<li>Paid = the marketing team's plan as delivered (scenario <code>{pm['scenario']}</code>, curves {pm['curve_run']}, MAU = paid DAU × {pm['mau_per_dau']}): a hypothetical with no seasonality
and no cross-channel interaction (their words: an upper bound), MAU as a global multiplier rather than a model. The plan starts 2027-01-01; October–December 2026 is the same model's
2026 run.</li>
<li>No other adjustments (launches, headwinds). The regular monthly mobile DAU forecast carries those; this one does not.</li>
<li>Not backtested; the per-app split is affected by how the model reconciles tiles, so the all-apps figure is the one to quote.</li>
</ul>

<h2>6. Where it came from</h2>
<p>mozaic-daily, branch <code>glean-2027-forecast</code>, commit <code>{commit}</code>. Organic: <code>{Path(meta['organic_parquet']).name}</code> (changepoint prior scale
{cfg['prophet_changepoint_prior_scale']}, changepoint range {cfg['prophet_changepoint_range']}, otherwise the October 2026 mobile settings). Paid: <code>{Path(meta['paid_plan']).name}</code>
(STMO query 128941 / <code>ahe_gmio_plan2027_mau_daily_20261014</code>, marketing DS). Owner: Brendan Wells. The full record lives with the repository
(<code>data-official/2027-glean-draft/</code>); this page is deliberately the short version.</p>
</body></html>"""
    (pkg / "index.html").write_text(html)
    write_agent_handoff(pkg, title=title, now=now, commit=commit, build=args.build_dir.name, data_end=data_end, history_start=history_start, meta=meta,
                        summary=summary, curves=curves, csv1=csv1, csv2=csv2, combined_name=combined_name)
    (pkg / "README.txt").write_text(f"""{title} — PRE-DRAFT (generated {now})

Open index.html. TOTAL mobile MAU = authentic organic (our model) + inauthentic organic (India/Indonesia excess, held flat) + paid (the marketing DS team's 2027 plan,
scenario {pm['scenario']}), with 90% bands for organic, paid and the total.
Using an AI agent? Point it at HANDOFF.md first.
  {csv1.name}     daily {history_start}..2027-12-31: date, data_type, total_/organic_/organic_inauthentic_/paid_ for world + per app, world bands ({STAT})
  {csv2.name}  total per country (all apps) from 2024-01-01
  plots/                           the charts (PNG)
Beside this package: {combined_name} — desktop and mobile world MAU side by side (do not sum them).

Headline: TOTAL mobile MAU (all apps) on 2027-12-15 = {fmt(w27['total'])} [90% {fmt(w27['total_low_90'])}, {fmt(w27['total_high_90'])}] = organic {fmt(w27['organic'])} + inauthentic {fmt(w27['organic_inauthentic'])} + paid {fmt(w27['paid'])}.
History ends {data_end}; rows after that are forecast. Seam step (7-day means) {seam_step:+.1%}.
Pre-draft: not backtested; organic band = Prophet predictive (uncalibrated); paid band = the plan's own; India + Indonesia organic cap provisional.
Source: mozaic-daily branch glean-2027-forecast @ {commit}, build {args.build_dir.name}. Owner: Brendan Wells.
""")
    zip_path = shutil.make_archive(str(args.out_dir / pkg.name), "zip", root_dir=args.out_dir, base_dir=pkg.name)
    d = pd.read_csv(args.desktop_forecast_csv, parse_dates=["date"]).set_index("date")[["data_type", "desktop_world"]]
    m = curves[["data_type", "total_world", "organic_world", "paid_world"]]
    combined = d.rename(columns={"data_type": "desktop_data_type", "desktop_world": "desktop_mau"}).join(
        m.rename(columns={"data_type": "mobile_data_type", "total_world": "mobile_mau", "organic_world": "mobile_organic_mau", "paid_world": "mobile_paid_mau"}), how="outer").sort_index()
    combined.index.name = "date"
    combined.to_csv(args.out_dir / combined_name, float_format="%.0f")
    print(f"page  {pkg / 'index.html'}\nzip   {zip_path} ({Path(zip_path).stat().st_size / 1e6:.1f} MB)\ncsv   {args.out_dir / combined_name}")


if __name__ == "__main__":
    main()
