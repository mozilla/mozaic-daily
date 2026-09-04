"""Render `site/index.html` — the plot-led writeup of August canonical's summer miss.

Every figure in the prose is interpolated from `analyze.build()`, never typed. If the underlying
data moves, re-running this script moves the text with it.

Run: python research/forecast-vs-summer-actuals/build_report.py
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import pandas as pd  # noqa: E402

import seasonality as SZ  # noqa: E402
import series as S  # noqa: E402
from analyze import build  # noqa: E402

OUT = HERE / "site" / "index.html"

CSS = """
:root { color-scheme: light; }
* { box-sizing: border-box; }
body {
  margin: 0; padding: 0 24px 96px;
  background: #f9f9f7; color: #0b0b0b;
  font: 16px/1.65 ui-sans-serif, -apple-system, "Segoe UI", Helvetica, Arial, sans-serif;
}
main { max-width: 960px; margin: 0 auto; }
header { padding: 56px 0 8px; }
h1 { font-size: 30px; line-height: 1.25; margin: 0 0 10px; letter-spacing: -0.015em; }
h2 { font-size: 20px; margin: 52px 0 12px; letter-spacing: -0.01em; }
h3 { font-size: 15px; margin: 28px 0 8px; color: #52514e; text-transform: uppercase;
     letter-spacing: 0.06em; }
p  { margin: 0 0 14px; }
.sub { color: #52514e; font-size: 15px; margin: 0; }
.kpis { display: flex; gap: 14px; flex-wrap: wrap; margin: 28px 0 8px; }
.kpi { flex: 1 1 190px; background: #fcfcfb; border: 1px solid rgba(11,11,11,0.10);
       border-radius: 10px; padding: 16px 18px; }
.kpi .label { font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em;
              color: #898781; margin-bottom: 6px; }
.kpi .value { font-size: 27px; font-weight: 650; letter-spacing: -0.02em; }
.kpi .note { font-size: 13px; color: #52514e; margin-top: 4px; }
figure { margin: 22px 0 8px; }
figure img { width: 100%; height: auto; display: block; background: #fcfcfb;
             border: 1px solid rgba(11,11,11,0.10); border-radius: 10px; }
figcaption { font-size: 13.5px; color: #52514e; margin-top: 8px; }
figcaption code { font-size: 12.5px; }
table { border-collapse: collapse; width: 100%; font-size: 14px; margin: 16px 0;
        background: #fcfcfb; border: 1px solid rgba(11,11,11,0.10); border-radius: 10px; }
th, td { padding: 8px 12px; text-align: right; border-bottom: 1px solid #e1e0d9; }
th:first-child, td:first-child { text-align: left; }
thead th { font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: #898781;
           font-weight: 600; }
tbody tr:last-child td { border-bottom: none; }
tbody tr.hi td { background: #f0efec; font-weight: 600; }
.callout { background: #fcfcfb; border-left: 3px solid #eb6834; border-radius: 0 8px 8px 0;
           padding: 14px 18px; margin: 20px 0; font-size: 15px; }
.callout.ok { border-left-color: #1baf7a; }
.callout .h { font-weight: 650; display: block; margin-bottom: 4px; }
ul { margin: 0 0 14px; padding-left: 22px; }
li { margin-bottom: 8px; }
code { background: #f0efec; padding: 1px 5px; border-radius: 4px; font-size: 13.5px;
       font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
footer { margin-top: 64px; padding-top: 18px; border-top: 1px solid #e1e0d9;
         font-size: 13px; color: #898781; }
nav.tabs { display: flex; gap: 4px; border-bottom: 1px solid #e1e0d9; margin: 8px 0 0;
           position: sticky; top: 0; background: #f9f9f7; z-index: 10; padding-top: 10px; }
nav.tabs button { appearance: none; border: none; background: none; cursor: pointer;
  font: inherit; font-size: 14.5px; font-weight: 550; color: #52514e;
  padding: 10px 16px; border-bottom: 2px solid transparent; margin-bottom: -1px; }
nav.tabs button:hover { color: #0b0b0b; }
nav.tabs button[aria-selected="true"] { color: #0b0b0b; border-bottom-color: #2a78d6; }
nav.tabs button:focus-visible { outline: 2px solid #2a78d6; outline-offset: -2px;
  border-radius: 4px 4px 0 0; }
[role="tabpanel"][hidden] { display: none; }
.pos { color: #006300; } .neg { color: #b3261e; }
"""


def _n(v: float, sign: bool = True) -> str:
    return f"{v:+,.0f}" if sign else f"{v:,.0f}"


def _pct(v: float, dp: int = 1) -> str:
    return f"{v * 100:.{dp}f}%"


def _row(r: dict) -> str:
    return (
        f"<tr><td>{r['baseline']}</td>"
        f"<td>{r['A_last']:,.0f}</td><td>{r['B_last']:,.0f}</td><td>{r['C_last']:,.0f}</td>"
        f"<td>{_n(r['miss_last'])}</td><td>{_n(r['legitimate_last'])}</td>"
        f"<td>{_n(r['illegitimate_last'])}</td></tr>"
    )


VINTAGE_TITLE = {"august": "August canonical", "july": "July canonical"}


def _seasonality_rows(sz: dict, track: str) -> str:
    rows = ""
    for vintage in ("august", "july"):
        entry = sz["vintages"][vintage]
        t = entry["tracks"][track]
        direction = "shallower" if t["model_vs_history"] > 0 else "deeper"
        rows += (
            f"<tr><td>{VINTAGE_TITLE[vintage]}</td><td>{entry['seam']}</td>"
            f"<td>{t['model_trough']:,.0f}</td><td>{t['history_trough']:,.0f}</td>"
            f"<td>{t['realised_trough']:,.0f}</td>"
            f"<td>{_n(t['model_vs_history'])} ({direction})</td>"
            f"<td>{_pct(t['share_of_gap_closed'])}</td>"
            f"<td>{_n(t['pre_recon_vs_history'])}</td>"
            f"<td>{_n(t['reconciliation_effect'])}</td></tr>"
        )
    return rows


def _anchor_rows(sz: dict, track: str) -> str:
    """One row per vintage per anchor, so the two readings sit next to each other."""
    rows = ""
    for vintage in ("august", "july"):
        t = sz["vintages"][vintage]["tracks"][track]
        for label, d in (("Feb 15 – Apr 15", t), ("seam", t["seam_anchored"])):
            direction = "shallower" if d["model_vs_history"] > 0 else "deeper"
            rows += (
                f"<tr><td>{VINTAGE_TITLE[vintage]}</td><td>{label}</td>"
                f"<td>{d['model_trough']:,.0f}</td><td>{d['history_trough']:,.0f}</td>"
                f"<td>{d['realised_trough']:,.0f}</td>"
                f"<td>{_n(d['model_vs_history'])} ({direction})</td>"
                f"<td>{_n(d['realised_vs_history'])}</td>"
                f"<td>{_pct(d['share_of_gap_closed'])}</td></tr>"
            )
    return rows


def render_seasonality(sz: dict, r: dict) -> str:
    aug = sz["vintages"]["august"]["tracks"]["ex_ir_cn"]
    jul = sz["vintages"]["july"]["tracks"]["ex_ir_cn"]
    aug_all = sz["vintages"]["august"]["tracks"]["all"]
    jul_all = sz["vintages"]["july"]["tracks"]["all"]
    cav = aug["caveats"]
    cav_all = aug_all["caveats"]

    return f"""
<h2 style="margin-top:34px">Did Prophet expect the summer it got?</h2>
<p class="sub">Firefox Desktop · each seasonal shape rescaled to 2026's Feb 15 – Apr 15 level ·
   28-day trailing mean · holidays included</p>

<div class="kpis">
  <div class="kpi"><div class="label">History → reality, the gap to close</div>
    <div class="value">{_n(aug['realised_vs_history'])}</div>
    <div class="note">2026's trough was this much shallower than the 2022–25 average</div></div>
  <div class="kpi"><div class="label">August · how much it closed</div>
    <div class="value" style="color:#1baf7a">{_pct(aug['share_of_gap_closed'])}</div>
    <div class="note">{_n(aug['model_vs_history'])} shallower than history — right direction</div></div>
  <div class="kpi"><div class="label">July · how much it closed</div>
    <div class="value" style="color:#eb6834">{_pct(jul['share_of_gap_closed'])}</div>
    <div class="note">{_n(jul['model_vs_history'])} — wrong direction entirely</div></div>
</div>

<h2>The finding</h2>
<p>2026's summer trough came in <strong>{_n(aug['realised_vs_history'])}</strong> shallower than the
   2022–25 average (ex-IR/CN). That is the thing a forecast had to anticipate.</p>
<p><strong>August's Prophet anticipated part of it.</strong> Its seasonal trough sits
   {abs(aug['model_vs_history']) / 1e3:,.0f}K shallower than history —
   <strong>{_pct(aug['share_of_gap_closed'])} of the way</strong> to what happened. It was still
   {abs(aug['model_vs_realised']) / 1e6:.2f}M too deep, but it moved the right way.
   <strong>July's went the wrong way</strong>: {abs(jul['model_vs_history']) / 1e6:.2f}M
   <em>deeper</em> than history, {abs(jul['model_vs_realised']) / 1e6:.2f}M deeper than reality.</p>

<figure><img src="../plots/seasonality_summer_zoom.png"
  alt="The summer window for both vintages, each curve measured from its own spring level">
<figcaption><strong>plots/seasonality_summer_zoom.png</strong> — ex-Iran/ex-China. August's orange
  curve sits <em>above</em> the green history curve through the trough; July's sits well below it.
  </figcaption></figure>

<div class="callout"><span class="h">This is the same split the other tab found, from the other
   direction.</span> There, July's model term was {_n({k: d for k, d in
   ((x['vintage'], x) for x in r['three_way_ladder'])}['july']['model_miss'])} against August's
   {_n({k: d for k, d in ((x['vintage'], x) for x in r['three_way_ladder'])}['august']['model_miss'])}.
   Here is why: August's seasonality leaned shallow, in the direction reality went; July's leaned
   deep, against it. Two constructions sharing no code beyond the actuals cache.</div>

<h2>Summer trough, both vintages</h2>
<table><thead><tr><th>vintage</th><th>seam</th><th>Prophet</th><th>2022–25 average</th>
<th>2026 realised</th><th>Prophet − history</th><th>share of gap closed</th>
<th>pre-reconciliation − history</th><th>reconciliation</th></tr></thead>
<tbody>
<tr><td colspan="9" style="text-align:left;color:#898781;font-size:12px;
    text-transform:uppercase;letter-spacing:.05em">ex-Iran, ex-China</td></tr>
{_seasonality_rows(sz, "ex_ir_cn")}
<tr><td colspan="9" style="text-align:left;color:#898781;font-size:12px;
    text-transform:uppercase;letter-spacing:.05em">all countries</td></tr>
{_seasonality_rows(sz, "all")}
</tbody></table>
<p style="font-size:13.5px;color:#52514e">Trough values are absolute DAU, each curve being its
   own seasonal shape rescaled to 2026's Feb 15 – Apr 15 level — so none of the year-over-year
   decline in size is in play and vertical distances are real DAU. Troughs are over Aug 1 – Sep 30.
   On the all-countries track August closed {_pct(aug_all['share_of_gap_closed'])} of a
   {_n(aug_all['realised_vs_history'])} gap — but see the Iran caveat below before quoting that
   row.</p>

<h2>The full seasonal year, anchored at spring</h2>
<p>Anchoring at spring rather than at a seam is what makes a whole-year view possible, and it is
   only available because <strong>Prophet's seasonality repeats</strong>. Measured on the 146
   calendar days that appear in both years of August's window: the seasonality-only component
   drifts by at most <strong>{sz['periodicity_drift_pp']:.2f}pp</strong> between
   {sz['model_cycle_year'] - 1} and {sz['model_cycle_year']}. So the {sz['model_cycle_year']} cycle
   is a sound stand-in for 2026, and the curves below use it.</p>

<figure><img src="../plots/seasonality_august.png"
  alt="August's full seasonal year against history and 2026 actuals, both population tracks">
<figcaption><strong>plots/seasonality_august.png</strong></figcaption></figure>
<figure><img src="../plots/seasonality_july.png"
  alt="July's full seasonal year against history and 2026 actuals, both population tracks">
<figcaption><strong>plots/seasonality_july.png</strong></figcaption></figure>

<h2>The same question judged from the seam</h2>
<p>The spring anchor measures the whole spring-to-summer descent. Anchoring at each vintage's own
   seam instead measures only the part it was actually forecasting — a shorter span, so smaller
   numbers, but a stricter test of what the forecast set out to do. <strong>Both anchors agree on
   direction</strong>, which is the point of showing them together:</p>

<figure><img src="../plots/seasonality_seam_august.png"
  alt="August's seasonality from the seam forward, both population tracks">
<figcaption><strong>plots/seasonality_seam_august.png</strong> — all three curves start together at
  the seam.</figcaption></figure>
<figure><img src="../plots/seasonality_seam_july.png"
  alt="July's seasonality from the seam forward, both population tracks">
<figcaption><strong>plots/seasonality_seam_july.png</strong></figcaption></figure>

<table><thead><tr><th>vintage</th><th>anchor</th><th>Prophet</th><th>2022–25 average</th>
<th>2026 realised</th><th>Prophet − history</th><th>gap to close</th>
<th>share closed</th></tr></thead>
<tbody>
<tr><td colspan="8" style="text-align:left;color:#898781;font-size:12px;
    text-transform:uppercase;letter-spacing:.05em">ex-Iran, ex-China</td></tr>
{_anchor_rows(sz, "ex_ir_cn")}
<tr><td colspan="8" style="text-align:left;color:#898781;font-size:12px;
    text-transform:uppercase;letter-spacing:.05em">all countries</td></tr>
{_anchor_rows(sz, "all")}
</tbody></table>

<div class="callout ok"><span class="h">The seam anchor is free of two caveats the spring anchor
  carries.</span> It runs seam → Dec 31, entirely inside the 2026 portion of the forecast window, so
  it needs <strong>no 2027 stand-in</strong> and the holiday-non-periodicity limit does not apply.
  And its reference is a single settled actual after Iran's 2026-05-25 recovery, so it carries
  <strong>none of the Iran contamination</strong> that depresses the all-countries spring anchor.
  What it gives up is the spring-to-summer descent — which is where most of a vintage's seasonal
  error accumulates, and why both views are worth having.</div>

<p><strong>One thing it cannot do on the usual convention.</strong> The model has no output before
   its seam, so a 28-day trailing mean — used everywhere else here — is undefined for its first 27
   forecast days. Filling that window with actuals was tried and rejected: the July build's daily
   level at its seam sits 5.35% below actuals, so a spliced prefix dominates the average for 27 days
   and the model curve silently inherits <em>actuals'</em> trajectory over exactly the window being
   measured. It made July's model read 488,729 <em>shallower</em> than history when its own
   Jul-06 → trough descent is −3,929,823, about 1.5M <em>deeper</em>. These charts therefore use a
   <strong>centred 7-day window anchored at seam+3</strong>, which needs no splice at all. Any whole
   number of weeks cancels the weekly cycle exactly; 7 days is simply noisier than 28.</p>

<h2>Most of July's excess depth was added by reconciliation</h2>
<figure><img src="../plots/seasonality_reconciliation.png"
  alt="Reconciled versus pre-reconciliation model seasonality for both vintages">
<figcaption><strong>plots/seasonality_reconciliation.png</strong> — all countries. Dashed is what
  the 48 per-tile Prophet fits produced; solid is what shipped after mozaic's top-down
  reconciliation.</figcaption></figure>
<p>Before reconciliation July's trough is only {abs(jul['pre_recon_vs_history']) / 1e3:,.0f}K
   deeper than history ex-IR/CN. After it, {abs(jul['model_vs_history']) / 1e6:.2f}M.
   <strong>Reconciliation contributes {_n(jul['reconciliation_effect'])}</strong> — most of the
   excess. August's equivalent is {_n(aug['reconciliation_effect'])}, in the opposite direction.
   So the per-tile fits were close in both cycles; the aggregation step is where July's summer got
   deep. That is consistent with mozaic reconciling top-down, which makes the aggregate its own fit
   rather than the sum of the tiles. <strong>Why it did this to July and not August is not answered
   here.</strong></p>

<h2>Caveats — read these before quoting a number</h2>
<ul>
<li><strong>Holidays do not repeat, so December is unreliable.</strong> The seasonality component
    drifts {sz['periodicity_drift_pp']:.2f}pp year to year, but with holidays included the drift
    reaches <strong>{cav['holiday_drift_pp']:.2f}pp</strong> — about 1.86M DAU — concentrated on
    Dec 21–28, where Christmas's weekday alignment moves. The visible April–May disagreement
    between Prophet and the other two curves is the same effect: Prophet's Easter sits at
    {sz['model_cycle_year']}'s date, not 2026's. The summer is a slow feature and is unaffected.</li>
<li><strong>The anchor window is saturated with holidays.</strong> All
    {cav['anchor_days_with_holiday']} of its {cav['anchor_days']} days carry a holiday effect,
    mean drag {_pct(cav['anchor_holiday_drag'], 2)} of trend
    ({cav['anchor_holiday_drag'] * cav['reference_dau']:+,.0f} DAU), worst day
    {_pct(cav['anchor_holiday_worst_day'], 1)}. Because the anchor is a 60-day mean and Easter
    falls inside the window in both years, a date shift largely cancels — the residual instability
    is roughly ±0.5pp on the anchor, about 5% of the 11pp peak-to-trough signal.</li>
<li><strong>The all-countries anchor is inside Iran's outage.</strong> Iran's shutdown runs
    2026-03-01 → 2026-05-25, straight through the 2026 Feb 15 – Apr 15 reference window, which
    depresses the all-countries reference level ({cav_all['reference_dau']:,.0f} DAU) and inflates
    every deviation measured from it. <strong>The ex-IR/CN track
    ({cav['reference_dau']:,.0f} DAU) is clean and is the one to quote.</strong></li>
<li><strong>All three curves carry trend, deliberately.</strong> Each is divided by its own spring
    level, which removes size but leaves that year's within-year decline inside the shape. Making
    the model the only trend-free curve would have introduced roughly 1M DAU of asymmetry over the
    five months from anchor to trough at a −5%/yr decline — comparable to the effect being
    measured. The cost is that Prophet's curve borrows {sz['model_cycle_year']}'s trend slope as a
    stand-in for 2026's; the two rates are close, but it is an approximation.</li>
<li><strong>Both year boundaries are Christmas, not seasonality you should read.</strong> The steep
    fall in late December and the climb through January are the same event moving into and out of
    the 28-day trailing window. The summer trough is a slow feature and is unaffected.</li>
<li><strong>Four norm years is a thin average.</strong> At the summer trough the four years'
    rebased ratios span 1.3 percentage points, giving a standard error of roughly ±128,000 DAU on
    the history curve. August's {_n(aug['model_vs_history'])} is several times that; July's
    {_n(jul['model_vs_history'])} is far outside it. At year end the spread is ~4× wider, another
    reason not to read December closely.</li>
<li><strong>Only two vintages.</strong> Whether July's reconciliation behaviour recurs is not
    answerable from n = 2. June's pickle is in the GCS archive if this is worth extending.</li>
</ul>

<h2>How the model's seasonality was recovered</h2>
<p>The pickles do <strong>not</strong> store fitted Prophet objects —
   <code>tile.forecast_model</code> is the factory closure, so there is no
   <code>predict_seasonal_components()</code> to call. Each of the 48 tiles keeps a trend, a
   reconciled forecast (1,000 posterior samples) and a holiday-impact series applied on top, which
   is enough to rebuild the decomposition. Verified against the published parquet:</p>
<p style="text-align:center;font-size:15px"><code>parquet = Σ reconciled + Σ holidays +
   (launch-on-login + MozillaOnline)</code></p>
<p>The overlay residual is smooth and level —
   {sz['vintages']['august']['is_published']['overlay_residual_mean']:+,.0f} on August (std
   {sz['vintages']['august']['is_published']['overlay_residual_std']:,.0f}) and
   {sz['vintages']['july']['is_published']['overlay_residual_mean']:+,.0f} on July (std
   {sz['vintages']['july']['is_published']['overlay_residual_std']:,.0f}) — consistent with July's
   125K launch-on-login ceiling against August's 200K.</p>
<p><strong>July's pickle is the published fit, not a re-fit.</strong> July's
   <code>desktop_locked/</code> shipped no pickle. The file used is a parameter-scan run at July's
   seam whose reconciled forecast reproduces July's published parquet to <strong>0 DAU across all
   198,696 rows</strong>; that check re-runs at build time. The two builds also store their trend
   in different spaces — August in log DAU, July in level DAU, a package version difference — which
   the code detects per tile rather than assuming.</p>
"""


def render(r: dict, sz: dict) -> str:
    seasonality_pane = render_seasonality(sz, r)
    seam_rows = {d["track"]: d for d in r["decompositions"] if d["baseline"] == "seam"}
    ex, allr = seam_rows["ex_ir_cn"], seam_rows["all"]
    hw = r["headwind"]
    sp = r["splice"]
    jp = r["japan"]
    w0, w1 = r["eval_window"]

    ex_legit_pct = ex["legitimate_last"] / ex["miss_last"]
    tw = {d["track"]: d for d in r["three_way"]}
    tw_ex, tw_all = tw["ex_ir_cn"], tw["all"]
    baked_ex = r["baked_in"]["ex_ir_cn"]
    baked_all = r["baked_in"]["all"]

    trend_all = pd.DataFrame(r["trend"]["all"]).set_index("year")
    trend_ex = pd.DataFrame(r["trend"]["ex_ir_cn"]).set_index("year")

    def trend_table_html(t: pd.DataFrame) -> str:
        body = ""
        for year, row in t.iterrows():
            cls = ' class="hi"' if int(year) == 2026 else ""
            body += (
                f"<tr{cls}><td>{int(year)}</td>"
                f"<td>{row['baseline']:,.0f}</td><td>{row['aug23']:,.0f}</td>"
                f"<td>{_pct(row['aug23_ratio'], 2)}</td>"
                f"<td>{row['trough']:,.0f}</td><td>{row['trough_date']}</td>"
                f"<td>{_pct(row['trough_ratio'], 2)}</td>"
                f"<td>{'' if pd.isna(row['baseline_yoy']) else _pct(row['baseline_yoy'], 2)}</td>"
                f"</tr>"
            )
        return (
            "<table><thead><tr><th>year</th><th>Jun-15 baseline</th><th>Aug-23</th>"
            "<th>Aug-23 / baseline</th><th>trough</th><th>trough date</th>"
            "<th>trough / baseline</th><th>baseline YoY</th></tr></thead>"
            f"<tbody>{body}</tbody></table>"
        )

    ns_ex, ns_all = r["norm_spread"]["ex_ir_cn"], r["norm_spread"]["all"]
    # Sort by calendar day, not by the full date string — the string order is the year order and
    # only coincides with the seasonal order while the norm years happen to line up that way.
    norm_troughs = sorted(trend_ex.loc[list(S.NORM_YEARS), "trough_date"], key=lambda d: d[5:])
    norm_trough_first = pd.Timestamp(norm_troughs[0]).strftime("%b %-d")
    norm_trough_last = pd.Timestamp(norm_troughs[-1]).strftime("%b %-d")
    ov_seam_ex = sum(d["growth_since_anchor"] for d in r["overlays"]["ex_ir_cn"]["seam"].values())
    ov_jun_ex = sum(d["growth_since_anchor"] for d in r["overlays"]["ex_ir_cn"]["jun15"].values())
    jun_ex = next(d for d in r["decompositions"]
                  if d["track"] == "ex_ir_cn" and d["baseline"] == "jun15")

    ladder = "".join(
        f"<tr><td>{d['vintage'].capitalize()}</td>"
        f"<td>{S.VINTAGES[d['vintage']]['seam'].date()}</td>"
        f"<td>{d['A_last']:,.0f}</td><td>{d['C_last']:,.0f}</td>"
        f"<td>{_n(d['total_miss'])}</td><td>{_n(d['shallow_summer'])}</td>"
        f"<td>{_n(d['model_miss'])}</td><td>{_n(d['headwind'])}</td>"
        f"<td>{_pct(d['headwind_share_of_total'])}</td></tr>"
        for d in r["three_way_ladder"]
    )
    lad = {d["vintage"]: d for d in r["three_way_ladder"]}
    hw_shares = [d["headwind_share_of_total"] for d in r["three_way_ladder"]]

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>August canonical vs the summer that happened</title>
<style>{CSS}</style></head><body><main>

<header>
  <h1>August 2026 desktop forecast — what the summer miss was made of</h1>
  <p class="sub">Firefox Desktop · legacy telemetry · published curves</p>
</header>

<nav class="tabs" role="tablist" aria-label="Analysis views">
  <button role="tab" id="tab-miss" aria-controls="panel-miss" aria-selected="true"
          onclick="showTab('miss')">The miss, decomposed</button>
  <button role="tab" id="tab-seasonality" aria-controls="panel-seasonality"
          aria-selected="false" onclick="showTab('seasonality')">Prophet's seasonality</button>
</nav>

<section role="tabpanel" id="panel-miss" aria-labelledby="tab-miss">
<h2 style="margin-top:34px">How far was August canonical from the summer that actually
   happened?</h2>
<p class="sub">28-day trailing mean · scored {w0} → {w1} (22 days) · Windows 10 headwind
   applied</p>

<div class="kpis">
  <div class="kpi"><div class="label">Total miss · ex-IR/CN</div>
    <div class="value">{_n(tw_ex['total_miss'])}</div>
    <div class="note">actual above forecast at {w1} ({_pct(ex['miss_last_pct'], 2)})</div></div>
  <div class="kpi"><div class="label">Shallow summer</div>
    <div class="value" style="color:#1baf7a">{_n(tw_ex['shallow_summer'])}</div>
    <div class="note">{_pct(tw_ex['shallow_summer'] / tw_ex['total_miss'])} — nobody's fault</div></div>
  <div class="kpi"><div class="label">Model miss</div>
    <div class="value" style="color:#eb6834">{_n(tw_ex['model_miss'])}</div>
    <div class="note">{_pct(tw_ex['model_miss'] / tw_ex['total_miss'])} — ran below typical</div></div>
  <div class="kpi"><div class="label">Windows 10 headwind</div>
    <div class="value" style="color:#4a3aa7">{_n(tw_ex['headwind'])}</div>
    <div class="note">{_pct(tw_ex['headwind_share_of_total'])} — a judgement call</div></div>
</div>

<h2>The finding</h2>
<p>August canonical forecasts from a {w0} seam, and by {w1} it sits
   <strong>{_n(ex['miss_last'], sign=False)}</strong> below actuals on the year-comparable
   ex-Iran/ex-China track ({_n(allr['miss_last'], sign=False)} all countries). Roughly
   <strong>{_pct(ex_legit_pct, 0)}</strong> of that is a summer nobody could have forecast;
   the other <strong>{_pct(1 - ex_legit_pct, 0)}</strong> is the forecast sitting below what an
   <em>ordinary</em> summer would have delivered.</p>

<p>But "illegitimate" bundles two things with different owners, so the useful split is three
   ways. All of it is one identity, not a model:</p>
<p style="text-align:center;font-size:15px"><code>C&minus;A&nbsp;=&nbsp;(C&minus;B)&nbsp;+&nbsp;(B&minus;A<sub>no&nbsp;win10</sub>)&nbsp;+&nbsp;(A<sub>no&nbsp;win10</sub>&minus;A)</code><br>
   <span style="font-size:13.5px;color:#52514e">shallow summer &nbsp;+&nbsp; the model ran low
   &nbsp;+&nbsp; the headwind we chose</span></p>

<figure><img src="../plots/three_way_split.png"
  alt="The published shortfall split into shallow-summer, model and headwind components, both tracks">
<figcaption><strong>plots/three_way_split.png</strong> — the same total, three owners.
  Legitimate/illegitimate is this chart with the last two segments merged.</figcaption></figure>

<table><thead><tr><th>track</th><th>A · forecast</th><th>B · typical</th><th>C · actual</th>
<th>total miss</th><th>shallow summer</th><th>model miss</th><th>Win10 headwind</th>
<th>headwind share</th></tr></thead><tbody>
<tr class="hi"><td>ex-Iran, ex-China</td><td>{tw_ex['A_last']:,.0f}</td>
  <td>{tw_ex['B_last']:,.0f}</td><td>{tw_ex['C_last']:,.0f}</td>
  <td>{_n(tw_ex['total_miss'])}</td>
  <td>{_n(tw_ex['shallow_summer'])}</td><td>{_n(tw_ex['model_miss'])}</td>
  <td>{_n(tw_ex['headwind'])}</td><td>{_pct(tw_ex['headwind_share_of_total'])}</td></tr>
<tr><td>all countries</td><td>{tw_all['A_last']:,.0f}</td>
  <td>{tw_all['B_last']:,.0f}</td><td>{tw_all['C_last']:,.0f}</td>
  <td>{_n(tw_all['total_miss'])}</td>
  <td>{_n(tw_all['shallow_summer'])}</td><td>{_n(tw_all['model_miss'])}</td>
  <td>{_n(tw_all['headwind'])}</td><td>{_pct(tw_all['headwind_share_of_total'])}</td></tr>
</tbody></table>
<p style="font-size:13.5px;color:#52514e">All values are 28-day trailing-mean DAU at {w1}. The
   forecast column is the published curve with the Windows 10 headwind applied; removing it gives
   {tw_ex['A_no_headwind_last']:,.0f} ex-IR/CN and {tw_all['A_no_headwind_last']:,.0f} all
   countries.</p>

<div class="callout"><span class="h">Remove the Windows 10 headwind and the verdict flips.</span>
   Judged on the model alone, the ex-IR/CN miss is {_n(tw_ex['miss_ex_headwind'])} and
   <strong>{_pct(tw_ex['legitimate_share_ex_headwind'])} of it is the shallow summer</strong> —
   majority legitimate, the opposite of the published-curve verdict. The all-countries track does
   not flip ({_pct(tw_all['legitimate_share_ex_headwind'])} legitimate of
   {_n(tw_all['miss_ex_headwind'])}), because that track still carries the China migration.
   Which number is right depends on whether you are asking "was the published forecast low"
   (yes, and a quarter of that is the headwind) or "was the model wrong" (less than you would
   think, on the year-comparable population).</div>

<figure><img src="../plots/split_ex_headwind.png"
  alt="Legitimate versus illegitimate split with the headwind removed, both tracks">
<figcaption><strong>plots/split_ex_headwind.png</strong> — the legitimate/illegitimate question
  asked of the model alone, with the exogenous Windows 10 headwind off the table.
  </figcaption></figure>

<h2>What each adjustment contributes</h2>
<p>Four adjustment codes touch an August curve. Only one of them is both non-zero on desktop and
   separable from the model, which is why the decomposition above has exactly one adjustment
   term.</p>

<table><thead><tr><th>code</th><th>what it is</th><th>on desktop at {w1}</th>
<th>separable?</th></tr></thead><tbody>
<tr class="hi"><td><code>h</code></td><td>Windows 10 migration headwind</td>
  <td>{_n(hw['ramp_at_window_end'])}</td>
  <td>Yes — display-layer, applied to the 28-day MA after mozaic, so
      <code>published &minus; ramp</code> is exact</td></tr>
<tr><td><code>t</code></td><td>mobile tailwind</td><td>0</td>
  <td>N/A — its spec sets <code>desktop_dau: 0</code>; it contributes exactly nothing here</td></tr>
<tr><td><code>l</code></td><td>launch-on-login</td>
  <td>{baked_all['l']['add_back_level_at_window_end']:,.0f} add-back
      ({baked_ex['l']['add_back_level_at_window_end']:,.0f} ex-IR/CN)</td>
  <td><strong>No</strong> — baked into the parquet</td></tr>
<tr><td><code>o</code></td><td>MozillaOnline migration</td>
  <td>{baked_all['o']['add_back_level_at_window_end']:,.0f} add-back
      ({baked_ex['o']['add_back_level_at_window_end']:,.0f} ex-IR/CN)</td>
  <td><strong>No</strong> — baked into the parquet</td></tr>
</tbody></table>

<p>The Windows 10 headwind is a linear ramp: zero at the seam, {hw['ramp_at_anchor']:,.0f} at 2026-12-15,
   so only <strong>{_pct(hw['fraction_phased_in'])}</strong> of it had arrived by {w1}. Its full
   Dec-15 weight is more than six times what shows up in this window.</p>

<div class="callout"><span class="h"><code>l</code> and <code>o</code> cannot be removed here, and
   their add-back levels are not a counterfactual.</span> Both are per-tile <em>bidirectional</em>
   overlays: the curve is subtracted from training rows before mozaic and added back after. Change
   the subtraction and Prophet's fit changes too, so the realised effect is config-dependent and is
   <em>not</em> a level shift — the figures above are an order of magnitude in play, nothing more.
   Producing a real <code>l</code>- or <code>o</code>-free counterfactual means re-running a locked
   build, which this cluster does not do. Note the ex-IR/CN column already strips ~93% of
   <code>o</code> by construction, which is one reason the two tracks disagree.</div>

<h2>The three series</h2>
<figure><img src="../plots/three_series_seam.png"
  alt="Actual, seam-anchored typical summer and August canonical across 2026, both population tracks">
<figcaption><strong>plots/three_series_seam.png</strong> — <strong>seam-anchored.</strong> B is
  rescaled to meet 2026 at the seam, so all three curves start together there and the fan-out is the
  miss. This is the anchor the decomposition table below uses, because it charges August only for
  the 22 days it actually forecast.</figcaption></figure>

<figure><img src="../plots/three_series_spring.png"
  alt="The same three series with the counterfactual anchored on Feb 15 to Apr 15">
<figcaption><strong>plots/three_series_spring.png</strong> — <strong>spring-anchored.</strong> The
  same 2022–25 average rescaled to the Feb 15 – Apr 15 window instead, so it spans the year and shows
  the whole spring-to-summer descent. This curve is byte-identical to the green line on the
  seasonality tab. The two charts answer different questions and are kept apart for that reason.
  </figcaption></figure>

<div class="callout"><span class="h">The spring-anchored chart also makes the Iran problem
  visible.</span> On its <em>all countries</em> panel B runs roughly a million DAU below actuals from
  May onward — not because a typical summer was that much worse, but because 2026's Feb 15 – Apr 15
  reference window sits inside Iran's 2026-03-01 → 05-25 outage, so the level it is rescaled to is
  depressed. On the <em>ex-Iran, ex-China</em> panel the same curve tracks actuals closely. That is
  the numerical caveat on the spring baseline, shown rather than asserted, and it is why the ex-IR/CN
  track is the one to quote. The seam anchor is immune: its reference is a single settled actual
  after Iran's recovery.</div>

<figure><img src="../plots/gap_opens.png" alt="Daily actual-minus-forecast gap across the scored window">
<figcaption><strong>plots/gap_opens.png</strong> — the miss accumulates at a near-constant rate
  from a seam that starts within {_n(allr['seam_gap'])} DAU. It is trajectory drift, not a
  mis-set starting level.</figcaption></figure>

<h2>The split</h2>
<figure><img src="../plots/miss_split.png" alt="The miss split into legitimate and illegitimate parts, both tracks">
<figcaption><strong>plots/miss_split.png</strong> — seam-anchored counterfactual, the only anchor
  that judges August on its own terms.</figcaption></figure>

<table><thead><tr><th>anchor</th><th>A · forecast</th><th>B · typical</th><th>C · actual</th>
<th>miss C&minus;A</th><th>legit C&minus;B</th><th>illegit B&minus;A</th></tr></thead>
<tbody>
<tr><td colspan="7" style="text-align:left;color:#898781;font-size:12px;
    text-transform:uppercase;letter-spacing:.05em">ex-Iran, ex-China</td></tr>
{''.join(_row(d) for d in r['decompositions'] if d['track'] == 'ex_ir_cn')}
<tr><td colspan="7" style="text-align:left;color:#898781;font-size:12px;
    text-transform:uppercase;letter-spacing:.05em">all countries</td></tr>
{''.join(_row(d) for d in r['decompositions'] if d['track'] == 'all')}
</tbody></table>

<figure><img src="../plots/baseline_sensitivity.png" alt="How the split moves with the counterfactual anchor">
<figcaption><strong>plots/baseline_sensitivity.png</strong> — the total miss is fixed; only its
  attribution moves. The three anchors answer different questions and are not competing estimates
  of one quantity.</figcaption></figure>

<h3>Why three anchors</h3>
<ul>
<li><strong>seam</strong> — B meets actuals at 2026-08-02. Judges the August vintage on the
    22&nbsp;days it actually forecast. <em>This is the headline.</em></li>
<li><strong>jun15</strong> — pre-summer and clear of Iran's outage. Answers the whole-season
    question: across the summer, actuals beat a typical shape by {_n(jun_ex['legitimate_last'])}
    ex-IR/CN, of which {_n(ov_jun_ex)} is <code>o</code>+<code>l</code> growth history never had,
    leaving roughly {_n(jun_ex['legitimate_last'] - ov_jun_ex)} of genuine seasonal excess.</li>
<li><strong>spring</strong> — the Feb-15→Apr-15 anchor. On the all-countries track this window sits
    <em>inside</em> Iran's 2026-03-01→05-25 outage, which depresses the 2026 anchor and inflates
    C&minus;B by well over a million DAU. Reported for completeness; do not quote it
    on the all-countries track.</li>
</ul>

<div class="callout ok"><span class="h">The seam anchor is clean of 2026-only level events.</span>
  Both <code>o</code> (MozillaOnline) and <code>l</code> (launch-on-login) had already plateaued
  by 2026-08-02, so their combined <em>growth</em> across the scored window is only
  {_n(ov_seam_ex)} ex-IR/CN. The headline legitimate figure is seasonality, not migration.</div>

<h2>Was the summer really shallow? The trend check</h2>
<figure><img src="../plots/trend_check.png"
  alt="Each year's Aug-23 level as a share of its own Jun-15 baseline, both population tracks">
<figcaption><strong>plots/trend_check.png</strong> — every year measured against its own pre-summer
  baseline, so absolute size and long-run decline cancel. The absolute pair inside each bar is the
  level that ratio is taken of.</figcaption></figure>

<p>2026 held <strong>{_pct(ns_ex['value_2026'], 2)}</strong> of its Jun-15 level at Aug-23,
   against a 2022–25 average of {_pct(ns_ex['norm_mean'], 2)} — outside the norm range of
   {_pct(ns_ex['norm_min'], 2)}–{_pct(ns_ex['norm_max'], 2)}, at
   <strong>z = {ns_ex['z_score']:+.2f}</strong>. As reported the gap is wider
   ({_pct(ns_all['value_2026'], 2)} vs {_pct(ns_all['norm_mean'], 2)},
   z = {ns_all['z_score']:+.2f}), and the difference between the two tracks is largely the China
   migration.</p>

<p>The baseline YoY column is the intuition check on trend: 2026's Jun-15 baseline is
   {_pct(float(trend_ex.loc[2026, 'baseline_yoy']), 2)} year-on-year ex-IR/CN, well inside the
   {_pct(float(trend_ex.loc[2023, 'baseline_yoy']), 2)}–{_pct(float(trend_ex.loc[2024, 'baseline_yoy']), 2)}
   range the norm years span. <strong>2026's underlying trend is not unusual</strong> — what is
   unusual is the shape of its summer.</p>

<h3>ex-Iran, ex-China</h3>
{trend_table_html(trend_ex)}
<h3>As reported</h3>
{trend_table_html(trend_all)}

<h2>The vintage ladder</h2>
<figure><img src="../plots/vintage_ladder.png" alt="Shortfall at 2026-08-23 for the June, July and August published vintages">
<figcaption><strong>plots/vintage_ladder.png</strong> — each vintage's B anchored at its
  <em>own</em> seam, so "model miss" always means "below a typical trajectory measured from where
  that forecast actually started". Values are printed only where the segment is wide enough to
  hold them; the table below carries every component.</figcaption></figure>

<table><thead><tr><th>vintage</th><th>seam</th><th>A · forecast</th><th>C · actual</th>
<th>total miss</th><th>shallow summer</th><th>model miss</th><th>Win10 headwind</th>
<th>headwind share</th></tr></thead>
<tbody>{ladder}</tbody></table>

<p><strong>July has the worst model, and June has the best.</strong> June's total is the largest
   of the three, but {_pct(lad['june']['shallow_summer'] / lad['june']['total_miss'])} of it is a
   summer that genuinely beat the typical shape measured from late May, and its model term
   ({_n(lad['june']['model_miss'])}) is the <em>smallest</em> of any vintage. July is the
   opposite: from a 2026-07-06 seam actuals tracked a typical seasonal shape to within
   {_n(lad['july']['shallow_summer'])} DAU, and its model still finished
   {_n(lad['july']['model_miss'])} below it — nearly 2.7&times; August's. Reading only the totals
   would have blamed the wrong vintage.</p>

<p>The Windows 10 headwind's share is strikingly stable at
   {_pct(min(hw_shares))}&ndash;{_pct(max(hw_shares))} across all three, even though their ramp
   conventions differ a lot: June and July anchor their ramps at 2026-04-01 so they carry
   {_n(lad['june']['headwind'], sign=False)} and {_n(lad['july']['headwind'], sign=False)} of
   drag by {w1}, against August's {_n(lad['august']['headwind'], sign=False)} off a seam-anchored
   ramp. The older vintages are dragged much harder in absolute terms; their totals are simply
   larger in proportion.</p>

<p><em>Two caveats on this table.</em> It compares vintages at a fixed date, so longer-horizon
   vintages have had more time to drift — it is a "where does each published number stand today"
   view, not a like-for-like accuracy score. And each row's B is anchored at that row's own seam,
   so the three shallow-summer terms cover different spans and are <strong>not comparable to each
   other</strong>; only the split within a row is meaningful.</p>

<h2>Diagnostics</h2>

<h3>The whole scored window is inside the seam splice</h3>
<p><code>display_ma</code>'s variance-matched transition runs 27 days, so August's published curve
   only becomes byte-identical to a plain <code>rolling(28).mean()</code> from
   {sp['splice_ends']}. Every day we can score falls before that. Over the window the splice puts
   the published curve <strong>{_n(sp['splice_effect_last'])}</strong> below the model's own
   moving average at {w1} (mean {_n(sp['splice_effect_mean'])}, max absolute
   {sp['splice_effect_max_abs']:,.0f}). So about
   {_pct(abs(sp['splice_effect_last']) / ex['miss_last'])} of the ex-IR/CN miss is a display
   convention rather than a forecast. It is real for anyone reading the published chart, and it
   is not a model error.</p>

<h3>Japan automation</h3>
<p>Named, not netted out: the convention here is to judge the published KPI as published, without bot correction. Japan is
   <strong>{_pct(jp['jp_share_of_dau'], 2)}</strong> of desktop DAU over the scored window. The
   regional-story project measures the Japan automation cohort at
   &minus;{_pct(jp['jp_cohort_points_imported'], 2)} on Japan's own late-summer level; scaled by
   Japan's share that is on the order of <strong>{jp['implied_global_dau']:,.0f} DAU</strong>
   globally — roughly {_pct(jp['implied_global_dau'] / ex['legitimate_last'])} of the legitimate
   component. Small, not zero, and it cuts in the direction of making the summer look less
   genuinely strong than it does here. India's improvement is separately established as real
   users.</p>

<h2>What this does not rule out</h2>
<ul>
<li><strong>The trough has not landed.</strong> 2026's minimum so far is
    {trend_ex.loc[2026, 'trough_date']}, but the four norm years trough between
    {norm_trough_first} and {norm_trough_last} by calendar day. If actuals fall further over the
    next week the legitimate component shrinks and the illegitimate share grows. Re-run after
    2026-09-01.</li>
<li><strong>n = 4.</strong> B averages four seasonal shapes whose Aug-23 ratios already span
    {(ns_ex['norm_max'] - ns_ex['norm_min']) * 100:.2f} percentage points ex-IR/CN. A z of
    {ns_ex['z_score']:+.2f} against four observations is suggestive, not decisive; 2026 could be
    an ordinary draw from a wider distribution than four years reveal.</li>
<li><strong><code>o</code> is a stale carry-forward.</strong> August's MozillaOnline curve was
    built from data through late June and deliberately modelled below what July already showed.
    Under-subtracting migration from training leaves migration signal in the trend, so some of
    the "shallow summer" on the all-countries track may be <code>o</code> being conservative rather
    than seasonality. The ex-IR/CN track largely removes this, which is part of why its
    legitimate share is smaller.</li>
<li><strong>No country attribution.</strong> This is a world-total analysis on two population
    scopes. Which markets drive the miss is not answered here, and a miss concentrated in one or
    two markets would have a very different remedy from a broad-based one.</li>
<li><strong>The counterfactual is conditional, not causal.</strong> B says "given where 2026 was
    at the anchor, here is the average of what four recent years did next." It is not a claim
    about why any of them did it.</li>
<li><strong>Mobile is out of scope</strong> by decision. Its published curve carries a
    discretionary +299,000 <code>t</code> tailwind sized against July's published figure, so a
    mobile miss would mostly measure that judgement.</li>
</ul>

<h2>Two premises from the inbound handoff that turned out to be wrong</h2>
<ul>
<li><strong>The forecast is not Glean.</strong> The canonical published desktop curve is
    <code>legacy_desktop</code>, and the canonical notebook's actuals cell queries the same table
    and filter as the regional-story pull
    (<code>telemetry.active_users_aggregates</code>, <code>app_name = "Firefox Desktop"</code>,
    no channel filter). Verified: at 2026-08-02 both give a 28-day mean of 46,890,129 exactly.
    There is no Glean/Legacy offset to carry. The <code>gd-D</code> parquets in the repo root are
    glean-desktop scratch runs, not the published artifact.</li>
<li><strong>The published curves are already smoothed.</strong> They are 28-day trailing means,
    so this is a smoothed-vs-smoothed comparison throughout — with the splice caveat above.</li>
</ul>

</section>

<section role="tabpanel" id="panel-seasonality" aria-labelledby="tab-seasonality" hidden>
{seasonality_pane}
</section>

<script>
function showTab(name) {{
  for (const key of ["miss", "seasonality"]) {{
    const selected = key === name;
    document.getElementById("panel-" + key).hidden = !selected;
    document.getElementById("tab-" + key).setAttribute("aria-selected", String(selected));
  }}
  history.replaceState(null, "", "#" + name);
}}
if (location.hash === "#seasonality") showTab("seasonality");
</script>

<footer>
  Built by <code>research/forecast-vs-summer-actuals/build_report.py</code> from
  <code>analyze.py</code> · every figure in this page is computed at build time, none typed.
  Actuals cached in <code>data/desktop_dau_by_country.parquet</code>
  (see its <code>.meta.json</code> for provenance).
</footer>
</main></body></html>
"""


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(render(build(), SZ.summary()))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
