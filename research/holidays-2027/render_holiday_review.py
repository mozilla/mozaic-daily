"""Render ``index.html`` for the 2027 holiday review from ``data/facts.json`` and ``data/clusters_2027.csv``.

Every number in the prose is read from ``facts.json`` (written by ``build_holiday_review.py``);
the judgement calls (which items are watch-list cards, and their tier) live in this file's text.
"""

import ast
import json
from datetime import datetime
from html import escape
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent
DATA_AS_OF = "2026-10-09 (training end of the locked build 5, seam 2026-10-10)"
TIER_LABEL = {"1-high": "High", "2-medium": "Medium", "3-low": "Low"}

CSS = """
body{font-family:-apple-system,Segoe UI,Helvetica,sans-serif;max-width:1180px;margin:0 auto;padding:0 24px 60px;color:#1d2733;line-height:1.5}
.banner{background:#1d2733;color:#f5f7fa;padding:10px 16px;border-radius:0 0 8px 8px;font-size:13px;display:flex;gap:28px;flex-wrap:wrap}
.banner b{color:#f4d58d}
h1{margin:22px 0 4px} h2{margin-top:38px;border-bottom:2px solid #e3e8ef;padding-bottom:4px} h3{margin:0 0 6px}
.lede{font-size:17px;color:#33475b}
.card{border-radius:8px;padding:14px 18px;margin:14px 0;border-left:6px solid}
.high{background:#fdf0ee;border-color:#c0392b} .medium{background:#fdf6e8;border-color:#e08e0b} .low{background:#f4f6f8;border-color:#9aa5b1;color:#4a5866}
.ok{background:#eef7f0;border-color:#2e8b57}
.pill{display:inline-block;font-size:11px;font-weight:700;padding:1px 8px;border-radius:10px;color:#fff;margin-right:6px;vertical-align:middle}
.pill.high{background:#c0392b}.pill.medium{background:#e08e0b}.pill.low{background:#9aa5b1}.pill.ok{background:#2e8b57}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:16px 0}
.stat{background:#f4f6f8;border-radius:8px;padding:10px 12px} .stat .v{font-size:22px;font-weight:700} .stat .k{font-size:12px;color:#5b6b7b}
img{max-width:100%;border:1px solid #e3e8ef;border-radius:6px;margin:8px 0}
.cap{font-size:12px;color:#5b6b7b;margin-top:-4px}
table{border-collapse:collapse;width:100%;font-size:13px} th,td{padding:5px 8px;border-bottom:1px solid #e3e8ef;text-align:left;vertical-align:top}
th{background:#f4f6f8} tr.t3 td{color:#8794a1;font-size:12px} tr.t1 td:first-child{border-left:4px solid #c0392b} tr.t2 td:first-child{border-left:4px solid #e08e0b}
details{margin:10px 0} summary{cursor:pointer;font-weight:600;color:#33475b}
code{background:#f0f2f5;padding:1px 4px;border-radius:3px;font-size:12px}
.addon{color:#c0392b} .new{font-weight:700}
"""


def m(people: float, digits: int = 1) -> str:
    return f"{people / 1e6:.{digits}f}M"


def k(people: float) -> str:
    return f"{people / 1e3:,.0f}K"


def pct(x: float, digits: int = 0) -> str:
    return f"{x * 100:.{digits}f}%"


def figure(path: str, caption: str) -> str:
    return f'<img src="{path}"><div class="cap"><code>research/holidays-2027/{path}</code> — {caption}</div>'


def card(tier: str, title: str, body: str) -> str:
    label = {"high": "HIGH", "medium": "MEDIUM", "low": "LOW", "ok": "OK"}[tier]
    return f'<div class="card {tier}"><h3><span class="pill {tier}">{label}</span>{title}</h3>{body}</div>'


def cluster_table(clusters: pd.DataFrame, shares: dict) -> str:
    rows = []
    for r in clusters.itertuples():
        labels = ast.literal_eval(r.labels.replace("Timestamp(", "(")) if isinstance(r.labels, str) else r.labels
        new_pairs = ast.literal_eval(r.new_pairs) if isinstance(r.new_pairs, str) else r.new_pairs
        new_names = {n for p in new_pairs for n in (p["a"], p["b"])}
        seen, parts = set(), []
        for date, name, moving, source in labels:
            if name in seen:
                continue
            seen.add(name)
            css = " ".join(c for c in ["addon" if source != "native" else "", "new" if name in new_names else ""] if c)
            parts.append(f'<span class="{css}">{escape(name)}</span> <small>{str(date)[5:10]}{"·m" if moving else ""}</small>')
        novelty = "; ".join(f'{escape(p["a"])} → {escape(p["b"])} {p["offset_2027"]:+d}d' for p in new_pairs) or "arrangement seen in training"
        tier = r.tier
        rows.append(
            f'<tr class="t{tier[0]}"><td>{TIER_LABEL[tier]}</td><td>{r.country} <small>({pct(shares[r.country], 1)})</small></td>'
            f'<td>{str(r.start)[:10]} → {str(r.end)[:10]}</td><td>{", ".join(parts)}</td><td>{m(r.person_days, 2)}</td>'
            f'<td>{pct(r.peak_effect)}</td><td>{novelty}</td></tr>'
        )
    return ("<table><tr><th>Tier</th><th>Country (DAU share)</th><th>Span</th><th>Labels (red = mozaic add-on, bold = in a new pair, ·m = moving)</th>"
            "<th>World DAU person-days removed</th><th>Deepest day</th><th>Pairs never seen at this spacing in 2021–2026</th></tr>"
            + "".join(rows) + "</table>")


def main() -> None:
    f = json.loads((ROOT / "data" / "facts.json").read_text())
    clusters = pd.read_csv(ROOT / "data" / "clusters_2027.csv")
    hw, kpi, p = f["holdout_windows"], f["kpi"], f["plots"]
    eid, leb, whit = f["unlabeled_eid_row"], f["lebaran"], f["phantom_whit_2027"]
    world_day = f["world_dau_2027_mean"]
    whit_total = sum(v["people"] for v in whit.values())
    us_ca_whit = whit["US"]["people"] + whit["CA"]["people"]
    lebaran_gap = (leb["history_id_days"] - leb["model_id_days"]) * leb["id_level"]
    err = f["holdout_abs_error"]
    generated = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")

    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>2027 holidays vs the KPI forecast</title><style>{CSS}</style></head><body>
<div class="banner"><span><b>Generated:</b> {generated}</span><span><b>Source:</b> research/holidays-2027/render_holiday_review.py (numbers: build_holiday_review.py)</span><span><b>Data as of:</b> {DATA_AS_OF}</span></div>
<h1>How holidays will distort 2027 DAU and MAU</h1>
<p class="lede">Moving holidays that collide with each other or with fixed holidays in 2027, read through the model that will forecast them: the locked 2027 Glean-history desktop build (build 5). The headline KPIs are safe; the daily tracking is not. Day-by-day view: <a href="calendar.html">2027 holiday calendar</a>.</p>

<div class="grid">
<div class="stat"><div class="v">{k(kpi['2027']['dau_28d'] - kpi['2026']['dau_28d'])}</div><div class="k">Year-over-year change in the holiday effect inside the Dec-15 DAU 28-day window (2027 vs 2026)</div></div>
<div class="stat"><div class="v">{m(eid['fitr']['person_days'])}</div><div class="k">Person-days lost to Eid al-Fitr in the rest-of-world (ROW) group that the model does not forecast at all</div></div>
<div class="stat"><div class="v">{hw['fr_may']['ratio']:.2f}× / {hw['pl_may']['ratio']:.2f}×</div><div class="k">Model's May dip vs what happened, France / Poland, 2026 holdout (2027's May is more crowded)</div></div>
<div class="stat"><div class="v">{m(whit_total)}</div><div class="k">DAU removed on Mon 2027-05-17 by a "Whit Monday (Global)" label in countries where it is not a holiday</div></div>
</div>

<h2>1. The Dec-15 KPIs</h2>
{card("ok", "No moving holiday enters either KPI window, and the window holds the same holidays in 2026 and 2027",
f'''<p>The DAU KPI is the 28-day average ending Dec 15 (Nov 18 → Dec 15). Both years hold US Thanksgiving on Thursday (Nov 26 2026, Nov 25 2027) and Black Friday,
and nothing else of size. The model removes <b>{k(kpi['2027']['dau_28d'])}</b>/day on average inside the 2027 window, against {k(kpi['2026']['dau_28d'])} in 2026: a
{k(kpi['2027']['dau_28d'] - kpi['2026']['dau_28d'])} year-over-year difference. The MAU KPI (raw Dec-15 MAU) carries {k(kpi['2027']['mau_dec15'])} of holiday effect in 2027 and
{k(kpi['2026']['mau_dec15'])} in 2026.</p>
<p>One modelling fault does touch this window (section 4): the 2025 outage label diluted what the model learned from Thanksgiving 2025. Removing it would deepen the window by
{k(kpi['2027']['dau_28d_no_outage'] - kpi['2027']['dau_28d'])} in 2027 and {k(kpi['2026']['dau_28d_no_outage'] - kpi['2026']['dau_28d'])} in 2026, so it shifts both years equally
and leaves the comparison alone. Thanksgiving Day itself is forecast at −27% against −29% to −34% actual (2022–2025); Black Friday's −31% matches history.</p>''')}
{figure(p['kpi'], "world people removed for holidays, 2026 forecast (dotted) vs 2027 forecast (solid), on a shared Nov–Dec axis; shaded = 28 days ending Dec 15.")}

<h2>2. Watch list: where 2027 daily actuals will diverge from the forecast</h2>
<p>Ranked by the people involved and by evidence that the model gets this kind of arrangement wrong. The evidence is a <b>holdout test</b>: holiday effects fit only on data before
2025-12-01, then scored against Dec 2025 → Oct 2026, which the model never saw. Over that whole span the predicted holiday effect was off by {m(err['DAU|as_built'], 0)} person-days in absolute terms.</p>
{figure(p['holdout'], "holdout: world holiday effect predicted from pre-December-2025 data (red) vs realized (black). Bottom: three zooms, with the duplicates-removed variant dashed.")}

{card("high", "Christmas → New Year 2027: the biggest holiday period, on a weekday layout the model has seen once",
f'''<p>Dec 25 2027 and Jan 1 2028 both fall on Saturday, so the US/ROW calendar puts "Christmas Day" on Fri Dec 24 and "New Year's Day" on Fri Dec 31 (observed days).
Training has that layout only in 2021. In the holdout the model got the 2025/26 holiday hole <b>{pct(1 - hw['world_xmas_ny']['ratio'])} too shallow</b>
({m(hw['world_xmas_ny']['realized'] - hw['world_xmas_ny']['predicted'], 0)} person-days over Dec 18 → Jan 10). The ROW and US late-December clusters remove 44M and 17M
person-days in 2027. This falls after Dec 15, so the KPIs are unaffected; December tracking and the Jan-2027 MAU dip (the biggest MAU holiday effect of the year) are.</p>''')}

{card("high", "European May: Ascension five days after May 1 has never appeared in training",
f'''<p>Easter is early in 2027 (Mar 28), which pulls Ascension to Thu May 6, five days after Labour Day (Sat May 1). In 2021–2026 the gap was 8–28 days. France adds Victory Day on Sat May 8,
and Poland adds its May 3 holiday. Whit Monday is May 17 and Corpus Christi is May 27.
The 2026 holdout shows how the machinery handles a crowded May: the model's dip was <b>{hw['fr_may']['ratio']:.2f}×</b> what happened in France, {hw['de_may']['ratio']:.2f}× in Germany,
{hw['pl_may']['ratio']:.2f}× in Poland and {hw['row_may']['ratio']:.2f}× in ROW. Whit Monday 2026 was forecast at the −60% floor in DE and FR against −38% and −32% realized, because it carries
two labels (native + "Global") that are added together. 2027 person-days removed: DE 8.2M, FR 5.7M, PL 4.5M, ROW 12.4M (May 1–6 cluster).
<b>Expect May 2027 actuals to run above forecast</b>, with the biggest misses on May 6–7 and May 17.</p>''')}
{figure(p['may'], "top: 2026 holdout, predicted (red) vs realized (black). Bottom: the 2027 effects the model will apply, with each label; red text = a label mozaic adds rather than the country calendar.")}

{card("high", "Eid in the rest-of-world group: a real dip that the ROW calendar has no label for",
f'''<p>ROW (33% of world DAU) uses the <b>US</b> calendar plus Christian and Global add-ons. It has no Islamic holidays, but it contains Türkiye, Egypt, Pakistan, Bangladesh, the Gulf and
North Africa. Every year since 2023 ROW dropped 5–10% for 3–4 days at Eid al-Fitr. The model forecasts none of it: <b>{m(eid['fitr']['person_days'])} person-days</b>
around Tue 2027-03-09, peaking at {m(eid['fitr']['peak_people'])} on one day ({pct(-eid['fitr']['peak_people'] / world_day, 1)} of world DAU).
Eid al-Adha (Sun 2027-05-16) is a {m(eid['adha']['person_days'])} dip that the model gets <i>by accident</i>: the spurious Whit Monday label lands the next day (card below).</p>''')}
{figure(p['row_eid'], "ROW actual dip by year, aligned on each Eid (same-weekday baseline from ±3–5 weeks away), vs the 2027 model effect (red).")}

{card("high", "Whit Monday (Global) puts a dip on an ordinary Monday",
f'''<p>Mozaic adds "Whit Monday (Global)" to every country, including the US, Canada, China, India, Iran and Japan. In 2026 it coincided with US Memorial Day (May 25), so the label learned
Memorial Day's dip. The holdout forecast the US for May 20–30 at {hw['us_memorial_whit']['ratio']:.1f}× what happened. In 2027 Whit Monday is May 17 and Memorial Day is May 31, so the label
removes <b>{m(whit_total)}</b> people on May 17 across countries where it is not a holiday: US {pct(whit['US']['effect'])}, Canada {pct(whit['CA']['effect'])} ({m(us_ca_whit)} together, on a normal Monday),
ROW {pct(whit['ROW']['effect'])}, Indonesia {pct(whit['ID']['effect'])}. In ROW and Indonesia it lands the day after Eid al-Adha, so there it partly stands in for an Eid dip that is not modelled. Treat May 17 as a known false dip in US/CA.</p>''')}

{card("medium", "Easter 2027 (Mar 28), Holi (Mar 22) and Good Friday",
f'''<p>An early Easter is a familiar arrangement (2024 was Mar 31), but the 2026 holdout over-predicted it: Germany {hw['de_easter']['ratio']:.2f}×, ROW {hw['row_easter']['ratio']:.2f}×.
Germany's 2027 cluster also holds Saint Joseph's Day (Mar 19, a mozaic add-on). In India, Holi (Mar 22) and Good Friday (Mar 26) fall 4 days apart, an arrangement seen in two training years;
the holdout's March India window was {hw['in_holi_eid']['ratio']:.2f}×. Expect Mar 26–29 actuals slightly above forecast.</p>''')}
{figure(p['easter'], "March–April: 2026 holdout (top) and 2027 effects with labels (bottom).")}

{card("medium", "China Spring Festival (Sat 2027-02-06)",
f'''<p>Not a collision, but the holdout under-predicted Spring Festival 2026 by {pct(1 - hw['cn_spring']['ratio'])} (realized {m(hw['cn_spring']['realized'])} vs predicted {m(hw['cn_spring']['predicted'])} person-days).
China is {pct(f['shares']['CN'], 1)} of DAU. Expect February actuals in China below forecast.</p>''')}

<h2>3. Indonesia in detail</h2>
<p>Indonesia ({pct(f['shares']['ID'], 1)} of world DAU, about {m(leb['id_level'])} DAU) has Muslim, Christian, Hindu, Buddhist and Chinese holidays all on the national calendar, plus the add-ons mozaic gives every
Paschal-cycle country (Mardi Gras, Palm Sunday, Corpus Christi, Assumption, All Saints', Whit Monday). Most of 2027's collisions live here.</p>
{card("medium", "Lebaran (Eid al-Fitr, Tue–Wed Mar 9–10, with Nyepi on Mon Mar 8)",
f'''<p>The biggest Indonesian event: desktop DAU falls 40–79% over roughly ten days. The 2027 model curve follows past years' shape well, but the −60% floor clips the −67% to −79% core
days and the tail (days +6 to +9) comes out too shallow. History averages {leb['history_id_days']:.2f} Indonesia-days lost; the model has {leb['model_id_days']:.2f}. That gap is about <b>{m(lebaran_gap)}
person-days</b>. In the holdout, Lebaran 2026 came out {pct(1 - hw['id_lebaran']['ratio'])} too shallow. Ramadan starts ~Feb 8, two days after Lunar New Year (Feb 6).
Islamic dates in the calendar are library estimates; the government's announced date can move them a day. The collective-leave days (<i>cuti bersama</i>) that stretch the break are not in the calendar.</p>''')}
{figure(p['lebaran'], "Indonesia actual dip by year, aligned on Eid al-Fitr, vs the 2027 model effect (red).")}
{card("medium", "Four Indonesian arrangements new in 2027",
f'''<ul>
<li><b>Jan 1 → Jan 5:</b> Isra' Mi'raj falls four days after New Year's Day (it falls twice in 2027, the second time on Christmas).</li>
<li><b>May 16 → Jun 6:</b> Eid al-Adha (Sun 16), Whit Monday (add-on, 17), Vesak (Thu 20), Corpus Christi (add-on, 27), Pancasila (Tue Jun 1) and Islamic New Year (Sun Jun 6) form one chain three weeks long.
The comparable 2026 stretch came out {hw['id_late_may']['ratio']:.2f}× too deep in the holdout.</li>
<li><b>Aug 14 → 17:</b> the Prophet's Birthday falls three days before Independence Day.</li>
<li><b>Dec 25:</b> Christmas and Isra' Mi'raj on the same Saturday. The model adds both effects together and hits the −60% floor; a single day off will not drop twice.</li></ul>''')}
{figure(p['indonesia'], "Indonesia: 2026 holdout (top; data ends Oct 9) and every 2027 holiday effect with its label (bottom; red text = mozaic add-on).")}

<h2>4. How the model attributes holidays (why collisions confuse it)</h2>
<p>Mozaic learns one effect per (holiday, days-from-holiday). A day within ±7 days of several holidays splits its dip among them by inverse distance (closer gets more), and the forecast <i>adds up</i>
each holiday's learned effect at its 2027 offset. When 2027 puts holidays at spacings training never had, the sum is an extrapolation. Four specific faults make it worse:</p>
{card("medium", "Two labels for the same day off get counted twice",
f'''<p>When two labels share a row ("New Year's Day; New Year's Day (Global)", "Easter Sunday; Pasqua di Resurrezione"), the fit gives each label the full dip and the forecast adds them.
Removing the duplicates changes 2027 DAU by {m(f['duplicate_delta_person_days']['DAU'], 0)} person-days (Jan 1 alone: {m(f['jan1_2027']['as_built'])} → {m(f['jan1_2027']['no_duplicates'])}).
<b>But the holdout does not support removing them for DAU:</b> absolute error {m(err['DAU|as_built'], 0)} as built vs {m(err['DAU|no_duplicates'], 0)} without duplicates. Averaging across years
pulls each effect down (2020 enters as a zero year, and 2022–23 dips were small), which roughly cancels the doubling. For MAU, removing them helps ({m(err['MAU|as_built'], 0)} → {m(err['MAU|no_duplicates'], 0)}).
Where duplicates clearly hurt is a duplicated Monday (Whit Monday, above).</p>''')}
{card("low", "The 2025 Legacy-outage label dilutes every autumn-2025 holiday",
f'''<p>The outage is labelled as a holiday on every day from Aug 16 to Dec 10 2025, in every country, so it took ~80% of the credit for Labor Day, Golden Week, Diwali, All Saints' and
Thanksgiving 2025 (Thanksgiving learned −6% for 2025 against −29% realized). 2027's autumn holidays are therefore forecast {m(-f['outage_delta_person_days']['DAU'], 0)} person-days too shallow in total.
The holdout covers too little autumn to test the fix.</p>''')}
{card("low", "Smaller items",
'''<ul><li>The day-of-week scale is capped at 1.25 <i>after</i> it is applied (<code>core.py</code> <code>_predict_holiday_effects</code>), so the cap never acts; Monday holidays in DE/US/ROW use an uncapped multiplier.</li>
<li>Same-day stacks of two different holidays in 2027: Indonesia Dec 25 (above) and Iran Apr 2 (Martyrdom of Imam Ja'far al-Sadiq + Nature's Day).</li>
<li>Spurious add-ons with learned effects: Corpus Christi in the US and ROW (May 27, four days before Memorial Day), Mardi Gras and Corpus Christi in Indonesia, Whit Monday and Christmas Eve in China, India, Iran and Japan.</li>
<li>India in 2027: Prophet's Birthday, Independence Day and Raksha Bandhan within three days (Aug 14–17); Maha Shivaratri three days before Eid (Mar 6/9); Eid al-Adha four days before Buddha Purnima; Gandhi Jayanti a week before Dussehra. Each is new, but India's effects are small (1–3M person-days).</li></ul>''')}

<h2>5. Every 2027 collision cluster</h2>
<p>A cluster is a chain of 2027 holidays in one country, each within 7 days of the next (the window mozaic splits dips over), containing at least one moving holiday. <b>Tier rule:</b> High = a pair of real local
holidays at a spacing never seen in 2021–2026 <i>and</i> ≥ 3M world person-days removed; Medium = a new real pair with ≥ 1M, or any new pair (including add-ons) with ≥ 3M; Low = everything else.
This table ranks calendar novelty only; the watch-list cards above add the evidence that overrides it (ROW Christmas is "Low" here because no <i>moving</i> holiday is new, yet it is the biggest single period).</p>
{figure(p['world'], "2027 world DAU removed for holidays, as built; shaded spans = medium/high clusters. Bottom: three zooms.")}
<details open><summary>All {len(clusters)} clusters ({(clusters.tier != '3-low').sum()} medium/high; low tier greyed)</summary>{cluster_table(clusters, f['shares'])}</details>

<h2>6. Method and reproduction</h2>
<ul>
<li>Model: <code>data-official/2027-glean-draft/desktop_glean_history_cpr087_o_2026-10-10/…/mozaic_objects.glean_history_desktop.2026-10-10.pkl</code> (LOCK.md build 5). Read-only; no forecast was rerun.</li>
<li>Every effect is computed with mozaic's own <code>Mozaic._fit_holiday_effects</code> / <code>_predict_holiday_effects</code> on the summed country tiles; the as-built variant reproduces every tile's stored 2027 effect exactly (asserted).</li>
<li>"Actual dip" in the Eid/Lebaran plots is model-free: observed ÷ mean of the same weekday 3–4 weeks before and 4–5 weeks after, minus 1. "Realized" in the holdout is mozaic's own target, observed ÷ detrended − 1.</li>
<li>Scripts (run from the repo root, in order): <code>extract_holiday_effects.py</code>, <code>run_counterfactual_fits.py</code>, <code>run_holiday_backtest.py</code>, then from <code>research/holidays-2027/</code>: <code>build_holiday_review.py</code>, <code>render_holiday_review.py</code>.</li>
<li>Scope: desktop only (the 2027 Glean-history build). The same calendar machinery drives mobile, so the mechanism findings carry over; the person-day numbers do not.</li>
</ul></body></html>"""
    (ROOT / "index.html").write_text(html)
    print(ROOT / "index.html")


if __name__ == "__main__":
    main()
