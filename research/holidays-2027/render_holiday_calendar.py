"""Render ``calendar.html``: a vertical day-by-day 2027 calendar of every holiday label the model uses.

One row per day. Each holiday is listed once with the countries observing it (ordered by DAU share);
mozaic add-on labels are faded, same-day duplicates are hidden. A bar shows the people the locked build
removes from world DAU that day. Days inside a medium/high collision cluster carry the tier colour, and
the review's watch-list notes (including dips no calendar label covers) are attached to their dates.

Reads ``data/`` (calendar, counterfactual effects, tile effects, clusters, facts); run from this directory
after ``build_holiday_review.py``.
"""

import json
from collections import defaultdict
from datetime import datetime
from html import escape
from pathlib import Path

import pandas as pd

from holiday_calendar import explode_labels, tag_labels
from review_metrics import country_base, country_impacts

ROOT = Path(__file__).parent
YEAR = 2027
TIER_RANK = {"1-high": 1, "2-medium": 2}

# Watch-list notes from index.html, pinned to dates. Dips with no calendar label go here too.
NOTES = {
    "2027-01-01": ("high", "Fri. Duplicate New Year labels stack; holdout says the net DAU depth is about right."),
    "2027-02-06": ("medium", "Spring Festival (Sat). CN was 40% too shallow in the 2026 holdout. Ramadan begins ~Feb 8."),
    "2027-03-08": ("medium", "ID Lebaran starts: Nyepi, then Eid Mar 9–10; the dip runs ~10 days, model ~1.8M person-days short."),
    "2027-03-09": ("high", "ROW Eid al-Fitr: −5–10% for 3–4 days, NOT in the ROW calendar (≈5.4M person-days missing)."),
    "2027-03-26": ("medium", "Good Friday; early Easter. 2026 holdout over-predicted DE 1.17×, ROW 1.35×."),
    "2027-05-01": ("high", "Sat. Start of the crowded European May (holdout FR 1.73×, DE 1.40×, PL 2.11× too deep)."),
    "2027-05-06": ("high", "Ascension 5 days after May 1: never seen in training. Watch the Fri May 7 bridge too."),
    "2027-05-16": ("high", "Eid al-Adha. ROW has no label; the next day's Whit Monday label covers it by accident."),
    "2027-05-17": ("high", "Whit Monday (Global) on an ordinary Monday in US/CA (−14%/−15%): a false dip."),
    "2027-05-27": ("medium", "Corpus Christi is a mozaic add-on in US/ROW/ID, four days before Memorial Day."),
    "2027-08-14": ("medium", "IN/ID: Prophet's Birthday 1–3 days before Independence Day (new arrangement)."),
    "2027-11-25": ("ok", "US Thanksgiving, inside the Dec-15 KPI window (same as 2026): no YoY distortion."),
    "2027-12-15": ("ok", "KPI date. The 28-day window back to Nov 18 holds no moving holiday."),
    "2027-12-24": ("high", "Christmas/New Year on Saturdays: observed Fridays Dec 24 and Dec 31 (seen only in 2021). Holdout 11% too shallow."),
    "2027-12-25": ("medium", "ID: Christmas and Isra' Mi'raj on the same day; the model adds both and hits the −60% floor."),
}

CSS = """
body{font-family:-apple-system,Segoe UI,Helvetica,sans-serif;max-width:1100px;margin:0 auto;padding:0 20px 60px;color:#1d2733}
.banner{background:#1d2733;color:#f5f7fa;padding:10px 16px;border-radius:0 0 8px 8px;font-size:13px;display:flex;gap:28px;flex-wrap:wrap}
.banner b{color:#f4d58d} h1{margin:20px 0 4px} .lede{color:#33475b;margin:0 0 10px}
.legend{font-size:12px;color:#4a5866;margin:8px 0 16px;display:flex;gap:18px;flex-wrap:wrap;align-items:center}
.toggle{font-size:12px;margin-bottom:10px}
.month{position:sticky;top:0;background:#fff;z-index:2;font-size:20px;font-weight:700;padding:14px 0 6px;border-bottom:2px solid #1d2733}
.day{display:grid;grid-template-columns:92px 150px 1fr;gap:10px;padding:3px 6px;border-bottom:1px solid #eef1f4;border-left:5px solid transparent;font-size:13px}
.day.empty{padding:0 6px;font-size:11px;color:#b3bcc6;min-height:14px}
.day.weekend{background:#fafbfc}
.day.t1{border-left-color:#c0392b;background:#fdf0ee} .day.t2{border-left-color:#e08e0b;background:#fdf8ee}
.date b{display:inline-block;width:26px} .dow{color:#6b7a89}
.bar{position:relative;height:14px;background:#eef1f4;border-radius:3px;margin-top:3px}
.bar span{position:absolute;right:0;top:0;bottom:0;background:#2f5590;border-radius:3px}
.barlabel{font-size:11px;color:#4a5866}
.h{margin:1px 0} .h .name{font-weight:600}
.h.addon{opacity:.5} .h.addon .name{font-weight:400;font-style:italic}
.chip{display:inline-block;font-size:10.5px;padding:0 5px;margin:0 2px 1px 0;border-radius:8px;background:#e3e8ef;color:#33475b}
.chip.big{background:#33475b;color:#fff} .chip.mid{background:#8fa3c0;color:#fff}
.chip.c1{box-shadow:0 0 0 2px #c0392b} .chip.c2{box-shadow:0 0 0 2px #e08e0b}
.note{font-size:12px;margin:3px 0 1px;padding:2px 8px;border-radius:4px}
.note.high{background:#c0392b;color:#fff} .note.medium{background:#e08e0b;color:#fff} .note.ok{background:#2e8b57;color:#fff}
.hidden{display:none}
"""


def chip_class(share: float) -> str:
    return "big" if share >= 0.10 else "mid" if share >= 0.04 else ""


def load_inputs():
    calendar = pd.read_csv(ROOT / "data" / "calendar.csv")
    tagged = tag_labels(explode_labels(calendar))
    tagged = tagged[(tagged["year"] == YEAR) & ~tagged["global_duplicate"] & (tagged["source"] != "outage")]

    effects = pd.read_csv(ROOT / "data" / "counterfactual_effects.csv", parse_dates=["date"])
    base = country_base(pd.read_csv(ROOT / "data" / "tile_effects.csv", parse_dates=["date"]))
    impacts = country_impacts(effects[effects["variant"] == "as_built"], base)
    dau = impacts[(impacts["metric"] == "DAU") & (impacts["date"].dt.year == YEAR)]
    world = dau.groupby("date")["impact"].sum()
    level = dau.groupby("date")["forecast_detrended"].sum()
    top_countries = (dau[dau["impact"] < -1e4].sort_values("impact").groupby("date")
                     .apply(lambda g: [(r.country, r.proportional_effect) for r in g.head(4).itertuples()]))

    clusters = pd.read_csv(ROOT / "data" / "clusters_2027.csv", parse_dates=["start", "end"])
    cluster_tier = {}  # (date, country) -> best tier rank
    for c in clusters[clusters["tier"].isin(TIER_RANK)].itertuples():
        for day in pd.date_range(c.start, c.end):
            key = (day, c.country)
            cluster_tier[key] = min(cluster_tier.get(key, 9), TIER_RANK[c.tier])
    shares = json.loads((ROOT / "data" / "facts.json").read_text())["shares"]
    return tagged, world, level, top_countries, cluster_tier, shares


def day_holidays(rows: pd.DataFrame, day, shares: dict, cluster_tier: dict) -> str:
    """One line per holiday name, countries as chips ordered by DAU share."""
    by_name = defaultdict(list)
    native_somewhere = defaultdict(bool)
    for r in rows.itertuples():
        by_name[r.name].append(r.country)
        native_somewhere[r.name] |= r.source == "native"
    ordered = sorted(by_name, key=lambda n: (not native_somewhere[n], -sum(shares.get(c, 0) for c in by_name[n])))
    lines = []
    for name in ordered:
        chips = []
        for country in sorted(set(by_name[name]), key=lambda c: -shares.get(c, 0)):
            tier = cluster_tier.get((day, country))
            ring = f" c{tier}" if tier else ""
            chips.append(f'<span class="chip {chip_class(shares.get(country, 0))}{ring}" '
                         f'title="{country}: {shares.get(country, 0) * 100:.1f}% of world DAU">{country}</span>')
        css = "h" if native_somewhere[name] else "h addon"
        lines.append(f'<div class="{css}"><span class="name">{escape(name)}</span> {"".join(chips)}</div>')
    return "".join(lines)


def main() -> None:
    tagged, world, level, top_countries, cluster_tier, shares = load_inputs()
    deepest = -world.min()
    body, month = [], None
    for day in pd.date_range(f"{YEAR}-01-01", f"{YEAR}-12-31"):
        if day.month != month:
            month = day.month
            body.append(f'<div class="month">{day.strftime("%B %Y")}</div>')
        rows = tagged[tagged["date"] == day]
        people = -float(world.get(day, 0.0))
        note = NOTES.get(day.strftime("%Y-%m-%d"))
        weekend = " weekend" if day.dayofweek >= 5 else ""
        date_cell = f'<div class="date"><b>{day.day}</b><span class="dow">{day.strftime("%a")}</span></div>'
        if rows.empty and people < 5e5 and not note:
            body.append(f'<div class="day empty{weekend}">{date_cell}<div></div><div></div></div>')
            continue
        tiers = [cluster_tier.get((day, c)) for c in rows["country"].unique()]
        tiers = [t for t in tiers if t]
        tier_css = f" t{min(tiers)}" if tiers else ""
        biggest = ", ".join(f"{c} {e * 100:.0f}%" for c, e in top_countries.get(day, []))
        bar = (f'<div><div class="barlabel">−{people / 1e6:.1f}M · {people / level.get(day, 1) * 100:.1f}% of world</div>'
               f'<div class="bar" title="Largest: {biggest}"><span style="width:{people / deepest * 100:.1f}%"></span></div></div>'
               if people >= 5e4 else "<div></div>")
        note_html = f'<div class="note {note[0]}">{escape(note[1])}</div>' if note else ""
        body.append(f'<div class="day{weekend}{tier_css}">{date_cell}{bar}<div>{note_html}'
                    f'{day_holidays(rows, day, shares, cluster_tier)}</div></div>')

    generated = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>2027 holiday calendar</title><style>{CSS}</style></head><body>
<div class="banner"><span><b>Generated:</b> {generated}</span><span><b>Source:</b> research/holidays-2027/render_holiday_calendar.py</span>
<span><b>Data as of:</b> 2026-10-09 (training end of the locked build 5)</span></div>
<h1>2027 holiday calendar</h1>
<p class="lede">Every holiday label the 2027 desktop model uses, day by day, with the people it removes from world DAU. Companion to <a href="index.html">the 2027 holiday review</a>.</p>
<div class="legend">
<span><span class="chip big">DE</span> ≥10% of world DAU</span><span><span class="chip mid">PL</span> 4–10%</span><span><span class="chip">AR</span> &lt;4%</span>
<span><span class="chip c1">FR</span> in a high-tier collision</span><span><span class="chip c2">IN</span> medium-tier</span>
<span><i style="opacity:.5">Faded italic</i> = label mozaic adds, not on the country's own calendar</span>
<span>Bar = model's holiday effect on world DAU (hover: biggest countries)</span>
</div>
<label class="toggle"><input type="checkbox" id="hide"> Hide days with no holiday</label>
{"".join(body)}
<script>document.getElementById('hide').addEventListener('change',e=>document.querySelectorAll('.day.empty').forEach(d=>d.classList.toggle('hidden',e.target.checked)));</script>
</body></html>"""
    (ROOT / "calendar.html").write_text(html)
    print(ROOT / "calendar.html")


if __name__ == "__main__":
    main()
