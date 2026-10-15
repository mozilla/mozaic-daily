#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Render the review page for one round of the organic mobile MAU changepoint probes (``probes_mobile_mau/<grid>.html``).

Round 1's page was written by hand in the session; from round 2 on this script builds it from what is on disk, so a
re-score refreshes the page and its freshness banner. Sections: the data correction in force (iOS-rate cap plot + numbers,
when the grid trains on it), this round's scores against the targets, the comparison round's scores, the world / Fenix /
capped-country forecast plot, and the 2 × 5 organic figure. Images are embedded (self-contained file). ``--commentary`` is
a small HTML fragment with the round's reading, written after the numbers are in.

    python scripts/render_mobile_mau_probes_page.py --grid round2 --compare-to round1 --commentary tmp/round2_commentary.html
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
if str(repo_root / "scripts") not in sys.path:
    sys.path.insert(0, str(repo_root / "scripts"))
from run_mobile_mau_probes import DRAFT, GRIDS, IOS_RATE_CAP_GLOB, PROBES, TARGET_DEC_TO_DEC, TARGET_SEAM_ABS  # noqa: E402

STYLE = """body{font:15px/1.5 -apple-system,Helvetica,Arial,sans-serif;max-width:1500px;margin:0 auto;padding:0 24px 60px;color:#1c1c1c}
.banner{background:#222;color:#eee;padding:10px 16px;margin:0 -24px 24px;font-size:13px;display:flex;gap:28px;flex-wrap:wrap} .banner b{color:#fff}
table{border-collapse:collapse;font-size:14px;margin:12px 0} th,td{border:1px solid #ddd;padding:5px 9px} th{background:#f3f3f3} td.num{text-align:right;font-variant-numeric:tabular-nums}
td.ok{background:#e6f4ea} td.bad{background:#fdecea} figure{margin:18px 0} img{max-width:100%;border:1px solid #ddd} figcaption{font-size:12px;color:#555}
.callout{border-left:4px solid #6a1b9a;background:#f5eefa;padding:8px 14px;margin:14px 0} .warn{border-left-color:#c62828;background:#fdecea}
h2{margin-top:30px;border-bottom:1px solid #ddd} code{font-size:13px}"""


def img(path: Path) -> str:
    data = base64.b64encode(path.read_bytes()).decode()
    rel = path.resolve().relative_to(repo_root)
    return f'<figure><img src="data:image/png;base64,{data}"><figcaption><code>{rel}</code></figcaption></figure>'


def score_table(scores: pd.DataFrame, with_targets: bool) -> str:
    head = ["probe", "seam step world", "seam step Fenix", "MAU 2026-12-15", "MAU 2027-12-15", "Dec→Dec", "Oct-9→Oct-9 2027"]
    rows = [f"<tr>{''.join(f'<th>{h}</th>' for h in head)}</tr>"]
    for _, r in scores.iterrows():
        seam_cls = (" ok" if abs(r.seam_step_world) <= TARGET_SEAM_ABS else " bad") if with_targets else ""
        dec_cls = (" ok" if TARGET_DEC_TO_DEC[0] <= r.dec_to_dec <= TARGET_DEC_TO_DEC[1] else " bad") if with_targets else ""
        rows.append(f"<tr><td><code>{r.probe}</code></td><td class='num{seam_cls}'>{r.seam_step_world:+.1%}</td><td class=num>{r.seam_step_fenix:+.1%}</td>"
                    f"<td class=num>{r.dec15_2026:,.0f}</td><td class=num>{r.dec15_2027:,.0f}</td><td class='num{dec_cls}'>{r.dec_to_dec:+.1%}</td>"
                    f"<td class=num>{r.yoy_forecast_oct9_2027:+.1%}</td></tr>")
    return f"<table>{''.join(rows)}</table>"


def cap_section(cap_meta: dict) -> str:
    n = cap_meta["numbers"]
    rows = "".join(f"<tr><td>{c}</td><td class=num>{n['organic_mau_last_day'][c]:,}</td><td class=num>{n['removed_mau_last_day'][c]:,}</td>"
                   f"<td class=num>{n['organic_growth_since_from'][c]:+.1%}</td><td class=num>{n['ios_ratio_smoothed_last_day'][c] - 1:+.1%}</td>"
                   f"<td>{n['first_capped_day'][c]}</td></tr>" for c in cap_meta["countries"])
    return f"""<h2>1. The data correction in force: iOS-rate cap, {' + '.join(cap_meta['countries'])} from {cap_meta['from_date']}</h2>
<div class="callout"><b>Provisional</b> (Brendan 2026-10-15; may be rolled back on the marketing DS team's read — rollback is dropping
<code>--ios-rate-cap</code>, see <code>data-official/2027-glean-draft/organic_mau/ios_rate_cap/IOS_RATE_CAP.md</code>).
Organic Fenix MAU training rows in these countries are capped at the level they would reach by growing at the country's own
Firefox iOS rate (trailing {cap_meta['smoothing_days']}-day mean ratio); the positive excess is treated as paid and removed, nothing is added back.
Evidence: {cap_meta['evidence']}.</div>
{img(repo_root / cap_meta['plot'])}
<table><tr><th>country</th><th>organic Fenix MAU {n['last_day']}</th><th>removed</th><th>organic growth since {cap_meta['from_date']}</th><th>iOS growth (28d mean) since {cap_meta['from_date']}</th><th>first capped day</th></tr>{rows}
<tr><td><b>total</b></td><td></td><td class=num><b>{n['removed_mau_last_day_total']:,}</b></td><td colspan=3>{n['removed_share_of_world_organic_fenix_last_day']:.2%} of world organic Fenix on {n['last_day']}</td></tr></table>"""


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--grid", required=True, choices=sorted(GRIDS))
    p.add_argument("--compare-to", default=None, choices=sorted(GRIDS))
    p.add_argument("--commentary", type=Path, default=None, help="HTML fragment with the round's reading")
    p.add_argument("--organic-2x5", type=Path, default=None, help="the 2x5 organic figure for this round (plot_mobile_mau_organic_paid.py --tag)")
    args = p.parse_args()

    other = None
    if args.compare_to:   # the comparison csv is written by `score --grid X --compare-to Y`, both grids scored by the same code
        both = pd.read_csv(PROBES / f"scores_{args.grid}_vs_{args.compare_to}.csv")
        scores, other = both[both.grid == args.grid].reset_index(drop=True), both[both.grid == args.compare_to].reset_index(drop=True)
    else:
        scores = pd.read_csv(PROBES / f"scores_{args.grid}.csv")
    capped = GRIDS[args.grid]["ios_rate_cap"]
    cap_meta = json.loads(sorted(DRAFT.glob(IOS_RATE_CAP_GLOB.replace(".parquet", ".meta.json")))[-1].read_text()) if capped else None
    split_meta = json.loads(sorted(DRAFT.glob("organic_mau/fenix_paid_organic_mau.*.meta.json"))[-1].read_text())
    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"], text=True).strip()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    knobs = GRIDS[args.grid]["knobs"]
    cprs = sorted({k["changepoint_range"] for k in knobs}); cpss = sorted({k["changepoint_prior_scale"] for k in knobs})
    meets = scores[scores.meets_seam & scores.meets_dec_to_dec]["probe"].tolist()
    last_actual = scores["last_actual"].iloc[0]
    yoy = scores["yoy_data_last"].iloc[0]
    commentary = args.commentary.read_text() if args.commentary else ""
    n = 1

    parts = [f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Organic mobile MAU — changepoint probes {args.grid}</title><style>{STYLE}</style></head><body>
<div class="banner"><span><b>Generated:</b> {now}</span><span><b>Source:</b> scripts/render_mobile_mau_probes_page.py --grid {args.grid} (scores from scripts/run_mobile_mau_probes.py), mozaic-daily {commit}</span>
<span><b>Data as of:</b> {split_meta['date_range'][1]} (real Glean mobile; organic split measured from {split_meta['date_range'][0]})</span><span><b>Status:</b> PRE-DRAFT, internal</span></div>
<h1>Organic mobile MAU — changepoint probes, {args.grid}</h1>
<div class="callout">Changepoint range {{{', '.join(map(str, cprs))}}} × changepoint prior scale {{{', '.join(map(str, cpss))}}} on the organic series
{'<b>with the India + Indonesia iOS-rate cap</b>' if capped else 'without any correction'}; October 2026 mobile lock otherwise, Iran 2026 bridged in training.
Last organic actual the probes trained on, {split_meta['date_range'][1]}: <b>{last_actual:,.0f}</b> ({yoy:+.1%} YoY).
Targets (the round-2 brief): seam step within ±{TARGET_SEAM_ABS:.0%}, Dec→Dec slope in the {TARGET_DEC_TO_DEC[0]:+.0%}..{TARGET_DEC_TO_DEC[1]:+.0%} band.
Meets both: <b>{', '.join(f'<code>{m}</code>' for m in meets) if meets else 'none'}</b>.</div>
{commentary}"""]
    if capped:
        parts.append(cap_section(cap_meta)); n += 1
    parts.append(f"<h2>{n}. Scores, {args.grid}</h2><p>Green = inside the target; seam step is the first forecast week against the last training week, world organic MAU.</p>{score_table(scores, True)}"); n += 1
    if other is not None:
        parts.append(f"<h2>{n}. For comparison: {args.compare_to} ({'uncorrected organic' if not GRIDS[args.compare_to]['ios_rate_cap'] else 'capped'}), same targets</h2>"
                     f"<p>Last actual there: {other['last_actual'].iloc[0]:,.0f}; the {args.grid} series is {last_actual - other['last_actual'].iloc[0]:+,.0f} on the same day.</p>{score_table(other, True)}"); n += 1
    parts.append(f"<h2>{n}. Forecasts over the actuals: world, Fenix, and the capped countries</h2>{img(PROBES / 'plots' / f'probes_{args.grid}_world.png')}"); n += 1
    if args.organic_2x5:
        parts.append(f"<h2>{n}. The 2 × 5 layout, organic only</h2><p>History is the series the probes trained on; one line per probe shown.</p>{img(args.organic_2x5)}"); n += 1
    parts.append("</body></html>")
    out = PROBES / f"{args.grid}.html"
    out.write_text("".join(parts))
    print(f"page  {out}")


if __name__ == "__main__":
    main()
