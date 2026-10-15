#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""REPORT.html for the headline 2027 desktop MAU forecast: the real-Glean build with the Linux bot overlay (LOCK.md, 2026-10-14).

Replaces the build-5 report as the external PRE-DRAFT copy; the build-5 report is still rendered by
``render_glean_history_report.py`` to ``REPORT_build5_glean_equivalent.html`` as the validation record. Pure rendering:
every number comes from the build's exported CSVs, the comparison tables and the probe scores; plots are embedded.

Usage:
    python scripts/render_glean_real_report.py   # writes data-official/2027-glean-draft/REPORT.html
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
DRAFT = repo_root / "data-official/2027-glean-draft"
BUILD = DRAFT / "comparison_glean_real_mau_additive_b_2026-10-14"
CURVES = BUILD / "csv/comparison_glean_real_mau_b_canonical_curves.csv"
SUMMARY = BUILD / "csv/comparison_glean_real_mau_b_dec15_summary.csv"
LOCKED_CURVES = DRAFT / "csv/glean2027_canonical_curves.csv"
ADOPTED_CURVES = DRAFT / "comparison_glean_real_mau_additive_2026-10-13/csv/comparison_glean_real_mau_canonical_curves.csv"
BOT_SPEC = DRAFT / "linux_bot/linux_bot.glean_history.json"
PROBES = DRAFT / "probes_glean_real_mau"
OS_ORDER = ["world", "windows", "mac", "linux"]
DEC26, DEC27 = pd.Timestamp("2026-12-15"), pd.Timestamp("2027-12-15")


def img(path: Path) -> str:
    data = base64.b64encode(path.read_bytes()).decode()
    return f'<figure><img src="data:image/png;base64,{data}" alt="{path.name}"><figcaption><code>{path.relative_to(repo_root)}</code></figcaption></figure>'


def fmt(v) -> str:
    return f"{v:,.0f}" if pd.notna(v) else "—"


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=DRAFT / "REPORT.html")
    args = p.parse_args()
    curves = pd.read_csv(CURVES, parse_dates=["date"]).set_index("date")
    locked = pd.read_csv(LOCKED_CURVES, parse_dates=["date"]).set_index("date")
    adopted = pd.read_csv(ADOPTED_CURVES, parse_dates=["date"]).set_index("date")
    parquet = sorted(BUILD.glob("cps*/mozaic_daily_forecast.*.parquet"))[0]
    params = json.loads((parquet.parent / "parameters.json").read_text())
    bot_spec = json.loads(BOT_SPEC.read_text())
    bot_meta = json.loads((BOT_SPEC.parent / bot_spec["model_meta_file"]).read_text())
    bt = pd.read_csv(DRAFT / "backtest/errors_summary.csv")
    bt_row = bt[(bt.metric == "MAU") & (bt.horizon_days == 431)].iloc[0]
    r3 = pd.read_csv(PROBES / "scores_round3.csv")
    r3 = r3[r3.probe != "locked_additive"]
    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"], text=True).strip()
    hist = curves[curves["data_type"] == "training"]
    data_end, data_start = hist.index.max().date(), hist.index.min().date()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    held = bot_meta["held_levels"]["MAU"]
    world27, world26 = curves.loc[DEC27, "mau_world"], curves.loc[DEC26, "mau_world"]
    nonbot27 = world27 - held

    rows = ""
    for os_name in OS_ORDER:
        c = f"mau_{os_name}"
        last = hist[c].iloc[-1]
        yoy = hist[c].loc["2026-09-10":"2026-10-09"].mean() / hist[c].loc["2025-09-10":"2025-10-09"].mean() - 1
        v26, v27 = curves.loc[DEC26, c], curves.loc[DEC27, c]
        rows += (f"<tr><td>{'Desktop, all OS' if os_name == 'world' else os_name}</td><td class=num>{fmt(last)}</td><td class=num>{yoy:+.1%}</td>"
                 f"<td class=num>{fmt(v26)}</td><td class=num><b>{fmt(v27)}</b></td><td class=num>{v27 / v26 - 1:+.1%}</td><td class=num>{fmt(locked.loc[DEC27, c])}</td></tr>")
    sens = ""
    for _, r in r3.sort_values("world_2027-12-15").iterrows():
        sens += f"<tr><td>{r.probe}</td><td class=num>{r.cps:g} / {int(r.ncp)} / {r.cpr:g}</td><td class=num>{fmt(r['world_2027-12-15'])}</td><td class=num>{r.world_dec_to_dec:+.1%}</td></tr>"
    seam_step = curves.loc["2026-10-10":"2026-10-16", "mau_world"].mean() / curves.loc["2026-10-03":"2026-10-09", "mau_world"].mean() - 1
    shares = ", ".join(f"{k} {v:.0%}" for k, v in list(bot_spec["allocation"]["shares"].items())[:3])

    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<title>Firefox Desktop MAU through 2027, on real Glean telemetry — PRE-DRAFT</title>
<style>
body{{font:15px/1.5 -apple-system,"Segoe UI",Helvetica,Arial,sans-serif;color:#1c1c1c;max-width:1100px;margin:0 auto;padding:0 24px 64px;position:relative}}
.banner{{background:#222;color:#eee;padding:10px 16px;margin:0 -24px 24px;font-size:13px;display:flex;gap:28px;flex-wrap:wrap}} .banner b{{color:#fff}} .banner code{{background:#444;color:#fff}}
h1{{font-size:26px;margin:8px 0 4px}} h2{{font-size:20px;margin:36px 0 8px;border-bottom:1px solid #d9d9d9;padding-bottom:4px}} h3{{font-size:16px;margin:22px 0 6px}}
table{{border-collapse:collapse;width:100%;margin:10px 0 18px;font-size:14px}} th,td{{border:1px solid #d9d9d9;padding:6px 9px;vertical-align:top;text-align:left}} th{{background:#f3f3f3}} td.num,th.num{{text-align:right;font-variant-numeric:tabular-nums}}
code{{font:13px Menlo,Consolas,monospace;background:#f4f4f4;padding:1px 4px;border-radius:3px}}
.callout{{border-left:4px solid;padding:8px 14px;margin:14px 0;border-radius:0 4px 4px 0}} .warn{{background:#fff6e0;border-color:#e0a800}} .ok{{background:#eef7ee;border-color:#3a8a3a}} .ask{{background:#eee6f5;border-color:#6a1b9a}}
figure{{margin:16px 0}} figure img{{max-width:100%;border:1px solid #d9d9d9}} figcaption{{font-size:13px;color:#5a5a5a;margin-top:6px}}
.wm{{position:fixed;top:40%;left:4%;font-size:150px;color:rgba(106,27,154,.10);transform:rotate(-20deg);pointer-events:none;z-index:0}}
</style></head><body>
<div class="wm">PRE-DRAFT</div>
<div class="banner"><span><b>Generated:</b> {now}</span><span><b>Source:</b> <code>scripts/render_glean_real_report.py</code> on <code>{parquet.relative_to(repo_root).parent.parent.name}</code>, mozaic-daily <code>{commit}</code>, branch glean-2027-forecast</span><span><b>Data as of:</b> {data_end} (real Glean, from {data_start})</span><span><b>Status:</b> PRE-DRAFT — the headline build since 2026-10-14 (<code>LOCK.md</code>); not for circulation</span></div>

<h1>PRE-DRAFT — Firefox Desktop MAU through 2027, on real Glean telemetry</h1>
<p>The headline 2027 desktop forecast: <b>raw daily MAU on 2027-12-15</b>, trained on the real Glean desktop telemetry we have ({data_start} → {data_end}, 17 months), with
the model re-tuned for a short history and one correction (Linux automated traffic) applied because the forecast was implausible without it. Everything else in the data
is left as it is, by design: this is what the data we actually have produces. The forecast trained on the simulated (Glean-equivalent) history is kept as the
<b>validation build</b> (<code>REPORT_build5_glean_equivalent.html</code>); the two agree once the Linux correction is accounted for (§3).</p>

<h2>1. The numbers</h2>
<table><tr><th></th><th class=num>actual {data_end}</th><th class=num>data YoY (Sep 10 – Oct 9)</th><th class=num>2026-12-15</th><th class=num>2027-12-15</th><th class=num>Dec → Dec</th><th class=num>validation build 2027-12-15</th></tr>{rows}</table>
<div class="callout warn"><b>Read the world number, not the split.</b> mozaic reconciles tiles top-down, so a Linux-only correction moved the Windows and Mac forecasts too;
the per-OS figures are indicative. The world total is {fmt(world27)} on 2027-12-15 ({world27 / world26 - 1:+.1%} from {fmt(world26)} on 2026-12-15), of which {fmt(held)} is the Linux
automated-traffic level held flat (§2); the forecast for everyone else is {fmt(nonbot27)}.</div>
{img(BUILD / 'plots/canonical/mau_world_2027.png')}
{img(BUILD / 'plots/canonical/mau_world_2026.png')}
{img(BUILD / 'plots/canonical/mau_by_os_2027.png')}

<h2>2. How it was built</h2>
<ul>
<li><b>Data.</b> <code>glean_telemetry.active_users_aggregates</code>, Firefox Desktop, MAU by country × OS (15 markets + ROW; Windows / Mac / Linux), {data_start} → {data_end}. No simulated
history. The 2025 Legacy-outage mask used by the validation build is off (real Glean has no outage). The Iran shutdown crater (March–May 2026) is filled for fitting with the
counterfactual built for the validation build; the fill's DAU scale is within 1% of real Glean, the only borrowed input.</li>
<li><b>Model.</b> mozaic (Prophet per tile, top-down reconciliation) with the October 2026 desktop lock, except the settings that govern how freely the trend may bend:
<b>additive</b> seasonality regime, changepoint prior scale <b>0.005</b>, <b>3</b> changepoints, changepoint range <b>0.8</b> (lock: multiplicative, 0.1649, 40, 0.87). The lock is tuned
for five-plus years of history; on 17 months it read the one autumn ramp it saw as trend and sent MAU up and to the right while the data trend is down (world −3.8% YoY).
Three probe rounds (<code>COMPARISONS.html</code> §3) found the additive, near-linear setting is a stable region; the centre was taken. Config <code>{params['slug']}</code>.</li>
<li><b>Linux automated traffic (code <code>b</code>).</b> Measured from real Glean client data, not from any reference series: a client counts as automated on a day if its ISP's share of
"first seen inside the 28-day window with at most two active days" is ≥ 0.8. Hosting and cloud providers (Hetzner, Aruba Networks, Azure, Google Cloud, Oracle, Tencent, …) come
out at 0.95–1.00, residential ISPs about 0.3. The component is {fmt(held)} MAU over the last 28 days of history
({bot_meta['share_of_linux_last28d']['MAU']:.0%} of Linux MAU; {shares}), against a 0.4–0.7M baseline through 2025. It is subtracted from the Linux training rows before the model
runs, so the surge is not learned as trend, and added back <b>held flat at the last-28-day level</b>: a no-growth planning assumption (Brendan, 2026-10-14), in a series that has
only grown, in a term the 2027 KPI may or may not count. Windows (0.2% of MAU) and Mac (~0) are not corrected. Alternatives: zero (−{fmt(held)}), growth (open).</li>
<li><b>China.</b> The June 2026 MozillaOnline migration step is handled as in the validation build (measured curve subtracted and added back, Windows/Mac, code <code>o</code>).</li>
</ul>
{img(DRAFT / 'linux_bot/plots/linux_bot_curve.png')}

<h2>3. Check against the validation build</h2>
<div class="callout ok"><b>With the Linux component removed, this forecast gives {fmt(nonbot27)} on 2027-12-15; the validation build, trained on 6.8 years of simulated history
with a different model configuration, gives {fmt(locked.loc[DEC27, 'mau_world'])}.</b> Two histories, two configurations, the same number once the one measured short component is
accounted for. That agreement, not the tuning itself, is the justification for the re-tuned parameters: they were chosen by inspection, and an inspected choice needs an
independent check.</div>
{img(DRAFT / 'plots/comparisons/glean_real_round5_mau_by_os.png')}
<p>Dotted orange is this forecast minus its own Linux add-back; green is the validation build. Blue is this build without the Linux correction (Linux to 21.9M, growing
without bound in the multiplicative runs). Full detail, including the first run with the locked configuration (world MAU 169M → 229M, unusable), is in <code>COMPARISONS.html</code>.</p>

<h2>4. How far off could it be</h2>
<p>No backtest is possible on 17 months. By analogy, the validation build's configuration run from eight past dates on the long history missed the following year by
<b>{bt_row['abs_err_p50']:.0%} or less half the time and {bt_row['abs_err_p90']:.0%} or less nine times in ten</b> at this horizon (median signed error {bt_row['median']:+.1%}, i.e. it ran low). Around
{fmt(world27)} that is ±{fmt(world27 * bt_row['abs_err_p50'])} to ±{fmt(world27 * bt_row['abs_err_p90'])}; treat it as a floor, since this build has less history. Two further ranges:</p>
<table><tr><th>probe (additive regime)</th><th class=num>cps / changepoints / range</th><th class=num>world 2027-12-15 (before the Linux correction)</th><th class=num>Dec→Dec</th></tr>{sens}</table>
<p>The parameter neighbourhood spans about ±1.6M. The Linux path spans {fmt(nonbot27)} (fleet vanishes) to {fmt(world27)} (held flat) and higher if it keeps growing.</p>

<h2>5. Known limitations, deliberately left in</h2>
<ul>
<li><b>The January 2027 dip is an artifact.</b> Real MAU fell 13M between mid-December 2025 and mid-January 2026; about 8M of that was Firefox 148 fixing duplicate client ids. With one
observed year the model reads it as seasonal and repeats it in January 2027. It does not affect December 15. A corrected twin is planned as a <b>secondary comparison</b>, not as
the headline (the real data stays uncorrected except where the forecast is implausible).</li>
<li><b>Published-Glean units.</b> The number includes the remaining duplicate-id inflation (about 1% of Windows MAU, shrinking) and the Linux automated traffic. The validation build's
units (<code>glean_equivalent</code>) exclude both, which is why its 157.2M is compared with this build's non-bot {fmt(nonbot27)}, not with {fmt(world27)}.</li>
<li><b>One observed year of seasonality.</b> Shape within the year is weaker than in the validation build; the Dec-15 point is anchored by the near-linear trend.</li>
<li><b>Seam step</b> −{abs(seam_step):.2%} on the 7-day means at 2026-10-10: the Linux component's October spike sits above its 28-day mean, so the first forecast days sit below the last actuals. Reported, not smoothed.</li>
<li><b>MAU only.</b> DAU for this build has not been run (Brendan, 2026-10-13: MAU only); the validation build carries DAU.</li>
</ul>

<h2>6. Provenance</h2>
<table><tr><th>item</th><th>value</th></tr>
<tr><td>forecast parquet</td><td><code>{parquet.relative_to(repo_root)}</code> (sidecar: adjustments <code>['b', 'o']</code>)</td></tr>
<tr><td>seam / training end / horizon</td><td>2026-10-10 / {data_end} / 2027-12-31</td></tr>
<tr><td>model config</td><td><code>{json.dumps(params['config'])}</code></td></tr>
<tr><td>inputs</td><td><code>raw_glean_real_osxcountry_2026-10-09/</code> (SQL <code>scripts/sql/glean_history_comparison_glean_desktop_os_country.sql</code>); Iran fill <code>iran_fill/</code>; holidays: none</td></tr>
<tr><td>Linux component</td><td><code>linux_bot/</code>: spec, curves (edge {bot_meta['data_edge']}), meta, measurement CSVs; SQL <code>scripts/sql/glean_real_bot_isp_daily_curve.sql</code>; producer <code>scripts/build_glean_real_linux_bot_overlay.py</code></td></tr>
<tr><td>record</td><td><code>LOCK.md</code> (headline + validation rows), <code>PROCEDURE.html</code> §12–§14, <code>COMPARISONS.html</code>; branch <code>glean-2027-forecast</code> @ {commit}</td></tr>
<tr><td>CSVs</td><td><code>{CURVES.relative_to(repo_root)}</code>, <code>{SUMMARY.relative_to(repo_root)}</code>; handoff package <code>handoff/glean2027_desktop_mau_handoff.zip</code></td></tr>
</table>
</body></html>
"""
    args.out.write_text(html)
    print(f"wrote {args.out} ({args.out.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
