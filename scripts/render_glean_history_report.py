#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Self-contained HTML report for the current Glean-history 2027 draft build (plots embedded, DRAFT watermark).

Reads the build's parquet + sidecar, the exported curves / Dec-15 summary, the build comparison and the
export's translation bands, and writes ``data-official/2027-glean-draft/REPORT.html`` with a freshness banner.
Pure rendering: it recomputes nothing a reader could not reproduce from the CSVs it cites.

Usage:
    python scripts/render_glean_history_report.py \\
        --parquet data-official/2027-glean-draft/desktop_glean_history_g01_o_2026-10-10/<slug>/mozaic_daily_forecast.2026-10-10.gh-D+M.adj-o.parquet
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
if str(repo_root / "src") not in sys.path:
    sys.path.insert(0, str(repo_root / "src"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402

DRAFT = repo_root / "data-official/2027-glean-draft"
OS_ORDER = ["world", "windows", "mac", "linux"]


def img(path: Path) -> str:
    data = base64.b64encode(path.read_bytes()).decode()
    return f'<figure><img src="data:image/png;base64,{data}" alt="{path.name}"><figcaption><code>{path.relative_to(repo_root)}</code></figcaption></figure>'


def fmt(v) -> str:
    return f"{v:,.0f}" if pd.notna(v) else "—"


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--parquet", type=Path, required=True)
    p.add_argument("--out", type=Path, default=DRAFT / "REPORT.html")
    args = p.parse_args()
    args.parquet = args.parquet.resolve()

    df, meta = load_forecast(str(args.parquet))
    df["target_date"] = pd.to_datetime(df["target_date"])
    seam = meta["forecast_start_date"]
    params = json.loads((args.parquet.parent / "parameters.json").read_text())
    summary = pd.read_csv(DRAFT / "csv/glean2027_dec15_summary.csv")
    curves = pd.read_csv(DRAFT / "csv/glean2027_canonical_curves.csv", parse_dates=["date"]).set_index("date")
    comparison = pd.read_csv(DRAFT / "csv/glean2027_build_comparison_seam2026-10-10.csv")
    inputs_meta = meta.get("extra", {}).get("inputs_meta") or json.loads((DRAFT / "raw_glean_history_2026-10-09/glean_history_inputs.meta.json").read_text())
    cn = json.loads((DRAFT / "mozillaonline/cn_offsets_measurements.json").read_text())
    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"], text=True).strip()
    data_end = curves[curves["data_type"] == "training"].index.max().date()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")

    def val(metric, os_name, on):
        r = summary[(summary.metric == metric) & (summary.os == os_name) & (summary.date == on)]
        return float(r.value.item()), float(r.translation_band_pct.item()), float(r.translation_band_abs.item())

    # Headline table
    rows = []
    for metric, label in [("mau", "MAU, raw daily (headline)"), ("dau", "DAU, 28-day trailing MA")]:
        for os_name in OS_ORDER:
            v26, _, _ = val(metric, os_name, "2026-12-15")
            v27, band_pct, band_abs = val(metric, os_name, "2027-12-15")
            hist = curves.loc[pd.Timestamp("2025-12-15"), f"{'dau_28ma' if metric == 'dau' else 'mau'}_{os_name}"]
            rows.append(f"<tr><td>{label if os_name == 'world' else ''}</td><td>{os_name}</td><td class=num>{fmt(hist)}</td>"
                        f"<td class=num>{fmt(v26)}</td><td class=num><b>{fmt(v27)}</b></td><td class=num>{v27 / v26 - 1:+.1%}</td>"
                        f"<td class=num>±{band_pct:.2f}% (±{fmt(band_abs)})</td></tr>")
    headline_table = "\n".join(rows)

    # Per-country table (build 4), MAU + DAU 2027-12-15 and 2026-12-15
    cur = comparison[comparison.build == "o_both"]
    pc = cur.pivot_table(index="country", columns="date", values=["mau", "dau_28ma"])
    pc_rows = []
    for c in sorted(pc.index, key=lambda x: (x != "ALL", x)):
        pc_rows.append(f"<tr><td>{c}</td><td class=num>{fmt(pc.loc[c, ('mau', '2026-12-15')])}</td><td class=num>{fmt(pc.loc[c, ('mau', '2027-12-15')])}</td>"
                       f"<td class=num>{pc.loc[c, ('mau', '2027-12-15')] / pc.loc[c, ('mau', '2026-12-15')] - 1:+.1%}</td>"
                       f"<td class=num>{fmt(pc.loc[c, ('dau_28ma', '2026-12-15')])}</td><td class=num>{fmt(pc.loc[c, ('dau_28ma', '2027-12-15')])}</td></tr>")
    country_table = "\n".join(pc_rows)

    # Build comparison (world)

    # Seam step
    def step(col):
        a = curves.loc[pd.Timestamp(seam) - pd.Timedelta(days=1), col]; f = curves.loc[pd.Timestamp(seam), col]
        return f"{fmt(a)} → {fmt(f)} ({f / a - 1:+.2%})"

    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<title>Firefox Desktop 2027 forecast on Glean-equivalent history — DRAFT (build 4, seam {seam})</title>
<style>
body{{font:15px/1.5 -apple-system,"Segoe UI",Helvetica,Arial,sans-serif;color:#1c1c1c;max-width:1100px;margin:0 auto;padding:0 24px 64px;position:relative}}
.banner{{background:#222;color:#eee;padding:10px 16px;margin:0 -24px 24px;font-size:13px;display:flex;gap:28px;flex-wrap:wrap}} .banner b{{color:#fff}} .banner code{{background:#444;color:#fff}}
h1{{font-size:26px;margin:8px 0 4px}} h2{{font-size:20px;margin:36px 0 8px;border-bottom:1px solid #d9d9d9;padding-bottom:4px}} h3{{font-size:16px;margin:22px 0 6px}}
table{{border-collapse:collapse;width:100%;margin:10px 0 18px;font-size:14px}} th,td{{border:1px solid #d9d9d9;padding:6px 9px;vertical-align:top;text-align:left}} th{{background:#f3f3f3}} td.num,th.num{{text-align:right;font-variant-numeric:tabular-nums}}
code{{font:13px Menlo,Consolas,monospace;background:#f4f4f4;padding:1px 4px;border-radius:3px}}
.callout{{border-left:4px solid;padding:8px 14px;margin:14px 0;border-radius:0 4px 4px 0}} .warn{{background:#fff6e0;border-color:#e0a800}} .ok{{background:#eef7ee;border-color:#3a8a3a}}
figure{{margin:16px 0}} figure img{{max-width:100%;border:1px solid #d9d9d9}} figcaption{{font-size:13px;color:#5a5a5a;margin-top:6px}}
.wm{{position:fixed;top:40%;left:10%;font-size:160px;color:rgba(120,120,120,.08);transform:rotate(-20deg);pointer-events:none;z-index:0}}
</style></head><body>
<div class="wm">DRAFT</div>
<div class="banner"><span><b>Generated:</b> {now}</span><span><b>Source:</b> <code>scripts/render_glean_history_report.py</code> on <code>{args.parquet.relative_to(repo_root)}</code>, mozaic-daily <code>{commit}</code>, branch glean-2027-forecast</span><span><b>Data as of:</b> {data_end} (legacy-to-glean export + D-118 continuation)</span><span><b>Status:</b> DRAFT, build 4</span></div>

<h1>Firefox Desktop DAU and MAU through 2027, on Glean-equivalent history</h1>
<p>A draft forecast of desktop <b>MAU</b> (headline, raw daily rolling-28 MAU on 2027-12-15) and <b>DAU</b> (28-day trailing mean), trained on the
legacy-to-glean project's Glean-equivalent history (Legacy telemetry restated the way Glean counts, Fx 148–153 reference state) as if it were real Glean
history, because Glean desktop telemetry is too short to train on and Legacy ends in 2026. Not a monthly cycle: no head- or tailwinds are applied; the one
adjustment in the build is a <em>data correction</em> for China's June 2026 MozillaOnline migration step. Mobile is not covered.</p>

<h2>1. Headline numbers (world and per OS)</h2>
<table><tr><th>metric</th><th>OS</th><th class=num>2025-12-15 (history)</th><th class=num>2026-12-15</th><th class=num>2027-12-15</th><th class=num>Dec→Dec</th><th class=num>translation band (90%-style, correlated)</th></tr>
{headline_table}</table>
<div class="callout warn"><b>Uncertainty shown is the translation band only</b> — the export project's own statement of how far the Glean-equivalent history may be off
(half-width in % of the series, held at its last delivered value). It says nothing about forecast error. The model's Prophet predictive band is not reported:
at a 15-month horizon it is ±94M on world MAU (±20M even on production's own Legacy build), which is not an uncertainty statement anyone can use. A
backtest-based forecast error (how far 12–15-month-ahead forecasts from past seams missed) is the agreed next step and is not in this copy.</div>

<h2>2. Curves (canonical format: grey = prior year calendar-aligned, black = history, green = this draft)</h2>
<h3>MAU, raw daily</h3>
{img(DRAFT / "plots/canonical/mau_world_2027.png")}
{img(DRAFT / "plots/canonical/mau_world_2026.png")}
{img(DRAFT / "plots/canonical/mau_by_os_2027.png")}
<h3>DAU, 28-day trailing MA</h3>
{img(DRAFT / "plots/canonical/dau_28ma_world_2027.png")}
{img(DRAFT / "plots/canonical/dau_28ma_world_2026.png")}
{img(DRAFT / "plots/canonical/dau_28ma_by_os_2027.png")}
<p>Full-range views (2024 → 2027, world and per OS, with the seam zoom): <code>plots/glean2027_mau_daily.png</code>, <code>plots/glean2027_dau_28ma.png</code>; every per-OS, per-year canonical panel is under <code>plots/canonical/</code>.</p>
<p>Seam step at {seam} (last history day → first forecast day): world MAU {step("mau_world")}; windows MAU {step("mau_windows")}; world DAU 28d MA {step("dau_28ma_world")}. The
history's final weeks rose steeply (autumn recovery, plus a +0.36% input step at the 09-08/09-09 join recorded by the export project); the fit starts below that peak. Reported, not smoothed.</p>

<h2>3. By country (2026-12-15 and 2027-12-15)</h2>
<table><tr><th>country</th><th class=num>MAU 2026-12-15</th><th class=num>MAU 2027-12-15</th><th class=num>Dec→Dec</th><th class=num>DAU 28d MA 2026-12-15</th><th class=num>DAU 28d MA 2027-12-15</th></tr>
{country_table}</table>
{img(DRAFT / "plots/glean2027_mau_by_country.png")}
{img(DRAFT / "plots/glean2027_dau_by_country.png")}

<h2>4. What the China correction did</h2>
<p>China's desktop DAU doubled in June 2026 when MozillaOnline users migrated onto mainline Firefox (visible on Windows and Mac, and on MAU, in the Glean-equivalent data).
Left in the training rows, the raw model mis-estimated China's holidays (China DAU to a 450K monthly mean in October 2027), produced negative China tiles,
and let China's sample paths dominate every interval. The correction subtracts the measured migration curve from China's training rows before the model runs
and adds it back after, on both metrics, split across Windows ({cn['measurements']['population_shares']['DAU']['windows']:.1%} DAU / {cn['measurements']['population_shares']['MAU']['windows']:.1%} MAU)
and Mac. The MAU curve is derived: the DAU curve's trailing 28-day mean × {cn['measurements']['ratio_world']:.3f}, China's own measured MAU-per-DAU of the migrated users
(windows {cn['measurements']['ratios']['windows']:.2f}, mac {cn['measurements']['ratios']['mac']:.2f}, against baselines of 3.74 / 3.41). Nobody has measured MozillaOnline MAU directly.</p>
<p>Because mozaic reconciles top-down, removing the step from the world series lowers the world trend and that is pushed into every country (about −1 to −3%
on MAU outside China at 2027-12-15). This is how the production cycle's MozillaOnline adjustment behaves too, and was accepted for this draft. The uncorrected
model is kept on disk for the record (<code>PROCEDURE.html</code> §8) and is not shown here.</p>
{img(DRAFT / "plots/cn_offsets_check.png")}

<h2>5. Method, in one paragraph each</h2>
<ul>
<li><b>Data.</b> The export's <code>glean_equivalent</code> headline (translated Legacy + steady Glean-only share; excludes the Linux bot-like temporary surges and pre-Fx-148 duplicate-id MAU inflation), 45 OS × country cells for 15 countries plus ROW, 2020-01-01 → {data_end}; rounded to whole people. Tiles are Windows / Mac / Linux per country, not production's Windows-version segments, because the translation has no version split.</li>
<li><b>Model.</b> mozaic (Prophet per tile, top-down reconciliation) with the October 2026 desktop lock (<code>{params['slug']}</code>), DAU and MAU fitted independently with the same configuration.</li>
<li><b>Iran.</b> The 2026 shutdown crater is replaced for fitting by a counterfactual fill regenerated for this layout with the production method (scale 1.0 on both metrics); the actuals keep the crater.</li>
<li><b>2025 Legacy outage.</b> The export inherits Legacy's outage dips (Windows 2025-08-16..11-06, Mac 08-28..12-10); masked as one holiday window so the model does not learn them.</li>
<li><b>Continuation.</b> The export project re-applied its translation to Legacy days 2026-09-09..{data_end} (their D-118), with the duplicate-id share and bands carried forward and no new Linux windows detected.</li>
</ul>

<h2>6. Caveats a reader should carry</h2>
<ul>
<li>The history is a <b>model translation in the Fx 148–153 Glean state</b>, one version state behind current Firefox; a Fx 155 re-measurement (late October) may reissue the whole series (−0.4% Windows / −0.7% Mac at client level is the known offset to Fx 154).</li>
<li>Forecasting the headline means published Glean MAU will sit above it by the duplicate-id floor (~0.5%) plus any open Linux temporary term, unless those are carried as explicit adjustments (registered as candidates, not applied).</li>
<li>One extra month of history moved the 2027 headline by +2.8% (build 1 → build 2). The number is sensitive to the autumn 2026 trajectory.</li>
<li>China windows MAU declines 18% through 2027 in this build while its DAU rises 12%; China is 4% of the world. Accepted for the draft.</li>
<li>Linux is the noisiest OS in the translation (headline 2–4% above published in 2025); its strong upward trend (+17% MAU Dec→Dec) is carried as the data gives it.</li>
<li>No comparison forecasts yet (Legacy through today, real Glean from 2025-05-22): next step after this copy.</li>
</ul>

<h2>7. Provenance</h2>
<table>
<tr><th>item</th><th>value</th></tr>
<tr><td>forecast parquet</td><td><code>{args.parquet.relative_to(repo_root)}</code> (sidecar lists <code>adjustments_applied = ['o']</code>)</td></tr>
<tr><td>seam / training end / horizon</td><td>{seam} / {data_end} / 2027-12-31</td></tr>
<tr><td>model config</td><td><code>{json.dumps(params['config'])}</code></td></tr>
<tr><td>export (history)</td><td>DAU sha1 <code>{inputs_meta['inputs']['DAU']['source_sha1']}</code>, MAU sha1 <code>{inputs_meta['inputs']['MAU']['source_sha1']}</code></td></tr>
<tr><td>export (continuation)</td><td>DAU sha1 <code>{inputs_meta['inputs']['DAU'].get('continuation_sha1')}</code>, MAU sha1 <code>{inputs_meta['inputs']['MAU'].get('continuation_sha1')}</code>; legacy-to-glean worktree export-rollups @ 22e01e2, decisions through D-118</td></tr>
<tr><td>China correction</td><td><code>data-official/2027-glean-draft/mozillaonline/</code> (spec, DAU curve from the 2026-09-02 official export, derived MAU curve, approach note, measurements)</td></tr>
<tr><td>Iran fill</td><td><code>data-official/2027-glean-draft/iran_fill/iran_fill.glean_history_desktop.parquet</code> (+ meta)</td></tr>
<tr><td>record</td><td><code>data-official/2027-glean-draft/PROCEDURE.html</code> (every decision and build), branch <code>glean-2027-forecast</code> @ {commit}</td></tr>
<tr><td>CSVs</td><td><code>csv/glean2027_canonical_curves.csv</code>, <code>csv/glean2027_dec15_summary.csv</code>, <code>csv/glean2027_build_comparison_seam2026-10-10.csv</code></td></tr>
</table>
</body></html>"""
    args.out.write_text(html)
    print(f"wrote {args.out} ({args.out.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
