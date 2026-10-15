#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""The two formal comparisons of the 2027 Glean-history forecast, as one self-contained PRE-DRAFT page.

(1) Legacy MAU / DAU: the same locked configuration run on Legacy telemetry, against the locked forecast.
(2) Real Glean MAU with NO simulated history: the 17 months of real Glean desktop telemetry that exist (2025-05-22 on).
    The first run (locked configuration) is unusable and is kept for the record; three probe rounds on 2026-10-13
    re-tuned the trend and seasonality parameters for a short history, and the additive-regime centre was adopted
    "temporarily" (Brendan). MAU only from that point (Brendan, 2026-10-13).

Pure rendering plus two derived tables a reader can reproduce from the cited curves CSVs, which it also writes:
``csv/glean2027_vs_glean_real_comparison.csv`` (the adopted real-Glean build against the locked one by horizon, MAU) and
``csv/glean2027_in_real_glean_units.csv`` (the locked forecast × the last-30-day real ÷ Glean-equivalent ratio).
Plots come from ``plot_glean_history_comparisons.py``, ``run_glean_real_mau_probes.py score`` and
``plot_glean_real_linux_components.py`` (run them first). The raw model is never shown.

Usage:
    python scripts/render_glean_history_comparisons.py   # writes data-official/2027-glean-draft/COMPARISONS.html
"""

from __future__ import annotations

import argparse
import base64
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
DRAFT = repo_root / "data-official/2027-glean-draft"
PLOTS = DRAFT / "plots/comparisons"
PROBES = DRAFT / "probes_glean_real_mau"
LOCKED_CURVES = DRAFT / "csv/glean2027_canonical_curves.csv"
LEGACY_CURVES = DRAFT / "comparison_legacy_cpr087_2026-10-10/csv/comparison_legacy_canonical_curves.csv"
GLEAN_REAL_FIRST_RUN = DRAFT / "comparison_glean_real_cpr087_2026-10-10/csv/comparison_glean_real_canonical_curves.csv"
GLEAN_REAL_MAU_CURVES = DRAFT / "comparison_glean_real_mau_additive_2026-10-13/csv/comparison_glean_real_mau_canonical_curves.csv"
LEGACY_TABLE = DRAFT / "csv/glean2027_vs_legacy_comparison.csv"
SEAM = pd.Timestamp("2026-10-10")
DEC26, DEC27 = pd.Timestamp("2026-12-15"), pd.Timestamp("2027-12-15")
OS_ORDER = ["world", "windows", "mac", "linux"]
METRIC_COLS = {"mau": "MAU (raw daily)", "dau_28ma": "DAU (28-day MA)"}
HORIZONS = [("1 month", "2026-11-10"), ("Dec 15 2026", "2026-12-15"), ("3 months", "2027-01-10"), ("6 months", "2027-04-10"),
            ("9 months", "2027-07-10"), ("Dec 15 2027 (headline)", "2027-12-15")]
ADOPTED = "cps0.005_ncp3_cpr0.8_additive"


def img(path: Path) -> str:
    data = base64.b64encode(path.read_bytes()).decode()
    return f'<figure><img src="data:image/png;base64,{data}" alt="{path.name}"><figcaption><code>{path.relative_to(repo_root)}</code></figcaption></figure>'


def fmt(v) -> str:
    return f"{v:,.0f}" if pd.notna(v) else "—"


def load(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates=["date"]).set_index("date").sort_index()


def legacy_table_html(table: pd.DataFrame) -> str:
    head = "".join(f"<th{' class=num' if i else ''}>{c}</th>" for i, c in enumerate(table.columns))
    rows = []
    for _, r in table.iterrows():
        cells = [f"<td>{r.iloc[0]}</td>"] + [f"<td class=num>{(fmt(v) if isinstance(v, (int, float)) and abs(v) > 100 else v)}</td>" for v in r.iloc[1:]]
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><tr>{head}</tr>{''.join(rows)}</table>"


def real_vs_locked(locked: pd.DataFrame, real: pd.DataFrame, metrics: dict[str, str] | None = None) -> pd.DataFrame:
    """The real-Glean build against the locked one at fixed horizons, with the build's own last actual as a second reference."""
    rows = []
    for metric, label in (metrics or METRIC_COLS).items():
        for os_name in OS_ORDER:
            col = f"{metric}_{os_name}"
            last_actual = real.loc[real["data_type"] == "training", col].dropna().iloc[-1]
            for name, day in HORIZONS:
                d = pd.Timestamp(day)
                rows.append({"metric": label, "os": os_name, "horizon": name, "date": day, "real_glean_only": real.loc[d, col],
                             "locked": locked.loc[d, col], "ratio_real_to_locked": real.loc[d, col] / locked.loc[d, col],
                             "real_glean_last_actual_2026-10-09": last_actual, "ratio_to_own_last_actual": real.loc[d, col] / last_actual})
    return pd.DataFrame(rows)


def in_real_units(locked: pd.DataFrame, real: pd.DataFrame) -> pd.DataFrame:
    """Locked forecast × the mean real ÷ Glean-equivalent ratio over the last 30 history days (a restatement, not a forecast)."""
    rows = []
    overlap = real[real["data_type"] == "training"].index
    for metric, label in METRIC_COLS.items():
        for os_name in OS_ORDER:
            col = f"{metric}_{os_name}"
            ratio = (real.loc[overlap, col] / locked.loc[overlap, col]).dropna()
            last30, first30 = ratio.iloc[-30:].mean(), ratio.iloc[:30].mean()
            rows.append({"metric": label, "os": os_name, "ratio_first30d_2025": first30, "ratio_last30d_2026": last30,
                         "ratio_min": ratio.min(), "ratio_max": ratio.max(),
                         "locked_2027-12-15": locked.loc[DEC27, col], "restated_2027-12-15": locked.loc[DEC27, col] * last30,
                         "locked_2026-12-15": locked.loc[DEC26, col], "restated_2026-12-15": locked.loc[DEC26, col] * last30})
    return pd.DataFrame(rows)


def probe_rows(scores: pd.DataFrame, highlight: str) -> str:
    rows = []
    for _, r in scores.sort_values("world_2027-12-15").iterrows():
        style = ' style="background:#eef7ee;font-weight:600"' if r.probe == highlight else ""
        regime = "additive" if "additive" in r.probe else "multiplicative"
        rows.append(f"<tr{style}><td>{r.probe}</td><td>{regime}</td><td class=num>{r.cps:g} / {int(r.ncp)} / {r.cpr:g}</td>"
                    f"<td class=num>{fmt(r['world_2027-12-15'])}</td><td class=num>{r.world_dec_to_dec:+.1%}</td><td class=num>{r.world_oct27_vs_oct26_actual:+.1%}</td>"
                    f"<td class=num>{fmt(r['windows_2027-12-15'])}</td><td class=num>{fmt(r['linux_2027-12-15'])}</td><td>{'yes' if r.any_negative else ''}</td></tr>")
    return "".join(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=DRAFT / "COMPARISONS.html")
    args = p.parse_args()
    locked, legacy = load(LOCKED_CURVES), load(LEGACY_CURVES)
    first_run, real = load(GLEAN_REAL_FIRST_RUN), load(GLEAN_REAL_MAU_CURVES)
    legacy_table = pd.read_csv(LEGACY_TABLE)
    rvl = real_vs_locked(locked, real, {"mau": METRIC_COLS["mau"]})
    rvl_first = real_vs_locked(locked, first_run, {"mau": METRIC_COLS["mau"]})
    units = in_real_units(locked, first_run)     # the overlap actuals are the same real series in both runs
    rvl.to_csv(DRAFT / "csv/glean2027_vs_glean_real_comparison.csv", index=False)
    units.to_csv(DRAFT / "csv/glean2027_in_real_glean_units.csv", index=False)
    scores = {n: pd.read_csv(PROBES / f"scores_{n}.csv") if (PROBES / f"scores_{n}.csv").exists() else None for n in ("round1", "round2", "round3")}
    if scores["round1"] is None:
        all_scores = pd.read_csv(PROBES / "scores.csv")
        scores["round1"] = all_scores[~all_scores.probe.isin(set(scores["round2"].probe) | set(scores["round3"].probe))]

    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"], text=True).strip()
    data_end = locked[locked["data_type"] == "training"].index.max().date()
    real_start = real[real["data_type"] == "training"].index.min().date()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    real_last = real.loc[real["data_type"] == "training", "mau_world"].dropna().iloc[-1]
    adopted_27, adopted_26 = real.loc[DEC27, "mau_world"], real.loc[DEC26, "mau_world"]
    restated_27 = units[(units.os == "world") & (units.metric == METRIC_COLS["mau"])]["restated_2027-12-15"].item()
    first_1m = rvl_first[(rvl_first.os == "world") & (rvl_first.horizon == "1 month")].iloc[0]

    # Real-Glean Dec-15 table (adopted build vs locked, MAU per OS)
    dec_rows = []
    for os_name in OS_ORDER:
        col = f"mau_{os_name}"
        last = real.loc[real["data_type"] == "training", col].dropna().iloc[-1]
        r26, r27, l26, l27 = real.loc[DEC26, col], real.loc[DEC27, col], locked.loc[DEC26, col], locked.loc[DEC27, col]
        yoy = real.loc[real["data_type"] == "training", col].loc["2026-09-10":"2026-10-09"].mean() / real.loc[real["data_type"] == "training", col].loc["2025-09-10":"2025-10-09"].mean() - 1
        dec_rows.append(f"<tr><td>{os_name}</td><td class=num>{fmt(last)}</td><td class=num>{yoy:+.1%}</td>"
                        f"<td class=num>{fmt(r26)}</td><td class=num><b>{fmt(r27)}</b></td><td class=num>{r27 / r26 - 1:+.1%}</td>"
                        f"<td class=num>{fmt(l26)}</td><td class=num>{fmt(l27)}</td><td class=num>{l27 / l26 - 1:+.1%}</td></tr>")
    hz_rows = []
    for (_, a), (_, f) in zip(rvl[rvl.os == "world"].iterrows(), rvl_first[rvl_first.os == "world"].iterrows()):
        hz_rows.append(f"<tr><td>{a.horizon} ({a.date})</td><td class=num>{fmt(a.locked)}</td><td class=num>{fmt(a.real_glean_only)}</td>"
                       f"<td class=num>{a.ratio_real_to_locked - 1:+.1%}</td><td class=num>{a.ratio_to_own_last_actual - 1:+.1%}</td>"
                       f"<td class=num>{fmt(f.real_glean_only)}</td><td class=num>{f.ratio_to_own_last_actual - 1:+.1%}</td></tr>")
    unit_rows = []
    for _, r in units.iterrows():
        unit_rows.append(f"<tr><td>{r.metric if r.os == 'world' else ''}</td><td>{r.os}</td><td class=num>{r['ratio_first30d_2025']:.3f}</td><td class=num>{r['ratio_last30d_2026']:.3f}</td>"
                         f"<td class=num>{r.ratio_min:.3f} – {r.ratio_max:.3f}</td><td class=num>{fmt(r['locked_2027-12-15'])}</td><td class=num><b>{fmt(r['restated_2027-12-15'])}</b></td></tr>")
    probe_head = ("<tr><th>probe</th><th>regime</th><th class=num>cps / changepoints / range</th><th class=num>world 2027-12-15</th><th class=num>Dec→Dec</th>"
                  "<th class=num>Oct 27 vs Oct 26 actual</th><th class=num>Windows 2027-12-15</th><th class=num>Linux 2027-12-15</th><th>negative values</th></tr>")

    w = legacy_table[legacy_table.series == "mau_world"].iloc[0]
    html = f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<title>2027 Glean-history forecast — the two comparisons (Legacy; real Glean with no simulated history) — PRE-DRAFT</title>
<style>
body{{font:15px/1.5 -apple-system,"Segoe UI",Helvetica,Arial,sans-serif;color:#1c1c1c;max-width:1100px;margin:0 auto;padding:0 24px 64px;position:relative}}
.banner{{background:#222;color:#eee;padding:10px 16px;margin:0 -24px 24px;font-size:13px;display:flex;gap:28px;flex-wrap:wrap}} .banner b{{color:#fff}} .banner code{{background:#444;color:#fff}}
h1{{font-size:26px;margin:8px 0 4px}} h2{{font-size:20px;margin:36px 0 8px;border-bottom:1px solid #d9d9d9;padding-bottom:4px}} h3{{font-size:16px;margin:22px 0 6px}}
table{{border-collapse:collapse;width:100%;margin:10px 0 18px;font-size:14px}} th,td{{border:1px solid #d9d9d9;padding:6px 9px;vertical-align:top;text-align:left}} th{{background:#f3f3f3}} td.num,th.num{{text-align:right;font-variant-numeric:tabular-nums}}
code{{font:13px Menlo,Consolas,monospace;background:#f4f4f4;padding:1px 4px;border-radius:3px}}
.callout{{border-left:4px solid;padding:8px 14px;margin:14px 0;border-radius:0 4px 4px 0}} .warn{{background:#fff6e0;border-color:#e0a800}} .ok{{background:#eef7ee;border-color:#3a8a3a}} .bad{{background:#fdecec;border-color:#c62828}} .ask{{background:#eee6f5;border-color:#6a1b9a}}
figure{{margin:16px 0}} figure img{{max-width:100%;border:1px solid #d9d9d9}} figcaption{{font-size:13px;color:#5a5a5a;margin-top:6px}}
.wm{{position:fixed;top:40%;left:4%;font-size:150px;color:rgba(106,27,154,.10);transform:rotate(-20deg);pointer-events:none;z-index:0}}
</style></head><body>
<div class="wm">PRE-DRAFT</div>
<div class="banner"><span><b>Generated:</b> {now}</span><span><b>Source:</b> <code>scripts/render_glean_history_comparisons.py</code> (plots: <code>plot_glean_history_comparisons.py</code>, <code>run_glean_real_mau_probes.py score</code>, <code>plot_glean_real_linux_components.py</code>), mozaic-daily <code>{commit}</code>, branch glean-2027-forecast</span><span><b>Data as of:</b> {data_end} (all inputs)</span><span><b>Status:</b> PRE-DRAFT — the real-Glean build with the Linux bot overlay is the HEADLINE since 2026-10-14 (<code>LOCK.md</code>, <code>REPORT.html</code>); build 5 on Glean-equivalent history is the validation build</span></div>

<h1>PRE-DRAFT — The two comparisons for the 2027 Glean-history forecast</h1>
<p>The locked 2027 forecast (world MAU 2027-12-15 <b>{fmt(locked.loc[DEC27, 'mau_world'])}</b>) is trained on a <em>simulated</em> Glean history: Legacy telemetry restated
the way Glean counts. Two comparisons test that choice, both at the identical seam {SEAM.date()}:</p>
<ol>
<li><b>Legacy telemetry</b> (2020-01-01 → {data_end}), the identical locked configuration. If the model is doing the same thing on both, the two
forecasts should differ by the translation factor and nothing else.</li>
<li><b>Real Glean telemetry, no simulated history</b> ({real_start} → {data_end}, 17 months), the data we actually have. The locked configuration is unusable on it
(§2); the trend and seasonality parameters were re-tuned for a short history (§3) and the result adopted temporarily (§4). MAU only.</li>
</ol>

<h2>1. Legacy MAU and DAU — same model, Legacy telemetry</h2>
<div class="callout ok"><b>Result: the two forecasts are the same forecast in different units.</b> Glean-equivalent ÷ Legacy world MAU is {w.iloc[1]:.3f} in the last
weeks of history and {w.iloc[3]:.3f} at 2027-12-15; Dec → Dec is {w.iloc[6]} on Legacy and {w.iloc[7]} on Glean-equivalent. The same holds per OS (Linux's larger factor,
{legacy_table[legacy_table.series == 'mau_linux'].iloc[0, 1]:.3f}, carries through unchanged). The simulated history changes <em>what the numbers mean</em>, not what the model does.</div>
{img(PLOTS / 'legacy_vs_locked_mau_by_os.png')}
{img(PLOTS / 'legacy_vs_locked_dau_28ma_by_os.png')}
<p>The ratio of the two, through the horizon. Solid is history (Glean-equivalent ÷ Legacy actuals), dashed is forecast ÷ forecast. A flat dashed line continuing
the solid one is what "translation-invariant" looks like; Mac's small drift (0.987 → 0.999 in MAU) is the one place the two forecasts diverge, and it is 1.2% on an
11.7M series.</p>
{img(PLOTS / 'legacy_ratio_by_os.png')}
<h3>Table: Dec 15 values and ratios (<code>csv/glean2027_vs_legacy_comparison.csv</code>)</h3>
{legacy_table_html(legacy_table)}
<p>Legacy inputs: <code>scripts/sql/glean_history_comparison_legacy_desktop_os_country.sql</code> → <code>raw_legacy_osxcountry_2026-10-09/</code> (Legacy reports
macOS as <code>Darwin</code>); its own Iran fill in Legacy units (<code>iran_fill_legacy/</code>); build <code>comparison_legacy_cpr087_2026-10-10/</code>. Both
histories carry the 2025 Legacy outage crater (shaded), because the simulated history is derived from Legacy; both runs mask it with the same calendar.</p>

<h2>2. Real Glean, first run: the locked configuration on 17 months (for the record)</h2>
<div class="callout bad"><b>Unusable, as expected.</b> World MAU from {fmt(first_run.loc[first_run['data_type'] == 'training', 'mau_world'].dropna().iloc[-1])} on {data_end} to
{fmt(first_run.loc[DEC27, 'mau_world'])} on 2027-12-15; Windows DAU and Linux MAU negative. Already +{first_1m.ratio_to_own_last_actual - 1:.0%} over its own last actual after one month, so no
horizon is short enough to make it informative. Build <code>comparison_glean_real_cpr087_2026-10-10/</code>; it is the dotted red line in §4's chart.</div>
<p>Three reasons, in order of size. <b>(1) One autumn, seen once:</b> with 17 months Prophet cannot separate yearly seasonality from trend, so the Aug–Oct 2026 ramp
is read as trend; the round-1 backtest of the locked configuration said the same for every seam with under four years of history. <b>(2) The run masked three months
it should not have:</b> the <code>LegacyOutage2025</code> calendar (Windows 2025-08-16 → 11-06, Mac 08-28 → 12-10) was applied unconditionally; real Glean has no outage there.
Fixed with the runner's <code>--no-outage-calendar</code> flag; on its own, removing the mask makes the locked configuration <em>worse</em> (345M), so the mask was not
the cause. <b>(3) The Iran fill is in Glean-equivalent units</b> (DAU ratio within 1%; small; a clean run would regenerate it from the real frames).</p>

<h2>3. Re-tuning for a short history: three probe rounds (2026-10-13, MAU only)</h2>
<p>Brendan: the locked parameters are tuned for five-plus years of data; adjust the changepoint and trend parameters freely until the real-Glean forecast is
reasonable — lower than the simulated-history version, slope roughly the data's year-over-year trend (world MAU −3.8% in the last month, Windows −8.7%, Mac +3.6%,
Linux +51%). All probes: outage mask off, Iran fill and China correction as before, seam {SEAM.date()}, MAU only. Driver
<code>scripts/run_glean_real_mau_probes.py</code>; every probe's parquet, sidecar and pickle under <code>probes_glean_real_mau/</code>.</p>
<h3>Round 1 — looser-to-tighter changepoints, multiplicative regime</h3>
<table>{probe_head}{probe_rows(scores['round1'], '')}</table>
<p>Tightening the changepoints tames the world total, but only because <b>Windows and Linux errors cancel</b>: Linux is sent to 20–44M (actual 17.7M; the bot-like
surge extrapolated) and Windows down to 111–147M. Loosening the seasonality prior sends Linux negative.</p>
{img(PROBES / 'probes_by_os_mau_round1.png')}
<h3>Round 2 — near-linear trend, plus one additive-regime run</h3>
<table>{probe_head}{probe_rows(scores['round2'], ADOPTED)}</table>
<p>Near-linear trends give each OS its mean slope, but under the <b>multiplicative</b> regime the slope is applied in log space, so Linux's +50%/yr compounds
(30–98M by Dec 2027) and drags the world up. <b>The one additive-regime run is the first result reasonable on every OS.</b></p>
{img(PROBES / 'probes_by_os_mau_round2.png')}
<h3>Round 3 — is the additive result a stable region?</h3>
<table>{probe_head}{probe_rows(scores['round3'], ADOPTED)}</table>
<p>Yes: the five near-linear additive probes land within 155.7–158.9M world and 21.9–22.5M Linux. The regime alone is not enough (the locked knobs under additive
give 188.7M). The centre, <b>additive, changepoint prior scale 0.005, 3 changepoints, range 0.8</b>, was adopted temporarily.</p>
{img(PROBES / 'probes_by_os_mau_round3.png')}
<div class="callout warn"><b>One thing no parameter fixes.</b> Real Glean MAU dropped 13M between mid-December 2025 and mid-January 2026; about 8M of that was the
duplicate-id fix arriving with Firefox 148 (real ÷ Glean-equivalent fell from 1.068 to 1.024 over those weeks), a one-off step. With one observed year the model can
only read it as seasonal, so every probe repeats the drop in January 2027.</div>

<h2>4. The real-Glean MAU comparison as adopted (temporarily)</h2>
<div class="callout ok"><b>World MAU 2027-12-15 = {fmt(adopted_27)}</b> (2026-12-15 {fmt(adopted_26)}, {adopted_27 / adopted_26 - 1:+.1%}). Against the two criteria: the
slope matches the data's YoY on every OS. On "lower than the simulated-history version": it is {adopted_27 / locked.loc[DEC27, 'mau_world'] - 1:+.1%} against the locked
{fmt(locked.loc[DEC27, 'mau_world'])} in nominal terms, but the two are in different units — real Glean counts about 4% more MAU than Glean-equivalent today (§5), so the locked
forecast restated in real-Glean units is {fmt(restated_27)}, and the real-only build sits {adopted_27 / restated_27 - 1:+.1%} below that.
Build <code>comparison_glean_real_mau_additive_2026-10-13/</code>.</div>
{img(PLOTS / 'glean_real_vs_locked_mau_by_os.png')}
<h3>Table: MAU per OS, adopted real-Glean build vs the locked build</h3>
<table><tr><th>OS</th><th class=num>real actual {data_end}</th><th class=num>data YoY (Sep 10–Oct 9)</th><th class=num>real-only 2026-12-15</th><th class=num>real-only 2027-12-15</th><th class=num>Dec→Dec</th>
<th class=num>locked 2026-12-15</th><th class=num>locked 2027-12-15</th><th class=num>Dec→Dec</th></tr>{''.join(dec_rows)}</table>
<h3>Table: world MAU by horizon, adopted build and first run (<code>csv/glean2027_vs_glean_real_comparison.csv</code>)</h3>
<table><tr><th>horizon</th><th class=num>locked</th><th class=num>adopted real-only</th><th class=num>vs locked</th><th class=num>vs own last actual</th><th class=num>first run</th><th class=num>first run vs own last actual</th></tr>
{''.join(hz_rows)}</table>
<div class="callout ok"><b>Round 5 (2026-10-14): the Linux bot-like surge separated and the build promoted.</b> The component was measured from real Glean client data (hosting-ISP
clients, §5), subtracted from Linux training rows and added back held flat at 4.2M (code <code>b</code>). World MAU 2027-12-15 <b>161,546,764</b>; minus the add-back 157.3M, equal to the
locked build's 157.2M. Brendan promoted this build to the headline forecast; <code>REPORT.html</code> and the handoff package describe it. Plot below; build
<code>comparison_glean_real_mau_additive_b_2026-10-14/</code>.</div>
{img(PLOTS / 'glean_real_round5_mau_by_os.png') if (PLOTS / 'glean_real_round5_mau_by_os.png').exists() else ''}

<h2>5. What the real data does tell us: the units, and the Linux short component</h2>
<p>Over the 17-month overlap, real Glean ÷ Glean-equivalent (<code>csv/glean2027_in_real_glean_units.csv</code>) is the part of real Glean the <code>glean_equivalent</code>
column deliberately excludes: the pre-Fx-148 duplicate-id MAU inflation (world MAU ratio 1.077 in mid-2025, falling to ~1.02 by spring 2026 as Fx 148+ rolled out) and
the Linux Glean-only temporary term (Linux MAU ratio 1.40 at the seam, still rising). DAU is within 1% throughout. These are the two combinatorics candidates
registered in <code>adjustments/</code> (<code>d</code>, <code>x</code>), measured.</p>
{img(PLOTS / 'glean_real_ratio_by_os.png')}
<table><tr><th>metric</th><th>OS</th><th class=num>ratio, first 30 d (2025)</th><th class=num>ratio, last 30 d (2026)</th><th class=num>range</th><th class=num>locked 2027-12-15</th><th class=num>× last-30-d ratio</th></tr>
{''.join(unit_rows)}</table>
<p>The last column is the locked forecast <em>restated</em> in real-Glean units at today's ratio — arithmetic, not a forecast, and an upper bound for Windows (the
duplicate-id term is still shrinking) and a guess for Linux (the temporary term has no settled level).</p>
<h3>Linux: where the surge is</h3>
<p>Real minus Glean-equivalent Linux, per country, is a daily series of the bot-like component the export project excludes. In the last 28 days it is +4.2M MAU on a
14M base (+31%) but only +4% of DAU: these are mostly once-a-month clients with fresh ids. ROW, DE and US carry 83% of Linux's growth since mid-2025 (DE 1.2M → 3.2M in
September–October alone; US in two episodes, July 2025 and March–October 2026); the other 13 countries sit within a few percent of Glean-equivalent. Caveat: the export
only extends windows it already recorded, so the newest ROW surge is partly inside Glean-equivalent too — the separation is good but not complete at the edge, which is
also why the locked build's Linux climbs to 16.6M.</p>
{img(DRAFT / 'plots/linux_real_vs_glean_equivalent.png')}
<div class="callout ok"><b>Measured from real Glean alone (2026-10-14), not from this ratio.</b> Brendan asked that the measurement not use the Glean-equivalent series (circular).
From client-level data (<code>firefox_desktop.baseline_active_users</code>, 10% sample): the behavioural definition "first seen inside the 28-day window, ≤ 2 active days" has a ~37%
baseline on Linux but is near-binary per ISP — hosting / cloud providers 0.95–1.00, residential ~0.3 — so "ISP share ≥ 0.8" is the definition. Linux hosting-ISP MAU is 5.4M in
October (27% of Linux), 0.4–0.7M through 2025; Windows 0.2%, Mac ~0. It agrees with the export's independent term (+4.2M on the same day). Tables under <code>linux_bot/</code>,
plots <code>plots/linux_bot_segment_shares.png</code>, <code>linux_bot/plots/linux_bot_curve.png</code>. The duplicate-id step cannot be isolated the same way: <code>profile_group_id</code>
is null for 12–58% of pre-148 clients and legitimate multi-profile installs give 148+ clients a 1.2% "duplicate" rate (<code>scripts/sql/glean_real_dupid_snapshot.sql</code>).</div>

<h2>6. Provenance</h2>
<ul>
<li>Locked build: <code>desktop_glean_history_cpr087_o_2026-10-10/</code>, curves <code>csv/glean2027_canonical_curves.csv</code> (<code>LOCK.md</code>).</li>
<li>Legacy: <code>comparison_legacy_cpr087_2026-10-10/</code> on <code>raw_legacy_osxcountry_2026-10-09/</code>, Iran fill <code>iran_fill_legacy/</code>, same China correction.</li>
<li>Real Glean, first run: <code>comparison_glean_real_cpr087_2026-10-10/</code> on <code>raw_glean_real_osxcountry_2026-10-09/</code> (<code>glean_telemetry.active_users_aggregates</code>,
Firefox Desktop, from {real_start}), locked knobs, the locked build's Iran fill, the <code>LegacyOutage2025</code> calendar (§2), same China correction.</li>
<li>Real Glean, adopted: <code>comparison_glean_real_mau_additive_2026-10-13/</code> (README there), the probe <code>probes_glean_real_mau/{ADOPTED}/</code>, MAU only, mask off.</li>
<li>Both comparison pulls: <code>scripts/sql/glean_history_comparison_*.sql</code> through <code>bq_query.py</code>, framed by <code>scripts/build_comparison_raw_frames.py</code>, run by
<code>scripts/run_glean_history_forecast.py</code>, exported by <code>scripts/export_glean_history_curves.py</code>. Full record: <code>PROCEDURE.html</code> §11b, §12, §13.</li>
</ul>
</body></html>
"""
    args.out.write_text(html)
    print(f"wrote {args.out} and csv/glean2027_vs_glean_real_comparison.csv, csv/glean2027_in_real_glean_units.csv")


if __name__ == "__main__":
    main()
