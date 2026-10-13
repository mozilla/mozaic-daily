#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""The two formal comparisons of the 2027 Glean-history forecast, as one self-contained PRE-DRAFT page.

(1) Legacy MAU / DAU: the same locked configuration run on Legacy telemetry, against the locked forecast.
(2) Real Glean MAU / DAU with NO simulated history: the same configuration on the 17 months of real Glean desktop
    telemetry that exist (2025-05-22 on). Expected poor, required because it is the data we actually have; shown
    honestly, with the options for making it informative laid out for a decision (nothing is run here).

Pure rendering plus two derived tables a reader can reproduce from the cited curves CSVs, which it also writes:
``csv/glean2027_vs_glean_real_comparison.csv`` (the real-Glean-only build against the locked one by horizon) and
``csv/glean2027_in_real_glean_units.csv`` (the locked forecast × the last-30-day real ÷ Glean-equivalent ratio).
Plots come from ``plot_glean_history_comparisons.py`` (run it first). The raw model is never shown.

Usage:
    python scripts/plot_glean_history_comparisons.py
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
LOCKED_CURVES = DRAFT / "csv/glean2027_canonical_curves.csv"
LEGACY_CURVES = DRAFT / "comparison_legacy_cpr087_2026-10-10/csv/comparison_legacy_canonical_curves.csv"
GLEAN_REAL_CURVES = DRAFT / "comparison_glean_real_cpr087_2026-10-10/csv/comparison_glean_real_canonical_curves.csv"
LEGACY_TABLE = DRAFT / "csv/glean2027_vs_legacy_comparison.csv"
SEAM = pd.Timestamp("2026-10-10")
DEC26, DEC27 = pd.Timestamp("2026-12-15"), pd.Timestamp("2027-12-15")
OS_ORDER = ["world", "windows", "mac", "linux"]
METRIC_COLS = {"mau": "MAU (raw daily)", "dau_28ma": "DAU (28-day MA)"}
HORIZONS = [("1 month", "2026-11-10"), ("Dec 15 2026", "2026-12-15"), ("3 months", "2027-01-10"), ("6 months", "2027-04-10"),
            ("9 months", "2027-07-10"), ("Dec 15 2027 (headline)", "2027-12-15")]


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


def real_vs_locked(locked: pd.DataFrame, real: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for metric, label in METRIC_COLS.items():
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


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=DRAFT / "COMPARISONS.html")
    args = p.parse_args()
    locked, legacy, real = load(LOCKED_CURVES), load(LEGACY_CURVES), load(GLEAN_REAL_CURVES)
    legacy_table = pd.read_csv(LEGACY_TABLE)
    rvl = real_vs_locked(locked, real)
    units = in_real_units(locked, real)
    rvl.to_csv(DRAFT / "csv/glean2027_vs_glean_real_comparison.csv", index=False)
    units.to_csv(DRAFT / "csv/glean2027_in_real_glean_units.csv", index=False)

    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"], text=True).strip()
    data_end = locked[locked["data_type"] == "training"].index.max().date()
    real_start = real[real["data_type"] == "training"].index.min().date()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")

    # Real-Glean Dec-15 table (both builds, per OS)
    dec_rows = []
    for metric, label in METRIC_COLS.items():
        for os_name in OS_ORDER:
            col = f"{metric}_{os_name}"
            last = real.loc[real["data_type"] == "training", col].dropna().iloc[-1]
            r26, r27, l26, l27 = real.loc[DEC26, col], real.loc[DEC27, col], locked.loc[DEC26, col], locked.loc[DEC27, col]
            dec_rows.append(f"<tr><td>{label if os_name == 'world' else ''}</td><td>{os_name}</td><td class=num>{fmt(last)}</td>"
                            f"<td class=num>{fmt(r26)}</td><td class=num>{fmt(r27)}</td><td class=num>{r27 / r26 - 1:+.0%}</td>"
                            f"<td class=num>{fmt(l26)}</td><td class=num>{fmt(l27)}</td><td class=num>{l27 / l26 - 1:+.1%}</td></tr>")
    # Horizon table, world only
    hz_rows = []
    for metric, label in METRIC_COLS.items():
        sub = rvl[(rvl.metric == label) & (rvl.os == "world")]
        for _, r in sub.iterrows():
            hz_rows.append(f"<tr><td>{label if r.horizon == HORIZONS[0][0] else ''}</td><td>{r.horizon} ({r.date})</td><td class=num>{fmt(r.real_glean_only)}</td>"
                           f"<td class=num>{fmt(r.locked)}</td><td class=num>{r.ratio_real_to_locked - 1:+.1%}</td><td class=num>{r.ratio_to_own_last_actual - 1:+.1%}</td></tr>")
    # Restatement table
    unit_rows = []
    for _, r in units.iterrows():
        unit_rows.append(f"<tr><td>{r.metric if r.os == 'world' else ''}</td><td>{r.os}</td><td class=num>{r['ratio_first30d_2025']:.3f}</td><td class=num>{r['ratio_last30d_2026']:.3f}</td>"
                         f"<td class=num>{r.ratio_min:.3f} – {r.ratio_max:.3f}</td><td class=num>{fmt(r['locked_2027-12-15'])}</td><td class=num><b>{fmt(r['restated_2027-12-15'])}</b></td></tr>")

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
<div class="banner"><span><b>Generated:</b> {now}</span><span><b>Source:</b> <code>scripts/render_glean_history_comparisons.py</code> (plots: <code>scripts/plot_glean_history_comparisons.py</code>), mozaic-daily <code>{commit}</code>, branch glean-2027-forecast</span><span><b>Data as of:</b> {data_end} (all three inputs)</span><span><b>Status:</b> PRE-DRAFT — comparisons against the LOCKED build 5 (<code>LOCK.md</code>); nothing here changes the locked forecast</span></div>

<h1>PRE-DRAFT — The two comparisons for the 2027 Glean-history forecast</h1>
<p>The locked 2027 forecast (world MAU 2027-12-15 <b>{fmt(locked.loc[DEC27, 'mau_world'])}</b>) is trained on a <em>simulated</em> Glean history: Legacy telemetry restated
the way Glean counts. Two comparisons test that choice, both with the identical locked configuration (g01 lock, changepoint range 0.87, Iran fill, 2025 outage calendar,
China correction) and the identical seam {SEAM.date()}:</p>
<ol>
<li><b>Legacy telemetry</b> (2020-01-01 → {data_end}) — the data the simulated history was made from. If the model is doing the same thing on both, the two
forecasts should differ by the translation factor and nothing else.</li>
<li><b>Real Glean telemetry, no simulated history</b> ({real_start} → {data_end}, 17 months) — the data we actually have. Expected to be poor, required anyway.</li>
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

<h2>2. Real Glean MAU and DAU — same model, real Glean only, no simulated history</h2>
<div class="callout bad"><b>Result: unusable, as expected.</b> With 17 months of history the model's "forecast" takes world MAU from {fmt(real.loc[real['data_type'] == 'training', 'mau_world'].dropna().iloc[-1])}
on {data_end} to <b>{fmt(real.loc[DEC27, 'mau_world'])}</b> on 2027-12-15 (+{real.loc[DEC27, 'mau_world'] / locked.loc[DEC27, 'mau_world'] - 1:.0%} over the locked forecast); Windows DAU goes
negative; Linux MAU goes negative. These are not forecasts of anything. The page shows them because they are what the real data, treated the same way, produces.</div>
{img(PLOTS / 'glean_real_vs_locked_mau_by_os.png')}
{img(PLOTS / 'glean_real_vs_locked_dau_28ma_by_os.png')}
<h3>Table: Dec 15 values, real-Glean-only build vs the locked build</h3>
<table><tr><th>metric</th><th>OS</th><th class=num>real Glean actual {data_end}</th><th class=num>real-only 2026-12-15</th><th class=num>real-only 2027-12-15</th><th class=num>Dec→Dec</th>
<th class=num>locked 2026-12-15</th><th class=num>locked 2027-12-15</th><th class=num>Dec→Dec</th></tr>
{''.join(dec_rows)}</table>

<h3>Why it fails (three reasons, in order of size)</h3>
<ol>
<li><b>One autumn, seen once.</b> Prophet needs at least two passes through a season to separate yearly seasonality from trend. The only autumn ramp this run saw
(Aug–Oct 2026, +12% in world MAU) is read as trend and extrapolated for 15 months. The backtest of the locked configuration said the same thing: seams with
under four years of history missed by 30–90% at 14 months (<code>backtest_round1/</code>), which is why the stated error band uses only seams with ≥ 4 years.</li>
<li><b>The run masked three months it should not have.</b> The <code>LegacyOutage2025</code> holiday calendar (Windows 2025-08-16 → 11-06, Mac 08-28 → 12-10, applied
as one union window) is wired into the runner unconditionally because every Glean-history input inherits the Legacy outage. Real Glean has no outage in that window
(the black line in the shaded region is clean), so the mask threw away the <em>first</em> autumn too, leaving the model with no 2025 autumn at all. This is a
defect of the run as executed, separate from the short-history problem; fixing it needs a runner flag and a one-minute rerun.</li>
<li><b>The Iran fill is in Glean-equivalent units</b> (the locked build's fill was passed through). Real Glean ÷ Glean-equivalent is within ±1% on DAU, so this is
small, but a clean run would regenerate the fill from the real-Glean frames.</li>
</ol>

<h3>Does a shorter horizon help? No — it is already wrong at one month</h3>
<p>The real-Glean-only build against the locked one, world, by horizon (<code>csv/glean2027_vs_glean_real_comparison.csv</code>). The last column is the build against its
own last actual, so no translation question enters: world MAU is +{rvl[(rvl.metric == 'MAU (raw daily)') & (rvl.os == 'world') & (rvl.horizon == '1 month')].ratio_to_own_last_actual.item() - 1:.0%}
above its own October level after one month, in a season where the locked build and every prior year flatten. The trend misread happens at the seam, not at the horizon,
so cutting the horizon only hides it.</p>
<table><tr><th>metric</th><th>horizon</th><th class=num>real-Glean-only</th><th class=num>locked</th><th class=num>real ÷ locked − 1</th><th class=num>real ÷ own last actual − 1</th></tr>
{''.join(hz_rows)}</table>

<h3>What the real data does tell us: the units</h3>
<p>Over the 17-month overlap, real Glean ÷ Glean-equivalent (<code>csv/glean2027_in_real_glean_units.csv</code>, plot below) is the part of real Glean the
<code>glean_equivalent</code> column deliberately excludes: the pre-Fx-148 duplicate-id MAU inflation (world MAU ratio 1.077 in mid-2025, falling to ~1.02 by spring 2026
as Fx 148+ rolled out) and the Linux Glean-only temporary term (Linux MAU ratio 1.40 at the seam, still rising). DAU is within 1% throughout. These are the two
combinatorics candidates registered in <code>adjustments/</code> (<code>d</code>, <code>x</code>), measured.</p>
{img(PLOTS / 'glean_real_ratio_by_os.png')}
<table><tr><th>metric</th><th>OS</th><th class=num>ratio, first 30 d (2025)</th><th class=num>ratio, last 30 d (2026)</th><th class=num>range</th><th class=num>locked 2027-12-15</th><th class=num>× last-30-d ratio</th></tr>
{''.join(unit_rows)}</table>
<p>The last column is the locked forecast <em>restated</em> in real-Glean units at today's ratio — arithmetic, not a forecast, and an upper bound for Windows (the
duplicate-id term is still shrinking) and a guess for Linux (the temporary term has no settled level). It is the honest answer to "what would the real Glean MAU number
read on 2027-12-15": world {fmt(units[(units.os == 'world') & (units.metric == 'MAU (raw daily)')]['restated_2027-12-15'].item())} rather than
{fmt(locked.loc[DEC27, 'mau_world'])}, if nothing about the two excluded terms changes.</p>

<h2>3. Options for the real-Glean comparison — for decision, nothing run</h2>
<div class="callout ask"><b>Recommendation: do (a), present (c) beside it, and record (b) and (d) as not informative.</b> (a) is a model run and waits for the word.</div>
<table><tr><th>option</th><th>what</th><th>needs a model run?</th><th>expected outcome</th></tr>
<tr><td><b>(a) Rerun real Glean without the outage mask, with a real-Glean Iran fill</b></td><td>Add a <code>--no-outage-calendar</code> flag to the runner; regenerate the fill from
<code>raw_glean_real_osxcountry_2026-10-09/</code>; same configuration otherwise.</td><td>Yes (~1 min)</td><td>Still not usable at 14 months — reason 1 stands — but the run is then
a fair statement of what the real data gives, with no treatment designed for the simulated history left in it. Worth doing for the record.</td></tr>
<tr><td><b>(b) Shorter horizon</b></td><td>Report the real-Glean-only build to, say, 2027-01 or 2027-03 only.</td><td>No</td><td>Not informative: the table above shows the build is already
+{rvl[(rvl.metric == 'MAU (raw daily)') & (rvl.os == 'world') & (rvl.horizon == '1 month')].ratio_real_to_locked.item() - 1:.0%} over the locked forecast at one month. The error is a trend misread at the seam.</td></tr>
<tr><td><b>(c) Restate the locked forecast in real-Glean units</b></td><td>Locked forecast × the seam-time real ÷ Glean-equivalent ratio per OS and metric (the table above; already written).</td><td>No</td>
<td>Informative as a reading aid, not as a forecast: it says what the dashboard would show on 2027-12-15 if the duplicate-id and Linux terms stayed where they are. Could be
shown beside the headline with that caveat.</td></tr>
<tr><td><b>(d) Real Glean trend + seasonality borrowed from Legacy</b></td><td>Fit the level/trend on real Glean, take yearly seasonality and holidays from the Legacy (or Glean-equivalent) tiles.</td>
<td>Yes, plus mozaic code</td><td>This is the simulated-history approach by another route: the seasonality would come from the same Legacy data. It would not be an independent check.</td></tr>
</table>

<h2>4. Provenance</h2>
<ul>
<li>Locked build: <code>desktop_glean_history_cpr087_o_2026-10-10/</code>, curves <code>csv/glean2027_canonical_curves.csv</code> (<code>LOCK.md</code>).</li>
<li>Legacy: <code>comparison_legacy_cpr087_2026-10-10/</code> on <code>raw_legacy_osxcountry_2026-10-09/</code>, Iran fill <code>iran_fill_legacy/</code>, same China correction.</li>
<li>Real Glean: <code>comparison_glean_real_cpr087_2026-10-10/</code> on <code>raw_glean_real_osxcountry_2026-10-09/</code> (<code>glean_telemetry.active_users_aggregates</code>,
Firefox Desktop, from {real_start}), the locked build's Iran fill, the <code>LegacyOutage2025</code> calendar (see reason 2), same China correction.</li>
<li>Both comparison pulls: <code>scripts/sql/glean_history_comparison_*.sql</code> through <code>bq_query.py</code>, framed by <code>scripts/build_comparison_raw_frames.py</code>, run by
<code>scripts/run_glean_history_forecast.py</code>, exported by <code>scripts/export_glean_history_curves.py</code>. Full record: <code>PROCEDURE.html</code> §11b and §12.</li>
</ul>
</body></html>
"""
    args.out.write_text(html)
    print(f"wrote {args.out} and csv/glean2027_vs_glean_real_comparison.csv, csv/glean2027_in_real_glean_units.csv")


if __name__ == "__main__":
    main()
