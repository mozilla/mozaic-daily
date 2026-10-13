#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Per-metric handoff package for the 2027 Glean-history desktop forecast: one self-contained page, one zip.

Audience: a coworker who wants to use the forecast and see what it looks like — not how it was built. Current
state only: no build history, no seam or China discussion. One package per metric (``--metric mau`` or ``dau``):

    <out>/glean2027_<metric>_handoff/
        index.html                       the page (plots embedded, PRE-DRAFT stamp, freshness banner)
        glean2027_<metric>_forecast.csv  date, data_type, world + per-OS values (MAU raw daily / DAU 28-day MA)
        glean2027_<metric>_by_country.csv  date, data_type, one column per country (all OS), same statistic
        plots/*.png                      the canonical plots for this metric
        README.txt                       what the files are and how to read them
    <out>/glean2027_<metric>_handoff.zip

Usage:
    python scripts/render_glean_history_handoff.py --metric mau
    python scripts/render_glean_history_handoff.py --metric dau
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
if str(repo_root / "src") not in sys.path:
    sys.path.insert(0, str(repo_root / "src"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402

DRAFT = repo_root / "data-official/2027-glean-draft"
LOCKED_PARQUET_GLOB = "desktop_glean_history_cpr087_o_2026-10-10/cps*/mozaic_daily_forecast.*.adj-o.parquet"
OS_ORDER = ["world", "windows", "mac", "linux"]
METRIC = {
    "mau": dict(name="MAU", long="monthly active users", column="mau", curve_prefix="mau", stat="raw daily value of the rolling 28-day MAU",
                stat_short="daily MAU", plot_prefix="mau", headline=True),
    "dau": dict(name="DAU", long="daily active users", column="dau", curve_prefix="dau_28ma", stat="28-day trailing mean of daily DAU",
                stat_short="DAU, 28-day MA", plot_prefix="dau_28ma", headline=False),
}
HEADLINE_HORIZON = 431


def img(path: Path) -> str:
    data = base64.b64encode(path.read_bytes()).decode()
    return f'<figure><img src="data:image/png;base64,{data}" alt="{path.name}"></figure>'


def fmt(v) -> str:
    return f"{v:,.0f}"


def build_csvs(df: pd.DataFrame, curves: pd.DataFrame, m: dict, out: Path, metric: str) -> tuple[Path, Path]:
    cols = {f"{m['curve_prefix']}_{o}": o for o in OS_ORDER}
    forecast = curves[["data_type", *cols]].rename(columns=cols)
    forecast.index.name = "date"
    p1 = out / f"glean2027_{metric}_forecast.csv"
    forecast.to_csv(p1, float_format="%.0f")

    allos = df[df["segment"] == json.dumps({"os": "ALL"})]
    by_country = allos.pivot_table(index="target_date", columns="country", values=m["column"])
    if metric == "dau":
        by_country = by_country.rolling(28).mean()
    by_country = by_country.rename(columns={"ALL": "world"})
    by_country.insert(0, "data_type", curves["data_type"].reindex(by_country.index))
    by_country.index.name = "date"
    by_country = by_country.loc[pd.Timestamp("2024-01-01"):]
    p2 = out / f"glean2027_{metric}_by_country.csv"
    by_country.to_csv(p2, float_format="%.0f")
    if metric == "dau":
        # the unsmoothed daily series as well, world + per OS
        daily = {}
        for os_name in OS_ORDER:
            seg = json.dumps({"os": "ALL" if os_name == "world" else os_name})
            w = df[(df["country"] == "ALL") & (df["segment"] == seg)].set_index("target_date").sort_index()
            daily[os_name] = w["dau"]
            daily_type = w["data_type"]
        daily = pd.DataFrame(daily)
        daily.insert(0, "data_type", daily_type)
        daily.index.name = "date"
        daily.to_csv(out / "glean2027_dau_daily_unsmoothed.csv", float_format="%.0f")
    return p1, p2


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--metric", choices=["mau", "dau"], required=True)
    p.add_argument("--out-dir", type=Path, default=DRAFT / "handoff")
    args = p.parse_args()
    m = METRIC[args.metric]
    parquet = sorted(DRAFT.glob(LOCKED_PARQUET_GLOB))[0]
    df, meta = load_forecast(str(parquet))
    df["target_date"] = pd.to_datetime(df["target_date"])
    curves = pd.read_csv(DRAFT / "csv/glean2027_canonical_curves.csv", parse_dates=["date"]).set_index("date")
    summary = pd.read_csv(DRAFT / "csv/glean2027_dec15_summary.csv")
    bt = pd.read_csv(DRAFT / "backtest/errors_summary.csv")
    bt_row = bt[(bt.metric == m["name"]) & (bt.horizon_days == HEADLINE_HORIZON)].iloc[0]
    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"], text=True).strip()
    data_end = curves[curves["data_type"] == "training"].index.max().date()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")

    pkg = args.out_dir / f"glean2027_{args.metric}_handoff"
    if pkg.exists():
        shutil.rmtree(pkg)
    (pkg / "plots").mkdir(parents=True)
    csv1, csv2 = build_csvs(df, curves, m, pkg, args.metric)
    plot_names = [f"{m['plot_prefix']}_world_2027.png", f"{m['plot_prefix']}_world_2026.png", f"{m['plot_prefix']}_by_os_2027.png"]
    for name in plot_names + [f"{m['plot_prefix']}_{o}_{y}.png" for o in ("windows", "mac", "linux") for y in (2026, 2027)]:
        shutil.copy(DRAFT / "plots/canonical" / name, pkg / "plots" / name)

    # headline table
    rows = ""
    for os_name in OS_ORDER:
        r26 = summary[(summary.metric == args.metric) & (summary.os == os_name) & (summary.date == "2026-12-15")].iloc[0]
        r27 = summary[(summary.metric == args.metric) & (summary.os == os_name) & (summary.date == "2027-12-15")].iloc[0]
        rows += (f"<tr><td>{'Desktop, all OS' if os_name == 'world' else os_name}</td><td class=num>{fmt(r26.value)}</td><td class=num><b>{fmt(r27.value)}</b></td>"
                 f"<td class=num>{r27.value / r26.value - 1:+.1%}</td></tr>")
    world27 = float(summary[(summary.metric == args.metric) & (summary.os == "world") & (summary.date == "2027-12-15")].value.item())
    band50, band90 = bt_row["abs_err_p50"], bt_row["abs_err_p90"]
    trans = float(summary[(summary.metric == args.metric) & (summary.os == "world") & (summary.date == "2027-12-15")].translation_band_pct.item())

    # by-country table at the two dates
    bc = pd.read_csv(csv2, parse_dates=["date"]).set_index("date")
    country_rows = ""
    for c in [c for c in bc.columns if c not in ("data_type", "world")]:
        v26, v27 = bc.loc["2026-12-15", c], bc.loc["2027-12-15", c]
        country_rows += f"<tr><td>{c}</td><td class=num>{fmt(v26)}</td><td class=num>{fmt(v27)}</td><td class=num>{v27 / v26 - 1:+.1%}</td></tr>"

    title = f"Firefox Desktop {m['name']} forecast through 2027"
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
<div class="banner"><span><b>Generated:</b> {now}</span><span><b>Source:</b> <code>scripts/render_glean_history_handoff.py</code> on build <code>{parquet.parent.parent.name}</code>, mozaic-daily <code>{commit}</code></span><span><b>Data as of:</b> {data_end}</span><span><b>Status:</b> PRE-DRAFT</span></div>

<h1>PRE-DRAFT — {title}</h1>
<p>A forecast of Firefox Desktop <b>{m['long']} ({m['name']})</b> from {data_end} through 2027-12-31, in <b>Glean</b> telemetry terms. The history it is trained on is
Glean-equivalent: Legacy telemetry restated the way Glean counts people, because Glean's own desktop history is too short to forecast from.
{'This is the headline metric for 2027: the number is the <b>raw daily MAU on 2027-12-15</b>.' if m['headline'] else 'DAU is the secondary metric for 2027; the headline is MAU (separate package). DAU is reported as a 28-day trailing mean, the usual convention.'}
No head- or tailwinds are applied: this is the model's view of the history, nothing added.</p>
<div class="callout"><b>Pre-draft.</b> Earlier than a normal draft: the history is a simulated (translated) series, the method is new, and the number will move as the
inputs are re-issued. Use it to see shape and magnitude; do not quote it as a plan number.</div>

<div class="callout"><b>Data in this package (the forecast itself):</b> <code>{csv1.name}</code> — every day from 2020-01-01 to 2027-12-31, history and forecast,
world and per OS; <code>{csv2.name}</code> — the same per country; {'<code>glean2027_dau_daily_unsmoothed.csv</code> — the raw daily DAU behind the 28-day mean; ' if args.metric == 'dau' else ''}<code>plots/</code> — the charts. Details in section 4.</div>

<h2>1. The numbers</h2>
<table><tr><th></th><th class=num>2026-12-15</th><th class=num>2027-12-15</th><th class=num>Dec → Dec</th></tr>{rows}</table>
<p><b>How far off could it be?</b> The same model, run from eight past dates, missed the following year by <b>{band50:.0%} or less half the time and by
{band90:.0%} or less nine times in ten</b> at this horizon. Around {fmt(world27)} that is roughly ±{fmt(world27 * band50)} to ±{fmt(world27 * band90)}.
Those past forecasts ran <b>low</b> more often than high (median {bt_row['median']:+.1%}). Separately, the translated history itself carries an uncertainty
of about ±{trans:.1f}% (how well Legacy can be restated as Glean); it is small next to the forecast error.</p>

<h2>2. What it looks like</h2>
<p>Grey: the previous year on the same calendar, for shape. Black: history. Green: the forecast. Dotted red: December 15.</p>
{img(pkg / "plots" / plot_names[0])}
{img(pkg / "plots" / plot_names[1])}
{img(pkg / "plots" / plot_names[2])}

<h2>3. By country</h2>
<p>{m['stat_short'].capitalize()}, all OS, 15 named markets plus the rest of the world (ROW). Full daily series in <code>{csv2.name}</code>.</p>
<table><tr><th>country</th><th class=num>2026-12-15</th><th class=num>2027-12-15</th><th class=num>Dec → Dec</th></tr>{country_rows}</table>

<h2>4. How to use the files</h2>
<ul>
<li><code>{csv1.name}</code> — one row per day from 2020-01-01 to 2027-12-31: <code>date</code>, <code>data_type</code> (<code>training</code> = history, <code>forecast</code>),
then <code>world</code>, <code>windows</code>, <code>mac</code>, <code>linux</code>. Values are the <b>{m['stat']}</b>, whole people. The world is the sum of the three OS.</li>
<li><code>{csv2.name}</code> — the same statistic per country (all OS) from 2024-01-01, plus <code>world</code>.</li>
<li><code>plots/</code> — the charts above and one per OS per year.</li>
<li>Read the 2027-12-15 row for the headline; read <code>data_type</code> to know where history ends ({data_end}) and the forecast begins.</li>
<li>{'MAU is already a 28-day quantity, so take the daily value as is; do not smooth it again.' if args.metric == 'mau' else 'The DAU columns are 28-day trailing means; <code>glean2027_dau_daily_unsmoothed.csv</code> has the raw daily values (world and per OS) if you need them.'}</li>
</ul>

<h2>5. What is and is not in it</h2>
<ul>
<li>Desktop only (Windows, Mac, Linux; Linux included in the world). Mobile comes from the regular monthly forecast.</li>
<li>Glean terms: the series is what Glean telemetry would have reported, restated from Legacy. Published Glean dashboards may read slightly higher
because they still include some double-counted and automated clients that this series excludes; those terms are tracked separately and not added here.</li>
<li>No adjustments for planned launches, marketing, or known headwinds. The regular monthly forecast carries those; this one does not.</li>
<li>The history and therefore the forecast will be re-issued when the translation is updated (expected late October 2026).</li>
</ul>

<h2>6. Where it came from</h2>
<p>mozaic-daily, branch <code>glean-2027-forecast</code>, commit <code>{commit}</code>; forecast file <code>{parquet.name}</code>. Owner: Brendan Wells. The full record of
how it was built lives with the repository (<code>data-official/2027-glean-draft/</code>); this page is deliberately the short version.</p>
</body></html>"""
    (pkg / "index.html").write_text(html)
    (pkg / "README.txt").write_text(f"""{title} — PRE-DRAFT (generated {now})

Open index.html. It explains the numbers, shows the charts, and describes the two CSVs:
  {csv1.name}     daily series 2020-01-01..2027-12-31: date, data_type, world, windows, mac, linux ({m['stat']})
  {csv2.name}  the same statistic per country (all OS) from 2024-01-01
{'  glean2027_dau_daily_unsmoothed.csv  raw daily DAU, world and per OS, 2020-01-01..2027-12-31' if args.metric == 'dau' else ''}
  plots/                           the charts (PNG)

Headline: world {m['name']} on 2027-12-15 = {fmt(world27)} ({m['stat']}). History ends {data_end}; rows after that are forecast.
Pre-draft: trained on a translated (Glean-equivalent) history; will be re-issued when the translation is updated.
Source: mozaic-daily branch glean-2027-forecast @ {commit}, build {parquet.parent.parent.name}. Owner: Brendan Wells.
""")
    zip_path = shutil.make_archive(str(args.out_dir / f"glean2027_{args.metric}_handoff"), "zip", root_dir=args.out_dir, base_dir=pkg.name)
    print(f"page  {pkg / 'index.html'}\nzip   {zip_path} ({Path(zip_path).stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
