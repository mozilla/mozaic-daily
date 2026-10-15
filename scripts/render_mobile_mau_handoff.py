#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Handoff package for the 2027 mobile MAU forecast, plus the combined desktop + mobile MAU curve file.

Mobile twin of ``render_glean_history_handoff.py`` (same audience, same shape: one self-contained page, two CSVs, the
canonical plots, a README, one zip). Brendan, 2026-10-14: the deliverable is the forecast CURVES from now to the end of
2027, desktop and mobile MAU over the same period.

    <out>/glean2027_mobile_mau_handoff/
        index.html                            the page (plots embedded, PRE-DRAFT stamp, freshness banner)
        glean2027_mobile_mau_forecast.csv     date, data_type, world + per-app daily MAU (raw daily value of the rolling 28-day MAU)
        glean2027_mobile_mau_by_country.csv   the same per country (all apps) from 2024-01-01
        plots/*.png                           the canonical plots
        README.txt
    <out>/glean2027_mobile_mau_handoff.zip
    <out>/glean2027_mau_desktop_and_mobile.csv   one row per day: desktop world MAU (headline build) and mobile world MAU side by side,
                                                 each with its own data_type column. NOT summed: a person on both devices is in both.

Usage:
    python scripts/render_mobile_mau_handoff.py [--build-dir data-official/2027-glean-draft/mobile_mau_2026-10-10]
"""

from __future__ import annotations

import argparse
import base64
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
DESKTOP_HANDOFF_CSV = DRAFT / "handoff/glean2027_mau_handoff/glean2027_mau_forecast.csv"
POPULATIONS = ["world", "fenix_android", "firefox_ios", "focus_android", "focus_ios"]
LABEL = {"world": "Mobile, all apps", "fenix_android": "Firefox Android (Fenix)", "firefox_ios": "Firefox iOS",
         "focus_android": "Focus Android", "focus_ios": "Focus iOS"}
STAT = "raw daily value of the rolling 28-day MAU"


def img(path: Path) -> str:
    return f'<figure><img src="data:image/png;base64,{base64.b64encode(path.read_bytes()).decode()}" alt="{path.name}"></figure>'


def fmt(v) -> str:
    return f"{v:,.0f}"


def build_csvs(df: pd.DataFrame, curves: pd.DataFrame, out: Path) -> tuple[Path, Path]:
    cols = {f"mau_{p}": p for p in POPULATIONS}
    forecast = curves[["data_type", *cols]].rename(columns=cols)
    forecast.index.name = "date"
    p1 = out / "glean2027_mobile_mau_forecast.csv"
    forecast.to_csv(p1, float_format="%.0f")

    allapps = df[df["app_name"] == "ALL MOBILE"]
    by_country = allapps.pivot_table(index="target_date", columns="country", values="mau").rename(columns={"ALL": "world"})
    by_country.insert(0, "data_type", curves["data_type"].reindex(by_country.index))
    by_country.index.name = "date"
    by_country = by_country.loc[pd.Timestamp("2024-01-01"):]
    p2 = out / "glean2027_mobile_mau_by_country.csv"
    by_country.to_csv(p2, float_format="%.0f")
    return p1, p2


def build_combined(mobile_forecast_csv: Path, desktop_forecast_csv: Path, out: Path) -> Path:
    """Desktop (headline build) and mobile world MAU side by side, one row per day over the union of their dates."""
    d = pd.read_csv(desktop_forecast_csv, parse_dates=["date"]).set_index("date")[["data_type", "world"]]
    m = pd.read_csv(mobile_forecast_csv, parse_dates=["date"]).set_index("date")[["data_type", "world"]]
    combined = d.rename(columns={"data_type": "desktop_data_type", "world": "desktop_mau"}).join(
        m.rename(columns={"data_type": "mobile_data_type", "world": "mobile_mau"}), how="outer").sort_index()
    combined.index.name = "date"
    path = out / "glean2027_mau_desktop_and_mobile.csv"
    combined.to_csv(path, float_format="%.0f")
    return path


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--build-dir", type=Path, default=DRAFT / "mobile_mau_2026-10-10")
    p.add_argument("--stem", default="mobile2027")
    p.add_argument("--out-dir", type=Path, default=DRAFT / "handoff")
    p.add_argument("--desktop-forecast-csv", type=Path, default=DESKTOP_HANDOFF_CSV)
    args = p.parse_args()

    parquet = sorted(args.build_dir.glob("cps*/mozaic_daily_forecast.*.gm-M.raw.parquet"))[0]
    df, meta = load_forecast(str(parquet))
    df["target_date"] = pd.to_datetime(df["target_date"])
    curves = pd.read_csv(args.build_dir / "csv" / f"{args.stem}_canonical_curves.csv", parse_dates=["date"]).set_index("date")
    summary = pd.read_csv(args.build_dir / "csv" / f"{args.stem}_dec15_summary.csv")
    plots_dir = args.build_dir / "plots" / "canonical"
    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"], text=True).strip()
    data_end = curves[curves["data_type"] == "training"].index.max().date()
    history_start = curves.index.min().date()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    iran_fill = (meta.get("extra") or {}).get("iran_fill") or meta.get("iran_fill")

    pkg = args.out_dir / "glean2027_mobile_mau_handoff"
    if pkg.exists():
        shutil.rmtree(pkg)
    (pkg / "plots").mkdir(parents=True)
    csv1, csv2 = build_csvs(df, curves, pkg)
    plot_names = ["mau_world_2027.png", "mau_world_2026.png", "mau_by_os_2027.png"]
    for name in plot_names + [f"mau_{pop}_{y}.png" for pop in POPULATIONS[1:] for y in (2026, 2027)]:
        shutil.copy(plots_dir / name, pkg / "plots" / name)

    def val(pop, on):
        return float(summary[(summary.population == pop) & (summary.date == on)].value.item())

    rows = "".join(f"<tr><td>{LABEL[pop]}</td><td class=num>{fmt(val(pop, '2026-12-15'))}</td><td class=num><b>{fmt(val(pop, '2027-12-15'))}</b></td>"
                   f"<td class=num>{val(pop, '2027-12-15') / val(pop, '2026-12-15') - 1:+.1%}</td></tr>" for pop in POPULATIONS)
    world27 = val("world", "2027-12-15")
    world = curves["mau_world"]
    seam = pd.Timestamp(meta["forecast_start_date"])
    seam_step = world[seam:seam + pd.Timedelta(days=6)].mean() / world[seam - pd.Timedelta(days=7):seam - pd.Timedelta(days=1)].mean() - 1
    last_actual = float(world[seam - pd.Timedelta(days=1)])
    seam_callout = (f'<div class="callout" style="border-color:#c62828;background:#fdecea"><b>Seam not closed.</b> The forecast opens {seam_step:+.1%} '
                    f'(7-day means) below the last observed value of {fmt(last_actual)}: mobile MAU rose {world[seam - pd.Timedelta(days=162)]:,.0f} → {fmt(last_actual)} '
                    f'over the five months before the seam and the model\'s trend, with its last changepoint in early 2025, does not follow that run-up. '
                    f'Read the curves as the model\'s view of the underlying level, not as a continuation of the last data point; the next build will address this.</div>'
                    if abs(seam_step) > 0.01 else "")
    bc = pd.read_csv(csv2, parse_dates=["date"]).set_index("date")
    country_rows = "".join(f"<tr><td>{c}</td><td class=num>{fmt(bc.loc['2026-12-15', c])}</td><td class=num>{fmt(bc.loc['2027-12-15', c])}</td>"
                           f"<td class=num>{bc.loc['2027-12-15', c] / bc.loc['2026-12-15', c] - 1:+.1%}</td></tr>"
                           for c in bc.columns if c not in ("data_type", "world"))

    title = "Firefox Mobile MAU forecast through 2027"
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
<div class="banner"><span><b>Generated:</b> {now}</span><span><b>Source:</b> <code>scripts/render_mobile_mau_handoff.py</code> on build <code>{args.build_dir.name}</code>, mozaic-daily <code>{commit}</code></span><span><b>Data as of:</b> {data_end}</span><span><b>Status:</b> PRE-DRAFT</span></div>

<h1>PRE-DRAFT — {title}</h1>
<p>A forecast of Firefox Mobile <b>monthly active users (MAU)</b> from {data_end} through 2027-12-31, for the four mobile apps together and each
on its own, in Glean telemetry terms. Mobile telemetry has always been Glean, so the model trains on the <b>real history from {history_start}</b>:
nothing is translated or simulated. The number to read is the <b>raw daily MAU on 2027-12-15</b>, the same statistic as the desktop package.
It is the production mobile model with its October 2026 settings, run on MAU instead of DAU. No head- or tailwinds, and no paid/organic treatment
(that mechanism exists for DAU only): this is the model's view of total MAU, nothing added or removed.</p>
<div class="callout"><b>Pre-draft.</b> First build of mobile MAU with this model, and not yet backtested (the history is long enough to do so).
The one data treatment is the 2026 Iran internet shutdown: Iran's MAU over 2026-02-28..2026-06-21 is replaced in the training data by a
counterfactual, so the model does not learn the outage as seasonality; the forecast then carries Iran as the model sees it. Use the curves
for shape and magnitude; do not quote them as plan numbers.</div>
<div class="callout"><b>Data in this package (the forecast itself):</b> <code>{csv1.name}</code> — every day from {history_start} to 2027-12-31, history and forecast,
all apps and per app; <code>{csv2.name}</code> — the same per country; <code>plots/</code> — the charts. Beside this package,
<code>glean2027_mau_desktop_and_mobile.csv</code> puts the desktop and mobile world curves side by side. Details in section 4.</div>

{seam_callout}
<h2>1. The numbers</h2>
<table><tr><th></th><th class=num>2026-12-15</th><th class=num>2027-12-15</th><th class=num>Dec → Dec</th></tr>{rows}</table>
<p><b>How far off could it be?</b> Not yet measured for this metric. The same model's desktop MAU forecasts, run from eight past dates on a long
history, missed the following year by 7% or less half the time and by 10% or less nine times in ten; around {fmt(world27)} that would be roughly
±{fmt(world27 * 0.07)} to ±{fmt(world27 * 0.10)}. A mobile backtest is the next step and will replace this analogy.</p>

<h2>2. What it looks like</h2>
<p>Grey: the previous year on the same calendar, for shape. Black: history. Green: the forecast. Dotted red: December 15.</p>
{img(pkg / "plots" / plot_names[0])}
{img(pkg / "plots" / plot_names[1])}
{img(pkg / "plots" / plot_names[2])}

<h2>3. By country</h2>
<p>Daily MAU, all apps, 15 named markets plus the rest of the world (ROW). Full daily series in <code>{csv2.name}</code>.</p>
<table><tr><th>country</th><th class=num>2026-12-15</th><th class=num>2027-12-15</th><th class=num>Dec → Dec</th></tr>{country_rows}</table>

<h2>4. How to use the files</h2>
<ul>
<li><code>{csv1.name}</code> — one row per day from {history_start} to 2027-12-31: <code>date</code>, <code>data_type</code> (<code>training</code> = history, <code>forecast</code>),
then <code>world</code>, <code>fenix_android</code>, <code>firefox_ios</code>, <code>focus_android</code>, <code>focus_ios</code>. Values are the <b>{STAT}</b>, whole people.
The world is the sum of the four apps (an app's column is empty before that app existed).</li>
<li><code>{csv2.name}</code> — the same statistic per country (all apps) from 2024-01-01, plus <code>world</code>.</li>
<li><code>glean2027_mau_desktop_and_mobile.csv</code> (next to this package) — <code>date</code>, <code>desktop_data_type</code>, <code>desktop_mau</code>,
<code>mobile_data_type</code>, <code>mobile_mau</code>: the two world curves on one date grid. <b>Do not add them</b>: a person active on both devices is counted in both.</li>
<li><code>plots/</code> — the charts above and one per app per year.</li>
<li>Read the 2027-12-15 row for the headline; read <code>data_type</code> to know where history ends ({data_end}) and the forecast begins.</li>
<li>MAU is already a 28-day quantity, so take the daily value as is; do not smooth it again.</li>
</ul>

<h2>5. What is and is not in it</h2>
<ul>
<li>Mobile only: Firefox Android (Fenix), Firefox iOS, Focus Android, Focus iOS. No other mobile product (Klar, Firefox Lite, ...) is included, matching the regular monthly forecast's universe.</li>
<li>Real Glean history from {history_start}, as the dashboards count it. Iran's 2026 shutdown is bridged in the training data only.</li>
<li>No adjustments for planned launches, marketing, or known headwinds. The regular monthly mobile DAU forecast carries those; this one does not.</li>
<li>Not yet backtested; the per-app split is affected by how the model reconciles tiles, so the all-apps figure is the one to quote.</li>
</ul>

<h2>6. Where it came from</h2>
<p>mozaic-daily, branch <code>glean-2027-forecast</code>, commit <code>{commit}</code>; forecast file <code>{parquet.name}</code>; Iran MAU fill <code>{Path(iran_fill).name if iran_fill else 'none'}</code>.
Owner: Brendan Wells. The full record lives with the repository (<code>data-official/2027-glean-draft/</code>); this page is deliberately the short version.</p>
</body></html>"""
    (pkg / "index.html").write_text(html)
    (pkg / "README.txt").write_text(f"""{title} — PRE-DRAFT (generated {now})

Open index.html. It explains the numbers, shows the charts, and describes the two CSVs:
  {csv1.name}     daily series {history_start}..2027-12-31: date, data_type, world, fenix_android, firefox_ios, focus_android, focus_ios ({STAT})
  {csv2.name}  the same statistic per country (all apps) from 2024-01-01
  plots/                              the charts (PNG)
Beside this package: glean2027_mau_desktop_and_mobile.csv — desktop and mobile world MAU side by side (do not sum them).

Headline: mobile MAU (all apps) on 2027-12-15 = {fmt(world27)} ({STAT}). History ends {data_end}; rows after that are forecast.
Pre-draft: production mobile model (October 2026 settings) run on real Glean MAU from {history_start}; Iran 2026 shutdown bridged in training; not yet backtested.
Seam step (7-day means, forecast vs last week of history): {seam_step:+.1%}.
Source: mozaic-daily branch glean-2027-forecast @ {commit}, build {args.build_dir.name}. Owner: Brendan Wells.
""")
    zip_path = shutil.make_archive(str(args.out_dir / pkg.name), "zip", root_dir=args.out_dir, base_dir=pkg.name)
    combined = build_combined(csv1, args.desktop_forecast_csv, args.out_dir)
    print(f"page  {pkg / 'index.html'}\nzip   {zip_path} ({Path(zip_path).stat().st_size / 1e6:.1f} MB)\ncsv   {combined}")


if __name__ == "__main__":
    main()
