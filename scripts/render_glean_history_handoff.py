#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Per-metric handoff package for the 2027 Glean-history desktop forecast: one self-contained page, one zip.

Audience: a coworker who wants to use the forecast and see what it looks like — not how it was built. Current
state only: no build history, no seam or China discussion. One package per metric (``--metric mau`` or ``dau``):

    <out>/glean2027_desktop_<metric>_handoff/
        index.html                       the page (plots embedded, PRE-DRAFT stamp, freshness banner)
        glean2027_desktop_<metric>_forecast.csv  date, data_type, desktop_world + per-OS values (MAU raw daily / DAU 28-day MA)
        glean2027_desktop_<metric>_by_country.csv  date, data_type, one column per country (all OS), same statistic
        plots/desktop_*.png              the canonical plots for this metric
        README.txt                       what the files are and how to read them
        HANDOFF.md                       briefing for the recipient's AI agent (pre-draft caveats first, numbers, file guide)
    <out>/glean2027_desktop_<metric>_handoff.zip

Every shipped name says "desktop" (package, zip, CSVs, plots, the `desktop_world` column) so nothing in it can be read as
all-Firefox or mobile once it leaves this directory.

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

# Which build the package describes. `real_glean` is the headline since 2026-10-14 (LOCK.md); `glean_equivalent` is build 5,
# the validation build (still renderable for the record). Prose differs because the histories and corrections differ.
FLAVORS = {
    "glean_equivalent": {
        "parquet_glob": LOCKED_PARQUET_GLOB,
        "curves": "csv/glean2027_canonical_curves.csv", "summary": "csv/glean2027_dec15_summary.csv", "plots_dir": "plots/canonical",
        "metrics": ("mau", "dau"),
        "intro": ("in <b>Glean</b> telemetry terms. The history it is trained on is Glean-equivalent: Legacy telemetry restated the way Glean counts people, "
                  "because Glean's own desktop history is too short to forecast from."),
        "predraft": ("Earlier than a normal draft: the history is a simulated (translated) series, the method is new, and the number will move as the "
                     "inputs are re-issued. Use it to see shape and magnitude; do not quote it as a plan number."),
        "history_start": "2020-01-01",
        "error_intro": "The same model, run from eight past dates, missed the following year by",
        "error_tail": ("Those past forecasts ran <b>low</b> more often than high (median {median:+.1%}). Separately, the translated history itself carries an uncertainty "
                       "of about ±{trans:.1f}% (how well Legacy can be restated as Glean); it is small next to the forecast error."),
        "scope_bullets": [
            "Glean terms: the series is what Glean telemetry would have reported, restated from Legacy. Published Glean dashboards may read slightly higher "
            "because they still include some double-counted and automated clients that this series excludes; those terms are tracked separately and not added here.",
            "No adjustments for planned launches, marketing, or known headwinds. The regular monthly forecast carries those; this one does not.",
            "The history and therefore the forecast will be re-issued when the translation is updated (expected late October 2026).",
        ],
        "readme_line": "Pre-draft: trained on a translated (Glean-equivalent) history; will be re-issued when the translation is updated.",
        "agent_caveats": [
            "The training history is **simulated**: Legacy telemetry restated as Glean would have counted it (a translation model, itself a draft). "
            "If the translation changes, every number here changes.",
            "This was the headline until 2026-10-14 and is now the **validation build**; the headline is the real-Glean build (161.5M).",
        ],
    },
    "real_glean": {
        "parquet_glob": "comparison_glean_real_mau_additive_b_2026-10-14/cps*/mozaic_daily_forecast.*.adj-bo.parquet",
        "curves": "comparison_glean_real_mau_additive_b_2026-10-14/csv/comparison_glean_real_mau_b_canonical_curves.csv",
        "summary": "comparison_glean_real_mau_additive_b_2026-10-14/csv/comparison_glean_real_mau_b_dec15_summary.csv",
        "plots_dir": "comparison_glean_real_mau_additive_b_2026-10-14/plots/canonical",
        "metrics": ("mau",),
        "intro": ("trained on the <b>real Glean desktop telemetry we have</b>: 17 months, from 2025-05-22. With that little history the standard model "
                  "extrapolates wildly, so the trend and seasonality settings were re-tuned for a short series (near-linear trend, additive seasonality). "
                  "One correction is applied, because the forecast is implausible without it: Linux automated traffic from hosting providers (about 5.4M MAU in October, "
                  "27% of Linux) is measured and taken out of the training data, then carried forward <b>flat at 4.2M</b>, a no-growth planning assumption. "
                  "Everything else in the data is left as it is."),
        "predraft": ("Earlier than a normal draft: one observed year of seasonality, no backtest is possible, and the Linux bot level is a planning choice "
                     "worth about 2.6% of the headline. Use it to see shape and magnitude; do not quote it as a plan number."),
        "history_start": "2025-05-22",
        "error_intro": ("No backtest is possible on 17 months of history. By analogy, the same model family run on the longer Glean-equivalent history from eight past dates "
                        "missed the following year by"),
        "error_tail": ("Treat that as the floor of the uncertainty, not the estimate: this build has a shorter history. Two other ranges matter: the parameter neighbourhood "
                       "(±1.6M on the headline) and the Linux bot path (0 if the fleet vanishes, +4.2M as built, more if it grows). The world number is the one to quote; "
                       "the per-OS split is affected by how the model reconciles tiles."),
        "scope_bullets": [
            "Real Glean terms: what the Glean dashboard counts, including the double-counted ids that older Firefox versions still produce (about 1% of Windows MAU) and the "
            "Linux automated traffic (held flat). Known artifact: real MAU fell 13M in January 2026 when Firefox 148 fixed the id duplication; the model repeats that dip in "
            "January 2027. It is not real, and it does not affect December 15.",
            "Cross-check: with the Linux bot component removed, this forecast gives 157.3M on 2027-12-15, and an independent model trained on 6.8 years of translated Legacy "
            "history gives 157.2M.",
            "No adjustments for planned launches, marketing, or known headwinds. The regular monthly forecast carries those; this one does not.",
            "The history will be re-pulled when the forecast is refreshed; the Linux bot measurement is re-run with it.",
        ],
        "readme_line": "Pre-draft: trained on 17 months of real Glean telemetry, re-tuned for a short history; Linux automated traffic measured and held flat (4.2M MAU).",
        "agent_caveats": [
            "**17 months of history = one observed year of seasonality.** Mozaic normally trains on 3+ years. The model was re-tuned by hand over three probe rounds "
            "to stop it extrapolating wildly (the untuned run went 169M → 229M); the settings were picked by judgement about plausibility, not validated.",
            "**No backtest is possible** on this history. The error band below is borrowed from a different build on a different (simulated) history and is a floor, not an estimate.",
            "**The Linux bot add-back (4.2M, ~2.6% of the headline) is a planning assumption**, held flat. If that fleet disappears the headline drops to ~157.3M; if it grows, the headline rises.",
            "**Parameter sensitivity:** neighbouring settings move the headline by about ±1.6M.",
            "**Known artifact:** the model repeats the January 2026 duplicate-id drop (−13M, a telemetry fix, not users) in January 2027. Do not read the January 2027 dip as a forecast of user loss.",
            "**Deliberately uncorrected:** the pre-Firefox-148 duplicate-id inflation is left in, matching what Glean dashboards count. A corrected twin is planned as a secondary comparison and does not exist yet.",
            "**Per-OS numbers are weaker than the world number**: mozaic reconciles top-down, which flatters the OS split. Quote the world total.",
            "**The one reassurance:** with the bot component removed (157.3M) it agrees with an independent build on 6.8 years of translated history (157.2M). "
            "That shows the two methods agree. It does not show either one is right.",
        ],
    },
}
OS_ORDER = ["world", "windows", "mac", "linux"]
# Shipped names: the package leaves the repo, so every file and the total column carry "desktop".
PACKAGE_PREFIX = "glean2027_desktop"
WORLD_COLUMN = "desktop_world"
PLOT_FILE_PREFIX = "desktop_"
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
    forecast = curves[["data_type", *cols]].rename(columns=cols).rename(columns={"world": WORLD_COLUMN})
    forecast.index.name = "date"
    p1 = out / f"{PACKAGE_PREFIX}_{metric}_forecast.csv"
    forecast.to_csv(p1, float_format="%.0f")

    allos = df[df["segment"] == json.dumps({"os": "ALL"})]
    by_country = allos.pivot_table(index="target_date", columns="country", values=m["column"])
    if metric == "dau":
        by_country = by_country.rolling(28).mean()
    by_country = by_country.rename(columns={"ALL": WORLD_COLUMN})
    by_country.insert(0, "data_type", curves["data_type"].reindex(by_country.index))
    by_country.index.name = "date"
    by_country = by_country.loc[pd.Timestamp("2024-01-01"):]
    p2 = out / f"{PACKAGE_PREFIX}_{metric}_by_country.csv"
    by_country.to_csv(p2, float_format="%.0f")
    if metric == "dau":
        # the unsmoothed daily series as well, world + per OS
        daily = {}
        for os_name in OS_ORDER:
            seg = json.dumps({"os": "ALL" if os_name == "world" else os_name})
            w = df[(df["country"] == "ALL") & (df["segment"] == seg)].set_index("target_date").sort_index()
            daily[WORLD_COLUMN if os_name == "world" else os_name] = w["dau"]
            daily_type = w["data_type"]
        daily = pd.DataFrame(daily)
        daily.insert(0, "data_type", daily_type)
        daily.index.name = "date"
        daily.to_csv(out / f"{PACKAGE_PREFIX}_dau_daily_unsmoothed.csv", float_format="%.0f")
    return p1, p2


def write_agent_handoff(pkg: Path, *, title: str, now: str, m: dict, flavor: dict, metric: str, summary: pd.DataFrame, bt_row: pd.Series,
                        csv1: Path, csv2: Path, data_end, commit: str, build: str) -> Path:
    """Write HANDOFF.md: the package briefing for the recipient's AI agent, leading with how preliminary the forecast is."""
    def value(os_name: str, date: str) -> float:
        return float(summary[(summary.metric == metric) & (summary.os == os_name) & (summary.date == date)].value.item())

    table = "\n".join(
        f"| {os_name} | {value(os_name, '2026-12-15'):,.0f} | {value(os_name, '2027-12-15'):,.0f} | "
        f"{value(os_name, '2027-12-15') / value(os_name, '2026-12-15') - 1:+.1%} |" for os_name in OS_ORDER)
    world27 = value("world", "2027-12-15")
    band50, band90 = bt_row["abs_err_p50"], bt_row["abs_err_p90"]
    caveats = "\n".join(f"{i}. {c}" for i, c in enumerate(flavor["agent_caveats"], 1))
    text = f"""# HANDOFF — {title} (PRE-DRAFT)

Generated {now} by `scripts/render_glean_history_handoff.py` (mozaic-daily `glean-2027-forecast` @ `{commit}`, build `{build}`). Data as of {data_end}.
Owner: Brendan Wells (brwells@mozilla.com). This file briefs an AI agent working with this package; `index.html` is the human version.

## Read this first: this is a PRE-DRAFT, and a very preliminary one

This is an **early, exploratory attempt**, not a forecast anyone has signed off on. It is earlier than the regular monthly
forecast drafts: new data source, new method, settings chosen by judgement over a few days of probing. Treat every number as
**"roughly this shape and magnitude"**, never as a plan, target, or commitment.

When you summarise, quote, chart, or build on anything here, you must:

- **Label it PRE-DRAFT / preliminary** every time, including in tables, slides, and derived files.
- **Quote the world number only, with its uncertainty** (below). Don't present per-OS or per-country values as findings.
- **Not combine it with other forecasts** (the regular monthly KPI forecast, the mobile MAU companion) as if they were one
  consistent forecast. They use different data, models, and adjustments.
- **Check with Brendan before** anything goes into a plan, OKR, deck for leadership, or external doc.

### Why it is preliminary

{caveats}

## The numbers ({m['stat']})

| | 2026-12-15 | 2027-12-15 | Dec → Dec |
|---|---:|---:|---:|
{table}

**Headline: world desktop {m['name']} on 2027-12-15 ≈ {world27 / 1e6:.1f}M (PRE-DRAFT).**
Error by analogy (the same model family backtested from eight past dates on longer history): within {band50:.0%} half the time and
{band90:.0%} nine times in ten at this horizon, i.e. about ±{world27 * band50 / 1e6:.1f}M to ±{world27 * band90 / 1e6:.1f}M. Read that as
a lower bound on the real uncertainty.

## Files

| file | contents |
|---|---|
| `{csv1.name}` | one row per day, {flavor['history_start']} → 2027-12-31: `date`, `data_type` (`training` = actual history, `forecast`), `desktop_world`, `windows`, `mac`, `linux`. Whole people; `desktop_world` = Windows + Mac + Linux (no mobile) |
| `{csv2.name}` | the same statistic per country (all OS), 2024-01-01 → 2027-12-31, plus `desktop_world` |
| `plots/*.png` | world and per-OS charts for 2026 and 2027 (grey = prior year, black = history, green = forecast, dotted red = Dec 15) |
| `index.html` | the human-readable page (same numbers, charts embedded) |

How to read the CSVs:
- History ends {data_end}; filter on `data_type`, not on a date.
- {'MAU is already a rolling 28-day quantity: use the daily value as is and do not smooth it again.' if metric == 'mau' else 'DAU columns are 28-day trailing means.'}
- **Desktop only** (Windows + Mac + Linux; no mobile anywhere in this package). No adjustments for launches, marketing, or headwinds are applied; the monthly forecast carries those, this one does not.

## What not to do

- Don't present it as "the 2027 forecast"; it is a preliminary exploration of what a 2027 desktop forecast on Glean data could look like.
- Don't infer a user decline from the January 2027 dip (artifact, see above).
- Don't refit, re-tune, or "fix" it from these files. The full build record (`LOCK.md`, `PROCEDURE.html`) lives in the mozaic-daily
  repo under `data-official/2027-glean-draft/`; ask Brendan for it.
"""
    path = pkg / "HANDOFF.md"
    path.write_text(text)
    return path


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--metric", choices=["mau", "dau"], required=True)
    p.add_argument("--build", choices=sorted(FLAVORS), default="real_glean", help="which build the package describes (default: the headline, LOCK.md)")
    p.add_argument("--out-dir", type=Path, default=DRAFT / "handoff")
    args = p.parse_args()
    m = METRIC[args.metric]
    flavor = FLAVORS[args.build]
    if args.metric not in flavor["metrics"]:
        raise SystemExit(f"build {args.build} has no {args.metric.upper()} forecast (metrics: {flavor['metrics']})")
    parquet = sorted(DRAFT.glob(flavor["parquet_glob"]))[0]
    df, meta = load_forecast(str(parquet))
    df["target_date"] = pd.to_datetime(df["target_date"])
    curves = pd.read_csv(DRAFT / flavor["curves"], parse_dates=["date"]).set_index("date")
    summary = pd.read_csv(DRAFT / flavor["summary"])
    bt = pd.read_csv(DRAFT / "backtest/errors_summary.csv")
    bt_row = bt[(bt.metric == m["name"]) & (bt.horizon_days == HEADLINE_HORIZON)].iloc[0]
    commit = subprocess.check_output(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"], text=True).strip()
    data_end = curves[curves["data_type"] == "training"].index.max().date()
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")

    pkg = args.out_dir / f"{PACKAGE_PREFIX}_{args.metric}_handoff"
    if pkg.exists():
        shutil.rmtree(pkg)
    (pkg / "plots").mkdir(parents=True)
    csv1, csv2 = build_csvs(df, curves, m, pkg, args.metric)
    plot_names = [f"{m['plot_prefix']}_world_2027.png", f"{m['plot_prefix']}_world_2026.png", f"{m['plot_prefix']}_by_os_2027.png"]
    for name in plot_names + [f"{m['plot_prefix']}_{o}_{y}.png" for o in ("windows", "mac", "linux") for y in (2026, 2027)]:
        shutil.copy(DRAFT / flavor["plots_dir"] / name, pkg / "plots" / f"{PLOT_FILE_PREFIX}{name}")

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
    for c in [c for c in bc.columns if c not in ("data_type", WORLD_COLUMN)]:
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
<p>A forecast of Firefox Desktop <b>{m['long']} ({m['name']})</b> from {data_end} through 2027-12-31, {flavor['intro']}
{'This is the headline metric for 2027: the number is the <b>raw daily MAU on 2027-12-15</b>.' if m['headline'] else 'DAU is the secondary metric for 2027; the headline is MAU (separate package). DAU is reported as a 28-day trailing mean, the usual convention.'}
No head- or tailwinds are applied: this is the model's view of the history, nothing added.</p>
<div class="callout"><b>Pre-draft.</b> {flavor['predraft']}</div>

<div class="callout"><b>Data in this package (the forecast itself):</b> <code>{csv1.name}</code> — every day from {flavor['history_start']} to 2027-12-31, history and forecast,
world and per OS; <code>{csv2.name}</code> — the same per country; {'<code>glean2027_desktop_dau_daily_unsmoothed.csv</code> — the raw daily DAU behind the 28-day mean; ' if args.metric == 'dau' else ''}<code>plots/</code> — the charts. Details in section 4.</div>

<h2>1. The numbers</h2>
<table><tr><th></th><th class=num>2026-12-15</th><th class=num>2027-12-15</th><th class=num>Dec → Dec</th></tr>{rows}</table>
<p><b>How far off could it be?</b> {flavor['error_intro']} <b>{band50:.0%} or less half the time and by
{band90:.0%} or less nine times in ten</b> at this horizon. Around {fmt(world27)} that is roughly ±{fmt(world27 * band50)} to ±{fmt(world27 * band90)}.
{flavor['error_tail'].format(median=bt_row['median'], trans=trans)}</p>

<h2>2. What it looks like</h2>
<p>Grey: the previous year on the same calendar, for shape. Black: history. Green: the forecast. Dotted red: December 15.</p>
{img(pkg / "plots" / (PLOT_FILE_PREFIX + plot_names[0]))}
{img(pkg / "plots" / (PLOT_FILE_PREFIX + plot_names[1]))}
{img(pkg / "plots" / (PLOT_FILE_PREFIX + plot_names[2]))}

<h2>3. By country</h2>
<p>{m['stat_short'].capitalize()}, all OS, 15 named markets plus the rest of the world (ROW). Full daily series in <code>{csv2.name}</code>.</p>
<table><tr><th>country</th><th class=num>2026-12-15</th><th class=num>2027-12-15</th><th class=num>Dec → Dec</th></tr>{country_rows}</table>

<h2>4. How to use the files</h2>
<ul>
<li><code>{csv1.name}</code> — one row per day from {flavor['history_start']} to 2027-12-31: <code>date</code>, <code>data_type</code> (<code>training</code> = history, <code>forecast</code>),
then <code>desktop_world</code>, <code>windows</code>, <code>mac</code>, <code>linux</code>. Values are the <b>{m['stat']}</b>, whole people. <code>desktop_world</code> is the sum of the three desktop OS; there is no mobile in it.</li>
<li><code>{csv2.name}</code> — the same statistic per country (all OS) from 2024-01-01, plus <code>desktop_world</code>.</li>
<li><code>plots/</code> — the charts above and one per OS per year.</li>
<li>Read the 2027-12-15 row for the headline; read <code>data_type</code> to know where history ends ({data_end}) and the forecast begins.</li>
<li>{'MAU is already a 28-day quantity, so take the daily value as is; do not smooth it again.' if args.metric == 'mau' else 'The DAU columns are 28-day trailing means; <code>glean2027_desktop_dau_daily_unsmoothed.csv</code> has the raw daily values (world and per OS) if you need them.'}</li>
</ul>

<h2>5. What is and is not in it</h2>
<ul>
<li>Desktop only (Windows, Mac, Linux; Linux included in the world). Mobile comes from the regular monthly forecast.</li>
{''.join(f"<li>{b}</li>" for b in flavor['scope_bullets'])}
</ul>

<h2>6. Where it came from</h2>
<p>mozaic-daily, branch <code>glean-2027-forecast</code>, commit <code>{commit}</code>; forecast file <code>{parquet.name}</code>. Owner: Brendan Wells. The full record of
how it was built lives with the repository (<code>data-official/2027-glean-draft/</code>); this page is deliberately the short version.</p>
</body></html>"""
    (pkg / "index.html").write_text(html)
    (pkg / "README.txt").write_text(f"""{title} — PRE-DRAFT (generated {now})

Open index.html. It explains the numbers, shows the charts, and describes the two CSVs.
Using an AI agent? Point it at HANDOFF.md first.
  {csv1.name}     daily series {flavor['history_start']}..2027-12-31: date, data_type, world, windows, mac, linux ({m['stat']})
  {csv2.name}  the same statistic per country (all OS) from 2024-01-01
{'  glean2027_desktop_dau_daily_unsmoothed.csv  raw daily DAU, world and per OS, 2020-01-01..2027-12-31' if args.metric == 'dau' else ''}
  plots/                           the charts (PNG)

Headline: world {m['name']} on 2027-12-15 = {fmt(world27)} ({m['stat']}). History ends {data_end}; rows after that are forecast.
{flavor['readme_line']}
Source: mozaic-daily branch glean-2027-forecast @ {commit}, build {parquet.parent.parent.name}. Owner: Brendan Wells.
""")
    write_agent_handoff(pkg, title=title, now=now, m=m, flavor=flavor, metric=args.metric, summary=summary, bt_row=bt_row,
                        csv1=csv1, csv2=csv2, data_end=data_end, commit=commit, build=parquet.parent.parent.name)
    zip_path = shutil.make_archive(str(args.out_dir / f"{PACKAGE_PREFIX}_{args.metric}_handoff"), "zip", root_dir=args.out_dir, base_dir=pkg.name)
    print(f"page  {pkg / 'index.html'}\nzip   {zip_path} ({Path(zip_path).stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
