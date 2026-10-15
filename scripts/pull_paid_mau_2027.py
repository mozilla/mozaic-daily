#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Download and checkpoint the marketing DS team's 2027 paid MAU plan for the 2027 mobile MAU forecast (no model run).

Brendan, 2026-10-15: "we need to incorporate paid ... download and checkpoint the relevant data ... it starts in January 2027,
so we need to patch the existing mobile forecast through the end of the year, and then start the 2027 one." Decisions the same
day: scenario = ``opt_dec15_mau`` ("Optimized for Dec-15 MAU", the optimal spend model); the 2026-10-10..2026-12-31 bridge = the
same model run's weekly "total" paid DAU x 4.792 x day-of-week factor, as delivered, unscaled; per-country allocation by OUR
measured split's trailing-28-day paid share; the 90% paid band is carried.

Sources (all read-only, saved verbatim under ``source_data/``):
  * ``mozdata.analysis.ahe_gmio_plan2027_mau_daily_20261014``   (STMO 128941) daily 2027 paid MAU, both scenarios, 90% band
  * ``mozdata.analysis.ahe_gmio_weekly_paid_dau_totals_20261013`` weekly 2026 paid DAU, "total" view (the bridge)
  * ``mozdata.analysis.ahe_gmio_weekly_paid_dau_by_country_20261013`` reference only
  * the quick site ``ahe-2027-planning-first-scenario`` (copied from its GCS bucket) and the STMO query JSON, if present in
    ``--extra-source-dir``

Outputs (``data-official/2027-glean-draft/paid_mau_2027/``), pull-date suffixed, never overwritten:
  paid_mau_2027.<scenario>.pull<date>.parquet   date, paid_mau, paid_mau_low_90, paid_mau_high_90, segment (bridge_2026 | plan_2027),
                                                week, day_ratio, weekly_paid_dau_p50 (bridge rows), scenario — daily 2026-10-10..2027-12-31
  paid_mau_2027.<scenario>.pull<date>.csv       twin
  paid_mau_2027.<scenario>.pull<date>.meta.json provenance (sha1 of every source file), the numbers, the construction rule
  paid_country_shares.<split end>.csv           per-country trailing-28-day share of measured paid Fenix MAU (IR excluded = 0)
  plots/paid_mau_2027.<scenario>.png            measured paid MAU (our split) -> bridge -> 2027 plan, with the band

Usage:
    python scripts/pull_paid_mau_2027.py --extra-source-dir tmp/paid_2027/quick_site --extra-source tmp/paid_2027/query_128941.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
DRAFT = repo_root / "data-official/2027-glean-draft"
OUT = DRAFT / "paid_mau_2027"
BQ_QUERY = Path("/Users/brendanwells/work/product-data-science-core/scratch/brwells/tools/bq_query.py")
SQL = {
    "plan_daily": repo_root / "scripts/sql/paid_mau_2027_plan_daily.sql",
    "weekly_totals": repo_root / "scripts/sql/paid_mau_2026_weekly_totals.sql",
    "weekly_by_country": repo_root / "scripts/sql/paid_mau_2026_weekly_by_country.sql",
}
DEFAULT_SCENARIO = "opt_dec15_mau"
BRIDGE_START = pd.Timestamp("2026-10-10")     # the day after our organic build's last training day
MAU_PER_DAU_KEY = "mau_per_dau"


def sha1(path: Path) -> str:
    return hashlib.sha1(path.read_bytes()).hexdigest()


def run_query(name: str, dest_dir: Path, limit: int = 100000) -> pd.DataFrame:
    """bq_query.py --machine on a SQL file; the JSON is saved verbatim, the rows returned as a frame."""
    out = dest_dir / f"{name}.json"
    result = subprocess.run([sys.executable, str(BQ_QUERY), "--file", str(SQL[name]), "--machine", "--limit", str(limit)],
                            capture_output=True, text=True, cwd=repo_root)
    text = result.stdout
    start = text.find("{")
    if start < 0:
        raise RuntimeError(f"{name}: bq_query produced no JSON (exit {result.returncode}): {result.stderr[-600:]}")
    payload = json.loads(text[start:])
    if payload.get("status") != "ok":
        raise RuntimeError(f"{name}: bq_query status {payload.get('status')}: {str(payload)[:400]}")
    if payload.get("truncated"):
        raise RuntimeError(f"{name}: result truncated at --limit {limit}; raise it")
    out.write_text(json.dumps(payload, indent=1))
    shutil.copy(SQL[name], dest_dir / SQL[name].name)
    return pd.DataFrame(payload["rows"])


def load_site_weekly(site_dir: Path | None, scenario_key: str) -> pd.DataFrame | None:
    """Weekly total paid DAU (p50, lo, hi) from the quick site's embedded DATA blob, 'total' channel. Same run as the BQ tables,
    but it carries the upper bound and week 53 of 2026 (2026-12-28, holding Dec 28-31), which the weekly BQ table does not."""
    if site_dir is None:
        return None
    hits = sorted(site_dir.glob("*.html"))
    import re
    for html_path in hits:
        m = re.search(r"var DATA = (\{.*?\});\s*(?:/\* ==GMIO_DATA_END== \*/|\n)", html_path.read_text(), flags=re.S)
        if m:
            data = json.loads(m.group(1))
            series = data["channels"]["total"]["series"][scenario_key]
            return pd.DataFrame({"week": pd.to_datetime(data["weeks"]), "p50": series["dau_p50"], "low_90": series["dau_lo"], "high_90": series["dau_hi"]}).set_index("week")
    return None


def build_bridge(weekly_bq: pd.DataFrame, weekly_site: pd.DataFrame | None, plan: pd.DataFrame, start: pd.Timestamp) -> pd.DataFrame:
    """Daily paid MAU for start..2026-12-31 from the weekly 'total' paid DAU: p50 x mau_per_dau x day-of-week factor, the plan's own rule.

    Weekly values come from the site blob when available (it has the upper bound and week 53); the BQ weekly table must agree
    with it on every shared week (same run), else this raises."""
    k = float(plan[MAU_PER_DAU_KEY].iloc[0])
    day_ratio = plan.drop_duplicates("day_of_week").set_index("day_of_week")["day_ratio"].to_dict()
    bq = weekly_bq.assign(week=pd.to_datetime(weekly_bq["week"])).set_index("week").sort_index()
    if weekly_site is not None:
        shared = bq.index.intersection(weekly_site.index)
        diff = (bq.loc[shared, "p50"] - weekly_site.loc[shared, "p50"]).abs().max()
        if diff > 1:
            raise ValueError(f"site blob and BQ weekly totals disagree on shared weeks (max |diff| {diff:,.0f} paid DAU); not the same run")
        weekly = weekly_site
    else:
        weekly = bq.assign(high_90=bq["p50"] ** 2 / bq["low_90"])   # symmetric in log space; the BQ table publishes only the lower bound
    end = pd.Timestamp(plan["date"].min()) - pd.Timedelta(days=1)
    days = pd.date_range(start, end, freq="D")
    week_of = days - pd.to_timedelta(days.weekday, unit="D")     # ISO Monday
    if not set(week_of.unique()) <= set(weekly.index):
        missing = sorted(set(week_of.unique()) - set(weekly.index))
        raise ValueError(f"weekly totals do not cover bridge weeks {[str(d.date()) for d in missing]}")
    w = weekly.loc[week_of]
    ratio = pd.Series([day_ratio[d.day_name()] for d in days], index=days).to_numpy()
    return pd.DataFrame({
        "date": days, "week": week_of, "day_ratio": ratio,
        "paid_mau": w["p50"].to_numpy() * k * ratio, "paid_mau_low_90": w["low_90"].to_numpy() * k * ratio,
        "paid_mau_high_90": w["high_90"].to_numpy() * k * ratio,
        "weekly_paid_dau_p50": w["p50"].to_numpy(), "segment": "bridge_2026",
    })


def country_shares(split_path: Path, window_days: int = 28, exclude=("IR",)) -> pd.DataFrame:
    split = pd.read_parquet(split_path)
    split["submission_date"] = pd.to_datetime(split["submission_date"])
    end = split["submission_date"].max()
    recent = split[(split["submission_date"] > end - pd.Timedelta(days=window_days)) & ~split["country"].isin(exclude)]
    paid = recent.groupby("country")["paid_mau"].mean()
    shares = (paid / paid.sum()).rename("paid_share").reset_index()
    shares["window"] = f"{(end - pd.Timedelta(days=window_days - 1)).date()}..{end.date()}"
    for c in exclude:
        shares = pd.concat([shares, pd.DataFrame({"country": [c], "paid_share": [0.0], "window": [shares['window'].iloc[0]]})], ignore_index=True)
    return shares.sort_values("paid_share", ascending=False).reset_index(drop=True)


def plot(series: pd.DataFrame, measured: pd.Series, scenario: str, out: Path) -> None:
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(measured.loc["2025-10-01":], color="#222", lw=1.6, label="measured paid Fenix MAU (our split, attributable)")
    for seg, color, label in (("bridge_2026", "#ff7f0e", "bridge: weekly total paid DAU × 4.792 × day factor (run 2026-10-13)"),
                              ("plan_2027", "#d62728", f"2027 plan, scenario {scenario}")):
        s = series[series["segment"] == seg].set_index("date")
        ax.plot(s["paid_mau"], color=color, lw=1.6, label=label)
        ax.fill_between(s.index, s["paid_mau_low_90"], s["paid_mau_high_90"], color=color, alpha=0.15)
    ax.axvline(BRIDGE_START, color="grey", ls=":", lw=1); ax.axvline(pd.Timestamp("2027-01-01"), color="grey", ls=":", lw=1)
    ax.axvline(pd.Timestamp("2027-12-15"), color="red", ls=":", lw=0.8)
    ax.set_title("Paid Fenix MAU: measured → bridge → 2027 plan (90% band = the marketing model's curve uncertainty only)")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e6:.1f}M"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2)); ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.grid(True, alpha=0.3); ax.legend(fontsize=9, loc="upper left")
    ax.text(0.5, 0.5, "PRE-DRAFT", transform=ax.transAxes, fontsize=72, color="#6a1b9a", alpha=0.14, ha="center", va="center", rotation=28, fontweight="bold")
    fig.tight_layout(); out.parent.mkdir(parents=True, exist_ok=True); fig.savefig(out, dpi=120); plt.close(fig)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scenario", default=DEFAULT_SCENARIO)
    p.add_argument("--out-dir", type=Path, default=OUT)
    p.add_argument("--organic-split", type=Path, default=None, help="default: latest organic_mau/fenix_paid_organic_mau.*.parquet")
    p.add_argument("--extra-source-dir", type=Path, default=None, help="e.g. the quick site copied from gs://moz-fx-quick-prod-sites/...")
    p.add_argument("--extra-source", type=Path, action="append", default=[], help="extra files to keep verbatim (e.g. the STMO query JSON)")
    p.add_argument("--pull-date", default=date.today().isoformat())
    args = p.parse_args()

    src = args.out_dir / "source_data"
    src.mkdir(parents=True, exist_ok=True)
    plan = run_query("plan_daily", src)
    weekly = run_query("weekly_totals", src)
    by_country = run_query("weekly_by_country", src)
    if args.extra_source_dir:
        dest = src / args.extra_source_dir.name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(args.extra_source_dir, dest)
    for f in args.extra_source:
        shutil.copy(f, src / f.name)

    plan["date"] = pd.to_datetime(plan["date"])
    scenarios = sorted(plan["scenario"].unique())
    if args.scenario not in scenarios:
        raise SystemExit(f"scenario {args.scenario!r} not in the plan table: {scenarios}")
    chosen = plan[plan["scenario"] == args.scenario].sort_values("date")
    site_key = {"opt_dec15_mau": "opt", "flat": "flat"}.get(args.scenario, args.scenario)
    weekly_site = load_site_weekly(args.extra_source_dir, site_key)
    bridge = build_bridge(weekly, weekly_site, plan, BRIDGE_START)
    plan_rows = pd.DataFrame({"date": chosen["date"], "week": pd.to_datetime(chosen["week"]), "day_ratio": chosen["day_ratio"],
                              "paid_mau": chosen["mau"], "paid_mau_low_90": chosen["mau_low_90"], "paid_mau_high_90": chosen["mau_high_90"],
                              "weekly_paid_dau_p50": chosen["mau"] / chosen["day_ratio"] / chosen[MAU_PER_DAU_KEY], "segment": "plan_2027"})
    series = pd.concat([bridge, plan_rows], ignore_index=True).assign(scenario=args.scenario)
    series["week"] = series["week"].dt.date.astype(str)

    split_path = args.organic_split or sorted(DRAFT.glob("organic_mau/fenix_paid_organic_mau.*.parquet"))[-1]
    split = pd.read_parquet(split_path); split["submission_date"] = pd.to_datetime(split["submission_date"])
    measured = split.groupby("submission_date")["paid_mau"].sum().astype(float)
    shares = country_shares(split_path)

    stem = f"paid_mau_2027.{args.scenario}.pull{args.pull_date}"
    target = args.out_dir / f"{stem}.parquet"
    if target.exists():
        raise SystemExit(f"{target} exists; pulls are never overwritten (use another --pull-date)")
    series.to_parquet(target, index=False)
    series.to_csv(args.out_dir / f"{stem}.csv", index=False, float_format="%.3f")
    shares_path = args.out_dir / f"paid_country_shares.{measured.index.max().date()}.csv"
    shares.to_csv(shares_path, index=False, float_format="%.6f")
    plot_path = args.out_dir / "plots" / f"paid_mau_2027.{args.scenario}.png"
    plot(series, measured, args.scenario, plot_path)

    at = lambda d: series.set_index("date").loc[pd.Timestamp(d)]
    last_measured = float(measured.iloc[-1])
    first_bridge = at(BRIDGE_START)
    meta = {
        "produced_by": "scripts/pull_paid_mau_2027.py", "pull_date": args.pull_date, "written_at": datetime.now().isoformat(timespec="seconds"),
        "scenario": args.scenario, "scenario_label": chosen["scenario_label"].iloc[0], "scenarios_available": scenarios,
        "channels": chosen["channels"].iloc[0], "basis": "attributed (quick site provenance line; the optimizer ran on incremental and re-projected)",
        "curve_run": chosen["curve_run"].iloc[0], "generated_at": chosen["generated_at"].iloc[0], "mau_per_dau": float(chosen[MAU_PER_DAU_KEY].iloc[0]),
        "ci_level": float(chosen["ci_level"].iloc[0]), "n_draws": int(chosen["n_draws"].iloc[0]),
        "sources": {
            "plan_daily": {"table": "mozdata.analysis.ahe_gmio_plan2027_mau_daily_20261014", "stmo_query": 128941, "json_sha1": sha1(src / "plan_daily.json"), "rows": int(len(plan))},
            "weekly_totals": {"table": "mozdata.analysis.ahe_gmio_weekly_paid_dau_totals_20261013", "view": "total", "json_sha1": sha1(src / "weekly_totals.json"), "rows": int(len(weekly))},
            "weekly_by_country": {"table": "mozdata.analysis.ahe_gmio_weekly_paid_dau_by_country_20261013", "view": "total", "json_sha1": sha1(src / "weekly_by_country.json"), "rows": int(len(by_country)), "use": "reference only"},
            "quick_site": {"url": "https://ahe-2027-planning-first-scenario.quick.mozilla.cloud/2027%20planning%20-%20flat%20budget.html", "copied_from": "gs://moz-fx-quick-prod-sites/ahe-2027-planning-first-scenario/", "dir": str(args.extra_source_dir.name) if args.extra_source_dir else None},
            "extra": [f.name for f in args.extra_source],
            "organic_split_for_shares": {"file": str(split_path.relative_to(repo_root)), "sha1": sha1(split_path)},
        },
        "construction": {
            "bridge": f"{BRIDGE_START.date()}..{(plan['date'].min() - pd.Timedelta(days=1)).date()}: weekly 'total' paid DAU (ISO Monday weeks; p50 / lo / hi from the quick site's "
                      f"embedded data, which covers week 53 of 2026 and carries the upper bound; cross-checked against the BQ weekly table on shared weeks{'' if weekly_site is not None else ' — SITE DATA ABSENT, BQ table used, high = p50^2/low_90'}) "
                      "x mau_per_dau x day-of-week ratio — the plan's own daily rule. As delivered, unscaled",
            "plan_2027": "the table's own daily rows for the chosen scenario",
            "country_allocation": "paid_country_shares csv: each country's share of OUR measured paid Fenix MAU over the last 28 measured days; IR = 0 (excluded from the split); apply to the world paid series",
        },
        "numbers": {
            "last_measured_paid_mau": {"date": str(measured.index.max().date()), "value": last_measured},
            "bridge_first_day": {"date": str(BRIDGE_START.date()), "paid_mau": float(first_bridge["paid_mau"]), "vs_last_measured": float(first_bridge["paid_mau"] / last_measured - 1)},
            "bridge_last_day": {"date": "2026-12-31", "paid_mau": float(at("2026-12-31")["paid_mau"])},
            "plan_first_day": {"date": "2027-01-01", "paid_mau": float(at("2027-01-01")["paid_mau"]), "step_vs_dec31": float(at("2027-01-01")["paid_mau"] / at("2026-12-31")["paid_mau"] - 1)},
            "paid_mau_2026-12-15": float(at("2026-12-15")["paid_mau"]),
            "paid_mau_2027-12-15": {"value": float(at("2027-12-15")["paid_mau"]), "low_90": float(at("2027-12-15")["paid_mau_low_90"]), "high_90": float(at("2027-12-15")["paid_mau_high_90"])},
            "flat_scenario_2027-12-15": float(plan[(plan.scenario == "flat") & (plan.date == "2027-12-15")]["mau"].item()) if "flat" in scenarios else None,
            "country_shares_top5": shares.head(5).set_index("country")["paid_share"].round(4).to_dict(),
        },
        "caveats_from_source": ["hypothetical plan, no seasonality, no cross-channel interaction (upper bound)", "MAU = DAU x 4.792, a global multiplier, not a model",
                                "retention extrapolated beyond the fitted range by late 2027", "interval calibration unverified; bands cover curve parameters only"],
    }
    (args.out_dir / f"{stem}.meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    n = meta["numbers"]
    print(f"wrote {target} ({len(series)} rows, {series['date'].min().date()}..{series['date'].max().date()})")
    print(f"last measured {n['last_measured_paid_mau']['value']:,.0f} on {n['last_measured_paid_mau']['date']}; bridge opens {n['bridge_first_day']['paid_mau']:,.0f} "
          f"({n['bridge_first_day']['vs_last_measured']:+.2%}); Dec-31 {n['bridge_last_day']['paid_mau']:,.0f} -> Jan-1 {n['plan_first_day']['paid_mau']:,.0f} ({n['plan_first_day']['step_vs_dec31']:+.2%})")
    print(f"paid MAU 2026-12-15 {n['paid_mau_2026-12-15']:,.0f}; 2027-12-15 {n['paid_mau_2027-12-15']['value']:,.0f} "
          f"[{n['paid_mau_2027-12-15']['low_90']:,.0f}, {n['paid_mau_2027-12-15']['high_90']:,.0f}]; flat scenario {n['flat_scenario_2027-12-15']:,.0f}")
    print(f"country shares: {n['country_shares_top5']}  -> {shares_path}\nplot  {plot_path}")


if __name__ == "__main__":
    main()
