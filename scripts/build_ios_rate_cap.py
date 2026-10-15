#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build the iOS-rate cap correction for the 2027 organic mobile MAU draft (data file + spec + meta + plot).

Brendan, 2026-10-15, after the paid-vs-organic correlation check: India's and Indonesia's organic Fenix MAU pulled above
their own Firefox iOS during the high spend period as paid share climbed past 30% — likely paid installs that did not come
through the Play Store, booked organic. Decision: "cap IN+ID organic Fenix at their iOS growth from 2026-04-01; document
it well, because we may need to roll this back depending on what marketing DS says."

This script reproduces the organic frame exactly as the runner sees it (same raw pull, same split, same rounding), computes
per country ``cap_level(t) = organic(from) × iOS_ma28(t) / iOS_ma28(from)`` and ``removed = max(organic − cap, 0)``, and
writes under ``data-official/2027-glean-draft/organic_mau/ios_rate_cap/``:

    ios_rate_cap.<COUNTRIES>.<from>.parquet    submission_date, country, organic_mau, ios_mau, ios_ratio_smoothed, cap_level, capped_mau, removed_mau
    ios_rate_cap.<COUNTRIES>.<from>.meta.json  inputs (paths + sha1), the numbers (removed per country on the last day, peak share)
    ios_rate_cap.json                          the spec the runner records: countries, from date, smoothing, data file
    plots/ios_rate_cap.<COUNTRIES>.<from>.png  per country: organic vs capped vs the iOS-rate path, with iOS indexed, full range + zoom

The runner applies it with ``--ios-rate-cap <parquet>``; turning it off is dropping that flag (``IOS_RATE_CAP.md``). Never runs the model.

Usage:
    python scripts/build_ios_rate_cap.py --countries IN ID --from-date 2026-04-01
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
for sub in ("src", "scripts"):
    if str(repo_root / sub) not in sys.path:
        sys.path.insert(0, str(repo_root / sub))
from mozaic_daily.ios_rate_cap import DEFAULT_SMOOTHING_DAYS, build_cap_frame  # noqa: E402
from run_mobile_mau_forecast import apply_organic_split, load_raw_frame  # noqa: E402

DRAFT = repo_root / "data-official/2027-glean-draft"
DEFAULT_RAW_DIR = DRAFT / "raw_glean_mobile_countryxapp_2026-10-09"
DEFAULT_OUT = DRAFT / "organic_mau/ios_rate_cap"
EVIDENCE = "data-official/2027-glean-draft/organic_mau/paid_organic_correlation.html (section D)"


def wide(frame: pd.DataFrame, flag: str) -> pd.DataFrame:
    w = frame[frame[flag]].pivot(index="x", columns="country", values="y").astype(float)
    w.index = pd.to_datetime(w.index)
    return w.sort_index()


def plot_cap(cap: pd.DataFrame, organic_wide: pd.DataFrame, ios_wide: pd.DataFrame, from_date: pd.Timestamp, out: Path) -> None:
    countries = sorted(cap["country"].unique())
    fig, axes = plt.subplots(2, len(countries), figsize=(9 * len(countries), 10), squeeze=False)
    for j, c in enumerate(countries):
        g = cap[cap["country"] == c].set_index("submission_date").sort_index()
        organic_full = organic_wide[c]
        ios_idx = ios_wide[c] / ios_wide[c].loc[from_date] * organic_full.loc[from_date]
        for i, (start, end) in enumerate([(pd.Timestamp("2025-04-01"), g.index.max()), (from_date - pd.Timedelta(days=45), g.index.max())]):
            ax = axes[i, j]
            ax.plot(organic_full.loc[start:end], color="#222", lw=1.6, label="organic Fenix MAU (uncorrected)")
            ax.plot(g["capped_mau"].loc[start:end], color="#d62728", lw=1.6, label="capped (what the model trains on)")
            ax.plot(g["cap_level"].loc[start:end], color="#ff7f0e", lw=1.0, ls="--", label="cap level = organic(from) × iOS 28d-mean ratio")
            ax.plot(ios_idx.loc[start:end], color="#1f77b4", lw=0.9, alpha=0.7, label="Firefox iOS, indexed to organic Fenix at from-date")
            ax.fill_between(g.index, g["capped_mau"], g["organic_mau"], where=g["removed_mau"] > 0, color="#d62728", alpha=0.15,
                            label="removed (treated as paid, no add-back)")
            ax.axvline(from_date, color="grey", ls=":", lw=1)
            ax.set_title(f"{c} — {'full range' if i == 0 else 'from-date zoom'}; removed on last day {int(g['removed_mau'].iloc[-1]):,}", fontsize=11)
            ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2 if i == 0 else 1)); ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            ax.grid(True, alpha=0.3)
            if i == 0 and j == 0:
                ax.legend(fontsize=8, loc="upper left")
            ax.text(0.5, 0.5, "PRE-DRAFT", transform=ax.transAxes, fontsize=48, color="#6a1b9a", alpha=0.14, ha="center", va="center", rotation=28, fontweight="bold")
    fig.suptitle(f"iOS-rate cap on organic Fenix MAU from {from_date.date()}: the excess over the country's own Firefox iOS growth is removed", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=110)
    plt.close(fig)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--countries", nargs="+", required=True, help="countries to cap, e.g. IN ID")
    p.add_argument("--from-date", required=True, help="first capped day, YYYY-MM-DD (the cap is exact there)")
    p.add_argument("--smoothing-days", type=int, default=DEFAULT_SMOOTHING_DAYS)
    p.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    p.add_argument("--organic-split", type=Path, default=None, help="default: the latest organic_mau/fenix_paid_organic_mau.*.parquet")
    p.add_argument("--organic-exclude", nargs="*", default=["IR"], help="must match the runner's --organic-exclude")
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = p.parse_args()

    split_path = args.organic_split or sorted(DRAFT.glob("organic_mau/fenix_paid_organic_mau.*.parquet"))[-1]
    from_date = pd.Timestamp(args.from_date)
    countries = sorted(args.countries)
    raw = load_raw_frame(args.raw_dir)
    organic, split_record = apply_organic_split(raw, split_path, list(args.organic_exclude))
    organic_fenix = wide(organic, "fenix_android")
    ios = wide(raw, "firefox_ios")
    cap = build_cap_frame(organic_fenix, ios, countries=countries, from_date=from_date, smoothing_days=args.smoothing_days)

    tag = f"{'_'.join(countries)}.{from_date.date()}"
    args.out_dir.mkdir(parents=True, exist_ok=True)
    data_path = args.out_dir / f"ios_rate_cap.{tag}.parquet"
    cap.to_parquet(data_path, index=False)
    plot_path = args.out_dir / "plots" / f"ios_rate_cap.{tag}.png"
    plot_cap(cap, organic_fenix, ios, from_date, plot_path)

    last = cap[cap["submission_date"] == cap["submission_date"].max()].set_index("country")
    world_organic_last = float(organic_fenix.iloc[-1].sum())
    sha1 = lambda path: hashlib.sha1(Path(path).read_bytes()).hexdigest()
    numbers = {
        "last_day": str(cap["submission_date"].max().date()),
        "removed_mau_last_day": {c: int(last.loc[c, "removed_mau"]) for c in countries},
        "removed_mau_last_day_total": int(last["removed_mau"].sum()),
        "removed_share_of_world_organic_fenix_last_day": float(last["removed_mau"].sum() / world_organic_last),
        "organic_mau_last_day": {c: int(last.loc[c, "organic_mau"]) for c in countries},
        "ios_ratio_smoothed_last_day": {c: float(last.loc[c, "ios_ratio_smoothed"]) for c in countries},
        "organic_growth_since_from": {c: float(last.loc[c, "organic_mau"] / cap[(cap.country == c) & (cap.submission_date == from_date)]["organic_mau"].item() - 1) for c in countries},
        "days_capped": {c: int((cap[cap.country == c]["removed_mau"] > 0).sum()) for c in countries},
        "first_capped_day": {c: str(cap[(cap.country == c) & (cap.removed_mau > 0)]["submission_date"].min().date()) if (cap[cap.country == c]["removed_mau"] > 0).any() else None for c in countries},
    }
    meta = {
        "produced_by": "scripts/build_ios_rate_cap.py", "written_at": datetime.now().isoformat(timespec="seconds"),
        "what": "organic Fenix MAU training rows for the named countries capped, from from_date, at organic(from_date) x the trailing "
                "28-day-mean ratio of the country's own Firefox iOS MAU; the positive excess is treated as paid and removed with no add-back",
        "why": "paid share passed 30% in these countries while organic Fenix pulled above the same country's iOS (IN +15pt, ID +7pt over the "
               "high spend period); peers at the same paid share did not; likely non-Play-Store campaign installs counted organic",
        "evidence": EVIDENCE, "decision": "Brendan 2026-10-15; provisional pending the marketing DS team's read; rollback = drop --ios-rate-cap",
        "countries": countries, "from_date": str(from_date.date()), "smoothing_days": args.smoothing_days,
        "raw_dir": str(args.raw_dir), "raw_sha1": sha1(args.raw_dir / "mozaic_parts.raw.glean.mobile.MAU.parquet"),
        "organic_split": str(split_path), "organic_split_sha1": sha1(split_path), "organic_exclude": list(args.organic_exclude),
        "organic_split_record": split_record, "data_file": data_path.name, "data_sha1": sha1(data_path), "plot": str(plot_path.relative_to(repo_root)),
        "numbers": numbers,
    }
    (args.out_dir / f"ios_rate_cap.{tag}.meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    spec = {"correction": "ios_rate_cap", "applies_to": "organic Fenix MAU training rows (2027 organic mobile MAU draft)",
            "countries": countries, "from_date": str(from_date.date()), "smoothing_days": args.smoothing_days,
            "data_file": data_path.name, "data_sha1": meta["data_sha1"], "meta_file": f"ios_rate_cap.{tag}.meta.json",
            "runner_flag": f"--ios-rate-cap {data_path.relative_to(repo_root)}", "rollback": "drop the runner flag and rerun; see IOS_RATE_CAP.md",
            "evidence": EVIDENCE}
    (args.out_dir / "ios_rate_cap.json").write_text(json.dumps(spec, indent=2) + "\n")
    print(f"wrote {data_path} ({len(cap)} rows)\nremoved on {numbers['last_day']}: {numbers['removed_mau_last_day']} "
          f"total {numbers['removed_mau_last_day_total']:,} = {numbers['removed_share_of_world_organic_fenix_last_day']:.2%} of world organic Fenix")
    print(f"organic growth since {from_date.date()}: {numbers['organic_growth_since_from']}; iOS 28d ratio: {numbers['ios_ratio_smoothed_last_day']}")
    print(f"days capped: {numbers['days_capped']}, first capped day: {numbers['first_capped_day']}\nplot  {plot_path}")


if __name__ == "__main__":
    main()
