#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compose the TOTAL mobile MAU curves (organic forecast + marketing's paid plan) for the 2027 draft, with bands, per app and per country.

Brendan, 2026-10-15: "the handoff for the mobile forecast needs to include both paid and organic". Inputs are all on disk:

    organic build      mobile_organic_mau_2026-10-10/<slug>/mozaic_daily_forecast.2026-10-10.gm-M.raw.parquet (+ its curves csv)
    raw actuals        raw_glean_mobile_countryxapp_2026-10-09/mozaic_parts.raw.glean.mobile.MAU.parquet (history = real actuals)
    paid plan          paid_mau_2027/paid_mau_2027.<scenario>.pull<date>.parquet (bridge + 2027, 90% band) + paid_country_shares csv
    cap                organic_mau/ios_rate_cap/ios_rate_cap.*.parquet (the IN + ID excess = "inauthentic organic", its own column; held flat in the forecast, Brendan 2026-10-15)
    organic band       mobile_organic_mau_2026-10-10/intervals/csv/mobile_organic_mau_daily_bands.csv (90% daily, world)

Writes ``data-official/2027-glean-draft/mobile_total_mau_2026-10-10/``:
    csv/mobile2027_total_curves.csv            date, data_type, organic_/organic_inauthentic_/paid_/total_<population>, the three 90% world bands
    csv/mobile2027_total_canonical_curves.csv  the total in the canonical ``mau_<population>`` layout (for plot_glean_history_canonical.py)
    csv/mobile2027_total_by_country.csv        date, data_type, total per country (all apps) + world
    csv/mobile2027_total_dec15_summary.csv     organic / paid / total per population on 2026-12-15 and 2027-12-15, with bands
    mobile2027_total.meta.json                 inputs (sha1), rules, numbers
    plots/mobile2027_total_components.png      world: organic, paid, total with bands, 2025-10 → 2027-12
    plots/mobile2027_total_2x5.png             full range + seam zoom per population, organic and total

Rules are in ``mozaic_daily.mobile_total_mau``. No model run. History total == raw actuals is asserted.

Usage:
    python scripts/compose_mobile_total_mau.py [--cap-policy hold_flat|drop]
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
import numpy as np
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
for sub in ("src", "scripts"):
    if str(repo_root / sub) not in sys.path:
        sys.path.insert(0, str(repo_root / sub))
from mozaic_daily.adjustments import load_forecast  # noqa: E402
from mozaic_daily.mobile_total_mau import cap_excess_series, combine_bands, compose_by_country, compose_populations  # noqa: E402
from export_mobile_mau_curves import APP_NAME_FOR, POPULATIONS, build_curves  # noqa: E402

DRAFT = repo_root / "data-official/2027-glean-draft"
ORGANIC_BUILD = DRAFT / "mobile_organic_mau_2026-10-10"
RAW = DRAFT / "raw_glean_mobile_countryxapp_2026-10-09/mozaic_parts.raw.glean.mobile.MAU.parquet"
OUT = DRAFT / "mobile_total_mau_2026-10-10"
HEADLINE_DATES = ["2026-12-15", "2027-12-15"]
FLAGS = {"fenix_android": "fenix_android", "firefox_ios": "firefox_ios", "focus_android": "focus_android", "focus_ios": "focus_ios"}


def sha1(p: Path) -> str:
    return hashlib.sha1(Path(p).read_bytes()).hexdigest()


def raw_curves(raw: pd.DataFrame) -> pd.DataFrame:
    """The raw pull in the curves layout: mau_world + mau_<app>, world = sum over apps and countries."""
    raw = raw.assign(x=pd.to_datetime(raw["x"]))
    out = pd.DataFrame({"mau_world": raw.groupby("x")["y"].sum().astype(float)})
    for pop, flag in FLAGS.items():
        out[f"mau_{pop}"] = raw[raw[flag]].groupby("x")["y"].sum().astype(float)
    out.index.name = "date"
    return out


def by_country(df: pd.DataFrame) -> pd.DataFrame:
    allapps = df[df["app_name"] == "ALL MOBILE"]
    return allapps.pivot_table(index="target_date", columns="country", values="mau").rename(columns={"ALL": "world"}).astype(float)


def raw_by_country(raw: pd.DataFrame) -> pd.DataFrame:
    raw = raw.assign(x=pd.to_datetime(raw["x"]))
    w = raw.groupby(["x", "country"])["y"].sum().unstack().astype(float)
    w["world"] = w.sum(axis=1)
    return w


def plot_components(c: pd.DataFrame, seam: pd.Timestamp, out: Path) -> None:
    fig, ax = plt.subplots(figsize=(15, 7))
    win = c.loc["2025-10-01":]
    hist, fc = win[win.index < seam], win[win.index >= seam]
    for col, color, label in (("organic_world", "#1f77b4", "authentic organic (our model)"), ("paid_world", "#d62728", "paid (marketing plan; history = measured)"),
                              ("organic_inauthentic_world", "#9467bd", "inauthentic organic (IN + ID cap excess; held flat)"),
                              ("total_world", "#222", "TOTAL = authentic organic + inauthentic organic + paid")):
        ax.plot(hist[col], color=color, lw=1.6, label=label)
        ax.plot(fc[col], color=color, lw=1.6, ls="--")
    for col, color in (("organic_world", "#1f77b4"), ("paid_world", "#d62728"), ("total_world", "#222")):
        ax.fill_between(fc.index, fc[f"{col}_low_90"], fc[f"{col}_high_90"], color=color, alpha=0.12)
    ax.axvline(seam, color="grey", ls=":", lw=1); ax.axvline(pd.Timestamp("2027-12-15"), color="red", ls=":", lw=0.8)
    ax.set_title("Mobile MAU, world: organic + paid = total (dashed = forecast; shaded = 90% bands; total band assumes independence)")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e6:.0f}M"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2)); ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.grid(True, alpha=0.3); ax.legend(fontsize=9, loc="upper left")
    ax.text(0.5, 0.5, "PRE-DRAFT", transform=ax.transAxes, fontsize=80, color="#6a1b9a", alpha=0.14, ha="center", va="center", rotation=28, fontweight="bold")
    fig.tight_layout(); out.parent.mkdir(parents=True, exist_ok=True); fig.savefig(out, dpi=120); plt.close(fig)


def plot_2x5(c: pd.DataFrame, seam: pd.Timestamp, out: Path) -> None:
    fig, axes = plt.subplots(2, len(POPULATIONS), figsize=(26, 9))
    for j, pop in enumerate(POPULATIONS):
        for i, (start, end) in enumerate([("2024-01-01", "2027-12-31"), (seam - pd.Timedelta(days=120), seam + pd.Timedelta(days=150))]):
            ax = axes[i, j]
            w = c.loc[pd.Timestamp(start):pd.Timestamp(end)]
            h, f = w[w.index < seam], w[w.index >= seam]
            ax.plot(h[f"total_{pop}"], color="#222", lw=1.4, label="total (history = actuals)")
            ax.plot(f[f"total_{pop}"], color="#2ca02c", lw=1.4, label="total forecast")
            if pop in ("world", "fenix_android"):
                ax.plot(h[f"organic_{pop}"], color="#1f77b4", lw=1.0, label="organic")
                ax.plot(f[f"organic_{pop}"], color="#1f77b4", lw=1.0, ls="--")
            ax.axvline(seam, color="k", ls=":", lw=0.8); ax.axvline(pd.Timestamp("2027-12-15"), color="#1f77b4", ls="--", lw=0.8)
            ax.set_title(f"{pop} — {'full range' if i == 0 else 'seam zoom'}", fontsize=11)
            span = np.nanmax(w[f"total_{pop}"]) - np.nanmin(w[f"total_{pop}"])
            ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _, s=span: f"{v / 1e6:.2f}M" if s < 3e6 else f"{v / 1e6:.1f}M"))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y")); plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            ax.grid(alpha=0.3)
            if i == 0 and j == 0:
                ax.legend(fontsize=8)
    fig.suptitle("Mobile MAU, TOTAL = organic + paid — history (real actuals) and forecast, per app", fontsize=13)
    fig.text(0.5, 0.5, "PRE-DRAFT", fontsize=110, color="#6a1b9a", alpha=0.15, ha="center", va="center", rotation=25)
    fig.tight_layout(rect=(0, 0, 1, 0.96)); out.parent.mkdir(parents=True, exist_ok=True); fig.savefig(out, dpi=100); plt.close(fig)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--organic-build", type=Path, default=ORGANIC_BUILD)
    p.add_argument("--paid", type=Path, default=None, help="default: latest paid_mau_2027/paid_mau_2027.*.parquet")
    p.add_argument("--shares", type=Path, default=None, help="default: latest paid_mau_2027/paid_country_shares.*.csv")
    p.add_argument("--cap", type=Path, default=None, help="default: latest organic_mau/ios_rate_cap/ios_rate_cap.*.parquet")
    p.add_argument("--cap-policy", default="hold_flat", choices=["hold_flat", "drop"])
    p.add_argument("--out-dir", type=Path, default=OUT)
    args = p.parse_args()

    parquet = sorted(args.organic_build.glob("cps*/mozaic_daily_forecast.*.gm-M.raw.parquet"))[0]
    df, meta = load_forecast(str(parquet)); df["target_date"] = pd.to_datetime(df["target_date"])
    seam = pd.Timestamp(meta["forecast_start_date"]); training_end = seam - pd.Timedelta(days=1)
    organic = build_curves(df)
    raw = pd.read_parquet(RAW)
    rawc = raw_curves(raw)
    paid_path = args.paid or sorted(DRAFT.glob("paid_mau_2027/paid_mau_2027.*.parquet"))[-1]
    plan = pd.read_parquet(paid_path)
    shares_path = args.shares or sorted(DRAFT.glob("paid_mau_2027/paid_country_shares.*.csv"))[-1]
    shares = pd.read_csv(shares_path).set_index("country")["paid_share"]
    cap_path = args.cap or sorted(DRAFT.glob("organic_mau/ios_rate_cap/ios_rate_cap.*.parquet"))[-1]
    excess_bc = cap_excess_series(pd.read_parquet(cap_path), training_end, organic.index.max(), args.cap_policy)
    bands = pd.read_csv(args.organic_build / "intervals/csv/mobile_organic_mau_daily_bands.csv", parse_dates=["date"]).set_index("date")
    # the band file's median must be the organic point forecast (the interval tool asserts it against the parquet; re-check here)
    med = bands["median"].dropna(); dev = (med - organic["mau_world"].reindex(med.index)).abs().max()
    if dev > 1:
        raise SystemExit(f"organic band median deviates from the build's world forecast by up to {dev:,.0f}")

    composed = compose_populations(organic, rawc, plan, excess_bc.sum(axis=1), POPULATIONS, training_end)
    composed = combine_bands(composed, bands["lower_90"], bands["upper_90"])
    hist = composed.index <= training_end
    for pop in POPULATIONS:
        got, want = composed.loc[hist, f"total_{pop}"], rawc[f"mau_{pop}"].reindex(composed.index[hist])
        if not np.allclose(got.fillna(-1), want.fillna(-1), atol=0.5):
            raise SystemExit(f"{pop}: history total != raw actuals (max |diff| {(got - want).abs().max():,.0f})")
    bc = compose_by_country(by_country(df), raw_by_country(raw), plan.set_index(pd.to_datetime(plan["date"]))["paid_mau"] + 0.0, shares, excess_bc, training_end)
    # the world column of the by-country file is the sum of the countries; it must match the population file's world total
    world_check = (bc.drop(columns="world").sum(axis=1) - composed["total_world"]).abs().max()
    if world_check > 1:
        raise SystemExit(f"by-country totals do not sum to the world total (max |diff| {world_check:,.0f})")
    bc["world"] = composed["total_world"]
    bc.insert(0, "data_type", composed["data_type"])
    bc.index.name = "date"

    out = args.out_dir; (out / "csv").mkdir(parents=True, exist_ok=True); (out / "plots").mkdir(exist_ok=True)
    composed.to_csv(out / "csv/mobile2027_total_curves.csv", float_format="%.3f")
    canonical = pd.DataFrame({"data_type": composed["data_type"]}, index=composed.index)
    for pop in POPULATIONS:
        canonical[f"mau_{pop}"] = composed[f"total_{pop}"]
    canonical.to_csv(out / "csv/mobile2027_total_canonical_curves.csv", float_format="%.3f")
    bc.loc[pd.Timestamp("2024-01-01"):].to_csv(out / "csv/mobile2027_total_by_country.csv", float_format="%.0f")
    rows = []
    for on in HEADLINE_DATES:
        d = pd.Timestamp(on); r = composed.loc[d]
        for pop in POPULATIONS:
            rows.append({"date": on, "population": pop, "organic": round(r[f"organic_{pop}"]), "organic_inauthentic": round(r[f"organic_inauthentic_{pop}"]), "paid": round(r[f"paid_{pop}"]), "total": round(r[f"total_{pop}"]),
                         "organic_low_90": round(r["organic_world_low_90"]) if pop == "world" else None, "organic_high_90": round(r["organic_world_high_90"]) if pop == "world" else None,
                         "paid_low_90": round(r["paid_world_low_90"]) if pop == "world" else None, "paid_high_90": round(r["paid_world_high_90"]) if pop == "world" else None,
                         "total_low_90": round(r["total_world_low_90"]) if pop == "world" else None, "total_high_90": round(r["total_world_high_90"]) if pop == "world" else None})
    summary = pd.DataFrame(rows); summary.to_csv(out / "csv/mobile2027_total_dec15_summary.csv", index=False)
    plot_components(composed, seam, out / "plots/mobile2027_total_components.png")
    plot_2x5(composed, seam, out / "plots/mobile2027_total_2x5.png")

    w = lambda d, col: float(composed.loc[pd.Timestamp(d), col])
    step = lambda col: composed.loc[seam:seam + pd.Timedelta(days=6), col].mean() / composed.loc[seam - pd.Timedelta(days=7):training_end, col].mean() - 1
    numbers = {
        "world": {on: {k: w(on, f"{k}_world") for k in ("organic", "organic_inauthentic", "paid", "total")} | {"total_low_90": w(on, "total_world_low_90"), "total_high_90": w(on, "total_world_high_90")} for on in HEADLINE_DATES},
        "fenix": {on: {k: w(on, f"{k}_fenix_android") for k in ("organic", "organic_inauthentic", "paid", "total")} for on in HEADLINE_DATES},
        "last_actual_total_world": w(training_end, "total_world"), "last_actual_paid_world": w(training_end, "paid_world"),
        "seam_step_7d": {"total": float(step("total_world")), "organic": float(step("organic_world")), "paid": float(step("paid_world"))},
        "cap_excess_held_flat_world": float(excess_bc.sum(axis=1).iloc[-1]), "cap_excess_by_country_forecast": {c: float(excess_bc[c].iloc[-1]) for c in excess_bc.columns},
        "dec_to_dec_total": w("2027-12-15", "total_world") / w("2026-12-15", "total_world") - 1,
    }
    meta_out = {
        "produced_by": "scripts/compose_mobile_total_mau.py", "written_at": datetime.now().isoformat(timespec="seconds"),
        "organic_parquet": str(parquet.relative_to(repo_root)), "organic_sha1": sha1(parquet), "organic_model_config": meta.get("model_config"),
        "organic_corrections": {"organic_only": meta.get("organic_only"), "ios_rate_cap": meta.get("ios_rate_cap"), "iran_fill": meta.get("iran_fill")},
        "raw_actuals": str(RAW.relative_to(repo_root)), "raw_sha1": sha1(RAW),
        "paid_plan": str(paid_path.relative_to(repo_root)), "paid_sha1": sha1(paid_path), "paid_meta": json.loads(Path(str(paid_path).replace(".parquet", ".meta.json")).read_text()),
        "country_shares": str(shares_path.relative_to(repo_root)), "cap_file": str(cap_path.relative_to(repo_root)), "cap_policy": args.cap_policy,
        "organic_bands": str((args.organic_build / "intervals/csv/mobile_organic_mau_daily_bands.csv").relative_to(repo_root)),
        "rules": ["history rows = raw actuals (paid shown = raw - authentic organic - inauthentic organic)", "forecast paid = delivered plan (bridge + 2027), a level, Fenix and world only",
                  f"India + Indonesia cap excess = organic_inauthentic_ column, {args.cap_policy} in the forecast (Brendan 2026-10-15: keep it flat, document the choice; it is in neither our model nor marketing's plan)",
                  "per-country forecast paid by fixed trailing-28d measured-paid share; the inauthentic column stays in its own country",
                  "total 90% band = organic and paid half-widths in quadrature (independence assumed); component bands as delivered"],
        "numbers": numbers, "seam": str(seam.date()), "training_end": str(training_end.date()),
    }
    (out / "mobile2027_total.meta.json").write_text(json.dumps(meta_out, indent=2, default=float) + "\n")
    n = numbers
    print(f"history total == raw actuals: OK ({int(hist.sum())} days, {len(POPULATIONS)} populations)")
    for on in HEADLINE_DATES:
        x = n["world"][on]
        print(f"{on}: organic {x['organic']:,.0f} + inauthentic {x['organic_inauthentic']:,.0f} + paid {x['paid']:,.0f} = TOTAL {x['total']:,.0f}  [90% {x['total_low_90']:,.0f}, {x['total_high_90']:,.0f}]")
    print(f"last actual total {n['last_actual_total_world']:,.0f} (paid {n['last_actual_paid_world']:,.0f}); seam step total {n['seam_step_7d']['total']:+.2%} "
          f"(organic {n['seam_step_7d']['organic']:+.2%}, paid {n['seam_step_7d']['paid']:+.2%}); Dec->Dec total {n['dec_to_dec_total']:+.1%}; cap excess held flat {n['cap_excess_held_flat_world']:,.0f}")
    print(f"wrote {out}/csv/, plots/, mobile2027_total.meta.json")


if __name__ == "__main__":
    main()
