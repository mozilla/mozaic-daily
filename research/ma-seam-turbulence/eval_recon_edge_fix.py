"""Score candidate fixes for the reconstruct_matched_daily seam-edge bias.

Read-only: parquets on disk, no BigQuery, no model re-run.

The gating question is *not* "does the seam step shrink" — June already bought a smaller
day-0 step and paid for it with a wrecked day-27 splice (see LOG.md). So the harness
measures both ends of that trade, and it **anchors the splice metric against June's
published figures** (ALL 0.086%, AR ~0.99%, BR ~0.74%, US ~0.031%; the rejected
concatenated-trend fix took ALL to 0.698%) before using it to judge anything. If the
metric cannot reproduce those numbers, the metric is wrong and its verdicts are worthless.

Sections, in the order the handoff asks for them:

  splice   — criterion 3, the one that killed the last attempt. Measured on the June
             desktop build so it is directly comparable to the LOG's numbers.
  headline — criterion 1/2: Aug-25, Dec-15, the trough minimum and the seam step on the
             locked August build (and the superseded one, whose apparent continuity was
             a cancellation).
  weekday  — does the fix hold for a seam on every weekday, or only the Tuesday we have?
  dowfix   — handoff §6: the same min_periods=4 defect in the 13-week day-of-week profile.

    source .venv/bin/activate && python3 research/ma-seam-turbulence/eval_recon_edge_fix.py
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

GIT_ROOT = Path(subprocess.run(["git", "rev-parse", "--show-toplevel"],
                               capture_output=True, text=True).stdout.strip())
os.chdir(GIT_ROOT)
sys.path.insert(0, str(GIT_ROOT / "src"))
sys.path.insert(0, str(GIT_ROOT / "research/ma-seam-turbulence"))

_spec = importlib.util.spec_from_file_location(
    "export_canonical_curves", GIT_ROOT / "data-official/2026-06/export_canonical_curves.py")
export = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export)

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from recon_variants import make_reconstructor, patched_reconstructor  # noqa: E402

WINDOW = 28
OS_ALL = '{"os": "ALL"}'

JUNE_DESKTOP = ("data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/"
                "mozaic_daily_forecast.2026-05-26.ld-D.raw.parquet")
JUNE_MOBILE = ("data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/"
               "mozaic_daily_forecast.2026-05-26.gm-D.adj-m.parquet")
JUNE_SEAM = pd.Timestamp("2026-05-26")

AUG_SEAM = pd.Timestamp("2026-07-28")
AUG_BUILDS = {
    "s01 (locked)": "data-official/2026-08/desktop_locked/"
                    "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet",
    "superseded": "data-official/2026-08/desktop_baseline_2026-07-28/"
                  "cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/"
                  "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet",
}
AUG_HEADWIND = "data-official/2026-08/adjustments/headwind.json"

DESKTOP_KEY = dict(segment=OS_ALL, data_source="legacy_desktop", app_name="desktop")
MOBILE_KEY = dict(segment="{}", data_source="glean_mobile", app_name="ALL MOBILE")

VARIANTS = {
    "current": make_reconstructor("current"),
    "forward7": make_reconstructor("forward7"),
    "concat (rejected)": make_reconstructor("concat"),
}


# --------------------------------------------------------------------------- loading

def daily_series(parquet: str, key: dict, country: str = "ALL") -> pd.Series:
    """One country's continuous daily DAU (training + forecast) from a forecast parquet."""
    df = pd.read_parquet(parquet)
    mask = ((df["country"] == country) & (df["segment"] == key["segment"])
            & (df["data_source"] == key["data_source"]) & (df["app_name"] == key["app_name"]))
    sub = df.loc[mask, ["target_date", "dau"]].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    s = sub.sort_values("target_date").set_index("target_date")["dau"].astype(float)
    return s[~s.index.duplicated(keep="last")]


def display(series: pd.Series, seam: pd.Timestamp, reconstruct, **kwargs) -> pd.Series:
    """``export.display_ma`` with a given reconstructor swapped in."""
    with patched_reconstructor(export, reconstruct):
        return export.display_ma(series.index.to_series(), series, seam, window=WINDOW, **kwargs)


# --------------------------------------------------------------------------- metrics

def splice_metrics(series: pd.Series, seam: pd.Timestamp, reconstruct) -> dict:
    """Day-27 handoff quality, as a fraction of level.

    Two candidate readings of the LOG's "day-27 hand-off leaves a small step", measured
    with ``continuous_splice=False`` (the cubic correction drives the level residual to
    zero by construction, so it would mask exactly what we are trying to compare):

      ``landing`` — the transition MA extrapolated one day onto the splice date, minus the
                    forecast-only MA there. This is display_ma's own ``r_level``.
      ``visible`` — the step a reader sees in the plotted curve between day 27 and day 28.

    Also reports the corner (2nd difference) across the handoff with the shipped
    ``continuous_splice=True``, which is what actually ships.
    """
    first_clean = seam + pd.Timedelta(days=WINDOW - 1)
    pre, fc = series[series.index < seam], series[series.index >= seam]

    with patched_reconstructor(export, reconstruct):
        matched = export.reconstruct_matched_daily(pre, fc, seam, WINDOW)
    transition_ma = pd.concat([pre, matched]).sort_index().rolling(WINDOW).mean()
    forecast_only_ma = fc.rolling(WINDOW).mean()

    uncorrected = display(series, seam, reconstruct, continuous_splice=False)
    corrected = display(series, seam, reconstruct, continuous_splice=True)
    level = uncorrected.loc[seam:first_clean].mean()

    def corner(ma):
        d = first_clean
        return abs((ma.loc[d + pd.Timedelta(days=1)] - ma.loc[d])
                   - (ma.loc[d] - ma.loc[d - pd.Timedelta(days=1)]))

    return {
        "landing_pct": 100 * (transition_ma.loc[first_clean]
                              - forecast_only_ma.loc[first_clean]) / level,
        "visible_pct": 100 * (uncorrected.loc[first_clean]
                              - uncorrected.loc[first_clean - pd.Timedelta(days=1)]) / level,
        "corner_spliced_pct": 100 * corner(corrected) / level,
    }


def seam_step(series: pd.Series, seam: pd.Timestamp, reconstruct) -> float:
    """Day-0 step: displayed forecast MA at the seam minus the trailing actuals-only MA."""
    ma = display(series, seam, reconstruct)
    actuals_only = series[series.index < seam].rolling(WINDOW).mean().dropna()
    return float(ma.loc[seam] - actuals_only.iloc[-1])


def headwind_ramp(date: pd.Timestamp, spec: dict) -> float:
    start, anchor = pd.Timestamp(spec["start_date"]), pd.Timestamp(spec["anchor_date"])
    full = float(spec["desktop_dau"])
    if date <= start:
        return 0.0
    return full if date >= anchor else full * (date - start).days / (anchor - start).days


def headline_metrics(series: pd.Series, seam: pd.Timestamp, reconstruct, spec: dict) -> dict:
    """Aug-25, Dec-15, the post-headwind trough minimum, and the day-0 seam step.

    The trough minimum scans from the seam (scripts/score_near_horizon.py's definition),
    so it deliberately includes the transition zone: if a fix lowers the transition it can
    move the argmin off Aug-25 and change the headline trough even though Aug-25 itself is
    byte-identical. That is a real risk, not a hypothetical, so it is measured.
    """
    ma = display(series, seam, reconstruct)
    post = ma + pd.Series([headwind_ramp(d, spec) for d in ma.index], index=ma.index)
    summer = post.loc[seam:pd.Timestamp("2026-10-15")].dropna()
    return {
        "aug25": float(post.loc[pd.Timestamp("2026-08-25")]),
        "dec15": float(post.loc[pd.Timestamp("2026-12-15")]),
        "trough_min": float(summer.min()),
        "trough_date": str(summer.idxmin().date()),
        "seam_step": seam_step(series, seam, reconstruct),
    }


def trend_at_seam(series: pd.Series, seam: pd.Timestamp) -> dict:
    """The trend estimate at the seam under each estimator, vs a DoW-complete reference."""
    from recon_variants import _centered_min4, _dow_complete
    fc = series[series.index >= seam]
    return {"current": float(_centered_min4(fc).loc[seam]),
            "forward7": float(_dow_complete(fc).loc[seam])}


# --------------------------------------------------------------------------- sections

def check_variant_fidelity() -> None:
    """`current` must reproduce the shipped function exactly, or nothing else is comparable."""
    series = daily_series(JUNE_DESKTOP, DESKTOP_KEY)
    pre, fc = series[series.index < JUNE_SEAM], series[series.index >= JUNE_SEAM]
    shipped = export.reconstruct_matched_daily(pre, fc, JUNE_SEAM, WINDOW)
    mine = make_reconstructor("current")(pre, fc, JUNE_SEAM, WINDOW)
    delta = (shipped - mine).abs().max()
    print(f"fidelity check: |current - shipped| max = {delta:.3e}  "
          f"{'OK' if delta < 1e-9 else 'MISMATCH -- harness is invalid'}")
    assert delta < 1e-9, "the 'current' variant does not reproduce the shipped estimator"


def section_splice() -> None:
    print("\n" + "=" * 78)
    print("CRITERION 3 -- the day-27 splice (June's rejection criterion)")
    print("=" * 78)
    print("Anchoring the metric: June's LOG reports ALL ~0.086%, AR ~0.99%, BR ~0.74%,")
    print("US ~0.031% for the shipped transition, and ALL 0.698% for the rejected fix.\n")

    rows = []
    for country in ["ALL", "AR", "BR", "US", "IN", "CN"]:
        series = daily_series(JUNE_DESKTOP, DESKTOP_KEY, country)
        if series.empty:
            continue
        for name, recon in VARIANTS.items():
            m = splice_metrics(series, JUNE_SEAM, recon)
            rows.append({"country": country, "variant": name, **m})
    table = pd.DataFrame(rows)
    print("June desktop, splice residual as % of level (continuous_splice=False):")
    pivot = table.pivot(index="country", columns="variant", values="landing_pct")
    visible = table.pivot(index="country", columns="variant", values="visible_pct")
    corner = table.pivot(index="country", columns="variant", values="corner_spliced_pct")
    order = ["current", "forward7", "concat (rejected)"]
    print("\n  landing residual (display_ma's own r_level):")
    print(pivot[order].to_string(float_format=lambda v: f"{v:+.3f}%"))
    print("\n  visible day-27 -> day-28 step:")
    print(visible[order].to_string(float_format=lambda v: f"{v:+.3f}%"))
    print("\n  handoff corner WITH the shipped continuous_splice correction:")
    print(corner[order].to_string(float_format=lambda v: f"{v:.4f}%"))

    print("\nMobile ALL (June):")
    mob = daily_series(JUNE_MOBILE, MOBILE_KEY)
    for name, recon in VARIANTS.items():
        m = splice_metrics(mob, JUNE_SEAM, recon)
        print(f"  {name:20s} landing {m['landing_pct']:+.3f}%  "
              f"visible {m['visible_pct']:+.3f}%  corner {m['corner_spliced_pct']:.4f}%")
    table.to_csv("research/ma-seam-turbulence/plots/splice_metrics.csv", index=False)


def section_headline() -> None:
    print("\n" + "=" * 78)
    print("CRITERIA 1 & 2 -- headline numbers and the day-0 seam step (August desktop)")
    print("=" * 78)
    spec = json.loads(Path(AUG_HEADWIND).read_text())
    for label, path in AUG_BUILDS.items():
        df, _ = load_forecast(path)
        series = daily_series(path, DESKTOP_KEY)
        print(f"\n{label}")
        trend = trend_at_seam(series, AUG_SEAM)
        print(f"  trend_fc[seam]: current {trend['current']:>14,.0f}   "
              f"forward7 {trend['forward7']:>14,.0f}   "
              f"delta {trend['forward7'] - trend['current']:>+14,.0f}")
        base = None
        for name, recon in VARIANTS.items():
            m = headline_metrics(series, AUG_SEAM, recon, spec)
            if base is None:
                base = m
            print(f"  {name:20s} aug25 {m['aug25']:>13,.0f} ({m['aug25']-base['aug25']:>+9,.0f})"
                  f"  dec15 {m['dec15']:>13,.0f} ({m['dec15']-base['dec15']:>+9,.0f})"
                  f"  trough {m['trough_min']:>13,.0f} @{m['trough_date']}"
                  f"  seam step {m['seam_step']:>+10,.0f}")


def section_weekday() -> None:
    """Shift the seam across 7 consecutive days so it lands on every weekday.

    Shifting the seam forward by k days moves the first k forecast days into the "pre"
    side. That is synthetic — those days are model output, not actuals — but the question
    here is purely about the trend estimator's edge behaviour, which does not care where
    the pre-side numbers came from.
    """
    print("\n" + "=" * 78)
    print("WEEKDAY SWEEP -- does the fix hold for a seam on any weekday?")
    print("=" * 78)
    from recon_variants import _centered_min4, _dow_complete
    series = daily_series(AUG_BUILDS["s01 (locked)"], DESKTOP_KEY)
    print(f"{'seam':12s} {'dow':4s} {'window @seam':>14s} "
          f"{'current':>14s} {'forward7':>14s} {'bias removed':>14s} {'step curr':>11s} {'step f7':>11s}")
    for k in range(7):
        seam = AUG_SEAM + pd.Timedelta(days=k)
        fc = series[series.index >= seam]
        n_distinct = len(set(fc.index[:4].dayofweek))  # what min_periods=4 actually samples
        cur = float(_centered_min4(fc).loc[seam])
        f7 = float(_dow_complete(fc).loc[seam])
        step_cur = seam_step(series, seam, VARIANTS["current"])
        step_f7 = seam_step(series, seam, VARIANTS["forward7"])
        print(f"{str(seam.date()):12s} {seam.day_name()[:3]:4s} "
              f"{f'{n_distinct}/7 dow':>14s} {cur:>14,.0f} {f7:>14,.0f} "
              f"{f7 - cur:>+14,.0f} {step_cur:>+11,.0f} {step_f7:>+11,.0f}")


def section_dow_profile() -> None:
    print("\n" + "=" * 78)
    print("HANDOFF §6 -- the same min_periods=4 defect in the 13-week day-of-week profile")
    print("=" * 78)
    from recon_variants import _dow_profile
    for label, (path, key, seam) in {
        "aug desktop s01": (AUG_BUILDS["s01 (locked)"], DESKTOP_KEY, AUG_SEAM),
        "june desktop": (JUNE_DESKTOP, DESKTOP_KEY, JUNE_SEAM),
        "june mobile": (JUNE_MOBILE, MOBILE_KEY, JUNE_SEAM),
    }.items():
        series = daily_series(path, key)
        pre = series[series.index < seam]
        cur = _dow_profile(pre, seam, complete_windows=False)
        fix = _dow_profile(pre, seam, complete_windows=True)
        names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        print(f"\n{label}")
        print("  " + "  ".join(f"{n:>7s}" for n in names))
        print("  " + "  ".join(f"{v:7.4f}" for v in cur.values) + "   current")
        print("  " + "  ".join(f"{v:7.4f}" for v in fix.values) + "   complete windows")
        print("  " + "  ".join(f"{d:+7.4f}" for d in (fix - cur).values)
              + f"   delta (max |{(fix - cur).abs().max():.4f}|)")

    print("\nEffect of the day-of-week fix on the August headline, on top of forward7:")
    spec = json.loads(Path(AUG_HEADWIND).read_text())
    series = daily_series(AUG_BUILDS["s01 (locked)"], DESKTOP_KEY)
    for label, recon in [("forward7", make_reconstructor("forward7")),
                         ("forward7 + dow fix", make_reconstructor("forward7", True))]:
        m = headline_metrics(series, AUG_SEAM, recon, spec)
        s = splice_metrics(series, AUG_SEAM, recon)
        print(f"  {label:20s} aug25 {m['aug25']:>13,.0f}  dec15 {m['dec15']:>13,.0f}"
              f"  trough {m['trough_min']:>13,.0f} @{m['trough_date']}"
              f"  seam step {m['seam_step']:>+10,.0f}  landing {s['landing_pct']:+.3f}%")


def main() -> int:
    os.makedirs("research/ma-seam-turbulence/plots", exist_ok=True)
    check_variant_fidelity()
    section_splice()
    section_headline()
    section_weekday()
    section_dow_profile()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
