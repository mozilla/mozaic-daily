"""Decompose August canonical's summer miss.

The published shortfall splits three ways, and the three have different owners:

    C - A  =  (C - B)      +  (B - A_nh)   +  (A_nh - A)
    total     summer was      the model       the `h` Win10
              shallower       ran below       headwind ramp
              than typical    typical         phasing in
              [nobody's       [model error]   [a judgement call]
               fault]

where `A` is the published curve, `A_nh` is that same curve with `h` removed, and `B` is a
typical summer. The two-way `legitimate` / `illegitimate` split is the same identity with the
last two terms merged: `illegitimate = model + headwind`. Both are reported, because "the
forecast was too low" and "the headwind we chose arrived faster than reality" are not the same
finding and do not have the same remedy.

Only `h` is separable. It is display-layer — applied to the 28-day MA after mozaic — so
`published - ramp` is exact. `t` contributes exactly 0 on desktop (its spec's `desktop_dau` is
0). `l` and `o` are per-tile bidirectional overlays baked into the parquet: removing either means
re-running a locked build, so their magnitude is reported as indicative only, never as a
counterfactual.

Baselines answer different questions and all three are reported:
    seam    B(2026-08-02) == C(2026-08-02). Decomposes the AUGUST VINTAGE's own 22-day miss.
    jun15   pre-summer, Iran-clean. The whole-season excess.
    spring  the handoff's Feb15-Apr15 anchor. Iran-contaminated on the all-countries track.

Run: python research/forecast-vs-summer-actuals/analyze.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import series as S  # noqa: E402

WINDOW = pd.date_range(S.EVAL_START, S.EVAL_END, freq="D")


def _seam_baseline(vintage: str) -> tuple[str, str]:
    """A single-day baseline window at a vintage's own seam."""
    mmdd = S.VINTAGES[vintage]["seam"].strftime("%m-%d")
    return (mmdd, mmdd)
BASELINE_KINDS = ("seam", "jun15", "spring")
RESULTS_PATH = HERE / "data" / "decomposition.json"


def decompose(vintage: str, track: str, baseline: str) -> dict:
    """The three series and their two-way split, at the window's last day and on average."""
    A = S.published_forecast(vintage, track).reindex(WINDOW)
    B = S.typical_summer(track, baseline).reindex(WINDOW)
    C = S.actuals_ma(track).reindex(WINDOW)

    last = WINDOW[-1]
    return {
        "vintage": vintage,
        "track": track,
        "baseline": baseline,
        "window": [WINDOW[0].date().isoformat(), last.date().isoformat()],
        "A_last": float(A[last]),
        "B_last": float(B[last]),
        "C_last": float(C[last]),
        "miss_last": float(C[last] - A[last]),
        "legitimate_last": float(C[last] - B[last]),
        "illegitimate_last": float(B[last] - A[last]),
        "miss_mean": float((C - A).mean()),
        "legitimate_mean": float((C - B).mean()),
        "illegitimate_mean": float((B - A).mean()),
        "miss_last_pct": float((C[last] - A[last]) / A[last]),
        "seam_gap": float((C - A).iloc[0]),
    }


def decompose_three_way(vintage: str, track: str, baseline: str) -> dict:
    """Split the published shortfall into shallow-summer, model, and headwind components.

    Also reports the headwind-removed two-way split — the same legitimate/illegitimate question
    asked of the model alone, with the exogenous `h` judgement taken off the table.
    """
    A = S.published_forecast(vintage, track).reindex(WINDOW)
    A_nh = S.published_forecast_no_headwind(vintage, track).reindex(WINDOW)
    B = S.typical_summer(track, baseline).reindex(WINDOW)
    C = S.actuals_ma(track).reindex(WINDOW)
    last = WINDOW[-1]

    shallow = float(C[last] - B[last])
    model = float(B[last] - A_nh[last])
    headwind = float(A_nh[last] - A[last])
    total = float(C[last] - A[last])

    residual = abs(total - (shallow + model + headwind))
    if residual > 1.0:
        raise ValueError(
            f"{vintage}/{track}: the three components sum to {shallow + model + headwind:,.2f} "
            f"but the measured miss is {total:,.2f}. The decomposition is an identity, so a "
            f"residual of {residual:,.2f} DAU means one series is on a different basis."
        )

    return {
        "vintage": vintage,
        "track": track,
        "baseline": baseline,
        "A_last": float(A[last]),
        "A_no_headwind_last": float(A_nh[last]),
        "B_last": float(B[last]),
        "C_last": float(C[last]),
        "total_miss": total,
        "shallow_summer": shallow,
        "model_miss": model,
        "headwind": headwind,
        # The same two-way split, asked of the model alone.
        "miss_ex_headwind": float(C[last] - A_nh[last]),
        "legitimate_share_ex_headwind": shallow / float(C[last] - A_nh[last]),
        "headwind_share_of_total": headwind / total,
    }


def baked_in_overlay_levels(track: str) -> dict:
    """Indicative magnitude of the overlays this analysis CANNOT remove.

    `l` and `o` are subtracted from training before mozaic and added back after, so their realised
    effect on the forecast is config-dependent and is NOT the add-back level. These figures say
    "this is the order of magnitude in play", nothing more. Quoting them as a counterfactual would
    be wrong, and producing a real one means re-running a locked build.
    """
    return {
        code: {
            "label": S.OVERLAYS[code]["label"],
            "add_back_level_at_window_end": float(
                S.overlay_ma(code, track).reindex([S.EVAL_END]).iloc[0]
            ),
        }
        for code in S.BAKED_IN_ADJUSTMENTS
    }


def overlay_contributions(track: str, baseline: str) -> dict:
    """Size the two 2026-only level events that the 2022-2025 seasonal norm cannot contain.

    B is built from years that had neither the MozillaOnline migration nor launch-on-login, so
    whatever these add to 2026 lands inside `C - B` labelled "shallow summer" when it is really
    "level events history did not have". What counts is the GROWTH since the baseline anchor, not
    the level: B is rescaled to match 2026 at the anchor, so everything the overlays had already
    reached by then is common to both series and cancels.
    """
    out = {}
    for code in S.OVERLAYS:
        curve = S.overlay_ma(code, track)
        out[code] = {
            "label": S.OVERLAYS[code]["label"],
            "level_at_window_end": float(curve.reindex([S.EVAL_END]).iloc[0]),
            "growth_since_anchor": S.overlay_delta(code, track, baseline),
        }
    return out


def plain_ma_diagnostic() -> dict:
    """How much of August's published curve over this window is the seam splice, not the model?

    `display_ma` replaces the first 27 forecast days with a variance-matched transition, and it is
    byte-identical to a plain rolling(28) only from seam+27 (2026-08-29) onward. Every day we can
    score falls inside that transition, so the published curve and the model's own moving average
    are two different things over exactly this window.
    """
    from export_desktop_no_headwind_csv import load_desktop_headwind_ramp
    from export_desktop_ex_ir_cn_csv import (
        DESKTOP_FORECAST_PATH,
        CURRENT_ADJUSTMENTS_DIR,
        forecast_ma,
        load_country_dau,
    )

    repo = HERE.parent.parent
    pivot, _training = load_country_dau(str(repo / DESKTOP_FORECAST_PATH))
    daily = pivot["ALL"]
    seam = S.VINTAGES["august"]["seam"]

    spliced = forecast_ma(daily, seam)
    plain = S.ma28(daily)
    ramp = load_desktop_headwind_ramp(str(repo / CURRENT_ADJUSTMENTS_DIR), spliced.index, seam)

    spliced, plain = (spliced + ramp).reindex(WINDOW), (plain + ramp).reindex(WINDOW)
    return {
        "published_last": float(spliced.iloc[-1]),
        "plain_ma_last": float(plain.iloc[-1]),
        "splice_effect_last": float(spliced.iloc[-1] - plain.iloc[-1]),
        "splice_effect_mean": float((spliced - plain).mean()),
        "splice_effect_max_abs": float((spliced - plain).abs().max()),
        "splice_ends": (seam + pd.Timedelta(days=27)).date().isoformat(),
    }


def headwind_contribution() -> dict:
    """How much of August's published shortfall is the `h` ramp rather than the model.

    `h` is a display-layer linear ramp: zero at the seam, its full anchor at 2026-12-15. By the
    window's last day only a fraction has phased in, and that fraction is a deliberate exogenous
    judgement, not something Prophet produced. It belongs inside the "illegitimate" bucket, but as
    its own line — a headwind that is too heavy is a different failure from a model that drifts.
    """
    from export_desktop_no_headwind_csv import load_desktop_headwind_ramp
    from export_desktop_ex_ir_cn_csv import CURRENT_ADJUSTMENTS_DIR

    seam = S.VINTAGES["august"]["seam"]
    index = pd.date_range(seam, pd.Timestamp("2026-12-31"), freq="D")
    ramp = load_desktop_headwind_ramp(
        str(HERE.parent.parent / CURRENT_ADJUSTMENTS_DIR), index, seam
    )
    return {
        "ramp_at_window_end": float(ramp.reindex([S.EVAL_END]).iloc[0]),
        "ramp_at_anchor": float(ramp.loc["2026-12-15"]),
        "fraction_phased_in": float(ramp.reindex([S.EVAL_END]).iloc[0] / ramp.loc["2026-12-15"]),
    }


def japan_bound() -> dict:
    """An order-of-magnitude bound on how much of the summer strength could be JP automation.

    The regional-story project measures the Japan automation cohort at -1.84 points on Japan's own
    late-summer level. That factor is imported; Japan's SHARE of desktop DAU is computed here, so
    the scaling to a global number uses this analysis's own data.
    """
    frame = S.load_actuals()
    window = frame[(frame["date"] >= S.EVAL_START) & (frame["date"] <= S.EVAL_END)]
    jp = window.loc[window["country"] == "JP", "dau"].sum()
    total = window["dau"].sum()
    share = float(jp / total)
    # Imported from regional-story/site/bots.html: removing the cohort moves JP's late-summer
    # level by -1.84 points. Not re-derived here.
    JP_COHORT_POINTS = 0.0184
    return {
        "jp_share_of_dau": share,
        "jp_cohort_points_imported": JP_COHORT_POINTS,
        "implied_global_dau": float(share * JP_COHORT_POINTS * total / len(
            pd.date_range(S.EVAL_START, S.EVAL_END))),
    }


def norm_spread(track: str) -> dict:
    """How tight the 2022-2025 norm is — the honest measure of how surprising 2026 looks."""
    table = S.trend_table(track)
    norm = table.loc[list(S.NORM_YEARS), "aug23_ratio"]
    return {
        "norm_mean": float(norm.mean()),
        "norm_std": float(norm.std(ddof=1)),
        "norm_min": float(norm.min()),
        "norm_max": float(norm.max()),
        "value_2026": float(table.loc[2026, "aug23_ratio"]),
        "z_score": float((table.loc[2026, "aug23_ratio"] - norm.mean()) / norm.std(ddof=1)),
        "trough_reached": bool(
            S.actuals_ma(track).dropna().index.max()
            > pd.Timestamp(f"2026-{table.loc[2026, 'trough_date'][5:]}") + pd.Timedelta(days=4)
        ),
    }


def build() -> dict:
    results = {
        "eval_window": [S.EVAL_START.date().isoformat(), S.EVAL_END.date().isoformat()],
        "trend": {t: S.trend_table(t).reset_index().to_dict("records") for t in S.TRACKS},
        "decompositions": [
            decompose("august", track, baseline)
            for track in S.TRACKS
            for baseline in BASELINE_KINDS
        ],
        # Each vintage is anchored at ITS OWN seam, so "illegitimate" always means "below a
        # typical trajectory measured from the point that forecast actually started".
        "vintage_ladder": [
            decompose(v, "all", _seam_baseline(v)) | {"vintage": v}
            for v in ("august", "july", "june")
        ],
        "overlays": {
            t: {b: overlay_contributions(t, b) for b in BASELINE_KINDS} for t in S.TRACKS
        },
        "three_way": [
            decompose_three_way("august", track, "seam") for track in S.TRACKS
        ],
        "three_way_ladder": [
            decompose_three_way(v, "all", _seam_baseline(v))
            for v in ("august", "july", "june")
        ],
        "baked_in": {t: baked_in_overlay_levels(t) for t in S.TRACKS},
        "splice": plain_ma_diagnostic(),
        "headwind": headwind_contribution(),
        "japan": japan_bound(),
        "norm_spread": {t: norm_spread(t) for t in S.TRACKS},
    }
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_PATH.write_text(json.dumps(results, indent=2, default=str) + "\n")
    return results


def _fmt(v: float) -> str:
    return f"{v:>+12,.0f}"


def main() -> None:
    r = build()

    print(f"\nEvaluation window: {r['eval_window'][0]} .. {r['eval_window'][1]}  (22 days)")
    print("Desktop only, legacy telemetry, 28-day trailing MA, published (`h` applied) curves.\n")

    for track in S.TRACKS:
        print(f"\n{'='*88}\n  AUGUST CANONICAL — {track}\n{'='*88}")
        print(f"{'baseline':<10} {'A (fcst)':>13} {'B (typical)':>13} {'C (actual)':>13} "
              f"{'miss C-A':>13} {'legit C-B':>13} {'illegit B-A':>13}")
        for row in r["decompositions"]:
            if row["track"] != track:
                continue
            print(f"{row['baseline']:<10} {row['A_last']:>13,.0f} {row['B_last']:>13,.0f} "
                  f"{row['C_last']:>13,.0f} {_fmt(row['miss_last'])} "
                  f"{_fmt(row['legitimate_last'])} {_fmt(row['illegitimate_last'])}")
        print(f"\n  2026-only level events sitting inside `legit C-B`, as GROWTH since each")
        print(f"  baseline's anchor (28d MA, through {r['eval_window'][1]}):")
        print(f"    {'':<32}" + "".join(f"{b:>14}" for b in BASELINE_KINDS))
        for code in S.OVERLAYS:
            cells = "".join(
                f"{r['overlays'][track][b][code]['growth_since_anchor']:>14,.0f}"
                for b in BASELINE_KINDS
            )
            print(f"    `{code}` {S.OVERLAYS[code]['label']:<27}" + cells)
        cells = "".join(
            f"{sum(r['overlays'][track][b][c]['growth_since_anchor'] for c in S.OVERLAYS):>14,.0f}"
            for b in BASELINE_KINDS
        )
        print(f"    {'combined':<32}" + cells)

    print(f"\n\n{'='*88}\n  VINTAGE LADDER (all countries; each B anchored at that vintage's OWN "
          f"seam)\n{'='*88}")
    print(f"{'vintage':<10} {'seam':>12} {'miss C-A':>13} {'legit C-B':>13} {'illegit B-A':>13}")
    for row in r["vintage_ladder"]:
        seam = S.VINTAGES[row["vintage"]]["seam"].date().isoformat()
        print(f"{row['vintage']:<10} {seam:>12} {_fmt(row['miss_last'])} "
              f"{_fmt(row['legitimate_last'])} {_fmt(row['illegitimate_last'])}")

    s = r["splice"]
    print(f"\n\n{'='*88}\n  SPLICE DIAGNOSTIC — published curve vs the model's plain 28d MA"
          f"\n{'='*88}")
    print(f"  splice runs through {s['splice_ends']}; the whole eval window is inside it")
    print(f"  published at window end   {s['published_last']:>13,.0f}")
    print(f"  plain rolling(28)         {s['plain_ma_last']:>13,.0f}")
    print(f"  splice effect (last/mean/max|.|) {s['splice_effect_last']:>+11,.0f} "
          f"{s['splice_effect_mean']:>+11,.0f} {s['splice_effect_max_abs']:>11,.0f}")
    print(f"\n\n{'='*88}\n  THREE-WAY DECOMPOSITION — August canonical, seam-anchored"
          f"\n{'='*88}")
    print(f"{'track':<10} {'total miss':>13} {'shallow summer':>16} {'model miss':>13} "
          f"{'`h` headwind':>14}")
    for d in r["three_way"]:
        print(f"{d['track']:<10} {_fmt(d['total_miss'])} {d['shallow_summer']:>+16,.0f} "
              f"{_fmt(d['model_miss'])} {d['headwind']:>+14,.0f}")
    print(f"\n  Same numbers as shares of the published miss:")
    for d in r["three_way"]:
        t = d["total_miss"]
        print(f"    {d['track']:<10} shallow {d['shallow_summer']/t:>6.1%}   "
              f"model {d['model_miss']/t:>6.1%}   headwind {d['headwind']/t:>6.1%}")
    print(f"\n  With `h` removed, the legitimate/illegitimate split of the MODEL's own miss:")
    for d in r["three_way"]:
        print(f"    {d['track']:<10} miss {d['miss_ex_headwind']:>+11,.0f}  =  "
              f"legit {d['shallow_summer']:>+11,.0f} ({d['legitimate_share_ex_headwind']:.1%})"
              f"  +  illegit {d['model_miss']:>+11,.0f}")

    print(f"\n\n{'='*88}\n  THREE-WAY LADDER (all countries, each B at its own seam)\n{'='*88}")
    print(f"{'vintage':<9} {'seam':>11} {'total':>12} {'shallow':>12} {'model':>12} "
          f"{'`h`':>12} {'`h` share':>10}")
    for d in r["three_way_ladder"]:
        seam = S.VINTAGES[d["vintage"]]["seam"].date().isoformat()
        print(f"{d['vintage']:<9} {seam:>11} {d['total_miss']:>+12,.0f} "
              f"{d['shallow_summer']:>+12,.0f} {d['model_miss']:>+12,.0f} "
              f"{d['headwind']:>+12,.0f} {d['headwind_share_of_total']:>10.1%}")

    print(f"\n  NOT separable (baked into the parquet; add-back level only, not a "
          f"counterfactual):")
    for track, codes in r["baked_in"].items():
        cells = "  ".join(
            f"`{c}` {d['add_back_level_at_window_end']:>9,.0f}" for c, d in codes.items()
        )
        print(f"    {track:<10} {cells}")

    h = r["headwind"]
    print(f"\n\n{'='*88}\n  HOW MUCH OF THE SHORTFALL IS THE `h` RAMP, NOT THE MODEL"
          f"\n{'='*88}")
    print(f"  `h` anchor at 2026-12-15      {h['ramp_at_anchor']:>13,.0f}")
    print(f"  phased in by {r['eval_window'][1]}      {h['ramp_at_window_end']:>13,.0f}"
          f"   ({h['fraction_phased_in']:.1%} of the anchor)")
    for row in r["decompositions"]:
        if row["baseline"] == "seam":
            model = row["illegitimate_last"] + h["ramp_at_window_end"]
            print(f"  {row['track']:<10} illegitimate {row['illegitimate_last']:>+12,.0f}"
                  f"  =  `h` ramp {h['ramp_at_window_end']:>+11,.0f}"
                  f"  +  model {model:>+12,.0f}")

    print(f"\n\n{'='*88}\n  HOW SURPRISING IS 2026?\n{'='*88}")
    for track, d in r["norm_spread"].items():
        print(f"  {track:<10} 2026 {d['value_2026']:.4f}  vs norm {d['norm_mean']:.4f}"
              f" +/- {d['norm_std']:.4f}  (range {d['norm_min']:.4f}-{d['norm_max']:.4f})"
              f"  z = {d['z_score']:+.2f}")

    j = r["japan"]
    print(f"\n  Japan bound: JP is {j['jp_share_of_dau']:.2%} of desktop DAU; an imported "
          f"-{j['jp_cohort_points_imported']:.2%}\n  cohort effect on JP scales to "
          f"~{j['implied_global_dau']:,.0f} DAU globally.")

    print(f"\nWrote {RESULTS_PATH}")


if __name__ == "__main__":
    main()
