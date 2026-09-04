#!/usr/bin/env python3
"""Score an August-2026 mobile (`glean_mobile` DAU, `adj-p`) forecast parquet.

The scored KPI is a single number: the **ALL-MOBILE, world, Dec-15 2026 28-day-MA,
post-headwind**, computed exactly the way `august_canonical_v2026-07-28.ipynb` computes
it. Everything else this module reports is diagnostic.

Target
------
**17,923,869 +- 50,000** — July's delivered mobile Dec-15. The August build currently
reads 17,601,155, so the gap is **+322,714**.

That gap was created deliberately on 2026-07-31 by the `m` -> `p` methodology swap and is
being closed as a **calibration** step: `p` is the more robust mechanism, and the search
re-fits the model under it so the headline matches the previously verified July result.
The trade-off is explicit and must stay visible in any report — `m` let Prophet extrapolate
a +16.12%/yr TOTAL growth rate as organic, `p` extrapolates the measured +11.60%/yr ORGANIC
rate, and lifting Dec-15 by +322,714 necessarily pushes the fitted organic trend back above
the measured rate. `organic_cagr` is reported on every probe so the size of that push is
never implicit. See `data-official/2026-08/organic/_index.md`.

Why parameters have more leverage here than the July scan's slopes suggest
--------------------------------------------------------------------------
Under `p`, paid DAU is a **level** stacked on after mozaic: 922,250 (anchor) + 637,227
(marketing's Dec-15 lift) = ~1,559,477, with zero Prophet interaction. The model therefore
controls only the ~16.07M organic remainder, and the whole +322,714 must come from it
(**+2.01% on organic**). But the add-back is additive, so a change to the model's curve
reaches the headline **1:1** — unlike the retired `m` overlay, whose bidirectional
subtract-then-add-back absorbed 58% of any curve change. July's `adj-m` slopes are
consequently NOT transferable to this cycle.

Seam diagnostics are REPORTED, NOT SCORED
-----------------------------------------
`seam_*` quantities measure how the forecast hands off to actuals at 2026-07-28. They are
deliberately excluded from `gap_to_target` and from any selection rule: this search is
one-dimensional on Dec-15 by instruction. They exist so a config that hits Dec-15 by
mangling the handoff is visible rather than silent.

Actuals come from the parquet's own `training` rows, not BigQuery. Under `p` the training
rows have the **measured** paid added back, which restores them to raw actuals exactly;
`scripts/verify_training_rows_are_actuals.py` is the check that enforces this. That saves
~1TB of scan per probe.

CLI
---
    source .venv/bin/activate
    python scripts/mobile_scoring.py \\
        data-official/2026-08/mobile_organic_2026-07-28/\\
cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/\\
mozaic_daily_forecast.2026-07-28.gm-D.adj-p.parquet

`FORECAST_START` and `DEFAULT_HEADWIND` are **cycle-scoped** and must be repointed at each
roll-forward. A stale headwind spec mis-scores silently — no error, just wrong numbers.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from mozaic_daily.seam_ma import display_ma  # noqa: E402

# --- Cycle-scoped constants (repoint every roll-forward) ---------------------------------
FORECAST_START = "2026-09-02"  # repointed 2026-09-04
DEC15 = "2026-12-15"
# September: the mobile headwind leg is its own spec (`u`); headwind.json is desktop-only from 2026-09.
DEFAULT_HEADWIND = REPO_ROOT / "data-official/2026-09/adjustments/tou_mobile_headwind.json"

#: July's delivered mobile Dec-15 28d-MA. The calibration target for the August search.
TARGET_DEC15 = 17_924_562  # August delivered mobile Dec-15 (repointed 2026-09-04)
#: Half-width of the acceptance band, per the search brief.
TOLERANCE = 50_000

# --- Fixed selectors ---------------------------------------------------------------------
MA_WINDOW = 28
#: Mobile's world headline row. Note the segment is "{}" (empty JSON), NOT '{"os": "ALL"}' —
#: mobile has no OS segmentation, and using desktop's selector silently returns nothing.
MOBILE_SEGMENT = "{}"
MOBILE_APP = "ALL MOBILE"
#: Days either side of the seam used to fit the reported slope match.
SEAM_SLOPE_WINDOW = 14


def load_headwind(path: Path | str | None = None) -> dict:
    """Parse a headwind.json spec. Returns {} when path is None (score pre-headwind)."""
    if path is None:
        return {}
    with open(path) as handle:
        return json.load(handle)


def headwind_ramp(date: pd.Timestamp, spec: dict) -> float:
    """Linear ramp: 0 at start_date, spec['mobile_dau'] at anchor_date, clamped after.

    Mobile reads ``mobile_dau`` (-27,162 this cycle), not ``desktop_dau``. Reading the wrong
    key would apply desktop's -1,220,000 to a 17M series, so it is not defaulted.
    """
    if not spec:
        return 0.0
    start = pd.Timestamp(spec["start_date"])
    anchor = pd.Timestamp(spec["anchor_date"])
    full = float(spec["mobile_dau"])
    if date <= start:
        return 0.0
    if date >= anchor:
        return full
    return full * (date - start).days / (anchor - start).days


def mobile_daily_series(df: pd.DataFrame) -> pd.DataFrame:
    """Extract the world ALL-MOBILE daily DAU series with its data_type labels."""
    mask = (
        (df["country"] == "ALL")
        & (df["segment"] == MOBILE_SEGMENT)
        & (df["data_source"] == "glean_mobile")
        & (df["app_name"] == MOBILE_APP)
    )
    out = df.loc[mask, ["target_date", "dau", "data_type"]].copy()
    if out.empty:
        raise ValueError(
            f"No ALL-MOBILE world rows matched. Checked country='ALL', "
            f"segment={MOBILE_SEGMENT!r}, data_source='glean_mobile', app_name={MOBILE_APP!r}. "
            f"Frame has {len(df)} rows, segments={sorted(df['segment'].unique())[:5]}."
        )
    out["target_date"] = pd.to_datetime(out["target_date"])
    return out.sort_values("target_date").reset_index(drop=True)


def seam_diagnostics(
    daily: pd.DataFrame,
    forecast_ma: pd.Series,
    forecast_start: pd.Timestamp,
    window: int = SEAM_SLOPE_WINDOW,
) -> dict:
    """Actuals-vs-forecast handoff at the seam. REPORTED, never scored.

    Two independent things can go wrong at a seam, so both are measured:

    - ``seam_step``: the level discontinuity, forecast 28d-MA on the first forecast day minus
      the actuals' own plain 28d-MA on the last training day. Under `p` a small positive step
      is EXPECTED by construction (~+0.24% of total): training rows carry *measured* paid while
      forecast rows carry *marketing's* paid level, and the two disagree at the seam. That
      component is a methodology artifact, not a model defect.
    - ``seam_slope_kink``: change in first derivative across the seam, OLS over ``window`` days
      each side. This is the part parameters actually move.

    Actuals are the parquet's own training rows (byte-identical to raw actuals under `p`).
    """
    actual = daily[daily["data_type"] == "training"].set_index("target_date")["dau"].astype(float)
    actual_ma = actual.rolling(MA_WINDOW).mean()

    last_training = actual.index.max()
    step = float(forecast_ma.loc[forecast_start]) - float(actual_ma.loc[last_training])

    def slope(series: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> float:
        seg = series.loc[start:end].dropna()
        if len(seg) < 3:
            return float("nan")
        days = (seg.index - seg.index[0]).days.to_numpy(dtype=float)
        return float(np.polyfit(days, seg.to_numpy(dtype=float), 1)[0])

    before = slope(actual_ma, last_training - pd.Timedelta(days=window - 1), last_training)
    after = slope(forecast_ma, forecast_start, forecast_start + pd.Timedelta(days=window - 1))

    return {
        "seam_actual_ma": float(actual_ma.loc[last_training]),
        "seam_forecast_ma": float(forecast_ma.loc[forecast_start]),
        "seam_step": step,
        "seam_step_pct": step / float(actual_ma.loc[last_training]) * 100.0,
        "seam_slope_before": before,
        "seam_slope_after": after,
        "seam_slope_kink": after - before,
        "last_training_date": str(last_training.date()),
    }


def growth_diagnostics(daily: pd.DataFrame, forecast_ma: pd.Series,
                       forecast_start: pd.Timestamp) -> dict:
    """How hard a config pushes the trend up. Reported so the calibration's cost is explicit.

    Two measures, because the obvious one is confounded:

    - ``yoy_dec15_pct`` — Dec-15-2026 forecast 28d-MA against Dec-15-**2025** actuals 28d-MA.
      Calendar-aligned, so seasonality cancels. This is the honest read, and the one to compare
      against the +11.60%/yr measured organic rate. It is a TOTAL rate (organic + the flat paid
      level), and because paid is held flat past the marketing curve's end it sits slightly
      below the implied organic rate rather than above it.
    - ``seam_to_dec15_pct`` — raw (not annualised) growth from the seam to Dec-15. NOT a trend
      estimate: the seam sits at the top of the summer trough, so this reads several points low
      on any config. Useful only as a relative shape comparator across probes.
    """
    seam_value = float(forecast_ma.loc[forecast_start])
    dec15_value = float(forecast_ma.loc[pd.Timestamp(DEC15)])

    actual = daily[daily["data_type"] == "training"].set_index("target_date")["dau"].astype(float)
    prior_dec15 = pd.Timestamp(DEC15) - pd.DateOffset(years=1)
    prior_ma = actual.rolling(MA_WINDOW).mean().get(prior_dec15, float("nan"))

    return {
        "yoy_dec15_pct": (dec15_value / prior_ma - 1.0) * 100.0,
        "prior_dec15_ma": float(prior_ma),
        "seam_to_dec15_pct": (dec15_value / seam_value - 1.0) * 100.0,
    }


def score_dataframe(
    df: pd.DataFrame,
    headwind_spec: dict | None = None,
    forecast_start: str | pd.Timestamp | None = None,
) -> dict:
    """Pure scorer over a mobile forecast dataframe (no file I/O).

    ``df`` must carry the pipeline output columns. ``headwind_spec`` is the parsed
    headwind.json dict; pass ``{}``/None to score pre-headwind.
    """
    if forecast_start is None:
        forecast_start = pd.Timestamp(df["forecast_start_date"].iloc[0])
    forecast_start = pd.Timestamp(forecast_start)
    spec = headwind_spec or {}

    daily = mobile_daily_series(df)
    forecast_ma = display_ma(daily["target_date"], daily["dau"],
                             forecast_start, window=MA_WINDOW)

    dec15 = pd.Timestamp(DEC15)
    pre = float(forecast_ma.loc[dec15])
    headwind = headwind_ramp(dec15, spec)
    post = pre + headwind

    result = {
        "dec15_pre": pre,
        "headwind_dec15": headwind,
        "dec15_post": post,
        "gap_to_target": post - TARGET_DEC15,
        "in_band": abs(post - TARGET_DEC15) <= TOLERANCE,
    }
    result.update(growth_diagnostics(daily, forecast_ma, forecast_start))
    result.update(seam_diagnostics(daily, forecast_ma, forecast_start))
    return result


def score_forecast(path: Path | str, headwind_path: Path | str | None = DEFAULT_HEADWIND) -> dict:
    """Load an `adj-p` mobile parquet and score it.

    ``require_state=["p"]`` is the guard against the date-gate trap: a run whose
    `organic.json` did not match its forecast start emits `.raw.` and would otherwise be
    scored as though the paid level were present.
    """
    df, meta = load_forecast(str(path), require_state=["p"])
    scores = score_dataframe(df, load_headwind(headwind_path))
    scores["slug"] = Path(path).parent.name
    scores["config"] = meta.get("model_config", {})
    return scores


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("parquet", type=Path, help="Path to a .gm-D.adj-p.parquet")
    parser.add_argument("--headwind", type=Path, default=DEFAULT_HEADWIND,
                        help=f"Headwind spec (default: {DEFAULT_HEADWIND})")
    parser.add_argument("--no-headwind", action="store_true",
                        help="Score pre-headwind (skip the display-layer ramp)")
    args = parser.parse_args()

    scores = score_forecast(args.parquet, None if args.no_headwind else args.headwind)

    print(f"slug                 {scores['slug']}")
    print(f"Dec-15 pre-headwind  {scores['dec15_pre']:>14,.0f}")
    print(f"headwind at Dec-15   {scores['headwind_dec15']:>14,.0f}")
    print(f"Dec-15 post-headwind {scores['dec15_post']:>14,.0f}   <-- SCORED KPI")
    print(f"target               {TARGET_DEC15:>14,.0f}  +-{TOLERANCE:,}")
    print(f"gap to target        {scores['gap_to_target']:>+14,.0f}"
          f"   {'IN BAND' if scores['in_band'] else 'out of band'}")
    print(f"\nYoY at Dec-15        {scores['yoy_dec15_pct']:>13.2f}%  "
          f"(vs Dec-15-2025 actuals {scores['prior_dec15_ma']:,.0f}; "
          f"measured organic rate +11.60%/yr)")
    print(f"seam->Dec-15 (raw)   {scores['seam_to_dec15_pct']:>13.2f}%  "
          f"(shape comparator only — the seam sits at the summer trough)")
    print("\nSeam handoff (reported, NOT scored):")
    print(f"  actuals 28d-MA @ {scores['last_training_date']}  {scores['seam_actual_ma']:>14,.0f}")
    print(f"  forecast 28d-MA @ {FORECAST_START}     {scores['seam_forecast_ma']:>14,.0f}")
    print(f"  step                              {scores['seam_step']:>+14,.0f}"
          f"  ({scores['seam_step_pct']:+.2f}%)")
    print(f"  slope before / after (DAU/day)    {scores['seam_slope_before']:>+9,.0f} / "
          f"{scores['seam_slope_after']:>+9,.0f}")
    print(f"  slope kink                        {scores['seam_slope_kink']:>+14,.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
