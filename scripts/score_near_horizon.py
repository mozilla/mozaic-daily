#!/usr/bin/env python3
"""Score a desktop forecast parquet at a near-horizon trough date (e.g. Aug-22).

The Aug-2026 desktop parameter search targets the 28-day trailing MA of the
world-headline ``legacy_desktop`` DAU at the summer trough, measured
post-headwind (display). This module computes that KPI (and the ex-CN/IR
variant, plus the Dec-15 side-effect) from a forecast parquet.

Two adjustment bases are reported for every metric:
- ``pre``  = adj-lo value as stored in the parquet (l/o overlays, no headwind).
- ``post`` = display value = ``pre`` + the linear Win10 headwind ramp at the date
  (the ramp is a level shift, applied to the 28d-MA the same way it is at the
  display layer in the canonical curves).

Two population scopes:
- ``global``   = country=ALL, os=ALL.
- ``ex_cn_ir`` = global minus CN minus IR (os=ALL) — checks the trough lift is
  not purely China (overlay ``o``) / Iran (fill) driven.

CLI
---
    source .venv/bin/activate
    python scripts/score_near_horizon.py \\
        data-official/2026-07/desktop_locked/mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from mozaic_daily.adjustments import load_forecast  # noqa: E402

DEFAULT_TARGET_DATE = "2026-08-22"
DEFAULT_DEC15 = "2026-12-15"
DEFAULT_HEADWIND = REPO_ROOT / "data-official/2026-07/adjustments/headwind.json"
TARGET_BULLSEYE = 45_060_000
TARGET_TOL = 100_000  # land within +-0.1M
OS_ALL = '{"os": "ALL"}'
MA_WINDOW = 28


def _daily_series(df: pd.DataFrame, country: str) -> pd.Series:
    """Continuous daily DAU series for (country, os=ALL), training+forecast merged."""
    sub = df[(df["country"] == country) & (df["segment"] == OS_ALL)].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    sub = sub.sort_values("target_date").set_index("target_date")["dau"].astype(float)
    return sub[~sub.index.duplicated(keep="last")]


def _headwind_ramp(date: pd.Timestamp, spec: dict) -> float:
    """Linear ramp: 0 at start_date, spec['desktop_dau'] at anchor_date (clamped)."""
    start = pd.Timestamp(spec["start_date"])
    anchor = pd.Timestamp(spec["anchor_date"])
    full = float(spec["desktop_dau"])
    if date <= start:
        return 0.0
    if date >= anchor:
        return full
    return full * (date - start).days / (anchor - start).days


def score_dataframe(
    df: pd.DataFrame,
    target_date: str = DEFAULT_TARGET_DATE,
    headwind_spec: dict | None = None,
) -> dict:
    """Pure scorer over a forecast dataframe (no file I/O).

    ``df`` must have the pipeline output columns (country, segment, target_date,
    dau). ``headwind_spec`` is the parsed headwind.json dict (linear_ramp).
    Reports global and ex-CN/IR scopes, each in pre-/post-headwind bases, at the
    target trough date and Dec-15.
    """
    spec = headwind_spec or {}

    global_series = _daily_series(df, "ALL")
    cn = _daily_series(df, "CN").reindex(global_series.index).fillna(0.0)
    ir = _daily_series(df, "IR").reindex(global_series.index).fillna(0.0)
    ex_series = global_series - cn - ir

    ma = {
        "global": global_series.rolling(MA_WINDOW).mean(),
        "ex_cn_ir": ex_series.rolling(MA_WINDOW).mean(),
    }

    out: dict = {"target_date": target_date}
    for label, date in [("target", target_date), ("dec15", DEFAULT_DEC15)]:
        d = pd.Timestamp(date)
        hw = _headwind_ramp(d, spec) if spec else 0.0
        for scope, series_ma in ma.items():
            pre = float(series_ma.loc[d])
            out[f"{scope}_{label}_pre"] = pre
            out[f"{scope}_{label}_post"] = pre + hw
        out[f"headwind_{label}"] = hw

    gt = out["global_target_post"]
    out["in_band"] = abs(gt - TARGET_BULLSEYE) <= TARGET_TOL
    out["gap_to_bullseye"] = gt - TARGET_BULLSEYE
    return out


def score_parquet(
    parquet_path: str | Path,
    target_date: str = DEFAULT_TARGET_DATE,
    headwind_spec_path: str | Path = DEFAULT_HEADWIND,
) -> dict:
    """Score a forecast parquet on disk (loads via ``load_forecast`` + headwind spec)."""
    df, _meta = load_forecast(str(parquet_path))
    spec = json.loads(Path(headwind_spec_path).read_text())
    out = score_dataframe(df, target_date=target_date, headwind_spec=spec)
    out["parquet"] = str(parquet_path)
    return out


def _fmt(v: float) -> str:
    return f"{v:,.0f}"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("parquet", type=Path)
    p.add_argument("--target-date", default=DEFAULT_TARGET_DATE)
    p.add_argument("--headwind", type=Path, default=DEFAULT_HEADWIND)
    p.add_argument("--json", action="store_true", help="Emit raw JSON instead of a table.")
    args = p.parse_args()

    r = score_parquet(args.parquet, args.target_date, args.headwind)
    if args.json:
        print(json.dumps(r, indent=2))
        return 0

    print(f"parquet     : {r['parquet']}")
    print(f"target date : {r['target_date']}   (headwind {_fmt(r['headwind_target'])})")
    print(f"{'scope':10s} {'trough pre':>16s} {'trough post':>16s} {'dec15 pre':>16s} {'dec15 post':>16s}")
    for scope in ("global", "ex_cn_ir"):
        print(f"{scope:10s} "
              f"{_fmt(r[f'{scope}_target_pre']):>16s} "
              f"{_fmt(r[f'{scope}_target_post']):>16s} "
              f"{_fmt(r[f'{scope}_dec15_pre']):>16s} "
              f"{_fmt(r[f'{scope}_dec15_post']):>16s}")
    band = "IN BAND" if r["in_band"] else "out of band"
    print(f"\nglobal trough post vs 45.06M bullseye: {r['gap_to_bullseye']:+,.0f}  [{band}, +-0.1M]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
