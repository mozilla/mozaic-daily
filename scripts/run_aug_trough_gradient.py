#!/usr/bin/env python3
"""Aug-2026 desktop near-horizon parameter search — finite-difference gradient.

Targets the 28-day trailing MA of world-headline ``legacy_desktop`` DAU at the
summer trough (2026-08-22), measured post-headwind (display). Bullseye 45.06M,
land within +-0.1M. Same anchor 2026-07-06, same cached raw data, desktop only.

This is the near-horizon counterpart to ``run_desktop_gradient.py`` (which targets
Dec-15). Differences: the search **center is the current locked July config**
(not package defaults), it sweeps ``seasonality_prior_scale`` (the yearly-seasonality
magnitude lever exposed for this search), it drops the holiday knobs (negligible for
an August trough), and it **scores every probe inline** via ``score_near_horizon``
(Global + ex-CN/IR, pre/post headwind, + Dec-15 side-effect) into a results CSV —
no separate notebook pass needed to see the slopes.

Round 1 = one-at-a-time +-delta around the locked center to estimate local slopes,
plus one far ``sps`` point (2.0) as a live-knob / nonlinearity gauge. Later rounds
(``--round 2+``) descend along the strongest levers; probe sets are added as we go.

Usage
-----
    source .venv/bin/activate
    python scripts/run_aug_trough_gradient.py --round 1 \\
        --raw-cache-dir research/param-scans/aug22-retune/_rawcache \\
        --parallel 4
    # preview the probe list without running:
    python scripts/run_aug_trough_gradient.py --round 1 --dry-run
"""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

RUN_PARAM_SCAN = REPO_ROOT / "scripts" / "run_param_scan.py"
RESULTS_ROOT = REPO_ROOT / "research/param-scans/aug22-retune"
FORECAST_START = "2026-07-06"

from score_near_horizon import score_parquet  # noqa: E402  (scripts/ on path via sibling import)

# DesktopModelConfig field -> run_param_scan.py CLI flag.
FLAG_MAP = {
    "prophet_changepoint_prior_scale": "--changepoint-prior-scale",
    "prophet_recent_weeks": "--recent-weeks",
    "prophet_changepoint_range": "--changepoint-range",
    "prophet_n_changepoints": "--n-changepoints",
    "prophet_seasonality_prior_scale": "--seasonality-prior-scale",
    "seasonality_regime": "--seasonality-regime",
}

# The current locked July desktop config = the finite-difference CENTER.
CPS = "prophet_changepoint_prior_scale"
CPR = "prophet_changepoint_range"
RW = "prophet_recent_weeks"
NCP = "prophet_n_changepoints"
SPS = "prophet_seasonality_prior_scale"

CENTER = {CPS: 0.08983, CPR: 0.65, RW: 13, NCP: 25, SPS: 0.00825}

# Round 1: one-at-a-time +-delta around CENTER (+ one far sps point).
# CAVEAT (Brendan, 2026-07-10): these are MEDIUM deltas, not small ones
# (cps +-17%, recent_weeks +-23%, ncp +-20%, sps x3 on a log step). So the
# per-knob slope reported here is a SECANT over a medium interval, not the true
# local derivative at center; for a nonlinear response it blends slope with
# curvature. Fine for direction-finding this round; tighten deltas if a knob's
# local behavior near center matters.
ROUND1 = [
    ("center", {}),
    ("cps__0.075", {CPS: 0.075}),
    ("cps__0.105", {CPS: 0.105}),
    ("cpr__0.60", {CPR: 0.60}),
    ("cpr__0.70", {CPR: 0.70}),
    ("recent__10", {RW: 10}),
    ("recent__16", {RW: 16}),
    ("ncp__20", {NCP: 20}),
    ("ncp__30", {NCP: 30}),
    ("sps__0.003", {SPS: 0.003}),
    ("sps__0.025", {SPS: 0.025}),
    ("sps__2.0", {SPS: 2.0}),   # live-knob check + nonlinearity gauge
]


REG = "seasonality_regime"

# Round 2: SHAPE/asymmetry test. sps (amplitude) can't lift the summer trough without
# dropping the Dec peak (Round 1), and Dec-15 must hold within 10k. seasonality_regime
# changes seasonal SHAPE: multiplicative scales the swing with trend level (higher into
# Dec), which should lift the summer trough more than it drops the winter peak. Sweep
# forced regimes x sps and tabulate the Aug/Dec frontier vs additive/auto references.
ROUND2 = [
    ("auto__sps00825", {}),                                  # baseline (= center)
    ("auto__sps003", {SPS: 0.003}),                          # auto low-sps reference
    ("mult__sps00825", {REG: "multiplicative"}),
    ("mult__sps005", {REG: "multiplicative", SPS: 0.05}),
    ("mult__sps05", {REG: "multiplicative", SPS: 0.5}),
    ("mult__sps2", {REG: "multiplicative", SPS: 2.0}),
    ("mult__sps10", {REG: "multiplicative", SPS: 10.0}),
    ("add__sps00825", {REG: "additive"}),
    ("add__sps003", {REG: "additive", SPS: 0.003}),
]


MULT = "multiplicative"

# Round 3: trend knobs UNDER multiplicative. Round 2 fixed regime=multiplicative (Aug 44.54M,
# Dec +193K, sps inert). Multiplicative -> linear growth (logistic cap OFF), so cps/cpr may have
# more near-horizon leverage than under auto's cap (Round 1). One-at-a-time sweep to find which
# knob adds the remaining +0.52M on Aug and/or trims the +193K Dec. All probes hold regime=mult.
ROUND3 = [
    ("mult_center", {REG: MULT}),
    ("mult_cps005", {REG: MULT, CPS: 0.05}),
    ("mult_cps12", {REG: MULT, CPS: 0.12}),
    ("mult_cps15983", {REG: MULT, CPS: 0.15983}),
    ("mult_cps20", {REG: MULT, CPS: 0.20}),
    ("mult_cps30", {REG: MULT, CPS: 0.30}),
    ("mult_cpr055", {REG: MULT, CPR: 0.55}),
    ("mult_cpr060", {REG: MULT, CPR: 0.60}),
    ("mult_cpr070", {REG: MULT, CPR: 0.70}),
    ("mult_cpr080", {REG: MULT, CPR: 0.80}),
    ("mult_cpr090", {REG: MULT, CPR: 0.90}),
    ("mult_ncp20", {REG: MULT, NCP: 20}),
    ("mult_ncp35", {REG: MULT, NCP: 35}),
    ("mult_recent8", {REG: MULT, RW: 8}),
    ("mult_recent20", {REG: MULT, RW: 20}),
]


def build_probes(round_num: int) -> list[tuple[str, dict]]:
    if round_num == 1:
        return ROUND1
    if round_num == 2:
        return ROUND2
    if round_num == 3:
        return ROUND3
    raise ValueError(f"round {round_num} not defined yet (add its probe set)")


# --- LHS sampling of the extended multiplicative bounding box (post-gradient) ---
# The feasible top-10 (by Aug@hold) are all multiplicative and cluster in a tight ~44.5M
# plateau; sample an EXTENDED box (esp. high cpr/ncp where Aug@hold peaks, low cpr for
# completeness) to map the frontier. regime=multiplicative and sps fixed (inert under mult).
SAMPLE_BOX = [
    (CPS, 0.03, 0.30, "float"),
    (CPR, 0.50, 0.95, "float"),
    (RW, 8, 20, "int"),
    (NCP, 18, 40, "int"),
]


def build_samples(n: int, seed: int = 42) -> list[tuple[str, dict]]:
    """Latin-hypercube samples over SAMPLE_BOX, each forced to regime=multiplicative."""
    import numpy as np
    rng = np.random.default_rng(seed)
    d = len(SAMPLE_BOX)
    u = np.empty((n, d))
    for j in range(d):
        u[:, j] = (rng.permutation(n) + rng.random(n)) / n  # 1 sample per stratum per dim
    probes = []
    for i in range(n):
        ov = {"seasonality_regime": "multiplicative"}
        for j, (name, lo, hi, typ) in enumerate(SAMPLE_BOX):
            v = lo + u[i, j] * (hi - lo)
            ov[name] = int(round(v)) if typ == "int" else round(float(v), 4)
        probes.append((f"s{i:02d}", ov))
    return probes


def _config_for(overrides: dict) -> dict:
    """Full config = CENTER with per-probe overrides applied."""
    cfg = dict(CENTER)
    cfg.update(overrides)
    return cfg


def _adj_lo_parquet(results_dir: Path) -> Path | None:
    hits = list(results_dir.glob("*/mozaic_daily_forecast.*.ld-D.adj-*.parquet")) or \
        list(results_dir.glob("*/mozaic_daily_forecast.*.ld-D.raw.parquet"))
    return hits[0] if hits else None


def run_probe(round_dir: Path, raw_cache_dir: Path, label: str,
              overrides: dict, force: bool) -> tuple[str, str, float]:
    results_dir = round_dir / label
    if not force and _adj_lo_parquet(results_dir) is not None:
        return (label, "skipped (exists)", 0.0)
    results_dir.mkdir(parents=True, exist_ok=True)

    cfg = _config_for(overrides)
    cmd = [sys.executable, str(RUN_PARAM_SCAN),
           "--forecast-start-date", FORECAST_START,
           "--results-dir", str(results_dir),
           "--raw-cache-dir", str(raw_cache_dir)]
    for field, value in cfg.items():
        cmd += [FLAG_MAP[field], str(value)]

    log_path = results_dir / "run.log"
    start = time.monotonic()
    with open(log_path, "w") as log:
        log.write("$ " + " ".join(cmd) + "\n\n")
        log.flush()
        proc = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT)
    elapsed = time.monotonic() - start
    status = "ok" if proc.returncode == 0 else f"FAILED (rc={proc.returncode}, see {log_path})"
    return (label, status, elapsed)


def score_round(round_dir: Path, probes: list[tuple[str, dict]]) -> list[dict]:
    rows = []
    for label, overrides in probes:
        parquet = _adj_lo_parquet(round_dir / label)
        if parquet is None:
            rows.append({"label": label, "error": "no parquet"})
            continue
        s = score_parquet(parquet)
        cfg = _config_for(overrides)
        rows.append({
            "label": label,
            "regime": cfg.get(REG, "auto"),
            "cps": cfg[CPS], "cpr": cfg[CPR], "recent": cfg[RW],
            "ncp": cfg[NCP], "sps": cfg[SPS],
            "global_trough_post": round(s["global_target_post"]),
            "global_trough_pre": round(s["global_target_pre"]),
            "ex_cn_ir_trough_post": round(s["ex_cn_ir_target_post"]),
            "dec15_post": round(s["global_dec15_post"]),
            "gap_to_bullseye": round(s["gap_to_bullseye"]),
            "dec15_delta": round(s["global_dec15_post"] - 48_585_483),
        })
    return rows


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--round", type=int, default=1)
    p.add_argument("--sample", type=int, default=None,
                   help="LHS-sample N configs over the extended multiplicative box (overrides --round).")
    p.add_argument("--raw-cache-dir", type=Path,
                   default=RESULTS_ROOT / "_rawcache")
    p.add_argument("--parallel", type=int, default=4)
    p.add_argument("--force", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--score-only", action="store_true",
                   help="Skip running; just re-score existing probe outputs.")
    args = p.parse_args()

    if args.sample:
        probes = build_samples(args.sample)
        round_dir = RESULTS_ROOT / "sampling"
        tag = f"sampling ({args.sample} LHS, extended multiplicative box)"
    else:
        probes = build_probes(args.round)
        round_dir = RESULTS_ROOT / f"round{args.round}"
        tag = f"gradient round {args.round}"

    print("=" * 72)
    print(f"Aug-trough {tag} — {len(probes)} probes")
    print(f"Center (locked): cps=0.08983 cpr=0.65 recent=13 ncp=25 sps=0.00825 regime=auto")
    print(f"Target: Global trough(2026-08-22) post-headwind 28d-MA -> 45.06M +-0.1M")
    print(f"Raw cache: {args.raw_cache_dir}")
    print(f"Round dir: {round_dir}")
    print("=" * 72)
    for label, ov in probes:
        desc = ", ".join(f"{k.split('_')[-1]}={v}" for k, v in ov.items()) or "(center: locked)"
        print(f"  {label:16s} {desc}")

    if args.dry_run:
        print("\n[dry-run] Nothing executed.")
        return 0

    if not args.raw_cache_dir.joinpath("mozaic_parts.raw.legacy.desktop.DAU.parquet").exists():
        print(f"\nERROR: raw cache missing at {args.raw_cache_dir}. Fetch it first.")
        return 2

    if not args.score_only:
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            futures = {pool.submit(run_probe, round_dir, args.raw_cache_dir,
                                   label, ov, args.force): label
                       for label, ov in probes}
            for i, fut in enumerate(as_completed(futures), 1):
                label, status, elapsed = fut.result()
                wall = (time.monotonic() - started) / 60
                print(f"[{i}/{len(futures)}] {label:16s} {status:30s} {elapsed:6.1f}s (wall {wall:4.1f}m)")

    rows = score_round(round_dir, probes)
    csv_path = round_dir / ("sampling_scores.csv" if args.sample else f"round{args.round}_scores.csv")
    if rows and "error" not in rows[0]:
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\nWrote {csv_path}")

    print(f"\n{'label':16s} {'regime':14s} {'trough_post':>13s} {'gap':>11s} "
          f"{'dec15_post':>13s} {'dec15_Δ':>11s}")
    for r in rows:
        if "error" in r:
            print(f"  {r['label']:16s} {r['error']}")
            continue
        print(f"  {r['label']:16s} {r['regime']:14s} {r['global_trough_post']:>13,} "
              f"{r['gap_to_bullseye']:>+11,} {r['dec15_post']:>13,} {r['dec15_delta']:>+11,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
