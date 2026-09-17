#!/usr/bin/env python3
"""Export a cycle's adjustment effects as tracked CSVs, from the combinatorics manifest's runs.

The combinatorics build forecasts every subset of the cycle's model-run adjustments (desktop
per-tile overlays; ``p`` on mobile) and leaves the runs as gitignored parquets. This script turns
them into four small **tracked** files under ``data-official/<cycle>/adjustment_combinatorics/`` so
a future cycle can answer "what did X add, and how did that change" without pulling anything back
from GCS:

* ``adjustment_subsets.csv``           -- the fact table: one row per subset per platform, Dec-15
                                          28d-MA without and with the display layer, run parquet.
* ``adjustment_effects.csv``           -- one row per code per platform: nominal Dec-15 (the curve's
                                          own value), the single / marginal / Shapley realized views,
                                          pass-through ratios, overlay fingerprint. Display-layer
                                          codes (``h``, ``t``, ``u``) appear exact, pass-through 1.
* ``adjustment_curves_28ma.csv``       -- long: world 28d-MA per subset from the seam to the end of
                                          the horizon, without and with the display layer.
* ``adjustment_dec15_by_country.csv``  -- long: per-country Dec-15 28d-MA per subset (no display layer).
* ``adjustment_effects.meta.json``     -- provenance: configs, spec sha1s, canonical parquets checked.

Display-layer values are rendered from the cycle's live ``adjustments/`` specs at export time, so an
``h`` re-anchor needs only a re-export, never a model run. Model-run-dependent numbers come from the
manifest's parquets, which are keyed by overlay fingerprint.

**Currency check.** With ``--desktop-canonical`` / ``--mobile-canonical`` the manifest is checked
against the canonical sidecars (seam, config, overlay set + fingerprints, and that the all-in / ``p``
run *is* the canonical parquet). ``--check-current`` only checks and exits 2 when stale, printing
the rebuild command; a plain export refuses to write when stale unless ``--allow-stale``.

Run (from the repo root):

    source .venv/bin/activate && python scripts/export_adjustment_effects.py --cycle 2026-09 \\
        --desktop-canonical data-official/2026-09/<desktop build>/mozaic_daily_forecast....adj-ijlo.parquet \\
        --mobile-canonical  data-official/2026-09/<mobile build>/mozaic_daily_forecast....adj-p.parquet

Never runs the model. Logic in ``mozaic_daily.adjustment_effects``.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from mozaic_daily.adjustment_effects import (  # noqa: E402
    RAW_LABEL, RunCode, country_daily_series, curves_long, dec15_by_country_table, effects_table,
    staleness_problems, subsets_table, world_daily_series,
)
from mozaic_daily.adjustments import (  # noqa: E402
    load_forecast, load_lift_series, load_overlay_spec, read_meta, render_adjustment,
)
from mozaic_daily.combinatorics import subset_label  # noqa: E402
from mozaic_daily.ladder import fingerprint_overlay  # noqa: E402
from mozaic_daily.organic import load_organic_spec, marketing_paid_level  # noqa: E402
from mozaic_daily.overlays import resolve_overlays  # noqa: E402
from mozaic_daily.queries import DataSource  # noqa: E402
from mozaic_daily.seam_ma import display_ma  # noqa: E402

MA_WINDOW_DAYS = 28
MANIFEST_NAME = "combinatorics_manifest.json"
OUTPUT_NAMES = {
    "subsets": "adjustment_subsets.csv",
    "effects": "adjustment_effects.csv",
    "curves": "adjustment_curves_28ma.csv",
    "by_country": "adjustment_dec15_by_country.csv",
    "meta": "adjustment_effects.meta.json",
}
STALE_EXIT_CODE = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cycle", required=True, help="cycle directory, e.g. 2026-09")
    parser.add_argument("--desktop-canonical", type=Path, default=None,
                        help="the cycle's canonical desktop parquet; enables the desktop currency check")
    parser.add_argument("--mobile-canonical", type=Path, default=None,
                        help="the cycle's canonical mobile parquet; enables the mobile currency check")
    parser.add_argument("--check-current", action="store_true", help="only run the currency check; exit 2 when stale")
    parser.add_argument("--allow-stale", action="store_true", help="export even when the currency check fails")
    parser.add_argument("--out-dir", type=Path, default=None,
                        help="default data-official/<cycle>/adjustment_combinatorics (where the manifest lives)")
    parser.add_argument("--measurement-date", default=None, help="default: the manifest's measurement_date")
    return parser.parse_args()


# --- inputs -------------------------------------------------------------------------------------------

def load_manifest(out_dir: Path) -> dict:
    path = out_dir / MANIFEST_NAME
    if not path.exists():
        raise SystemExit(f"no {MANIFEST_NAME} in {out_dir}; run scripts/build_adjustment_combinatorics.py first")
    return json.loads(path.read_text())


def horizon_index(seam: str) -> pd.DatetimeIndex:
    year = pd.Timestamp(seam).year
    return pd.date_range(f"{year}-01-01", f"{year + 1}-12-31", freq="D")


def display_layer(cycle_dir: Path, date_index: pd.DatetimeIndex, measurement_date: pd.Timestamp) -> tuple[dict, dict, list]:
    """Per platform: {code: Dec-15 value} (non-zero only) and the summed display series; plus spec provenance."""
    effects = {"desktop": {}, "mobile": {}}
    series = {"desktop": pd.Series(0.0, index=date_index), "mobile": pd.Series(0.0, index=date_index)}
    provenance = []
    for spec_path in sorted(glob.glob(str(cycle_dir / "adjustments" / "*.json"))):
        spec_path = Path(spec_path)
        spec = json.loads(spec_path.read_text())
        code = spec.get("adjustment_code") or spec_path.stem[0]
        rendered = render_adjustment(spec, date_index, spec_dir=spec_path.parent)
        for platform in effects:
            value = float(rendered[platform].get(measurement_date, 0.0))
            if value != 0.0:
                effects[platform][code] = value
            series[platform] = series[platform] + rendered[platform].reindex(date_index, fill_value=0.0)
        provenance.append({"code": code, "spec_file": str(spec_path.relative_to(REPO)),
                           "spec_sha1": hashlib.sha1(spec_path.read_bytes()).hexdigest()})
    return effects, series, provenance


def desktop_nominals(seam: str, codes: list[str], date_index: pd.DatetimeIndex, measurement_date: pd.Timestamp
                     ) -> tuple[dict[str, float], dict[str, str], dict[str, str]]:
    """Each desktop overlay's own Dec-15 28d-MA add-back, its live fingerprint, and its spec path."""
    overlays = {o.code: o for o in resolve_overlays(seam) if o.data_source == DataSource.LEGACY_DESKTOP}
    missing = sorted(set(codes) - set(overlays))
    if missing:
        raise SystemExit(f"overlay code(s) {missing} in the manifest do not gate seam {seam} any more; "
                         f"re-gate the specs or rebuild the combinatorics")
    nominals, fingerprints, spec_paths = {}, {}, {}
    for code in codes:
        spec_path = overlays[code].spec_path
        spec = load_overlay_spec(spec_path)
        curve = load_lift_series(spec, spec_path.parent).reindex(date_index, fill_value=0.0)
        nominals[code] = float(curve.rolling(MA_WINDOW_DAYS).mean().loc[measurement_date])
        fingerprints[code] = fingerprint_overlay(spec_path)
        spec_paths[code] = str(spec_path.relative_to(REPO)) if spec_path.is_absolute() else str(spec_path)
    return nominals, fingerprints, spec_paths


def paid_nominal(p_parquet: Path, seam: str, forecast_end: pd.Timestamp, measurement_date: pd.Timestamp) -> tuple[float, str]:
    """Marketing's paid level (what ``p`` stacks back) as a Dec-15 28d-MA, and the spec it came from."""
    meta = read_meta(p_parquet)
    applied = [a for a in meta.get("adjustments_applied", []) if a["code"] == "p"]
    if not applied:
        raise SystemExit(f"{p_parquet} sidecar does not list adjustment p")
    spec_path = Path(applied[0]["spec_file"])
    if not spec_path.is_absolute():
        spec_path = REPO / spec_path
    spec = load_organic_spec(spec_path)
    level = marketing_paid_level(spec, spec_path.parent, forecast_start=pd.Timestamp(seam), forecast_end=forecast_end)
    return float(level.rolling(MA_WINDOW_DAYS).mean().loc[measurement_date]), str(spec_path.relative_to(REPO))


# --- per-run measurements ------------------------------------------------------------------------------

def measure_run(parquet: Path, codes: list[str], platform: str, seam: str, measurement_date: pd.Timestamp) -> dict:
    """World display-MA curve from the seam on, its Dec-15 value, per-country Dec-15, and the horizon end."""
    df, meta = load_forecast(parquet, require_state=codes)
    world = world_daily_series(df, platform)
    curve = display_ma(world.index.to_series(), world, pd.Timestamp(seam))
    curve = curve[curve.index >= pd.Timestamp(seam)]
    per_country = {country: float(s.rolling(MA_WINDOW_DAYS).mean().loc[measurement_date])
                   for country, s in country_daily_series(df, platform).items()}
    return {
        "curve": curve,
        "dec15": float(world.rolling(MA_WINDOW_DAYS).mean().loc[measurement_date]),
        "per_country": pd.Series(per_country),
        "forecast_end": world.index.max(),
        "artifact_sha1": meta.get("artifact_sha1"),
    }


def desktop_tables(manifest: dict, cycle_dir: Path, display_effects: dict, display_series: pd.Series,
                   measurement_date: pd.Timestamp, date_index: pd.DatetimeIndex) -> tuple[dict, dict]:
    cycle, seam = manifest["cycle"], manifest["forecast_start"]
    codes = sorted(set(manifest["droppable_codes"]) | set(manifest.get("pinned_overlay_codes", [])))
    pinned = frozenset(manifest.get("pinned_overlay_codes", []))
    nominals, fingerprints, spec_paths = desktop_nominals(seam, codes, date_index, measurement_date)
    values, parquets, curves, per_country = {}, {}, {}, {}
    for label, run in manifest["runs"].items():
        enabled = frozenset(run["overlays_enabled"])
        measured = measure_run(REPO / run["parquet"], sorted(enabled), "desktop", seam, measurement_date)
        subset = enabled - pinned  # the table is over the droppable codes; pinned ones are in every run
        key = frozenset(subset) if pinned else enabled
        values[key] = measured["dec15"]
        parquets[key] = run["parquet"]
        curves[label] = measured["curve"]
        per_country[label] = measured["per_country"]
    if pinned:
        # With pinned overlays the Shapley table is over the droppable codes only; pinned codes are
        # reported as part of every subset, not attributed. Nominals are still recorded for them.
        run_codes = [RunCode(c, nominals[c], fingerprints[c]) for c in manifest["droppable_codes"]]
    else:
        run_codes = [RunCode(c, nominals[c], fingerprints[c]) for c in codes]
    tables = {
        "subsets": subsets_table(cycle=cycle, seam=seam, platform="desktop", values=values,
                                 display_total_dec15=sum(display_effects.values()), parquets=parquets),
        "effects": effects_table(cycle=cycle, seam=seam, platform="desktop", values=values,
                                 run_codes=run_codes, display_effects=display_effects),
        "curves": curves_long(cycle=cycle, platform="desktop", run_curves=curves, display_series=display_series),
        "by_country": dec15_by_country_table(cycle=cycle, platform="desktop", per_country=per_country),
    }
    provenance = {"overlay_nominal_dec15": nominals, "overlay_fingerprints_live": fingerprints,
                  "overlay_specs": spec_paths, "pinned_overlay_codes": sorted(pinned)}
    return tables, provenance


def mobile_tables(manifest: dict, display_effects: dict, display_series: pd.Series,
                  measurement_date: pd.Timestamp) -> tuple[dict, dict]:
    cycle, seam = manifest["cycle"], manifest["forecast_start"]
    runs = manifest["mobile_runs"]
    values, parquets, curves, per_country = {}, {}, {}, {}
    forecast_end = None
    for kind, codes in ((RAW_LABEL, []), ("p", ["p"])):
        measured = measure_run(REPO / runs[kind]["parquet"], codes, "mobile", seam, measurement_date)
        key = frozenset(codes)
        values[key] = measured["dec15"]
        parquets[key] = runs[kind]["parquet"]
        curves[subset_label(key)] = measured["curve"]
        per_country[subset_label(key)] = measured["per_country"]
        forecast_end = measured["forecast_end"]
    nominal, spec_path = paid_nominal(REPO / runs["p"]["parquet"], seam, forecast_end, measurement_date)
    tables = {
        "subsets": subsets_table(cycle=cycle, seam=seam, platform="mobile", values=values,
                                 display_total_dec15=sum(display_effects.values()), parquets=parquets),
        "effects": effects_table(cycle=cycle, seam=seam, platform="mobile", values=values,
                                 run_codes=[RunCode("p", nominal, None)], display_effects=display_effects),
        "curves": curves_long(cycle=cycle, platform="mobile", run_curves=curves, display_series=display_series),
        "by_country": dec15_by_country_table(cycle=cycle, platform="mobile", per_country=per_country),
    }
    return tables, {"paid_level_nominal_dec15": nominal, "organic_spec": spec_path}


# --- currency ------------------------------------------------------------------------------------------

def currency_report(manifest: dict, desktop_canonical: Path | None, mobile_canonical: Path | None,
                    live_fingerprints: dict[str, str]) -> dict[str, list[str]]:
    report = {}
    if desktop_canonical is not None:
        report["desktop"] = staleness_problems(manifest, read_meta(desktop_canonical), live_fingerprints, platform="desktop")
    if mobile_canonical is not None:
        report["mobile"] = staleness_problems(manifest, read_meta(mobile_canonical), {}, platform="mobile")
    return report


def print_currency(report: dict[str, list[str]], manifest: dict) -> bool:
    stale = False
    for platform, problems in report.items():
        if problems:
            stale = True
            print(f"[STALE] {platform}: the combinatorics manifest does not describe the canonical build")
            for problem in problems:
                print(f"        - {problem}")
        else:
            print(f"[current] {platform}: manifest matches the canonical build")
    if stale:
        n = len(manifest.get("runs", {}))
        print(f"\nRebuild (about {n} desktop forecasts, several minutes each; prompts before running):\n"
              f"  python scripts/build_adjustment_combinatorics.py --cycle {manifest['cycle']} "
              f"--forecast-start-date <seam> --raw-cache-dir <raw pull> --config-from <canonical>.meta.json "
              f"--reuse-run raw=<raw build> --reuse-run <all codes>=<canonical> --mobile-run p=<canonical mobile> "
              f"--mobile-run raw=<no-organic-split build>")
    return stale


# --- main ------------------------------------------------------------------------------------------------

def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(REPO))
    except ValueError:
        return str(path)


def git_commit() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def main() -> None:
    args = parse_args()
    cycle_dir = REPO / "data-official" / args.cycle
    out_dir = (args.out_dir or (cycle_dir / "adjustment_combinatorics")).resolve()
    manifest = load_manifest(out_dir)
    seam = manifest["forecast_start"]
    measurement_date = pd.Timestamp(args.measurement_date or manifest["measurement_date"])
    date_index = horizon_index(seam)

    desktop_codes = sorted(set(manifest["droppable_codes"]) | set(manifest.get("pinned_overlay_codes", [])))
    _, live_fingerprints, _ = desktop_nominals(seam, desktop_codes, date_index, measurement_date)
    report = currency_report(manifest, args.desktop_canonical, args.mobile_canonical, live_fingerprints)
    stale = print_currency(report, manifest) if report else False
    if args.check_current:
        raise SystemExit(STALE_EXIT_CODE if stale else 0)
    if stale and not args.allow_stale:
        raise SystemExit(STALE_EXIT_CODE)

    display_effects, display_series, display_provenance = display_layer(cycle_dir, date_index, measurement_date)
    print(f"\nAdjustment effects for {args.cycle} at seam {seam}, measured at {measurement_date.date()}")
    print(f"  display layer Dec-15: {display_effects}")

    desktop, desktop_prov = desktop_tables(manifest, cycle_dir, display_effects["desktop"], display_series["desktop"],
                                           measurement_date, date_index)
    platforms = [desktop]
    provenance = {"desktop": desktop_prov}
    if manifest.get("mobile_runs"):
        mobile, mobile_prov = mobile_tables(manifest, display_effects["mobile"], display_series["mobile"], measurement_date)
        platforms.append(mobile)
        provenance["mobile"] = mobile_prov
    else:
        print("  (no mobile_runs block in the manifest; desktop only)")

    out_dir.mkdir(parents=True, exist_ok=True)
    for key in ("subsets", "effects", "curves", "by_country"):
        frame = pd.concat([p[key] for p in platforms], ignore_index=True)
        path = out_dir / OUTPUT_NAMES[key]
        frame.to_csv(path, index=False)
        print(f"  wrote {_rel(path)}  ({len(frame):,} rows)")

    meta = {
        "cycle": args.cycle, "seam": seam, "measurement_date": str(measurement_date.date()),
        "manifest": _rel(out_dir / MANIFEST_NAME), "manifest_built_at": manifest.get("built_at"),
        "desktop_model_config": manifest["model_config"], "mobile_model_config": manifest.get("mobile_model_config"),
        "display_layer_specs": display_provenance, "display_layer_dec15": display_effects,
        "provenance": provenance,
        "currency_check": {"desktop_canonical": str(args.desktop_canonical) if args.desktop_canonical else None,
                           "mobile_canonical": str(args.mobile_canonical) if args.mobile_canonical else None,
                           "problems": report},
        "mozaic_daily_commit": git_commit(),
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "produced_by": "scripts/export_adjustment_effects.py",
    }
    (out_dir / OUTPUT_NAMES["meta"]).write_text(json.dumps(meta, indent=2))
    print(f"  wrote {_rel(out_dir / OUTPUT_NAMES['meta'])}")

    effects = pd.concat([p["effects"] for p in platforms], ignore_index=True)
    print(f"\n{'platform':<8s} {'code':<5s} {'kind':<14s} {'nominal':>14s} {'single':>14s} {'marginal':>14s} {'shapley':>14s} {'pass-thru':>10s}")
    for _, row in effects.iterrows():
        pt = row["pass_through_shapley"]
        print(f"{row['platform']:<8s} {row['code']:<5s} {row['kind']:<14s} {row['nominal_dec15']:>14,.0f} "
              f"{row['single_dec15']:>14,.0f} {row['marginal_dec15']:>14,.0f} {row['shapley_dec15']:>14,.0f} "
              f"{'' if pd.isna(pt) else f'{pt:.3f}':>10s}")


if __name__ == "__main__":
    main()
