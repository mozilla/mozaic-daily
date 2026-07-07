"""Regenerate the July canonical combined forecast with the full desktop + mobile influences.

The raw combined parquet (gm+ld, DAU+NP) is assembled into the canonical by surgically
replacing two `dau` columns and leaving all `new_profiles` byte-identical:

  - glean_mobile DAU  <- grid-search-accepted `grad_moderate` run (adj-m; TOTAL marketing
    lift baked in-pipeline by run_mobile_param_scan).
  - legacy_desktop DAU <- fresh desktop re-run carrying the `l` (launch-on-login) AND `o`
    (MozillaOnline migration) bidirectional overlays (adj-lo), produced on the CURRENT
    package (native Iran counterfactual fill).

The headwind (adj-h) is still applied downstream in the canonical notebook, not baked here.

    base combined (raw)        : mozaic_daily_forecast.2026-06-29.gm+ld-D+NP.parquet
    mobile DAU (grad_moderate) : research/param-scans/mobile-july/results/
                                 cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6/
                                 mozaic_daily_forecast.2026-06-29.gm-D.adj-m.parquet   (adj-m)
    desktop DAU (l+o)          : desktop_lo_rerun/
                                 mozaic_daily_forecast.2026-06-29.ld-D.adj-lo.parquet  (adj-lo)
    output (adj-lmo)           : mozaic_daily_forecast.2026-06-29.gm+ld-D+NP.adj-lmo.parquet

`grad_moderate` = the mobile grid-search conclusion (cps=0.035, changepoint_range=0.75,
holiday_threshold=-0.055). Desktop re-run applies `l` (125K flat LOL cap) + `o` (Brad's
official MozillaOnline migration, ~567K Dec-15 28d-MA) via the per-tile bidirectional
appliers.

PACKAGE NOTE: mobile DAU and desktop DAU are now BOTH on the current package (mobile on
0883806 grad_moderate; desktop freshly re-run with native Iran fill + l + o). Only the
`new_profiles` values remain carried byte-identical from the prior-package Jun-30 base
(New Profiles was intentionally not re-run this cycle — DAU is the KPI).
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]

BASE = HERE / "mozaic_daily_forecast.2026-06-29.gm+ld-D+NP.parquet"
# grad_moderate — the grid-search-accepted mobile model (already adj-m: marketing baked in-pipeline).
MOBILE_SLUG = "cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6"
MKTG = REPO / "research/param-scans/mobile-july/results" / MOBILE_SLUG / "mozaic_daily_forecast.2026-06-29.gm-D.adj-m.parquet"
MKTG_META = Path(str(MKTG) + ".meta.json")
# desktop re-run carrying launch-on-login (l) + MozillaOnline (o) overlays.
DESKTOP = HERE / "desktop_lo_rerun" / "mozaic_daily_forecast.2026-06-29.ld-D.adj-lo.parquet"
DESKTOP_META = Path(str(DESKTOP) + ".meta.json")
OUT = HERE / "mozaic_daily_forecast.2026-06-29.gm+ld-D+NP.adj-lmo.parquet"
LIFT_PARQUET = "data-official/2026-07/marketing/marketing_lift_model.total.2026-06-29.parquet"

KEY = ["data_source", "target_date", "country", "app_name", "segment"]


def _swap_dau(out: pd.DataFrame, replacement: pd.DataFrame, data_source: str, label: str):
    """Overwrite the `dau` column of one data_source's dau-bearing rows in-place.

    Returns (mask, before_values) so the caller can report the delta. Raises if
    any dau-bearing base row has no matching key in the replacement run.
    """
    repl_dau = replacement[replacement["data_source"] == data_source].set_index(KEY)["dau"]
    mask = (out["data_source"] == data_source) & out["dau"].notna()
    base_keys = out.loc[mask].set_index(KEY).index
    missing = base_keys.difference(repl_dau.index)
    if len(missing):
        raise SystemExit(f"{len(missing)} dau-bearing {label} rows have no {label}-run match, e.g. {missing[:3].tolist()}")
    before = out.loc[mask, "dau"].to_numpy()
    out.loc[mask, "dau"] = base_keys.map(repl_dau).to_numpy()
    return mask, before


def main() -> None:
    base = pd.read_parquet(BASE)
    mktg = pd.read_parquet(MKTG)
    desktop = pd.read_parquet(DESKTOP)
    for df in (base, mktg, desktop):
        df["target_date"] = pd.to_datetime(df["target_date"])

    out = base.copy()
    # Swap glean_mobile DAU (grad_moderate + marketing) and legacy_desktop DAU (l + o).
    # NP-only base rows (dau null, 2023-2025) have no DAU to change and no match in the
    # DAU-only runs, so they're excluded by the `dau.notna()` mask.
    gm_mask, gm_before = _swap_dau(out, mktg, "glean_mobile", "glean_mobile")
    ld_mask, ld_before = _swap_dau(out, desktop, "legacy_desktop", "legacy_desktop")

    # Sanity: only the two dau columns changed; new_profiles and all non-dau rows untouched.
    untouched = ~(gm_mask | ld_mask)
    assert base.loc[untouched].equals(out.loc[untouched]), "rows outside the two dau swaps changed!"
    assert base["new_profiles"].equals(out["new_profiles"]), "new_profiles changed!"

    out.to_parquet(OUT, index=False)

    def _sha1(p): return hashlib.sha1(Path(p).read_bytes()).hexdigest()
    git_hash = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO).decode().strip()
    mobile_meta = json.loads(MKTG_META.read_text())  # grad_moderate provenance (config, sha, package)
    desktop_meta = json.loads(DESKTOP_META.read_text())  # adj-lo re-run provenance
    meta = {
        "artifact": OUT.name,
        "description": (
            "July canonical combined forecast (glean_mobile + legacy_desktop, DAU + New Profiles). "
            "Mobile DAU is the grid-search-accepted `grad_moderate` model (cps=0.035, changepoint_range=0.75, "
            "holiday_threshold=-0.055) with the TOTAL marketing lift (adj-m) baked in-pipeline. Desktop DAU is "
            "a fresh legacy_desktop re-run on the current package (native Iran counterfactual fill) carrying "
            "the launch-on-login (adj-l, 125K flat cap) and MozillaOnline migration (adj-o, Brad's official "
            "model) desktop overlays. Produced by swapping the glean_mobile and legacy_desktop `dau` columns of "
            "the raw combined parquet; all new_profiles values are byte-identical to the raw base. Headwind "
            "(adj-h) is applied downstream in july_canonical_v2026-06-29.ipynb, not here. Only New Profiles "
            "remains carried from the prior-package Jun-30 base (New Profiles not re-run this cycle)."
        ),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mozaic_daily_git_hash": git_hash,
        "mozaic_pkg_commit_mobile_dau": "088380695d317886e261cb7818d2dceac6a28916",
        "new_profiles_carried_from_prior_package": True,
        "mobile_model": "grad_moderate (grid-search accepted; research/param-scans/mobile-july)",
        "mobile_model_config": mobile_meta.get("model_config"),
        "adjustments_applied": [
            {
                "code": "l", "name": "launch_on_login", "scope": "legacy_desktop DAU only",
                "spec_file": "data-official/2026-07/launch_on_login/lol.json",
            },
            {
                "code": "m", "name": "marketing_lift", "scope": "glean_mobile DAU only",
                "spec_file": "data-official/2026-07/marketing/marketing.json",
                "lift_parquet": LIFT_PARQUET, "variant": "total (June-anchored, forward=Total Paid DAU)",
            },
            {
                "code": "o", "name": "mozillaonline_migration", "scope": "legacy_desktop DAU only",
                "spec_file": "data-official/2026-07/mozillaonline/mozillaonline.json",
            },
        ],
        "parents": {
            "base_raw": str(BASE.relative_to(REPO)), "base_raw_sha1": _sha1(BASE),
            "mobile_run": str(MKTG.relative_to(REPO)), "mobile_run_sha1": _sha1(MKTG),
            "mobile_run_produced_by": mobile_meta.get("produced_by"),
            "desktop_run": str(DESKTOP.relative_to(REPO)), "desktop_run_sha1": _sha1(DESKTOP),
            "desktop_run_produced_by": desktop_meta.get("produced_by"),
        },
        "artifact_sha1": _sha1(OUT),
    }
    (HERE / (OUT.name.replace(".parquet", ".meta.json"))).write_text(json.dumps(meta, indent=2))

    # Report the change at the KPI level.
    def all_level(df, data_source, app_name, segment, d="2026-12-15", ma=False):
        x = df[(df.data_source == data_source) & (df.country == "ALL")
               & (df.app_name == app_name) & (df.segment == segment)]
        s = x.set_index("target_date")["dau"].sort_index()
        if ma:
            return s.rolling(28, min_periods=28).mean().loc[pd.Timestamp(d)]
        return s.loc[pd.Timestamp(d)]

    print(f"Wrote {OUT.name}  sha1={meta['artifact_sha1'][:12]}")
    print(f"glean_mobile dau rows updated:  {int(gm_mask.sum())}  (mean Δ {(out.loc[gm_mask,'dau'].to_numpy()-gm_before).mean():+,.0f})")
    print(f"legacy_desktop dau rows updated: {int(ld_mask.sum())}  (mean Δ {(out.loc[ld_mask,'dau'].to_numpy()-ld_before).mean():+,.0f})")
    mob = ("glean_mobile", "ALL MOBILE", "{}")
    dsk = ("legacy_desktop", "desktop", '{"os": "ALL"}')
    print(f"ALL-MOBILE  Dec-15 28d-MA:  base {all_level(base,*mob,ma=True):,.0f}  ->  canonical {all_level(out,*mob,ma=True):,.0f}")
    print(f"ALL-DESKTOP Dec-15 28d-MA:  base {all_level(base,*dsk,ma=True):,.0f}  ->  canonical {all_level(out,*dsk,ma=True):,.0f}  (pre-headwind)")


if __name__ == "__main__":
    main()
