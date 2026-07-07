"""Regenerate the July canonical combined forecast with the grid-search mobile model.

The combined parquet (gm+ld, DAU+NP) feeding july_canonical_v2026-06-29.ipynb carries
default-param mobile DAU. This surgically replaces the glean_mobile `dau` column with
the grid-search-accepted `grad_moderate` mobile run — which already has the TOTAL
marketing lift baked in (run_mobile_param_scan applies adj-m in-pipeline) — leaving
legacy_desktop rows and all new_profiles values byte-identical. The headwind (adj-h) is
applied downstream in the canonical notebook, not baked into the parquet.

    base combined (raw)        : mozaic_daily_forecast.2026-06-29.gm+ld-D+NP.parquet
    mobile DAU (grad_moderate) : research/param-scans/mobile-july/results/
                                 cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6/
                                 mozaic_daily_forecast.2026-06-29.gm-D.adj-m.parquet   (adj-m; grid-search accepted)
    output (adj-m)             : mozaic_daily_forecast.2026-06-29.gm+ld-D+NP.adj-m.parquet

`grad_moderate` = the grid-search conclusion (cps=0.035, changepoint_range=0.75,
holiday_threshold=-0.055; +120,777 at the Dec-15 28d-MA vs the prior default-param mobile).

MIXED-PACKAGE NOTE: the mobile DAU source (both the prior default run and grad_moderate)
was produced on mozaic 0883806 ("skip IR-2026 shutdown holidays in detrend"); grad_moderate
raw-cached from the same tmp/mobile_holidayskip_2026-06-29 inputs. The combined base — and
thus all legacy_desktop rows and every new_profiles value — is carried over byte-identical
from the Jun-30 build on the PRIOR package (pre-holiday-skip). Per an explicit "mobile DAU
only" decision, desktop + New Profiles were intentionally NOT re-run, so this canonical
mixes packages: mobile DAU on 0883806, everything else on the old package.
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
OUT = HERE / "mozaic_daily_forecast.2026-06-29.gm+ld-D+NP.adj-m.parquet"
LIFT_PARQUET = "data-official/2026-07/marketing/marketing_lift_model.total.2026-06-29.parquet"

KEY = ["data_source", "target_date", "country", "app_name", "segment"]


def main() -> None:
    base = pd.read_parquet(BASE)
    mktg = pd.read_parquet(MKTG)
    base["target_date"] = pd.to_datetime(base["target_date"])
    mktg["target_date"] = pd.to_datetime(mktg["target_date"])

    # New (with-marketing) glean_mobile DAU keyed for lookup.
    gm_mktg = mktg[mktg["data_source"] == "glean_mobile"].set_index(KEY)["dau"]

    out = base.copy()
    # Only dau-bearing glean_mobile rows get updated; NP-only rows (dau null,
    # 2023-2025) have no DAU to change and no match in the DAU-only marketing run.
    gm_mask = (out["data_source"] == "glean_mobile") & out["dau"].notna()

    # Coverage: every dau-bearing glean_mobile row must have a marketing-run match.
    base_keys = out.loc[gm_mask].set_index(KEY).index
    missing = base_keys.difference(gm_mktg.index)
    if len(missing):
        raise SystemExit(f"{len(missing)} dau-bearing glean_mobile rows have no marketing-run match, e.g. {missing[:3].tolist()}")

    new_dau = out.loc[gm_mask].set_index(KEY).index.map(gm_mktg)
    before = out.loc[gm_mask, "dau"].to_numpy()
    out.loc[gm_mask, "dau"] = new_dau.to_numpy()

    # Sanity: desktop + new_profiles untouched; only glean_mobile dau changed.
    assert base.loc[~gm_mask].equals(out.loc[~gm_mask]), "non-mobile rows changed!"
    assert base["new_profiles"].equals(out["new_profiles"]), "new_profiles changed!"

    out.to_parquet(OUT, index=False)

    def _sha1(p): return hashlib.sha1(Path(p).read_bytes()).hexdigest()
    git_hash = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO).decode().strip()
    mobile_meta = json.loads(MKTG_META.read_text())  # grad_moderate provenance (config, sha, package)
    meta = {
        "artifact": OUT.name,
        "description": (
            "July canonical combined forecast (glean_mobile + legacy_desktop, DAU + New Profiles). "
            "Mobile DAU is the grid-search-accepted `grad_moderate` model (cps=0.035, changepoint_range=0.75, "
            "holiday_threshold=-0.055) with the TOTAL marketing lift (adj-m) baked in-pipeline. Produced by "
            "swapping the glean_mobile `dau` column of the raw combined parquet with the grad_moderate run; "
            "legacy_desktop rows and all new_profiles values are byte-identical to the raw base. Headwind "
            "(adj-h) is applied downstream in july_canonical_v2026-06-29.ipynb, not here. MIXED PACKAGE: "
            "mobile DAU on mozaic 0883806 (IR-2026 shutdown holiday-skip); desktop + all New Profiles carried "
            "from the prior-package Jun-30 base (mobile-DAU-only regen)."
        ),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mozaic_daily_git_hash": git_hash,
        "mozaic_pkg_commit_mobile_dau": "088380695d317886e261cb7818d2dceac6a28916",
        "mixed_package": True,
        "mobile_model": "grad_moderate (grid-search accepted; research/param-scans/mobile-july)",
        "mobile_model_config": mobile_meta.get("model_config"),
        "adjustments_applied": [{
            "code": "m", "name": "marketing_lift", "scope": "glean_mobile DAU only",
            "spec_file": "data-official/2026-07/marketing/marketing.json",
            "lift_parquet": LIFT_PARQUET, "variant": "total (June-anchored, forward=Total Paid DAU)",
        }],
        "parents": {
            "base_raw": str(BASE.relative_to(REPO)), "base_raw_sha1": _sha1(BASE),
            "mobile_run": str(MKTG.relative_to(REPO)), "mobile_run_sha1": _sha1(MKTG),
            "mobile_run_produced_by": mobile_meta.get("produced_by"),
        },
        "artifact_sha1": _sha1(OUT),
    }
    (HERE / (OUT.name.replace(".parquet", ".meta.json"))).write_text(json.dumps(meta, indent=2))

    # Report the change at the KPI level.
    def all_mobile(df, d="2026-12-15"):
        x = df[(df.data_source == "glean_mobile") & (df.country == "ALL")
               & (df.app_name == "ALL MOBILE") & (df.segment == "{}")]
        return x.set_index("target_date")["dau"].loc[pd.Timestamp(d)]
    print(f"Wrote {OUT.name}  sha1={meta['artifact_sha1'][:12]}")
    print(f"glean_mobile dau rows updated: {int(gm_mask.sum())}  (mean Δ {(out.loc[gm_mask,'dau'].to_numpy()-before).mean():+,.0f})")
    print(f"ALL-MOBILE Dec-15 daily:  raw {all_mobile(base):,.0f}  ->  adj-m {all_mobile(out):,.0f}")


if __name__ == "__main__":
    main()
