#!/usr/bin/env python3
"""EXPERIMENT: refit the August g01 desktop model on data through 2026-08-16.

Experiment 2 of `research/headwinds/aug-post-seam-retune/`. Experiment 1 held the model fixed and
searched the headwind anchor; this one does the opposite — it **keeps the headwind at its published
−1,315,000** and instead re-runs the model itself against every day of data we now have (training
through 2026-08-16, a new seam at 2026-08-17, 15 days later than the published build's 2026-08-01 /
2026-08-02).

Nothing here is canonical. It writes only into this experiment directory and touches no live spec.

Why this needs its own runner rather than `scripts/run_param_scan.py`
--------------------------------------------------------------------
The desktop `l` (launch-on-login) and `o` (MozillaOnline) overlay specs are **date-gated** on
``applies_to_forecast_start == "2026-08-02"``. At a 2026-08-17 seam the production finders return
``None``, the overlays silently do not apply, and the output would be a ``.raw.`` parquet that is not
comparable to the published ``.adj-lo.`` build — the exact confound this experiment must avoid.

So the two **spec-finder lookups** are overridden below to return August's specs regardless of date.
That is precisely the experiment's design ("hold `l`/`o` fixed, refit only the model"): the overlay
curves themselves are unchanged and are date-indexed, so a later seam simply subtracts 15 more days
of the same curve from training. Only the date *gate* is bypassed — no spec content is edited, and
no live file is written. Pipeline logic is untouched: the real ``main()`` still applies the overlays
through its normal code path, which is why this does **not** reintroduce the hand-copied
``process_data_source`` branch that CLAUDE.md prohibits.

Usage
-----
    source .venv/bin/activate
    python research/headwinds/aug-post-seam-retune/run_refit.py

Expects the fresh raw pull to already exist (produced by ``scripts/fetch_raw_pull.py``
--forecast-start-date 2026-08-17), since the raw BigQuery result is model-config independent.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import importlib  # noqa: E402

from mozaic.models import DesktopModelConfig  # noqa: E402
from mozaic_daily.queries import DataSource, Metric  # noqa: E402

run_main_module = importlib.import_module("mozaic_daily.main")
run_param_scan = importlib.import_module("run_param_scan")

# --- Experiment constants ---------------------------------------------------------------------
# Seam moves 15 days later than the published build; the model config is IDENTICAL to canonical
# g01, because this experiment varies the training window and nothing else.
REFIT_FORECAST_START = "2026-08-17"   # training_end = 2026-08-16
PUBLISHED_FORECAST_START = "2026-08-02"

G01_CONFIG = DesktopModelConfig(
    prophet_changepoint_prior_scale=0.1649,
    prophet_changepoint_range=0.814,
    prophet_n_changepoints=40,
    prophet_recent_weeks=17,
    prophet_seasonality_prior_scale=0.00825,
    seasonality_regime="multiplicative",
    holiday_threshold=-0.032,
    holiday_max_radius=5,
    holiday_min_radius=3,
    holiday_effect_floor=-0.6,
)

OUT_DIR = Path(__file__).resolve().parent / "refit_2026-08-17"
LOL_SPEC = REPO_ROOT / "data-official/2026-08/launch_on_login/lol.json"
MOZILLAONLINE_SPEC = REPO_ROOT / "data-official/2026-08/mozillaonline/mozillaonline.json"


def _carry_forward_august_overlay_specs() -> None:
    """Override the two date-gated spec finders to reuse August's `l`/`o` specs at the new seam.

    See the module docstring: without this the overlays would silently drop out at a 2026-08-17
    seam and the refit would not be comparable to the published adj-lo build. Asserts the specs
    really are August's, so a spec that moved on disk fails loudly instead of silently reverting
    this to a raw run.
    """
    for spec_path in (LOL_SPEC, MOZILLAONLINE_SPEC):
        if not spec_path.exists():
            raise FileNotFoundError(f"Expected August overlay spec not found: {spec_path}")
        with open(spec_path) as f:
            spec = json.load(f)
        gate = spec.get("applies_to_forecast_start")
        if gate != PUBLISHED_FORECAST_START:
            raise ValueError(
                f"{spec_path.name} is gated to {gate!r}, not the expected published seam "
                f"{PUBLISHED_FORECAST_START!r}. This experiment carries the PUBLISHED cycle's "
                f"overlays forward; refusing to guess which spec was intended."
            )

    run_main_module._find_launch_on_login_spec_for_forecast = lambda _date: LOL_SPEC
    run_main_module._find_mozillaonline_spec_for_forecast = lambda _date: MOZILLAONLINE_SPEC
    print(f"Carrying August overlay specs forward to the {REFIT_FORECAST_START} seam:")
    print(f"  l -> {LOL_SPEC.relative_to(REPO_ROOT)}")
    print(f"  o -> {MOZILLAONLINE_SPEC.relative_to(REPO_ROOT)}")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    raw_pull = OUT_DIR / "mozaic_parts.raw.legacy.desktop.DAU.parquet"
    if not raw_pull.exists():
        raise FileNotFoundError(
            f"Fresh raw pull missing: {raw_pull}\n"
            f"Produce it first (the raw BQ result is model-config independent):\n"
            f"  python scripts/fetch_raw_pull.py --forecast-start-date {REFIT_FORECAST_START} "
            f"--data-source legacy_desktop --metric DAU --output-dir {OUT_DIR}"
        )

    print("=" * 70)
    print("EXPERIMENT 2 -- desktop g01 refit on data through 2026-08-16")
    print("=" * 70)
    print(f"Forecast start (new seam) : {REFIT_FORECAST_START}  (published: {PUBLISHED_FORECAST_START})")
    print(f"Output dir                : {OUT_DIR.relative_to(REPO_ROOT)}")
    print(f"Config (canonical g01)    : {json.dumps(G01_CONFIG.to_dict(), indent=2)}")

    (OUT_DIR / "parameters.json").write_text(json.dumps({
        "forecast_start_date": REFIT_FORECAST_START,
        "slug": G01_CONFIG.to_slug(),
        "config": G01_CONFIG.to_dict(),
        "experiment": "aug-post-seam-retune / experiment 2 (refit through 2026-08-16)",
        "note": (
            "NOT CANONICAL. Model config is byte-identical to published g01; only the training "
            "window differs. August's l/o overlay specs are carried forward past their "
            "applies_to_forecast_start gate -- see run_refit.py."
        ),
    }, indent=2))

    _carry_forward_august_overlay_specs()

    run_main_module.main(
        checkpoints=True,
        data_source_filter={DataSource.LEGACY_DESKTOP},
        metric_filter={Metric.DAU},
        forecast_start_date=REFIT_FORECAST_START,
        output_dir=str(OUT_DIR),
        launch_on_login=True,
        mozillaonline=True,
        model_configs={DataSource.LEGACY_DESKTOP: G01_CONFIG},
    )

    out_path = run_param_scan.stamp_marker_and_meta(
        OUT_DIR, REFIT_FORECAST_START, G01_CONFIG,
        applied_codes=["l", "o"],
        code_to_spec_file={"l": LOL_SPEC, "o": MOZILLAONLINE_SPEC},
    )
    print(f"\nRenamed forecast to: {out_path}")
    print(f"Wrote sidecar meta:  {out_path}.meta.json")
    print(f"\nDone. Results in: {OUT_DIR}")


if __name__ == "__main__":
    main()
