"""Load the August-cycle desktop forecast builds as comparable 28d-MA curves.

Read-only. Nothing here writes to `data-official/`; it exists so the autumn-decoupling
exploration can reconstruct the published chart's curves without touching the canonical
notebook or its artifacts.

The one subtlety worth knowing: the Win10 headwind `h` is a *display-layer* linear ramp, and
July and August use different ramp conventions (July ramps from 2026-04-01 to -1,345,000;
August ramps from the 2026-07-28 seam to -1,245,000). Both hit their anchor on 2026-12-15, so
Dec-15 comparisons are apples-to-apples, but every interior date is not. `headwind_ramp()`
renders either convention so the two effects can be separated.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from mozaic_daily.seam_ma import display_ma  # noqa: E402

AUG_SEAM = pd.Timestamp("2026-07-28")
JUL_SEAM = pd.Timestamp("2026-07-06")
DEC15 = pd.Timestamp("2026-12-15")

# Headwind conventions, read off the two cycles' adjustment specs. Kept as literals so a future
# edit to either spec dir cannot silently redefine what this exploration compared against.
JULY_HEADWIND = {"start": pd.Timestamp("2026-04-01"), "anchor": DEC15, "dau": -1_345_000}
AUGUST_HEADWIND = {"start": AUG_SEAM, "anchor": DEC15, "dau": -1_245_000}


@dataclass(frozen=True)
class Build:
    """One desktop forecast artifact and the seam its display MA should be spliced at."""

    label: str
    path: str
    seam: pd.Timestamp

    @property
    def full_path(self) -> Path:
        return REPO_ROOT / self.path


BUILDS = {
    # July's delivered desktop forecast: 07-06 seam, July's own LOL ceiling (125K), previous model
    # config. data-official/2026-07/ was deliberately left intact by the 2026-07-30 curve cleanup.
    "july_delivered": Build(
        "July delivered (125K LOL, prev config)",
        "data-official/2026-07/desktop_locked/"
        "mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet",
        JUL_SEAM,
    ),
    # August data refresh on the PREVIOUS model config — isolates the retune when paired with
    # s01_prev_ceiling (both sit on the same, since-deleted, pre-200K LOL curve).
    "aug_prevconfig": Build(
        "Aug data, prev config (pre-200K LOL)",
        "data-official/2026-08/desktop_baseline_2026-07-28/"
        "cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/"
        "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet",
        AUG_SEAM,
    ),
    # s01 config on the pre-200K ceiling. Its data-official/ copy
    # (desktop_superseded_lol180k_2026-07-28/) was DELETED 2026-07-30 along with the intermediate LOL
    # curves; this path is the SAME RUN, verified by identical sidecar (model_config,
    # adjustments_applied incl. the `l` spec sha1 e23a6267, commit) and a hard-linked .pkl.
    "s01_prev_ceiling": Build(
        "Aug s01 retune (pre-200K LOL)",
        "research/param-scans/summer-trough-v2/s01_gradient/"
        "cps0.1849_thresh032_recent17_cpr0.734_ncp35_clip0.6_sps0.00825_regimemultiplicative/"
        "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet",
        AUG_SEAM,
    ),
    "s01_200k_locked": Build(
        "Aug LOCKED: s01 retune (200K LOL, the only surviving curve)",
        "data-official/2026-08/desktop_locked/"
        "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet",
        AUG_SEAM,
    ),
}


def load_desktop_daily(build: Build) -> tuple[pd.DataFrame, dict]:
    """Return World(ALL) daily desktop DAU for a build, plus its sidecar meta."""
    df, meta = load_forecast(str(build.full_path), require_state=["l", "o"])
    mask = (
        (df["country"] == "ALL")
        & (df["segment"] == '{"os": "ALL"}')
        & (df["data_source"] == "legacy_desktop")
        & (df["app_name"] == "desktop")
    )
    out = df.loc[mask, ["target_date", "dau", "data_type"]].copy()
    out["target_date"] = pd.to_datetime(out["target_date"])
    return out.sort_values("target_date").reset_index(drop=True), meta


def headwind_ramp(index: pd.DatetimeIndex, convention: dict) -> pd.Series:
    """Render a Dec-15-anchored linear headwind ramp over `index`.

    Matches the canonical notebook's `render_adjustment` for type=linear_ramp: the ramp grows
    without bound past the anchor rather than flattening, and is clipped at zero before `start`.
    """
    total_days = (convention["anchor"] - convention["start"]).days
    elapsed = (index - convention["start"]).days.to_numpy().clip(min=0)
    return pd.Series(convention["dau"] * elapsed / total_days, index=index)


def build_ma(build: Build, convention: dict | None) -> pd.Series:
    """28d-MA display curve for a build, optionally with a headwind convention applied.

    The headwind applies only from that build's own seam forward — applying it over a stretch
    the build treated as training would move history that is actually settled.
    """
    daily, _ = load_desktop_daily(build)
    ma = display_ma(daily["target_date"], daily["dau"], build.seam)
    if convention is None:
        return ma
    adj = headwind_ramp(ma.index, convention)
    out = ma.copy()
    forecast = out.index >= build.seam
    out[forecast] += adj[forecast]
    return out
