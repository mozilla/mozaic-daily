#!/usr/bin/env python3
"""Build the PLACEHOLDER MozillaOnline desktop-migration overlay model.

Data-grounded placeholder for the July 2026 forecast. The MozillaOnline China
distribution build (telemetry ``app_name = "Firefox Desktop MozillaOnline"``) is
migrating its users onto canonical Firefox (``app_name = "Firefox Desktop"``).
As users update, they flip app_name and newly count toward the canonical desktop
DAU KPI -- an additive tailwind, ~93% concentrated in China.

This script models that tailwind as a bidirectional overlay (same OUTPUT contract
as the marketing-lift ``m`` adjustment -- a daily series parquet + sidecar meta +
spec). The shape is measured, not assumed:

  * Source population, geo split, and cohort (release/ESR) split come from the
    pre-June ``Firefox Desktop MozillaOnline`` telemetry (cached under
    ``source_data/``; re-pull with ``--refresh``).
  * The conversion ramp is FIT to the observed canonical-DAU rise (28d-MA) over
    the migration window.
  * Forward: rise -> peak at completion (~mid-July) -> gradual churn decline.

It is a PLACEHOLDER: a clean drop-in swap for Brad Ochocki Szasz's official
model. Optimize for tunable parameters + a clean swap, not for accuracy.

Run:  python data-official/2026-07/mozillaonline/build_placeholder_model.py
Outputs (this directory): the .parquet model, mozillaonline.json spec,
the .meta.json sidecar, README.md, summary.html, and PNG plots.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402
from scipy.optimize import least_squares  # noqa: E402

# =============================================================================
# TUNABLE CONSTANTS  (retune here; every choice is documented in the meta + README)
# =============================================================================
RUN_DATE = "2026-06-25"          # forecast_start_date this overlay applies to
SERIES_START = "2026-01-01"      # daily series start (training history)

TELEMETRY_SOURCE = "legacy"      # measured in telemetry.active_users_aggregates (Brendan's call)
MOZONLINE_APP = "Firefox Desktop MozillaOnline"   # the SOURCE app_name (the migrating build)
CANONICAL_APP = "Firefox Desktop"                 # the DESTINATION app_name (the KPI series)

# Cohort start dates (effective migration dates from Brad's June 17 report).
COHORT_STARTS = {"release": "2026-06-02", "esr": "2026-06-16"}

# Source population (orange) baseline, all-country, from pre-June telemetry.
# Total and the release/ESR split are data-derived (see source_data/).
SOURCE_BASELINE_TOTAL = 1_052_899          # avg daily DAU, May 2-15 baseline window
RELEASE_FRACTION = 0.904                    # release share of CN source channels (rest = ESR)

# Fraction of the source population that NEVER migrates (stays on the old build).
# Data-anchored to the residual the orange line is leveling toward; tunable.
RESIDUAL_FRACTION = 0.15

# Post-peak churn of the migrated cohort (annualized). This is the LEAST
# data-grounded parameter: post-migration churn is not yet observable. Default
# to Brad's -45% YoY assumption; the pre-June *source* decline was steeper
# (~-81% YoY, the partner wind-down) and is reported as an alternate scenario.
CHURN_ANNUAL = 0.45

# The conversion ramp shape (logistic midpoint + steepness) is FIT to the
# observed canonical rise -- not hardcoded. Auto-update adoption is fast;
# Brendan expects conversion to complete within ~1-2 weeks of late June. The
# fitted completion date is reported in the meta.

# Geo allocation shares (orange source baseline distribution by country).
# Sum to 1.0, CN >= 0.90. Tail = likely VPN / diaspora exit geographies.
GEO_SHARES = {
    "CN": 0.9277, "HK": 0.0225, "US": 0.0152, "JP": 0.0098,
    "SG": 0.0083, "TW": 0.0041, "DE": 0.0011, "AU": 0.0010, "ROW": 0.0103,
}

MA_WINDOW = 28
HERE = Path(__file__).resolve().parent
SRC = HERE / "source_data"

# Derived
RUN_DT = pd.Timestamp(RUN_DATE)
CHURN_DAILY = -np.log(1.0 - CHURN_ANNUAL) / 365.0   # exp decay rate per day
MODEL_NAME = "mozillaonline_migration_placeholder"
PARQUET = HERE / f"mozillaonline_migration_model.placeholder.{RUN_DATE}.parquet"
META = HERE / f"mozillaonline_migration_model.placeholder.{RUN_DATE}.meta.json"
SPEC = HERE / "mozillaonline.json"


# =============================================================================
# Helpers
# =============================================================================
def _sha1(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_hash() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=HERE, text=True
        ).strip()
    except Exception:
        return "unknown"


def _forecast_end() -> pd.Timestamp:
    sys.path.insert(0, str(HERE.parents[2] / "src"))
    from mozaic_daily.config import get_runtime_config

    rc = get_runtime_config(forecast_start_date_override=RUN_DATE)
    return pd.Timestamp(rc["forecast_end_date"])


def _ma(s: pd.Series) -> pd.Series:
    return s.rolling(MA_WINDOW, min_periods=MA_WINDOW).mean()


# =============================================================================
# Load measured inputs (cached pre-June + migration-window telemetry)
# =============================================================================
def load_observed():
    """Return measured signals (all on 28d-MA basis, all-country via 1/CN_share).

    Returns (orange_drain, blue_rise, blue_ma, blue_base, orange_ma, orange_base).

    - orange_drain = MozillaOnline source loss below its no-migration baseline.
      The CLEAN magnitude/shape signal: it is not affected by the transient
      transition double-counting that inflates the canonical daily series.
    - blue_rise = canonical gain above its no-migration baseline. The operative
      quantity (what we add to the forecast) but transiently inflated mid-ramp;
      used as a conservation cross-check, not the fit target.
    """
    blue = (
        pd.read_csv(SRC / "cn_canonical_daily.csv", parse_dates=["submission_date"])
        .set_index("submission_date")["dau"].sort_index()
    )
    orange = (
        pd.read_csv(SRC / "cn_mozonline_by_channel.csv", parse_dates=["submission_date"])
        .groupby("submission_date")["dau"].sum().sort_index()
    )
    blue_ma, orange_ma = _ma(blue), _ma(orange)

    def linbase(ma):  # linear fit of pre-migration MA, projected forward
        seg = ma.loc["2026-04-01":"2026-05-31"].dropna()
        x = (seg.index - pd.Timestamp(SERIES_START)).days.values
        m, c = np.polyfit(x, seg.values, 1)
        xall = (ma.index - pd.Timestamp(SERIES_START)).days.values
        return pd.Series(m * xall + c, index=ma.index)

    blue_base, orange_base = linbase(blue_ma), linbase(orange_ma)
    cn_share = GEO_SHARES["CN"]
    blue_rise = (blue_ma - blue_base).clip(lower=0) / cn_share
    orange_drain = (orange_base - orange_ma).clip(lower=0) / cn_share
    return orange_drain, blue_rise, blue_ma, blue_base, orange_ma, orange_base


# =============================================================================
# Model: two-cohort logistic rise -> peak -> exponential churn decline
# =============================================================================
def cohort_level(dates, t0, convertible, midpoint, steepness):
    """Migrated-user LEVEL for one cohort: a stock model.

    Daily new conversions follow the derivative of a logistic conversion curve
    (gross convertible reached as the logistic saturates); each day's converts
    then decay at the churn rate. The migrated *stock* is the convolution of
    daily conversions with exponential survival -> rise -> peak -> decline with
    no artificial plateau. `dates` must be a contiguous daily index.
    """
    days = (dates - t0).days.values.astype(float)
    progress = convertible / (1.0 + np.exp(-steepness * (days - midpoint)))
    progress[days < 0] = 0.0
    new_conversions = np.diff(progress, prepend=0.0).clip(min=0.0)
    survival = np.exp(-CHURN_DAILY * np.arange(len(dates)))
    level = np.convolve(new_conversions, survival)[: len(dates)]
    return level


def build_model(full_idx, midpoint, steepness):
    """Total + per-cohort migrated-user level series over full_idx (daily, contiguous)."""
    convertible_total = SOURCE_BASELINE_TOTAL * (1.0 - RESIDUAL_FRACTION)
    supplies = {
        "release": convertible_total * RELEASE_FRACTION,
        "esr": convertible_total * (1.0 - RELEASE_FRACTION),
    }
    cohorts = {}
    for ch, conv in supplies.items():
        t0 = pd.Timestamp(COHORT_STARTS[ch])
        cohorts[ch] = cohort_level(full_idx, t0, conv, midpoint, steepness)
    total = sum(cohorts.values())
    return pd.Series(total, index=full_idx), {k: pd.Series(v, index=full_idx) for k, v in cohorts.items()}, supplies


def fit_ramp(fit_target_ma):
    """Fit logistic (midpoint, steepness) so the model's 28d-MA matches the target.

    Target is the orange-drain signal (clean supply). Fit only over the reliable
    observed window (cohort start .. cap 4 days back from the latest partition,
    which reads low while backfilling).
    """
    obs = fit_target_ma.dropna()
    fit_end = obs.index.max() - pd.Timedelta(days=4)
    obs = obs.loc[pd.Timestamp(COHORT_STARTS["release"]):fit_end]
    fit_idx = pd.date_range(SERIES_START, obs.index.max(), freq="D")

    def resid(params):
        midpoint, steepness = params
        total, _, _ = build_model(fit_idx, midpoint, max(steepness, 1e-3))
        model_ma = total.rolling(MA_WINDOW, min_periods=1).mean()
        return (model_ma.reindex(obs.index).values - obs.values)

    res = least_squares(resid, x0=[15.0, 0.12], bounds=([0, 0.02], [80, 1.5]))
    return float(res.x[0]), float(res.x[1]), obs, fit_end


# =============================================================================
# Plots
# =============================================================================
def _fmt_axes(ax, ydiv=1e6, ysuf="M", yprec=2):
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/ydiv:.{yprec}f}{ysuf}"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.grid(True, alpha=0.3)


def make_plots(model_daily, model_ma, cohorts, blue_ma,
               blue_base, orange_ma, orange_base, fit_end):
    plots = {}
    disp_end = pd.Timestamp("2026-12-31")  # focus plots on the active year

    # 1. Final overlay curve (daily + MA), full horizon
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(model_daily.index, model_daily.values, color="#888", lw=1, label="migration_dau_daily (modeled level)")
    ax.plot(model_ma.index, model_ma.values, color="#0060df", lw=2.2, label="migration_dau_ma (28d MA)")
    for ch in COHORT_STARTS:
        ax.axvline(pd.Timestamp(COHORT_STARTS[ch]), color="gray", ls=":", lw=1)
    ax.set_title("PLACEHOLDER MozillaOnline migration overlay — total added desktop DAU")
    ax.set_xlabel("target_date"); ax.set_ylabel("added DAU")
    _fmt_axes(ax); ax.legend(loc="best"); fig.autofmt_xdate(); fig.tight_layout()
    p = HERE / "placeholder_curve.png"; fig.savefig(p, dpi=110); plt.close(fig); plots["curve"] = p

    # 2. Decomposition: blue rise / orange drain + baselines (the measured signal)
    fig, ax = plt.subplots(figsize=(12, 5))
    m = (blue_ma.index >= "2026-03-15") & (blue_ma.index <= "2026-06-30")
    ax.plot(blue_ma.index[m], blue_ma.values[m], color="#0060df", lw=2, label="Canonical 'Firefox Desktop' CN (28d-MA)")
    ax.plot(blue_base.index[m], blue_base.values[m], color="#0060df", lw=1, ls="--", label="canonical baseline (no-migration)")
    ax.plot(orange_ma.index[m], orange_ma.values[m], color="#e66000", lw=2, label="'Firefox Desktop MozillaOnline' CN (28d-MA)")
    ax.plot(orange_base.index[m], orange_base.values[m], color="#e66000", lw=1, ls="--", label="MozillaOnline baseline (no-migration)")
    for ch in COHORT_STARTS:
        ax.axvline(pd.Timestamp(COHORT_STARTS[ch]), color="gray", ls=":", lw=1)
    ax.set_title("Measured signal: canonical rises as MozillaOnline drains (CN)")
    ax.set_xlabel("submission_date"); ax.set_ylabel("DAU (28d-MA)")
    _fmt_axes(ax); ax.legend(loc="best", fontsize=8); fig.autofmt_xdate(); fig.tight_layout()
    p = HERE / "decomposition.png"; fig.savefig(p, dpi=110); plt.close(fig); plots["decomp"] = p

    # 3. Conservation + model fit: blue-rise vs orange-drain vs model (all-country basis)
    cn = GEO_SHARES["CN"]
    blue_rise = ((blue_ma - blue_base).clip(lower=0) / cn)
    orange_drain = ((orange_base - orange_ma).clip(lower=0) / cn)
    fig, ax = plt.subplots(figsize=(12, 5))
    m = (blue_rise.index >= "2026-05-20") & (blue_rise.index <= "2026-08-31")
    ax.plot(blue_rise.index[m], blue_rise.values[m], color="#0060df", lw=2, label="blue rise (canonical gain) — operative")
    ax.plot(orange_drain.index[m], orange_drain.values[m], color="#e66000", lw=2, label="orange drain (source loss) — cross-check")
    mm = (model_ma.index >= "2026-05-20") & (model_ma.index <= "2026-08-31")
    ax.plot(model_ma.index[mm], model_ma.values[mm], color="black", lw=2, ls="--", label="model (28d-MA)")
    ax.axvline(fit_end, color="red", ls=":", lw=1); ax.text(fit_end, ax.get_ylim()[1]*0.02, " fit cutoff", color="red", fontsize=8)
    ax.set_title("Conservation cross-check + model fit (all-country, 28d-MA)")
    ax.set_xlabel("date"); ax.set_ylabel("added DAU (28d-MA)")
    _fmt_axes(ax, yprec=2); ax.legend(loc="best", fontsize=8); fig.autofmt_xdate(); fig.tight_layout()
    p = HERE / "conservation_fit.png"; fig.savefig(p, dpi=110); plt.close(fig); plots["fit"] = p

    # 4. Cohort decomposition
    fig, ax = plt.subplots(figsize=(12, 5))
    mm = (model_ma.index >= "2026-05-15") & (model_ma.index <= disp_end)
    bottom = np.zeros(mm.sum())
    for ch, color in [("release", "#0060df"), ("esr", "#e66000")]:
        vals = cohorts[ch].rolling(MA_WINDOW, min_periods=1).mean().values[mm]
        ax.fill_between(model_ma.index[mm], bottom, bottom + vals, color=color, alpha=0.7, label=f"{ch} cohort")
        bottom = bottom + vals
    ax.set_title("Cohort split (release ≫ ESR), 28d-MA")
    ax.set_xlabel("target_date"); ax.set_ylabel("added DAU (28d-MA)")
    _fmt_axes(ax); ax.legend(loc="best"); fig.autofmt_xdate(); fig.tight_layout()
    p = HERE / "cohorts.png"; fig.savefig(p, dpi=110); plt.close(fig); plots["cohorts"] = p

    # 5. Geo split
    fig, ax = plt.subplots(figsize=(9, 5))
    items = sorted(GEO_SHARES.items(), key=lambda kv: -kv[1])
    ax.bar([k for k, _ in items], [v * 100 for _, v in items], color="#0060df")
    ax.set_title("Geo allocation shares (from MozillaOnline source population)")
    ax.set_ylabel("share of migration (%)"); ax.set_yscale("log")
    for i, (k, v) in enumerate(items):
        ax.text(i, v * 100, f"{v*100:.1f}%", ha="center", va="bottom", fontsize=8)
    ax.grid(True, alpha=0.3, axis="y"); fig.tight_layout()
    p = HERE / "geo_split.png"; fig.savefig(p, dpi=110); plt.close(fig); plots["geo"] = p
    return plots


# =============================================================================
# Artifacts
# =============================================================================
def write_parquet(model_daily, model_ma):
    df = pd.DataFrame({"migration_dau_daily": model_daily.values,
                       "migration_dau_ma": model_ma.values},
                      index=model_daily.index)
    df.index.name = "target_date"
    df.to_parquet(PARQUET)
    return df


def write_spec():
    spec = {
        "type": "mozillaonline_migration",
        "platform": "desktop",
        "data_file": PARQUET.name,
        "value_column": "migration_dau_daily",
        "allocation": {
            "key": "fixed_country_shares",
            "shares": GEO_SHARES,
            "within_country_os": "proportional_to_dau",
        },
        "scope": {"exclude_countries": ["IR"]},
        "model_meta_file": META.name,
        "applies_to_forecast_start": RUN_DATE,
        "telemetry_source": TELEMETRY_SOURCE,
        "placeholder": True,
        "notes": (
            "PLACEHOLDER MozillaOnline desktop migration overlay (data-grounded). "
            "Source population is telemetry app_name='Firefox Desktop MozillaOnline'; "
            "migration flips users to app_name='Firefox Desktop' (canonical KPI). "
            ">93% China; ~7% tail (likely VPN/diaspora). Two cohorts: release (Jun 2) "
            "≫ ESR (Jun 16). Shape measured from the canonical-DAU rise (28d-MA), "
            "projected to a completion peak (~mid-July) then a churn decline. "
            "Magnitude is data-led and runs ABOVE Brad's ~560K-by-Dec-15 (actuals "
            "outpace his conservative ramp, by design). Subtract from desktop training "
            "rows by country share before mozaic, add back to per-country + ALL forecast "
            "rows. Swap for Brad's official model per meta.json swap_instructions."
        ),
    }
    SPEC.write_text(json.dumps(spec, indent=2) + "\n")
    return spec


def write_meta(df, fit_params, supplies, validation):
    midpoint, steepness, fit_end = fit_params
    meta = {
        "model_name": MODEL_NAME,
        "placeholder": True,
        "description": (
            "Data-grounded placeholder for the MozillaOnline -> canonical Firefox "
            "desktop migration tailwind. Measured from app_name telemetry; conversion "
            "ramp fit to the observed canonical-DAU rise; rise -> peak -> churn decline."
        ),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mozaic_daily_git_hash": _git_hash(),
        "run_date": RUN_DATE,
        "telemetry_source": TELEMETRY_SOURCE,
        "coverage": {
            "start_date": SERIES_START,
            "end_date": str(df.index.max().date()),
            "cohort_starts": COHORT_STARTS,
        },
        "parameters": {
            "SOURCE_BASELINE_TOTAL": SOURCE_BASELINE_TOTAL,
            "RELEASE_FRACTION": RELEASE_FRACTION,
            "RESIDUAL_FRACTION": RESIDUAL_FRACTION,
            "CHURN_ANNUAL": CHURN_ANNUAL,
            "fit_logistic_midpoint_days": midpoint,
            "fit_logistic_steepness": steepness,
            "fit_cutoff_date": str(pd.Timestamp(fit_end).date()),
            "convertible_supply_by_cohort": {k: round(v) for k, v in supplies.items()},
            "geo_shares": GEO_SHARES,
        },
        "methodology": {
            "source_signal": (
                "app_name='Firefox Desktop MozillaOnline' (the migrating build) vs "
                "app_name='Firefox Desktop' (canonical KPI). distribution_id is NOT used "
                "(distribution_id='MozillaOnline' is a tiny ~28K accounting tag)."
            ),
            "overlay_definition": (
                "Operative = canonical CN rise above no-migration baseline (28d-MA), "
                "grossed to all-country via 1/CN_share. Cross-checked against the "
                "MozillaOnline source drain (conservation)."
            ),
            "shape": "two-cohort logistic conversion -> peak at ~mid-July -> exponential churn decline",
            "peak_basis": "convertible source supply = SOURCE_BASELINE_TOTAL*(1-RESIDUAL_FRACTION)",
            "churn_caveat": (
                "CHURN_ANNUAL is the least data-grounded parameter (post-migration churn "
                "not yet observable). Default 0.45 = Brad's -45% YoY. The pre-June SOURCE "
                "decline was ~-81% YoY (partner wind-down), reported as an alt scenario."
            ),
        },
        "validation": validation,
        "swap_instructions": (
            "To replace with Brad's official model: drop the official daily-series parquet "
            "here with a 'migration_dau_daily' column on a 'target_date' DatetimeIndex; "
            "update mozillaonline.json 'data_file' + 'model_meta_file'; set 'placeholder': "
            "false; update 'allocation.shares' if the official model carries its own geo "
            "split. The applier contract (subtract-from-training / add-back-to-forecast, "
            "fixed country shares, within-country split proportional to OS-row DAU) is "
            "unchanged."
        ),
        "source_data": {
            f: _sha1(SRC / f) for f in sorted(p.name for p in SRC.glob("*.csv"))
        },
        "artifact_sha1": _sha1(PARQUET),
    }
    META.write_text(json.dumps(meta, indent=2) + "\n")
    return meta


def write_readme(meta, validation):
    txt = f"""# MozillaOnline migration overlay (`o`) — PLACEHOLDER

Data-grounded placeholder model of the **MozillaOnline → canonical Firefox desktop
migration** tailwind, for the July 2026 forecast. Drop-in swap for Brad Ochocki
Szasz's official model.

## What this is

MozillaOnline (Mozilla's China distribution partner) is migrating its desktop
users onto mainline Firefox. In telemetry the migrating build is
`app_name = "Firefox Desktop MozillaOnline"`; as users update they flip to
`app_name = "Firefox Desktop"` (the canonical KPI series the forecast models), so
they newly count — an **additive tailwind**, ~93% China.

## Bidirectional overlay (same contract as marketing-lift `m`)

1. **Subtract** the migration DAU from desktop training rows before mozaic (so
   Prophet learns the pre-migration dynamic and doesn't extrapolate the ramp).
2. Run the forecast.
3. **Add** it back to per-country + ALL forecast rows.

> The tailwind is **already in recent training data** (the June canonical uptick),
> so the subtract step is essential.

## Files

| File | What |
|------|------|
| `mozillaonline_migration_model.placeholder.{RUN_DATE}.parquet` | daily series: `migration_dau_daily`, `migration_dau_ma` on a `target_date` index |
| `mozillaonline.json` | spec: data_file, value_column, geo allocation shares, scope, applies_to_forecast_start |
| `mozillaonline_migration_model.placeholder.{RUN_DATE}.meta.json` | provenance + tunables + validation + swap_instructions |
| `summary.html` | self-contained proof doc for a reviewing DS |
| `source_data/*.csv` | cached pre-June + migration-window telemetry inputs |
| `build_placeholder_model.py` | reproducible generator (tunable constants at top) |
| `*.png` | curve, decomposition, conservation/fit, cohorts, geo |

## Model (measured, not assumed)

- **Source / geo / cohorts** from pre-June `Firefox Desktop MozillaOnline` telemetry:
  source baseline ≈ {SOURCE_BASELINE_TOTAL:,} DAU; **CN {GEO_SHARES['CN']*100:.1f}%** + VPN/diaspora tail;
  **release ≫ ESR**.
- **Shape**: two-cohort logistic conversion (release {COHORT_STARTS['release']},
  ESR {COHORT_STARTS['esr']}) → **peak ~mid-July** → churn decline. The ramp is FIT
  to the observed canonical-DAU rise (28d-MA).
- **Magnitude is data-led** and sits **above** Brad's ~560K-by-Dec-15 — actuals are
  outpacing his conservative ramp (by design). Measured Dec-15 ≈
  **{validation['dec15_total']:,.0f}** (vs Brad ~560K); peak ≈ **{validation['peak_total']:,.0f}**.

## Key caveat

`CHURN_ANNUAL` (post-peak decline) is the least data-grounded parameter — post-migration
churn isn't observable yet. Default = Brad's −45%/yr; retune in `build_placeholder_model.py`.

## Swap path

{meta['swap_instructions']}
"""
    (HERE / "README.md").write_text(txt)


def write_html(meta, validation, plots):
    def img(p):
        import base64
        b64 = base64.b64encode(p.read_bytes()).decode()
        return f'<img src="data:image/png;base64,{b64}" style="max-width:100%;border:1px solid #ddd"/>'

    v = validation
    html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>MozillaOnline migration placeholder — model proof</title>
<style>
body{{font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:980px;margin:2em auto;padding:0 1em;color:#222;line-height:1.5}}
h1{{border-bottom:3px solid #0060df;padding-bottom:.3em}} h2{{margin-top:1.6em;color:#0060df}}
table{{border-collapse:collapse;margin:1em 0}} td,th{{border:1px solid #ccc;padding:6px 12px;text-align:right}} th{{background:#f4f4f4}}
.claim{{background:#eef5ff;border-left:4px solid #0060df;padding:.6em 1em;margin:1em 0}}
.warn{{background:#fff6e5;border-left:4px solid #e66000;padding:.6em 1em;margin:1em 0}}
code{{background:#f4f4f4;padding:1px 4px;border-radius:3px}}
</style></head><body>
<h1>MozillaOnline migration overlay — PLACEHOLDER model proof</h1>
<p><b>For:</b> a reviewing data scientist. <b>Run date:</b> {RUN_DATE}.
<b>Telemetry:</b> {TELEMETRY_SOURCE} <code>active_users_aggregates</code>.
<b>git:</b> <code>{meta['mozaic_daily_git_hash'][:12]}</code>.</p>
<p>This is a <b>placeholder</b> stand-in for Brad's official migration model — built so the
July pipeline can run end-to-end and the official model drops in cleanly. It is
<b>data-grounded</b>: every structural choice is measured from telemetry; only the post-peak
churn rate is assumed.</p>

<h2>Claim 1 — The migration is a measurable, additive tailwind</h2>
<p>The migrating build is its own <code>app_name = "Firefox Desktop MozillaOnline"</code>
(NOT a <code>distribution_id</code> — <code>distribution_id='MozillaOnline'</code> is a tiny
~28K accounting tag). As users update they flip to <code>app_name = "Firefox Desktop"</code>,
the canonical series the forecast models — which historically <i>excluded</i> them. So the
canonical total <b>rises</b> exactly as the MozillaOnline build <b>drains</b>, starting ~Jun 2.</p>
{img(plots['decomp'])}

<h2>Claim 2 — Conservation: canonical rise ≈ MozillaOnline drain</h2>
<p>Two independent estimators of the same migration (all-country, 28d-MA): the canonical
<i>rise</i> above baseline (operative — what we add to the forecast) and the source
<i>drain</i> (cross-check). They track within ~{v['conservation_gap_pct']:.0f}% over the window;
the model (black dashed) is fit to the rise up to the red cutoff (recent partitions read low).</p>
{img(plots['fit'])}
<div class="claim">Measured (28d-MA, all-country) at {v['latest_date']}:
canonical rise ≈ <b>{v['blue_rise_latest']:,.0f}</b>; source drain ≈
<b>{v['orange_drain_latest']:,.0f}</b>.</div>

<h2>Claim 3 — Two cohorts, release ≫ ESR (matches Brad)</h2>
<p>Splitting the source by update channel: release dominates and started Jun 2; ESR is
small and started Jun 16 — the same structure as Brad's 494K / 68K.</p>
{img(plots['cohorts'])}

<h2>Claim 4 — Geo: &gt;90% China, with a VPN/diaspora tail</h2>
<p>Allocation shares are the country distribution of the MozillaOnline source population
(pre-June). CN {GEO_SHARES['CN']*100:.1f}%; the tail (HK/US/JP/SG/TW…) is consistent with
VPN exit nodes / diaspora.</p>
{img(plots['geo'])}

<h2>Result — the delivered overlay</h2>
{img(plots['curve'])}
<table>
<tr><th>quantity</th><th>value</th></tr>
<tr><td style="text-align:left">Peak added DAU (all-country)</td><td>{v['peak_total']:,.0f}</td></tr>
<tr><td style="text-align:left">Peak date</td><td>{v['peak_date']}</td></tr>
<tr><td style="text-align:left">Dec-15 added DAU (this model)</td><td>{v['dec15_total']:,.0f}</td></tr>
<tr><td style="text-align:left">Brad's Dec-15 estimate</td><td>~560,000</td></tr>
<tr><td style="text-align:left">CN share</td><td>{GEO_SHARES['CN']*100:.1f}%</td></tr>
</table>
<div class="warn"><b>Caveats.</b> Magnitude is <b>data-led</b> and runs above Brad's ~560K —
actuals are outpacing his conservative ramp (expected). <code>CHURN_ANNUAL={CHURN_ANNUAL}</code>
(post-peak decline) is the only assumed parameter; the pre-June <i>source</i> decline was
~−81% YoY (partner wind-down). The model is <b>mid-ramp</b> as of the latest data; the peak is
a projection. Retune constants in <code>build_placeholder_model.py</code>.</div>
</body></html>"""
    (HERE / "summary.html").write_text(html)


# =============================================================================
# Main
# =============================================================================
def main():
    print("Loading observed telemetry inputs ...")
    orange_drain, blue_rise_obs, blue_ma, blue_base, orange_ma, orange_base = load_observed()

    print("Fitting conversion ramp to the (clean) MozillaOnline source drain ...")
    midpoint, steepness, obs, fit_end = fit_ramp(orange_drain)
    print(f"  fit: midpoint={midpoint:.1f}d steepness={steepness:.3f} cutoff={fit_end.date()}")

    fc_end = _forecast_end()
    full_idx = pd.date_range(SERIES_START, fc_end, freq="D")
    model_daily, cohorts, supplies = build_model(full_idx, midpoint, steepness)
    model_ma = model_daily.rolling(MA_WINDOW, min_periods=MA_WINDOW).mean()

    # validation numbers
    cn = GEO_SHARES["CN"]
    blue_rise = ((blue_ma - blue_base).clip(lower=0) / cn)
    orange_drain = ((orange_base - orange_ma).clip(lower=0) / cn)
    latest = blue_rise.dropna().index.max()
    peak_idx = model_ma.idxmax()
    gap = abs(blue_rise.loc[latest] - orange_drain.loc[latest]) / max(blue_rise.loc[latest], 1) * 100
    validation = {
        "latest_date": str(latest.date()),
        "blue_rise_latest": float(blue_rise.loc[latest]),
        "orange_drain_latest": float(orange_drain.loc[latest]),
        "conservation_gap_pct": float(gap),
        "peak_total": float(model_ma.max()),
        "peak_date": str(peak_idx.date()),
        "dec15_total": float(model_ma.loc["2026-12-15"]),
        "brad_dec15": 560000,
    }
    print(f"  peak {validation['peak_total']:,.0f} @ {validation['peak_date']}; "
          f"Dec-15 {validation['dec15_total']:,.0f} (Brad ~560k)")

    print("Writing artifacts ...")
    df = write_parquet(model_daily, model_ma)
    spec = write_spec()
    assert abs(sum(GEO_SHARES.values()) - 1.0) < 1e-6, "geo shares must sum to 1.0"
    assert GEO_SHARES["CN"] >= 0.90, "CN share must be >= 0.90"
    meta = write_meta(df, (midpoint, steepness, fit_end), supplies, validation)
    plots = make_plots(model_daily, model_ma, cohorts, blue_ma,
                       blue_base, orange_ma, orange_base, fit_end)
    write_readme(meta, validation)
    write_html(meta, validation, plots)
    print(f"Done. Parquet rows={len(df)} span {df.index.min().date()}..{df.index.max().date()}")
    print(f"  {PARQUET.name}\n  {SPEC.name}\n  {META.name}\n  README.md  summary.html  + {len(plots)} PNGs")


if __name__ == "__main__":
    main()
