"""Build the self-contained diagnostic HTML report from the parquet + diagnostic CSVs.

Regenerates its own before/after and root-cause figures (base64-embedded, no external
deps) so report.html is reproducible. Reads:
  - the canonical 2026-05-26 desktop parquet (daily per-country dau)
  - plots/per_country_metrics.csv  (from diagnose_seam.py)
  - plots/weekly_amplitude.csv     (from weekly_amplitude.py)

    source .venv/bin/activate && python3 research/ma-seam-turbulence/build_report.py
"""

import base64
import importlib.util
import io
import os
import subprocess

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

GIT_ROOT = subprocess.run(
    ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
).stdout.strip()
os.chdir(GIT_ROOT)

# Import the real display_ma so the report's "after" curve is exactly the shipped fix.
_spec = importlib.util.spec_from_file_location(
    "export_canonical_curves", "data-official/2026-06/export_canonical_curves.py"
)
export = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export)

PARQUET = (
    "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/"
    "mozaic_daily_forecast.2026-05-26.ld-D.raw.parquet"
)
OUT_DIR = "research/ma-seam-turbulence"
FORECAST_START = pd.Timestamp("2026-05-26")
SEAM_CLEAR = FORECAST_START + pd.Timedelta(days=27)
ZOOM = (pd.Timestamp("2026-04-15"), pd.Timestamp("2026-08-01"))


def load_country(df, country):
    mask = (
        (df["country"] == country)
        & (df["segment"] == '{"os": "ALL"}')
        & (df["data_source"] == "legacy_desktop")
        & (df["app_name"] == "desktop")
    )
    sub = df.loc[mask, ["target_date", "dau"]].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    return sub.sort_values("target_date").reset_index(drop=True)


def fig_to_b64(fig, save_as=None):
    if save_as:
        fig.savefig(f"{OUT_DIR}/plots/{save_as}", format="png", dpi=110, bbox_inches="tight")
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def embed_png(path):
    """Base64-embed an already-rendered PNG (e.g. an April-backtest overlay), or None if absent."""
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("ascii")


def before_after_fig(df, country):
    """Two panels: naive blend (before, wobbly) vs display_ma bridge (after), zoomed to seam."""
    daily = load_country(df, country)
    actuals_daily = daily[daily["target_date"] < FORECAST_START]
    blend = export.daily_to_28ma(daily["target_date"], daily["dau"])
    after = export.display_ma(daily["target_date"], daily["dau"], FORECAST_START)
    actuals_ma = export.daily_to_28ma(actuals_daily["target_date"], actuals_daily["dau"])

    fig, axes = plt.subplots(1, 2, figsize=(15, 5), sharey=True)
    for ax, series, title, color in [
        (axes[0], blend, f"BEFORE — {country} forecast 28dMA (blended at seam)", "crimson"),
        (axes[1], after, f"AFTER — {country} forecast 28dMA (variance-matched transition)", "green"),
    ]:
        ax.plot(actuals_ma.index, actuals_ma.values, color="black", lw=2, label="Actuals 28dMA")
        fc = series[series.index >= FORECAST_START]
        ax.plot(fc.index, fc.values, color=color, lw=2.2, ls="--", label="Forecast 28dMA")
        ax.axvline(FORECAST_START, color="blue", ls=":", alpha=0.7, label="Forecast start")
        ax.axvline(SEAM_CLEAR, color="gray", ls=":", alpha=0.7, label="Seam clears (+27d)")
        ax.set_xlim(*ZOOM)
        ax.set_title(title, fontsize=12)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        ax.grid(True, alpha=0.3)
        ax.legend(loc="best", fontsize=8)
    axes[0].set_ylabel("DAU")
    fig.tight_layout()
    return fig_to_b64(fig, save_as=f"report_before_after_{country}.png")


def evidence_fig(metrics, amplitude):
    """Scatter: weekly-amplitude ratio (forecast/actuals) vs seam wobble, per country."""
    merged = metrics.merge(amplitude, on="country")
    # CN is the documented exception: matched amplitude but a level/phase seam jump.
    is_cn = merged["country"] == "CN"
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(merged.loc[~is_cn, "amp_ratio_fc_over_act"], merged.loc[~is_cn, "blend_wk0_3_ppm"],
               s=60, color="steelblue")
    ax.scatter(merged.loc[is_cn, "amp_ratio_fc_over_act"], merged.loc[is_cn, "blend_wk0_3_ppm"],
               s=90, color="crimson", zorder=3, label="CN — amplitude matched, but a level/phase seam jump")
    for _, r in merged.iterrows():
        ax.annotate(r["country"], (r["amp_ratio_fc_over_act"], r["blend_wk0_3_ppm"]),
                    textcoords="offset points", xytext=(5, 3), fontsize=9)
    ax.axvline(1.0, color="gray", ls=":", alpha=0.7, label="forecast amplitude = actuals amplitude")
    ax.set_xlabel("Forecast weekly amplitude / actuals weekly amplitude  (← more damped)")
    ax.set_ylabel("Seam wobble (blended-MA |2nd diff|, wk 0-3, ppm)")
    ax.set_title("Matched-amplitude countries (US, CA, JP; ratio ~ 1) stay smooth;\n"
                 "damped-amplitude countries wobble more (CN = the one level/phase exception)", fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper center", fontsize=9)
    fig.tight_layout()
    return fig_to_b64(fig, save_as="report_evidence.png")


def df_to_html_table(df, float_cols):
    fmt = {c: "{:,.1f}".format for c in float_cols}
    return df.to_html(index=False, formatters=fmt, border=0, classes="data")


def main():
    df = pd.read_parquet(PARQUET)
    metrics = pd.read_csv(f"{OUT_DIR}/plots/per_country_metrics.csv")
    amplitude = pd.read_csv(f"{OUT_DIR}/plots/weekly_amplitude.csv")

    ar_ba = before_after_fig(df, "AR")
    us_ba = before_after_fig(df, "US")
    evidence = evidence_fig(metrics, amplitude)

    # April-backtest overlays (realized vs OLD straight bridge vs NEW variance-matched),
    # produced by backtest_seam.py. The global ALL-level panel is the headline evidence.
    backtest_all = embed_png(f"{OUT_DIR}/plots/april_backtest_desktop_ALL.png")
    backtest_ar = embed_png(f"{OUT_DIR}/plots/april_backtest_desktop_AR.png")
    backtest_block = ""
    if backtest_all:
        backtest_block = f"""
<h2>4. Validation — the April backtest (out-of-sample)</h2>
<p>We can check the smoothing against reality: April 2026's forecast transition is now in the
past, so its realized 28-day average is known (it lives in the current parquet's training
rows). We re-ran both methods on the <b>April</b> forecast and scored each transition against
what actually happened, using a <b>bias-removed (shape) error</b> so a forecast's level miss
doesn't get charged to the charting method.</p>
<p><b>On the global (all-countries) curve — the headline deliverable — the new transition is a
clear win:</b> desktop shape error drops <b>70%</b> (302,518 &rarr; 90,660 DAU; raw error
&minus;80%), mobile <b>18%</b>. The straight bridge ignored the forecast's real trend curvature
across the seam; the variance-matched transition rides it.</p>
<img src="data:image/png;base64,{backtest_all}" alt="April backtest — global desktop">
<p>Per country it is a win for most markets (AR shape error &minus;69%, plus US, CA, DE, ID, IT,
MX, PL, ROW), and AR shows the curvature capture vividly:</p>
<img src="data:image/png;base64,{backtest_ar}" alt="April backtest — AR desktop">
<div class="refuted" style="background:#fffdf0;border-left-color:#b8860b">
<b>Scope &amp; known limitation (by design).</b> This is a bandaid for the <b>global</b> curve,
not a per-country fix. A handful of small high-volatility countries (e.g. desktop IN, CN, FR)
do worse on demeaned <i>shape</i> — there the April forecast's own trend curvature diverged
from what happened, so the new method faithfully tracks a curve that was wrong in hindsight while
a straight line happened to sit closer. These cancel out in the global aggregate (what
stakeholders see). Separately, the reconstruction leaves a small (&lt;~1%) kink at the day-27
hand-off for the highest-swing countries (AR, BR); the global hand-off is smooth (~0.09%). Both
are accepted v1 limitations, documented, not chased.</div>
"""

    metrics_tbl = df_to_html_table(
        metrics, ["recent_actuals_cv_pct", "seam_step_pct", "blend_wk0_3_ppm",
                  "forecast_only_ppm", "ratio_blend_over_fconly"])
    amp_tbl = df_to_html_table(
        amplitude.sort_values("amp_ratio_fc_over_act"),
        ["weekly_amp_actuals_pct", "weekly_amp_forecast_pct", "amp_ratio_fc_over_act",
         "amp_mismatch_pct_pts"])

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Per-country forecast curve "turbulence" — diagnosis & fix</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; max-width: 1000px;
         margin: 2rem auto; padding: 0 1rem; color: #1a1a1a; line-height: 1.55; }}
  h1 {{ font-size: 1.7rem; border-bottom: 3px solid #0060df; padding-bottom: .3rem; }}
  h2 {{ font-size: 1.3rem; margin-top: 2.2rem; color: #0060df; }}
  h3 {{ font-size: 1.05rem; margin-top: 1.4rem; }}
  .summary {{ background: #f0f6ff; border-left: 4px solid #0060df; padding: 1rem 1.2rem;
             border-radius: 4px; }}
  .verdict {{ background: #e8f5e9; border-left: 4px solid #2e7d32; padding: 1rem 1.2rem;
             border-radius: 4px; }}
  .refuted {{ background: #fff4f4; border-left: 4px solid #c62828; padding: .6rem 1rem;
             border-radius: 4px; margin: .6rem 0; }}
  img {{ width: 100%; border: 1px solid #ddd; border-radius: 4px; margin: .6rem 0; }}
  table.data {{ border-collapse: collapse; font-size: .82rem; width: 100%; }}
  table.data th, table.data td {{ border: 1px solid #ddd; padding: 3px 8px; text-align: right; }}
  table.data th {{ background: #f3f3f3; }}
  table.data td:first-child, table.data th:first-child {{ text-align: left; font-weight: 600; }}
  code {{ background: #f3f3f3; padding: 1px 4px; border-radius: 3px; font-size: .9em; }}
  .caption {{ font-size: .85rem; color: #555; margin-top: -.3rem; }}
</style></head><body>

<h1>Per-country forecast curves: the early "turbulence", explained &amp; fixed</h1>
<p class="caption">Desktop DAU, June 2026 forecast cycle · diagnosis in
<code>research/ma-seam-turbulence/</code> · full hypothesis log in <code>LOG.md</code></p>

<div class="summary">
<b>Executive summary.</b> When we first plotted the per-country forecast curves (28-day
moving averages), the forecast line oscillated for about a month right after the
forecast-start date and then went smooth — strongest for small, "spiky" countries like
Argentina, and absent for large steady ones like the US. <b>This was a charting artifact,
not a problem with the forecast.</b> The 28-day average, for its first ~4 weeks, was
silently mixing the last few weeks of <i>actual</i> data with the new forecast; because a
country's weekend-vs-weekday swing in the forecast is smaller than in its raw history, that
mix didn't cancel cleanly and produced a visible wobble. The underlying daily forecast and
every headline number (e.g. the Dec-15 figure) were always correct and are
<b>unchanged</b>. We fixed the chart so the line is smooth from day one.
</div>

<h2>1. The phenomenon</h2>
<p>Below, the Argentina (AR) forecast curve <b>before</b> the fix wobbles for ~4 weeks after
the forecast start, then settles exactly when the 28-day window stops overlapping the
actuals (the "+27 days" marker). The fix (right) replaces that transition with a smooth,
trend-following transition. The actuals line (black) and everything past the marker are
untouched.</p>
<img src="data:image/png;base64,{ar_ba}" alt="AR before/after">
<p>The US, by contrast, never wobbled — and the fix barely changes it:</p>
<img src="data:image/png;base64,{us_ba}" alt="US before/after">

<h2>2. What it is NOT (ruled-out explanations)</h2>
<div class="refuted"><b>Not "overfit weekly seasonality in the model."</b> A 28-day average is
exactly four weeks, so it mathematically cancels any 7-day cycle; a weekly pattern also has
constant size into the future and so cannot produce a <i>fading</i> wobble. We confirmed the
forecast-only average (no actuals in the window) is smooth for every country.</div>
<div class="refuted"><b>Not the logistic growth cap, the hierarchical reconciliation step, or
Monte-Carlo sampling noise.</b> Each of those would disturb the daily forecast itself, which
would show up in the forecast-only average — it doesn't.</div>
<div class="refuted"><b>Not a data glitch on the last day.</b> Argentina's last training day is
a Monday and its weekends run ~50% of weekdays, so the apparent "86% jump" at the seam is just
its normal day-of-week swing, not bad data.</div>

<h2>3. Root cause</h2>
<p>A 28-day average cancels a weekly cycle only when the cycle is steady across the whole
window. For the first 27 forecast days the window straddles the actuals&rarr;forecast seam,
and the forecast's weekly swing is <b>damped</b> relative to the recent actuals' swing
(the model smooths the weekend dip). That amplitude mismatch leaves a weekly residual that
slides as the window advances — the wobble. The bigger the mismatch, the bigger the wobble;
countries whose forecast matches their actuals (US, CA, JP) stay smooth. (Amplitude is the
dominant channel; <b>CN</b> is the one country with matched amplitude that still wobbles — its
seam carries a level/phase jump instead. Either way the chart fix below handles it.)</p>
<img src="data:image/png;base64,{evidence}" alt="amplitude mismatch vs wobble">
<h3>Weekly amplitude: recent actuals vs early forecast (per country)</h3>
{amp_tbl}
<h3>Seam turbulence metrics (per country)</h3>
<p class="caption"><code>blend_wk0_3_ppm</code> = wobble of the (old) blended average over forecast
weeks 0-3; <code>forecast_only_ppm</code> = the clean forecast-only average; ratio &raquo; 1 means
the wobble is in the seam blend, not the daily forecast.</p>
{metrics_tbl}
{backtest_block}
<h2>5. The fix</h2>
<p>In the chart export (<code>data-official/2026-06/export_canonical_curves.py</code>,
function <code>display_ma</code>) the forecast 28-day average from "+27 days" onward is built
from <b>forecast data only</b> (unchanged). For the first 27 days — where the trailing window
still straddles the seam — we no longer draw a straight line. Instead we <b>rebuild the
forecast's first 27 daily values to carry the recent actuals' weekly amplitude</b> (its
weekday/weekend swing), keeping the forecast's own trend. Both sides of the seam then share the
same weekly shape, the 28-day window cancels it cleanly, and the transition <b>rides the
forecast's true trend — curvature and all — instead of a featureless straight line</b>. Result:
the line starts exactly at the forecast date, no gap, no wobble, and it tracks where the
forecast is actually heading. <b>Every date from "+27 days" onward (including Dec-15) is
byte-for-byte identical to before</b>, so no headline number moved. The same transition is
applied to the prior-cycle (April) comparison line.</p>

<h2>6. Why it won't recur</h2>
<p>A regression suite — <code>tests/test_export_canonical_curves.py</code> — builds synthetic
countries with a deliberate weekend-amplitude drop at the seam and asserts: the old method
<i>would</i> wobble (anti-tautology); the new transition is far smoother; the day-27 hand-off
to the forecast-only average has no kink; every date past "+27 days" stays byte-identical to the
plain average (Dec-15 preservation); and — on a curved-trend fixture — the variance-matched
transition tracks the realized average better than a straight line, proving the refinement is
real curvature capture, not cosmetic. The out-of-sample April backtest
(<code>research/ma-seam-turbulence/backtest_seam.py</code>) gates the change on the global
curve. If anyone reintroduces the seam blend, reverts to the straight bridge, or perturbs the
far-horizon values, the tests fail.</p>

</body></html>
"""
    out_path = f"{OUT_DIR}/report.html"
    with open(out_path, "w") as f:
        f.write(html)
    n_figs = 3 + sum(x is not None for x in (backtest_all, backtest_ar))
    print(f"Wrote {out_path} ({len(html):,} bytes, {n_figs} embedded figures)")


if __name__ == "__main__":
    main()
