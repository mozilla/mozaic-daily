# `research/headwinds/` — headwind ramp shape and anchor magnitude

Two distinct questions live here. **Ramp shape** — given a target, what profile gets you there
(anchor date, slope, MA28 alignment math). **Anchor magnitude** — is the target itself the right
size, tested against telemetry.

## Files

| File | Purpose |
|---|---|
| `headwind_options.ipynb` | *Shape.* Compares ramp profiles; covers the Order-1 vs. Order-2 alignment trick (apply to MA28, not daily series) |
| `win10_anchor_validation.ipynb` | *Magnitude.* Validates the Win10 desktop anchor against Legacy telemetry — measures net Win10+Win11 change (migration is DAU-neutral, so the Win10 curve is not the headwind), ex-IR/CN, net of the `l`/`o` overlays |
| `WIN10_ANCHOR_FINDINGS.md` | *Magnitude.* Verdict: −1,295,000 vs −1,270,000 vs the live −1,245,000 are mutually indistinguishable, but all three require ~−540K of loss by Jul-22 that no specification variant finds. Also flags the forecast-seam double-count |
| `win10_anchor_report.html` | *Magnitude.* Self-contained narrative writeup of the same work for a non-specialist reader (figures embedded as base64; forwardable without the repo) |
| `build_report.py` | Generates `win10_anchor_report.html`; holds the report prose. Edit here, not in the generated HTML. Run from the repo root |
| `plot_headwind_ramps.py` | *Shape.* Renders `plots/headwind_ramps_july_vs_august.png` — the July vs. August ramps for desktop and mobile, plus August's mobile `t` tailwind. Reads the live specs and renders them through the production `render_adjustment`, so the figure cannot drift from what ships. Run from the repo root |
| `extracts/` | Committed BigQuery extracts + SQL for the magnitude work, so the notebook reruns without re-scanning 515 GB |
| `plots/` | Figures from `win10_anchor_validation.ipynb` and `plot_headwind_ramps.py` |
| `aug-post-seam-retune/` (refit pkl + parquets archived to `gs://…/august-2026/research/headwinds/aug-post-seam-retune/` on 2026-09-04; notebook, script, plots stay) | *EXPERIMENTS only, self-contained* (does not reconcile with `WIN10_ANCHOR_FINDINGS.md`). Two levers for the August desktop curve running below actuals. **Exp 1**: closed-form RMSE fit of the `h` anchor to 15 days of post-seam actuals → an extreme extrapolated **+2,954,764** (vs. live −1,315,000), an artifact of fitting a Dec-15-anchored ramp from 11% of its length. **Exp 2**: keep `h` at −1,315,000 and refit g01 on data through 2026-08-16 → Dec-15 **+132,382** from fresher data alone. Nothing adopted; all artifacts watermarked EXPERIMENT |

## Production usage

The chosen ramp lives in `data-official/{YYYY-MM}/adjustments/headwind.json` and is applied via the composite-style applier registered in `src/mozaic_daily/adjustments.py`. The applier subtracts the full anchor at the anchor date, not the window-averaged ramp — see `feedback_headwind_ma28_alignment` memory.

The April-vs-June mechanism cluster was the original consumer: the headwind ramp is one of the levers explored there to close the Dec-15 MA28 gap. **That cluster is archived to GCS and no longer on disk** (`gs://…/research-superseded/april-vs-june-mechanism/`, or the `july-forecast` branch history).
