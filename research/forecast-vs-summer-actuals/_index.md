# `research/forecast-vs-summer-actuals/` — how far was the published forecast from the summer?

**Read-only with respect to every forecast artifact.** Nothing here modifies `data-official/`.
This cluster audits published curves against actuals; it does not produce, retune, or correct one.

Question: August canonical is running below actuals. **How much of that miss is a summer nobody
could have forecast, and how much is a forecast that sat below even an ordinary summer?**

The decomposition is an identity, not a model. Reported **three ways**, because "illegitimate"
bundles a model error and a policy choice, which have different owners and different remedies:

```
C − A  =  (C − B)        +  (B − A_no_h)   +  (A_no_h − A)
          shallow summer    the model         the Windows 10
          [nobody's fault]  ran below         headwind we chose
                            typical           [a judgement call]
                            [model error]
```

The two-way `legitimate` / `illegitimate` split is the same identity with the last two terms
merged.

## Headline (2026-08-23, desktop, ex-Iran/ex-China, published Windows-10-headwind-applied curves)

| component | DAU | share of miss |
|---|--:|--:|
| **Total miss `C − A`** | **+695,352** | 1.63% of the published curve |
| Shallow summer `C − B` | +285,551 | 41.1% |
| Model miss `B − A_no_h` | +205,245 | 29.5% |
| Windows 10 headwind (code `h`) `A_no_h − A` | +204,556 | 29.4% |

All countries: +778,734 = +188,956 shallow + +385,222 model + +204,556 headwind.

**Remove the headwind and the verdict flips.** Judged on the model alone the ex-IR/CN miss is
+490,796 and **58% of it is the shallow summer** — majority legitimate, the opposite of the
published-curve reading. The all-countries track does not flip (33% legitimate), because it still
carries the China migration.

Report: **`site/index.html`** — open it directly, it is a static page with no build step. Two tabs:
*The miss, decomposed* (above) and *Prophet's seasonality* (below).

## Second pane: did Prophet expect the summer it got?

Same question one layer down — not "how far was the curve out" but "did the model's *seasonality*
anticipate a shallower-than-average summer". Each curve is its own seasonal shape **rescaled to
2026's Feb 15 – Apr 15 level**, holidays included, 28-day trailing mean — the same units and
convention as the published curves, so it reads like the first pane's charts.

2026's summer trough came in **+1,120,867 shallower than the 2022–25 average** (ex-IR/CN). That is
the gap a forecast had to anticipate.

| vintage | Prophet − history at the trough | share of that gap closed | of which reconciliation |
|---|--:|--:|--:|
| **August** | **+718,996** (shallower — right direction) | **64.1%** | +188,739 |
| **July** | **−1,532,344** (deeper — wrong direction) | **−136.7%** | **−1,651,953** |

**August's seasonality leaned shallow, the way reality went, and got most of the way there** — still
~0.40M too deep, but 64% of the distance covered. **July's leaned deep, against reality**, and the
excess came from mozaic's **top-down reconciliation** rather than the per-tile Prophet fits.

This corroborates the first pane from an unrelated direction: there, July's model term is
+1,969,428 against August's +385,222, and the two constructions share no code beyond the actuals
cache.

**The spring anchor is only available because Prophet's seasonality repeats** — measured at
**0.59pp** max drift between 2026 and 2027 on the 146 calendar days present in both years of
August's window, so the 2027 cycle stands in for 2026. **Holidays do not repeat** (up to 3.95pp,
~1.86M DAU, concentrated Dec 21–28), which is why December is the least reliable part of any chart
here and why Prophet's Easter dip sits at 2027's date.

Three further caveats are printed on the pane itself: the anchor window carries a holiday drag on
all 60 of its days (~±0.5pp of residual instability); the **all-countries anchor sits inside Iran's
outage** so only the ex-IR/CN track should be quoted; and all curves carry trend deliberately, so
Prophet borrows 2027's slope.

## What's here

| file | what it does |
|---|---|
| `fetch_actuals.py` | Caches per-country legacy-desktop DAU to `data/`. Same table and filter as the canonical notebook's `[bq-actuals]` cell. Seeds from the regional-story pull and tops up only the missing tail, so a refresh is ~28 GB per month of tail rather than the 5.3 TB a standalone 2019+ pull costs. `--full` forces the standalone pull. |
| `series.py` | The three series on one basis, for two population tracks. `published_forecast` (A), `typical_summer` (B), `actuals_ma` (C), plus `trend_table` and the `o`/`l` overlay sizing. |
| `analyze.py` | The decomposition, the vintage ladder, and four diagnostics (splice, headwind share, Japan bound, norm spread). Writes `data/decomposition.json` and prints a console summary. |
| `plots.py` | The miss-decomposition figures in `plots/`. |
| — | **Two anchors, always on separate plots.** Both tabs show the counterfactual anchored (a) at each vintage's **seam**, which judges it on the days it actually forecast, and (b) on **Feb 15 – Apr 15**, which shows the whole spring-to-summer descent. They answer different questions and are never overlaid — a five-line chart where the distinction is a dash pattern reads as one claim, not two. The seam anchor needs no 2027 stand-in and no Iran caveat; the spring anchor sees the descent. |
| — | **House chart layout:** anything with a per-population-track split is one figure with two stacked or side-by-side panels (`all countries` / `ex-Iran, ex-China`), full-year x-axis where the data allows, absolute DAU on the y-axis, 28-day trailing mean, direct end labels, construction caveats in a footnote. Both panes follow it so they read as one analysis. |
| `seasonality.py` | Recovers Prophet's seasonal component from the fitted-model pickles, and puts the 2022–25 average and 2026 actual on the same DAU baseline. Holds the pickle registry and the checks that a pickle really is the published fit. |
| `seasonality_plots.py` | The seasonality figures. |
| `build_report.py` | Renders `site/index.html`. Every number in the prose is interpolated from `analyze.build()` — none are typed. |
| `LOG.md` | Append-only findings log, dead ends included. |

Reproduce end-to-end:

```bash
source .venv/bin/activate
python research/forecast-vs-summer-actuals/fetch_actuals.py --refresh   # only when actuals moved
python research/forecast-vs-summer-actuals/plots.py
python research/forecast-vs-summer-actuals/seasonality_plots.py
python research/forecast-vs-summer-actuals/build_report.py
```

The seasonality pane needs both vintages' fitted-model pickles (~630 MB each, gitignored). They are
not kept on disk between cycles: pull the July one from `gs://…/july-2026/param-scans/aug22-retune/_rawcache/`
and the August one from `gs://…/august-2026/data-official/2026-08/desktop_g01_2026-08-02/<slug>/`.
August's is in the repo; **July's must be fetched**, and gsutil must be forced single-process or it
hangs with a byte-complete but corrupt file (see `LOG.md` F14):

```bash
gsutil -o "GSUtil:parallel_process_count=1" -o "GSUtil:parallel_thread_count=1" \
       -o "GSUtil:sliced_object_download_threshold=0" \
  cp gs://moz-data-science-brwells-bucket/mozaic-daily-archive/july-2026/param-scans/aug22-retune/\
round1/center/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/\
mozaic_objects.legacy_desktop.2026-07-06.pkl \
  research/forecast-vs-summer-actuals/data/pkl/
```

## Which adjustments are separable, and which are not

Repo adjustment codes (`h`, `t`, `l`, `o`) appear in this table and in `LOG.md` because engineers
need them to find the specs. **They never appear on a chart or in the report** — a reader of a
chart cannot resolve `h`, so chart-facing text says "Windows 10 headwind" (or "Win10 headwind"
where space is tight).

| code | desktop effect at 2026-08-23 | separable here? |
|---|--:|---|
| `h` Windows 10 migration headwind | −204,556 (15.6% of its −1,315,000 Dec-15 anchor) | **Yes** — display-layer, applied to the 28d MA after mozaic, so `published − ramp` is exact |
| `t` mobile tailwind | **0** | N/A — its spec sets `desktop_dau: 0` |
| `l` launch-on-login | 200,000 add-back (188,226 ex-IR/CN) | **No** — baked into the parquet |
| `o` MozillaOnline | 646,408 add-back (46,735 ex-IR/CN) | **No** — baked into the parquet |

`l` and `o` are per-tile **bidirectional** overlays: subtracted from training before mozaic, added
back after. Change the subtraction and Prophet's fit changes with it, so the realised effect is
config-dependent and is **not** a level shift — the add-back levels above are an order of
magnitude in play, never a counterfactual. A real `l`- or `o`-free curve needs a re-run of a
locked build, which this cluster does not do. Note the ex-IR/CN scope already strips ~93% of `o`
by construction.

## Three things to know before quoting a number

1. **The scored window is 22 days and it is entirely inside the seam splice.** August's seam is
   2026-08-02; `display_ma`'s variance-matched transition runs 27 days, so the published curve is
   byte-identical to a plain `rolling(28).mean()` only from 2026-08-29. Actuals land through
   2026-08-23. The splice puts the published curve 72,141 below the model's own MA at the window's
   end — about 10% of the ex-IR/CN miss is a display convention. Real for a chart reader, not a
   model error.
2. **The counterfactual's anchor decides the split, not the total.** Three anchors are reported and
   they answer different questions; they are not competing estimates of one quantity. `seam` is the
   headline (it judges August on the days August actually forecast); `jun15` answers the
   whole-season question; `spring` is the inbound handoff's choice and is **Iran-contaminated on
   the all-countries track** — 2026's Feb-15→Apr-15 window sits inside the 2026-03-01→05-25 outage.
3. **The trough has not landed.** 2026's minimum so far is 2026-08-21; the norm years trough
   between Aug 19 and Aug 27. Re-run after 2026-09-01 before treating the legitimate share as
   settled.

## What isn't here

- **Mobile.** Out of scope by decision: the published mobile curve carries a discretionary
  +299,000 `t` tailwind sized against July's published figure, so a mobile miss would mostly
  measure that judgement rather than a forecast error.
- **Country attribution.** World totals on two population scopes only. Which markets drive the
  miss is unanswered, and it matters — a concentrated miss has a different remedy from a broad one.
- **Any bot correction.** As-reported by decision. Japan's automation cohort is *named and bounded*
  (~19,500 DAU globally, ~7% of the legitimate component) using a factor imported from
  regional-story, with Japan's DAU share computed here. It is not netted out.
- **Rebuilding any forecast.** Past builds are locked. This cluster reads them.

## Where new work goes

Extending the audit to more vintages, more populations, or country attribution → here. Anything
that would *change* a forecast → not here; that belongs to the cycle directory under
`data-official/{YYYY-MM}/`.
