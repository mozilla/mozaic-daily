# Forecast-vs-summer-actuals — findings log

Append-only. Dead ends and corrections stay in.

---

## Round 1 — 2026-08-25 — Premise checks, then the decomposition

### F0. Two premises in the inbound handoff were wrong, and one of them was load-bearing

The task arrived via `HANDOFF_FORECAST_COMPARISON.md` from `scratch/brwells/regional-story`. Its
§5, flagged there as "THE BIGGEST TRAP", claims the forecast is Glean telemetry while the actuals
are Legacy, quantifies a ~1.6M spread, and instructs that the offset be measured on a quiet window
and carried as a stated constant.

**There is no offset.** The canonical published desktop curve is `legacy_desktop`
(`data-official/2026-08/csv/README.md`), and the canonical notebook's `[bq-actuals]` cell queries
`moz-fx-data-shared-prod.telemetry.active_users_aggregates`, `app_name = "Firefox Desktop"`, no
channel filter — the same table and filter as `regional-story/data/dau_mau.parquet`. Verified by
tie-out: at 2026-08-02 the published CSV's `desktop_actuals` reads 46,890,129 and an independent
28-day mean over the regional-story pull reads 46,890,129. Identical.

The handoff's numbers came from `mozaic_daily_forecast.*.gd-D.parquet` in the repo root, which are
**glean_desktop scratch runs, not the published artifact**. Had the offset been "measured and
carried" as instructed, it would have injected a fabricated correction of roughly the size of the
effect being measured.

Second, smaller: §5 asks whether the forecast values are daily or smoothed and says not to guess.
They are 28-day trailing means, stated in the CSV README. Smoothed-vs-smoothed throughout.

*Lesson recorded, not just the fact:* a handoff written from outside the owning repo can be
confidently wrong about that repo's artifacts. Check the owning project's own docs before
importing a "trap" as a constraint.

### F1. The miss is real, large, and opens as drift rather than a seam error

August canonical (seam 2026-08-02, published `h` anchor −1,315,000) against actuals over
2026-08-02 → 2026-08-23:

| track | at seam | at 2026-08-23 | mean |
|---|--:|--:|--:|
| all countries | +24,589 | **+778,734** | +357,480 |
| ex-IR/CN | +16,636 | **+695,352** | +321,546 |

The gap starts near zero and grows at a near-constant rate (`plots/gap_opens.png`). It is a
trajectory error, not a mis-set starting level.

### F2. Most of the miss is the part we should have got right — but half of that is the headwind

Seam-anchored counterfactual (B meets actuals at 2026-08-02, so all three series start together):

| track | miss `C−A` | legitimate `C−B` | illegitimate `B−A` |
|---|--:|--:|--:|
| all countries | +778,734 | +188,956 (24%) | +589,778 (76%) |
| ex-IR/CN | +695,352 | +285,551 (41%) | +409,801 (59%) |

The illegitimate bucket splits again. `h` is a display-layer linear ramp, zero at the seam and
−1,315,000 at 2026-12-15; by 2026-08-23 **15.6% has phased in, i.e. 204,556 DAU**. So ex-IR/CN:

```
illegitimate +409,801  =  `h` ramp 204,556  +  model +205,245
```

Roughly half of "we should have got this right" is a deliberate exogenous judgement, not a Prophet
defect. On the all-countries track the model share is larger (+385,222 of +589,778).

### F3. The seam anchor is clean of the 2026-only level events; the pre-summer anchors are not

`o` (MozillaOnline) and `l` (launch-on-login) are in 2026 and absent from every norm year, so their
growth inside the window is mislabelled as "shallow summer". Measured as growth since each anchor,
ex-IR/CN:

| anchor | `o` | `l` | combined | legitimate `C−B` |
|---|--:|--:|--:|--:|
| seam | −2,593 | +7,923 | **+5,330** | +285,551 |
| jun15 | +25,751 | +107,922 | +133,673 | +660,746 |
| spring | +46,735 | +188,226 | +234,962 | +1,152,239 |

Both curves had plateaued by 2026-08-02 (`l` hit its 200,000 ceiling on 2026-07-19), so the
headline legitimate figure is seasonality rather than migration. The whole-season figure is not:
of the jun15 legitimate +660,746, about +133,673 is `o`+`l`, leaving ~+527,073 of genuine excess.

### F4. 2026's trend is ordinary; its summer shape is not

Each year against its own Jun-15 28d-MA baseline, ex-IR/CN:

| year | Aug-23 / baseline | baseline YoY |
|---|--:|--:|
| 2022 | 91.64% | — |
| 2023 | 92.96% | −6.20% |
| 2024 | 91.34% | −2.87% |
| 2025 | 92.88% | −5.05% |
| **2026** | **93.63%** | −5.22% |

2026's baseline YoY sits mid-range, so the underlying decline is not anomalous — the shape is.
2026 is above the norm range (91.34–92.96%) at z = +1.70 ex-IR/CN, z = +2.31 all countries. The gap
between the two tracks is largely the China migration.

**With n = 4 this is suggestive, not decisive.** The norm's own Aug-23 ratios span 1.63 percentage
points; a single year 1.43 points above a four-year mean is not strong evidence of a regime change.

### F5. July is the worst vintage, not June

Each vintage's B anchored at its own seam, all scored at 2026-08-23, all countries:

| vintage | seam | miss | legitimate | illegitimate | illegit share |
|---|---|--:|--:|--:|--:|
| August | 2026-08-02 | +778,734 | +188,956 | +589,778 | 76% |
| July | 2026-07-06 | +2,767,856 | +47,730 | +2,720,126 | **98%** |
| June | 2026-05-26 | +3,544,214 | +2,012,090 | +1,532,124 | 43% |

June's miss is the largest but nearly half of it is a summer that genuinely beat the typical shape
measured from late May. July's is almost pure trajectory error: from its seam, actuals tracked a
typical seasonal shape to within 47,730 DAU, and the published curve still finished 2.72M below it.

Caveat carried in the report: the ladder compares at a fixed date, so longer-horizon vintages have
had more time to drift. It is "where does each published number stand today", not a like-for-like
accuracy score.

### F6. The entire scored window sits inside the seam splice

`display_ma`'s variance-matched transition runs 27 days from the seam, so August's published curve
matches a plain `rolling(28).mean()` only from 2026-08-29. Actuals end 2026-08-23. Every scoreable
day is spliced.

Measured: the splice puts the published curve **−72,141** below the model's own MA at 2026-08-23
(mean −38,481, max absolute 73,542). About 10% of the ex-IR/CN miss is therefore a display
convention. It is real for anyone reading the published chart and it is not a model error — the
two claims need separating whenever this miss is discussed.

### F7. Japan automation is small but not nil

Named, not netted out (the convention is to judge the published KPI as published, with no bot correction). Japan is 2.34% of desktop DAU over the window.
Importing regional-story's measured −1.84 points on Japan's own late-summer level and scaling by
that share gives ~19,515 DAU globally, ~6.8% of the ex-IR/CN legitimate component. The direction
matters: it makes the summer look *less* genuinely strong than the headline suggests.

---

## Open / next

- **Re-run after 2026-09-01.** 2026's minimum so far is 2026-08-21 but the norm years trough
  between Aug 19 and Aug 27. If actuals fall further, the legitimate share shrinks.
- **Country attribution.** A broad-based miss and a two-market miss imply different remedies, and
  nothing here distinguishes them.
- **Not attempted: why the model drifts low.** This cluster measures the miss and splits it. It
  does not diagnose the mechanism, and the +205,245 ex-IR/CN model component is currently just a
  residual with a name.

---

## Round 2 — 2026-08-25 — Separating the headwind from the model

Prompted by the observation that "illegitimate" was doing too much work: it bundled a model error
with a deliberate policy choice, and those have different owners.

### F8. Only `h` is separable — and `t` is exactly zero on desktop

Four adjustment codes touch an August curve. Checked each rather than assuming:

| code | desktop at 2026-08-23 | separable? |
|---|--:|---|
| `h` Win10 headwind | −204,556 | **yes** — display-layer, applied to the 28d MA post-mozaic |
| `t` mobile tailwind | **0** | N/A — `tailwind.json` sets `desktop_dau: 0` |
| `l` launch-on-login | 200,000 add-back (188,226 ex-IR/CN) | no — baked into the parquet |
| `o` MozillaOnline | 646,408 add-back (46,735 ex-IR/CN) | no — baked into the parquet |

`t` being literally zero on desktop is worth recording: it is a live, sizeable (+299,000) mobile
adjustment this cycle, so "the tailwind" is a natural thing to reach for in a desktop conversation.
It contributes nothing here.

`l` and `o` are bidirectional — subtracted from training pre-mozaic, added back post-mozaic — so
their add-back level is **not** their effect. Prophet's fit moves when the subtraction moves, and
CLAUDE.md is explicit that the realised effect is config-dependent and must never be modelled as a
level shift. Their levels are reported as indicative magnitude only. A genuine counterfactual needs
a model re-run, and past builds are locked.

The `h`-removed curve is not derived by hand: August ships `WIN10_HEADWIND_REMOVED` CSVs, and
`published_forecast_no_headwind` reads them and **asserts** `A_nh == A − ramp` to within 1 DAU
(rounding) rather than trusting the identity. July and June have no such file, so the ramp is
subtracted from their published curves using their own specs.

### F9. The three-way split, and the flip when `h` comes out

August canonical, seam-anchored, at 2026-08-23:

| track | total | shallow summer | model | `h` headwind |
|---|--:|--:|--:|--:|
| all countries | +778,734 | +188,956 (24.3%) | +385,222 (49.5%) | +204,556 (26.3%) |
| ex-IR/CN | +695,352 | +285,551 (41.1%) | +205,245 (29.5%) | +204,556 (29.4%) |

**Removing `h` flips the verdict on the year-comparable track.** The model's own miss is +490,796
ex-IR/CN and **58.2% of it is the shallow summer** — majority legitimate, the opposite of the
published-curve reading (41.1%). The all-countries track does not flip (32.9% legitimate) because it
still carries the China migration.

Both readings are correct; they answer different questions. "Was the published number low?" — yes,
and about a quarter of that is the headwind. "Was the model wrong?" — much less than the published
miss suggests, on the population where years are comparable.

### F10. The headwind takes a near-constant share of every vintage, and July owns the model failure

Three-way ladder, all countries, each B at its own seam, all scored 2026-08-23:

| vintage | seam | total | shallow summer | model | `h` | `h` share |
|---|---|--:|--:|--:|--:|--:|
| August | 2026-08-02 | +778,734 | +188,956 | +385,222 | +204,556 | 26.3% |
| July | 2026-07-06 | +2,767,856 | +47,730 | **+1,969,428** | +750,698 | 27.1% |
| June | 2026-05-26 | +3,544,214 | +2,012,090 | **+739,566** | +792,558 | 22.4% |

Two findings the two-way split had hidden:

1. **June has the *best* model of the three** (+739,566), not the worst. Its total is the largest
   only because it is the oldest and because 56.8% of its miss is a summer that genuinely beat the
   typical shape from late May. Reading totals alone blames the wrong vintage.
2. **The `h` share is stable at 22.4–27.1%** despite very different ramp conventions — June and
   July anchor at 2026-04-01 and so carry 792,558 and 750,698 of drag by 2026-08-23, against
   August's 204,556 off a seam-anchored ramp. The absolute drag differs 3.9×; the share barely
   moves.

Caveats retained: the ladder scores at a fixed date, so longer-horizon vintages have had more time
to drift; and each row's B is anchored at its own seam, so the shallow-summer terms cover different
spans and are **not comparable across rows** — only the split within a row is meaningful.

### Dead end noted: yellow for the third stack segment

The obvious 4th categorical slot (yellow `#eda100`) fails the validator's normal-vision floor
against orange (ΔE 13.7, floor 15) on the adjacent pairlist, which is the pairlist a stacked bar
uses. Violet (slot 7, `#4a3aa7`) clears every gate beside aqua and orange and is used instead.
Recorded so the next chart in this cluster does not re-derive it.

---

## Round 3 — 2026-08-25 — Prophet's seasonality, and where July's summer got deep

New question: not "how far was the curve out" but "did the model expect a deeper summer than the
last four years delivered". Reported in absolute DAU rather than as ratios, because a ratio hides
the year-over-year decline in size — 1% of 2022's 58M is 580,000 DAU, 1% of 2026's 48M is 480,000.

### F11. Fitted Prophet objects are not stored; the components are recoverable anyway

`tile.forecast_model` is the factory **closure**, not a fitted model, so there is no
`predict_seasonal_components()` to call and no `seasonalities` dict to read. What each of the 48
tiles keeps is enough to rebuild the decomposition arithmetically: a trend, a reconciled forecast
(1,000 posterior samples, level space), and a holiday-impact series (1,000 samples, ≤ 0).

Verified against the published parquet rather than assumed:

```
parquet = Σ forecast_reconciled + Σ holiday_impacts + (l + o overlays)
```

Overlay residual is smooth and level, as it must be: **+763,385 (std 62,932)** on August,
**+694,627 (std 70,492)** on July — consistent with July's 125K launch-on-login ceiling against
August's 200K. Holidays are NOT inside `forecast_reconciled`; they are applied on top, which is why
omitting them leaves a −12.3M gap at 2027-12-31. So the model's seasonal component is
`Σ reconciled + Σ holidays − trend`, and **holidays stay in** — actuals contain them, so stripping
them from the model side only would make the two incomparable.

### F12. August's seasonality matched history. July's was ~1.8–2.1M too deep

Summer seasonal trough (2026-08-01 → 09-30), DAU against a common trend baseline:

| vintage | track | model | history | model − history | pre-recon − history | reconciliation |
|---|---|--:|--:|--:|--:|--:|
| August | ex-IR/CN | −4,629,918 | −4,790,514 | **+160,596** | +223,149 | −62,554 |
| August | all | −4,837,342 | −4,821,611 | **−15,730** | +44,379 | −60,110 |
| July | ex-IR/CN | −6,705,545 | −4,869,748 | **−1,835,798** | −562,590 | −1,273,207 |
| July | all | −6,961,434 | −4,892,073 | **−2,069,361** | −756,897 | −1,312,464 |

**August's seasonality was right** — within 15,730 of history on all countries, and marginally
*shallower* than history ex-IR/CN. The original hypothesis behind this whole investigation ("Prophet
expected a deeper slump than the multi-year average") is **false for August and true for July**.

### F13. The two panes agree independently, which is the strongest result here

Round 2's miss decomposition put July's model term at **+1,969,428** and August's at **+385,222**,
a 5.1× gap it could measure but not explain. Round 3 explains it: July's seasonal component is
**2,069,361** too deep at the trough on the same all-countries basis — agreeing with the model miss
to within **100K** — while August's seasonality is right, so August's model miss must be *trend*
rather than seasonality.

Nothing was tuned to make these agree. The two constructions share no code path beyond the actuals
cache: one differences published 28-day-MA curves, the other decomposes fitted-model internals on a
centred 7-day basis.

**Most of July's excess depth is reconciliation, not Prophet.** Before reconciliation July is only
562,590 deeper than history ex-IR/CN; after, 1,835,798. Reconciliation contributes 1,273,207, or
**69%**. August's equivalent is −62,554. So the per-tile fits were close in both cycles and the
aggregation step is where July's summer got deep — consistent with mozaic reconciling top-down,
which makes the aggregate its own fit rather than the sum of the tiles. **Why it did so for July and
not August is not answered here.**

### F14. Two traps that would have produced confident garbage

**The two builds store `trend` in different spaces.** August (package `4f33650`) stores **log** DAU
— tile trends span 9.4–16.4, aggregate ~17.6. July's build stores **level** DAU — aggregate spans
45.0M–49.1M. Assuming either silently destroys the result: `exp()` of a level-space trend overflows
to `inf`, and a log-space trend read as a level is off by ten orders of magnitude. `trend_in_dau()`
now **detects** the space per tile against that tile's own forecast scale and raises when neither
interpretation fits within 4×.

**gsutil produced a byte-complete but corrupt 631 MB download.** The default sliced parallel
download hung at 0% CPU with all 661,420,403 bytes written; renaming the `.gstmp` gave a file of
exactly the right size that failed to unpickle (`invalid load key, '\x00'` — holes). This is the
macOS gsutil multiprocessing bug from the project guidance, and it bites `cp` of a large single
object, not just `-m` and `rsync`. Fix: `-o "GSUtil:parallel_process_count=1" -o
"GSUtil:parallel_thread_count=1" -o "GSUtil:sliced_object_download_threshold=0"`.

*Lesson recorded:* a size check is not an integrity check. What caught this was loading the file and
verifying it reproduced a known artifact — which is also what proves the pickle is the published
fit rather than a look-alike.

### F15. July's published pickle exists after all — in GCS, under a scan directory

Round 3 initially concluded July's fitted objects were never saved: `data-official/2026-07/
desktop_locked/` holds only the parquet, both on disk and in GCS, and the one `desktop_lo_rerun`
pickle in the July archive is the superseded **2026-06-29** pass, not the published 2026-07-06
build. That conclusion was wrong, and the correction came from the user: everything is backed up to
GCS before deletion, so it had to be somewhere.

It is at `july-2026/param-scans/aug22-retune/round1/center/cps0.08983_thresh032_recent13_cpr0.65_
ncp25_clip0.6_sps0.00825/`. Its reconciled forecast reproduces July's published parquet **exactly —
0 DAU across all 198,696 rows**, as does `round2/auto__sps00825/`. So it is the published fit, not
a re-fit of the published config: the `_sps0.00825` in the slug is the package default made explicit
later, which is why July's `parameters.json` records no `sps` at all.

*Lesson:* "the artifact was never saved" is a claim about a search, not about the world. Enumerate
the whole archive prefix before concluding absence — the file was three directory levels away from
where its name suggested it would be.

### Methodology note: why this pane smooths on 7 days, not 28

Any whole number of weeks cancels DAU's weekly cycle exactly, so a 7-day window gives up nothing
there. A 28-day **trailing** mean is undefined for the first 27 days of a forecast window, and
August's seam sits only 21 days before the last landed actual — on a 28-day basis the model curve
and the actual curve **do not overlap at all**, which is what forced the change. Centring also
removes the ~13-day phase lag a trailing window puts between curves, which would be fatal to a
comparison of shapes. Cost: slightly noisier actuals, 3 days lost at each end.

### Dead end: per-tile trends do not sum to the aggregate trend

Σ exp(tile.trend) drifts from the Mozaic-level trend by up to 0.40% (August) and 1.80% (July)
inside 2026, reaching 2.02% / 4.89% by end-2027. This is expected — reconciliation is top-down, the
fits are independent, and summing `exp()` cannot add up in principle (Jensen) — but it means the
tile sum is not a drop-in for the aggregate. Resolved by treating the aggregate trend as
authoritative and rescaling the tile sums onto it per date, so the all-countries track reproduces
the aggregate exactly and ex-IR/CN is the same subset under the same correction. First attempt was
a tolerance check that simply failed on July; a tolerance was the wrong tool for a real,
understood divergence.

---

## Open / next (updated)

- **Why reconciliation deepened July's summer and not August's.** The effect is measured, not
  explained. The builds differ in changepoint settings, seasonality regime, and mozaic package
  version, and nothing here separates those. This is the most promising open thread.
- **n = 2 on the reconciliation finding.** Whether this is a recurring property of top-down
  reconciliation or a July-specific accident needs more vintages — June's pickle is in GCS.
- Everything under Round 1's open list still stands (re-run after 2026-09-01, country attribution).

---

## Round 4 — 2026-08-25 — Re-anchoring the seasonality pane on Feb 15 – Apr 15

Round 3 anchored the seasonality comparison on each build's own fitted trend. At review the anchor
was changed to the **Feb 15 – Apr 15 window** used elsewhere in the analysis, and the trend-anchored
version was **replaced**, not kept alongside.

### F16. Prophet's seasonality repeats; its holidays do not

The spring anchor is only possible if the model's 2027 cycle can stand in for 2026 — August's window
starts 2026-08-02, so 2026 has no spring in it. Testable, because Aug 05 – Dec 28 appears in **both**
years of the window: 146 matched calendar days, weekly cycle removed.

| component | 2027 − 2026 drift | in DAU at 47M |
|---|--:|--:|
| seasonality only | mean −0.34pp, **max 0.59pp** | mean −160K, max 276K |
| seasonality + holidays | mean −0.52pp, **max 3.95pp** | mean −244K, **max 1,856,032** |

Expected: Prophet's yearly term is a Fourier series on a fixed 365.25-day period, so it is periodic
by construction, and the 0.59pp residual is consistent with the 365.25-vs-365 drift. This also
empirically rules out a conditional seasonality tied to `prophet_recent_weeks` being active in the
forecast window — if one were, it would not repeat.

Holidays break periodicity, worst on 12-21, 12-24 and 12-28 — moving Christmas weekday alignment.
Visible consequence on the charts: Prophet's Easter dip sits at 2027's date (Mar 28), the empirical
curves' at 2026's (Apr 5). **Answer to "can we use 2027": yes for seasonality, no for holidays.**
Holidays stay in per review decision, with the drift printed.

### F17. A construction bug caught in the first draft of the re-anchoring

The first spring-anchored version divided the model by its trend (`level/trend`) while dividing each
empirical year by its own spring *level*. That made the model the only **trend-free** curve of the
three. Over the ~5 months from anchor to trough, at 2026's −5.22%/yr, that asymmetry is worth
roughly **1M DAU** — comparable to the effect being measured, and it flattered the model badly
(ex-IR/CN model-vs-history read +1,881,538 instead of +752,221).

Fixed by making all three trend-inclusive: each is divided by its own spring level, matching the
`spring` baseline on the other tab. Residual approximation: Prophet's shape comes from 2027, so it
borrows 2027's within-year trend slope as a stand-in for 2026's. The rates are close (~−5%/yr both
years) but it is an approximation, not an identity.

*Lesson:* when re-anchoring, re-derive every curve from the new anchor rather than adapting the old
expressions. Two of the three curves changed construction and one did not, which is exactly how the
asymmetry got in.

### F18. On the spring anchor, August leaned the right way and July the wrong way

2026's trough came in **+1,762,139 shallower** than the 2022–25 average (ex-IR/CN). Against that:

| vintage | track | Prophet | history | realised | Prophet − history | share of gap closed | reconciliation |
|---|---|--:|--:|--:|--:|--:|--:|
| August | ex-IR/CN | −6,450,861 | −7,203,082 | −5,440,943 | **+752,221** | **42.7%** | +255,919 |
| August | all | −6,650,325 | −7,086,109 | −4,064,724 | +435,784 | 14.4% | −221,906 |
| July | ex-IR/CN | −8,997,476 | −7,203,082 | −5,440,943 | **−1,794,394** | **−101.8%** | −1,533,005 |
| July | all | −8,963,297 | −7,086,109 | −4,064,724 | −1,877,189 | −62.1% | −1,730,072 |

**This changes the framing from Round 3 without contradicting it.** On the trend anchor August read
as "matched history" (+160,596, ~1σ). On the spring anchor it reads as "leaned shallow by 752,221,
covering 42.7% of the way to reality". Both are true — they answer different questions, and the
spring version is the more useful one: it says August's Prophet *did* partly anticipate a
shallower-than-normal summer, and was still ~1.01M too deep.

July is unchanged in direction and larger in magnitude: −1.79M deeper than history, −3.56M deeper
than reality, with reconciliation contributing −1,533,005 of the excess (85%).

### F19. The all-countries spring anchor is inside Iran's outage

Iran's shutdown runs 2026-03-01 → 2026-05-25, straight through the 2026 Feb 15 – Apr 15 reference
window. So the all-countries reference level (49,081,506) is depressed and every deviation measured
from it is inflated — which is why the all-countries "share of gap closed" reads 14.4% against
ex-IR/CN's 42.7%. **Quote the ex-IR/CN track** (reference 47,830,894). This is the same defect the
first tab's `spring` baseline carries, for the same reason, and it is printed on both panes.

Separately: the anchor window carries a holiday effect on **all 60 days**, mean drag −2.67% of trend
(−1.25M DAU), worst day −13.1%. Because the anchor is a 60-day mean and Easter falls inside the
window in both years, a date shift largely cancels; residual instability ~±0.5pp, about 5% of the
11pp peak-to-trough signal.

### F20 (correction to Round 4) — the 7-day window was carried over needlessly, and it cost readability

The first spring-anchored draft kept the **centred 7-day** smoothing from the seam-anchored
construction. That window was genuinely required there: the model's curve began at the seam and a
28-day trailing mean is undefined for 27 days after it, which left **zero overlap** with actuals for
August. Anchoring at spring removes the constraint completely — Prophet's shape comes from a
*complete* 2027 cycle with 2026 data sitting behind it inside the same forecast window, so a 28-day
trailing mean is defined across the whole year.

Keeping 7 days bought nothing and cost a lot: the charts were spiky, because mozaic's holiday
impacts are sharp discrete events and a 7-day window barely attenuates them. Switched to the
**28-day trailing mean** used by the published curves and the first pane, and switched the y-axis
from deviations to **absolute DAU levels**, so the pane reads like the rest of the analysis.

Numbers moved with the smoothing, in the same direction:

| | 7-day centred (withdrawn) | 28-day trailing (current) |
|---|--:|--:|
| gap to close, ex-IR/CN | +1,762,139 | **+1,120,867** |
| August, Prophet − history | +752,221 | **+718,996** |
| August, share of gap closed | 42.7% | **64.1%** |
| July, Prophet − history | −1,794,394 | **−1,532,344** |

The narrower window exaggerated the trough depth on every curve, which inflated the gap and
understated how much of it August covered. Conclusions are unchanged in direction; August looks
better on the correct convention.

*Lesson:* a constraint that forced a choice in one construction does not survive into the next one.
When the construction changed, the window should have been re-derived rather than inherited — the
same failure mode as F17, one step earlier in the pipeline.

### Chart convention, adopted 2026-08-25

The seasonality pane's layout was adopted as the house standard and applied back to the first pane,
so the two read as one analysis rather than two:

- one figure per comparison, with **two panels** for the population tracks, not two files;
- **full-year x-axis** where the data allows, monthly ticks;
- **absolute DAU** on the y-axis, 28-day trailing mean — the published-curve convention;
- direct labels at each curve's end, legend above;
- construction caveats in a footnote under the axes, not in the caption.

Changed: `three_series_all.png` + `three_series_ex_ir_cn.png` → `three_series.png` (two panels, Jan
→ Dec instead of May → Oct); `trend_check_all.png` + `trend_check_ex_ir_cn.png` →
`trend_check.png`. No numbers moved — this was layout only. The bar-chart decompositions keep their
own form, since a full-year axis does not apply to them.

One consequence worth stating: `three_series.png` now spans the whole year, so Jan–Jul shows actuals
alone. That is correct rather than empty — B is rescaled to meet 2026 at the seam, so drawing it
earlier would show a divergence that is an artifact of the rescaling, and A did not exist before the
seam. It also makes the December Christmas cliff visible on both panes, which is the same trailing-
window feature in both.

### F21 — the two panes' "typical summer" curves were the same object under different anchors

Raised at review, and correct: after the chart layouts were unified the two panes had visually
identical green lines that meant different things. Pane 1's counterfactual was rescaled to meet 2026
**at the seam** (2026-08-02, so it started there); pane 2's was rescaled to the **Feb 15 – Apr 15**
window (so it spanned the year). Same 2022–25 average underneath, two anchors, one indistinguishable
line style.

Checked first rather than assumed: `series.typical_summer(track, "spring")` and the seasonality
pane's history curve are **byte-identical — 0.0000 DAU max difference over all 365 days**, on both
tracks. So they are literally the same series, not merely similar constructions.

Resolved by drawing **both** on pane 1, using the hue-plus-dash convention this cluster already uses
for A / A-without-headwind: one entity, two constructions, one hue, dash carries the difference. The
footnote states which anchor the decomposition table uses (seam — it charges August only for the 22
days it forecast) and that the dashed one is the other tab's curve.

**Unexpected benefit: it made the Iran contamination visible.** On the all-countries panel the
spring-anchored curve runs roughly a million DAU below actuals from May onward — not because a
typical summer was that much worse, but because the 2026 Feb 15 – Apr 15 reference window sits inside
Iran's 2026-03-01 → 05-25 outage, depressing the level it is rescaled to. On ex-IR/CN the same curve
tracks actuals closely. That caveat had been stated numerically since Round 1 (`spring` baseline) and
again in Round 4 (F19); this is the first time it is *shown*.

*Lesson:* unifying chart grammar across two analyses raises the bar on their methods matching too.
Making two charts look alike is a claim that they are alike, and that claim has to be checked, not
just the pixels.

---

## Round 5 — 2026-08-25 — Both anchors, on separate plots, on both tabs

F21 put both anchors of the counterfactual on one chart. At review that was judged the wrong shape:
they serve different purposes and should be separate plots — and the seasonality tab should get the
seam-anchored view it was missing. Both done.

Result: each tab now carries both anchors as distinct figures. `three_series.png` split into
`three_series_seam.png` + `three_series_spring.png`; new `seasonality_seam_august.png` +
`seasonality_seam_july.png`.

### F22. A splice bug that made July's seam-anchored seasonality read the wrong sign

The seam-anchored seasonality view needs the model's curve to start at the seam. On the 28-day
trailing convention used everywhere else, it cannot: the model has no output before its seam, so the
mean is undefined for 27 days. First attempt filled that window with actuals — the repo's own
`display_ma` convention — scale-matched at the seam to keep the spliced series continuous.

**That silently imported actuals' trajectory into the model's curve over exactly the window being
measured.** The July build's daily level at its seam sits **5.35% below actuals** (46,958,868 vs
49,615,597 — far more than the ~1.5% the `l`+`o` overlays account for), so the scaled prefix
dominates the 28-day average for 27 days and the curve follows actuals' shape, not the model's.

Consequence: July's model read **+488,729 shallower** than history at the summer trough. Its own
2027 Jul-06 → trough descent is **−3,929,823**, against history's ~−2.6M — i.e. about **1.5M
deeper**. The sign was inverted.

Caught because the seam and spring views disagreed in sign over the same span, which is impossible
for the same two series, so one had to be wrong.

Fixed by dropping the splice entirely: a **centred 7-day window anchored at seam+3** needs only 3
days either side of the anchor, all of which the model has. Any whole number of weeks cancels the
weekly cycle exactly, so 7 loses nothing there; it is simply noisier than 28.

*This partially retracts F20.* F20 was right that the SPRING view does not need a short window — it
uses a complete 2027 cycle with 2026 behind it. It was wrong to imply the short window was never
needed: the SEAM view genuinely requires it, for the reason above. The two views legitimately use
different windows, and each footnote now says which and why.

### F23. Both anchors agree on direction; the spans differ, so the magnitudes do

| vintage | anchor | Prophet − history at trough | gap to close | share closed |
|---|---|--:|--:|--:|
| August | Feb 15 – Apr 15 | +718,996 | +1,120,867 | **64.1%** |
| August | seam | +497,427 | +1,027,242 | **48.4%** |
| July | Feb 15 – Apr 15 | −1,532,344 | +1,120,867 | **−136.7%** |
| July | seam | −175,869 | +992,165 | **−17.7%** |

(ex-Iran/ex-China.) August leans shallow — the direction reality went — on both anchors, covering
roughly half to two-thirds of the distance. July leans deep on both. The spring anchor gives larger
magnitudes because it measures a ~5-month descent against the seam anchor's ~6 weeks; neither is
more correct, and the disagreement in size is the reason to publish both rather than pick one.
