# `data-official/2026-08/organic/` — measured Fenix paid/organic split (`p`)

The input to adjustment **`p`** (`paid_organic_split`), which replaces `m` for mobile from this
cycle. Where `m` modelled paid as an *increment since an anchor*, `p` **measures** it from the
client-level paid/organic flag, forecasts organic only, and stacks marketing's paid **level** on
top.

| file | role |
|---|---|
| `organic.json` | the spec — gated to `applies_to_forecast_start: 2026-07-28` |
| `fenix_paid_organic.2026-07-28.parquet` | measured split, `date × country` (787 days × 16 countries = 12,592 rows) |
| `fenix_paid_organic.2026-07-28.parquet.meta.json` | sidecar: definition, sources, coverage, all four build checks |

Produced by `scripts/build_fenix_organic_split.py` (~141 GB scan, ~$0.70). Rerun per cycle:

```bash
source .venv/bin/activate
python scripts/build_fenix_organic_split.py \
    --forecast-start-date <T-0> \
    --production-raw data-official/<cycle>/<mobile build>/<slug>/mozaic_parts.raw.glean.mobile.DAU.parquet
```

**Always pass `--production-raw`.** It is the only thing that catches the mirror and the
production table covering different Fenix populations, which would invalidate the whole
share × level construction.

## Why this replaces `m`

Desktop's `l` and `o` can honestly model "incremental DAU since launch" because launch-on-login
and the MozillaOnline migration have hard start dates. **Mobile paid acquisition has run
continuously for years**, so the anchor is an accounting choice and everything downstream
inherits it. Two consequences that `p` removes:

- The bidirectional overlay **absorbed 58%** of any change to the curve — the two August metric
  bases differ by −141,653 at Dec-15 but moved the KPI only −59,875, because a smaller
  subtraction leaves Prophet a higher organic trend that partly offsets the smaller add-back.
  *A lift-curve comparison was not a KPI estimate.* Under `p`, paid contributes exactly its own
  value.
- Marketing's source has no cohort dimension, so the lift understates post-anchor cohort DAU by
  an amount that source cannot measure. `p` does not rely on the lift's *level* at all for the
  historical half — it measures it.

## What "organic" means

A Fenix client is **paid** iff `mozdata.fenix.new_profile_clients` has
`paid_vs_organic_gclid = 'Paid'` **and** `normalized_channel = 'release'` **and**
`install_source = 'com.android.vending'`. **Organic is the residual** — sideloads, beta/nightly,
unclassified clients, and profiles predating that table. This is the authoritative marketing
definition (Redash 118471, matching the `fenix-dau-paid-gclid-vs-organic` dashboard). It
deliberately does *not* use `paid_vs_organic_gclid = 'Organic'` positively, which leaves a third
unclassified bucket that does not sum to the KPI.

All channels are included: beta and nightly are never marketed, so labelling them organic is
correct and it preserves the published KPI level.

## The share, not the level

The artifact carries `organic_dau` / `paid_dau` / `total_dau` **for audit only**. What the
applier consumes is `organic_share`, multiplied against the *production* query's level:

```
organic_y(d, c) = y_production(d, c) * organic_share(d, c)
```

This is the **shredder correction**. The mirror is built from client-level
`mozdata.fenix.active_users`, which loses clients as deletion requests are processed; the
canonical aggregate is accumulating and was written when each day was fresh. Measured against
production's own table:

| date | mirror | production | gap |
|---|--:|--:|--:|
| 2024-06-01 | 9,160,189 | 9,429,437 | **−2.855%** |
| 2025-06-01 | 10,460,425 | 10,655,022 | −1.826% |
| 2026-04-01 | 11,586,328 | 11,648,395 | −0.533% |
| 2026-06-25 | 12,969,783 | 12,969,783 | **0.000%** |

Monotone (rank corr 0.998), ~2.9pp of pure artifact growth. Taking the level from the mirror
would read as **+3.3pp of fake growth**. Taking only the share cancels it between numerator and
denominator — exact if attrition is share-neutral between paid and organic, which is the
assumption.

The exact zeros at the trailing edge are also a population check: the two sources agree perfectly
once attrition has not yet bitten, confirming that `Fenix MozillaOnline` (~76.2K DAU/day) and
`Fenix browserstack` are excluded identically on both sides.

## Per-country, because the spread is enormous

Paid share on 2026-07-27 ranges from **0.2% (RU)** to **27.6% (ID)**:

| | organic share 2024-06-01 | organic share 2026-07-27 | paid now | Fenix DAU |
|---|--:|--:|--:|--:|
| ROW | 0.9979 | 0.9044 | 9.6% | 4,336,080 |
| DE | 0.9947 | 0.9219 | 7.8% | 2,001,678 |
| US | 0.9900 | 0.8735 | 12.7% | 1,682,270 |
| IN | 0.9752 | **0.7253** | **27.5%** | 1,111,785 |
| BR | 0.9698 | 0.7430 | 25.7% | 392,229 |
| ID | 1.0000 | **0.7236** | **27.6%** | 241,419 |
| MX | 0.9997 | 0.7524 | 24.8% | 206,660 |
| RU | 1.0000 | 0.9981 | **0.2%** | 183,722 |
| CN | 0.9999 | 0.9949 | 0.5% | 106,694 |
| IR | 0.9996 | 0.9882 | 1.2% | 538,533 |

A single global share (as the external prototype used) would attribute paid DAU to RU and CN
where essentially none exists, and under-attribute it in IN, ID, BR and MX — and that
misallocation would then propagate into each country's fitted trend.

## The assumption you need to know about

**Measured coverage starts 2024-06-01.** `mozdata.fenix.active_users` retains only a rolling ~25
months, so the split cannot be reconstructed earlier. Mobile DAU trains from **2020-12-31**, so
**~3.5 years of Fenix training rows carry a held-flat share, not a measured one.**

Masking those rows instead is not available: mozaic requires one common date grid across tiles,
and NaN-ing Fenix pre-2024-06 would corrupt the published `ALL MOBILE` training rows.

**The bound is small and it is knowable.** The ex-IR paid share is monotone over the whole
measured window — 1.10% at 2024-06, 11.58% at 2026-07:

| month | paid % | | month | paid % |
|---|--:|---|---|--:|
| 2024-06 | **1.10%** | | 2025-10 | 5.40% |
| 2024-12 | 2.54% | | 2026-01 | 6.79% |
| 2025-06 | 4.23% | | 2026-04 | 8.63% |
| 2025-08 | 4.61% | | 2026-07 | **11.58%** |

So holding 1.10% flat back to 2020-12-31 assumes paid was 1.1% throughout 2021–2024. Given the
monotone ramp, the true early figure was almost certainly *lower*, which means organic is
slightly **understated** in that era — an artifact of at most ~1.1pp of apparent extra growth
spread over 3.5 years (~0.2pp/yr). Against `prophet_changepoint_range = 0.75`, which makes the
extrapolated slope a recent-history quantity, this is small. The alternative — truncating all
mobile training to 2024-06-01 — would cost Firefox iOS and Focus 3.5 years of history and land
only ~55 days above Prophet's 730-observation yearly-seasonality gate.

Quantified as a sensitivity arm in `research/mobile-organic/reproduction.ipynb`.

## Scope decisions

- **Non-Fenix apps are 100% organic.** Firefox iOS has no paid signal at all:
  `paid_vs_organic_gclid` is 100% NULL in every month, Adjust attribution collapsed in Sept 2024
  (2,242,021 → 28,746 clients with a network), and 2026-07 shows **0 paid-attributed clients out
  of 3.66M**. No campaigns run for Focus. *This measures attribution, not spend* — the ask to
  marketing about live iOS spend is open, tracked in `research/mobile-organic/_index.md`.
- **IR is excluded from the split.** It is 98.8% organic, marketing's curve is explicitly ex-IR,
  and subtracting a paid component we never add back would knowingly bias the total low. This
  also honours `scope.exclude_countries`, which the `m` code path declared but never read — so
  IR has been silently receiving a share of the marketing lift until now.
- **DAU only.** New Profiles and Engagement are untouched.

## The paid half

`paid(d) = marketing_lift_daily(d) + 922,250.47`, from
`../marketing/marketing_lift_model.uac_meta_total.2026-07-28.parquet`. The delivered artifact is
a *lift*, not a level, because that is what `m` consumed; stacking needs the level, so the anchor
is added back. **Compare cycles on levels, never lifts** — August's lift is 18% below July's but
its level is 3.4% *higher*, because it also raised the anchor.

The curve ends 2026-12-31 while `forecast_end_date` is 2027-12-31, so it is **held flat**
thereafter. Zero-filling (what `m` does) would drop ~1.56M on 2027-01-01.

**Open question, deferred:** training rows get the *measured* paid added back and forecast rows
get *marketing's modelled* level, so the seam carries a step equal to their disagreement — the
prototype measured `organic + paid` against the observed total at +0.20% median, 0.744% max.
`research/mobile-organic/paid_seam_methods.ipynb` contrasts three treatments and ends in an
explicit human go/no-go. Until that lands, the build ships the honest splice.

## Where new files go

A refreshed split for a later `training_end` → rerun the producer, new `fenix_paid_organic.<T-0>`
parquet + sidecar, bump `applies_to_forecast_start`. Analysis that argues for a methodology
change → `research/mobile-organic/`, not here.
