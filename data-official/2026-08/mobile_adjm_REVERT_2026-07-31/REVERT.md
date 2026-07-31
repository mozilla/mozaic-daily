# `adj-m` mobile — REVERT TARGET (not an archive)

**This directory exists so the August mobile forecast can be put back the way it was.** A revert
was a live possibility at the time of the swap (2026-07-31) and nothing here should be deleted
while the August cycle is live.

It holds the August canonical mobile build as it stood from 2026-07-31 until the paid/organic
swap, plus a snapshot of the marketing spec that went with it.

## What was replaced, and why

The mobile forecast moved from the **`m` marketing-lift bidirectional overlay** to the **`p`
measured paid/organic split**. `m` modelled paid as an *increment since an anchor*; `p` measures
paid from the client-level gclid flag, forecasts organic only, and stacks marketing's paid
**level** on top.

The reason is that `m`'s framing does not survive contact with mobile. Desktop's `l` and `o` can
honestly model "incremental DAU since launch" because those interventions have hard start dates.
Mobile paid acquisition has run continuously for years, so the anchor is an accounting choice —
and the bidirectional overlay **absorbed 58%** of any change to the curve, which made a
lift-curve comparison useless as a KPI estimate.

## THREE things changed together — a revert must undo ALL of them

| # | change | from | to |
|---|---|---|---|
| 1 | mobile paid treatment | `m` (`marketing/marketing.json`) | `p` (`organic/organic.json`) |
| 2 | `marketing.json` date gate | `applies_to_forecast_start: "2026-07-28"` | **cleared** (so `m` no longer fires) |
| 3 | canonical mobile build | `mobile_uac_meta_2026-07-28/` | `mobile_organic_2026-07-28/` |

**Change 2 is not cosmetic.** `main.process_data_source` raises if both a `marketing.json` and an
`organic.json` claim the same `applies_to_forecast_start`, because running both would subtract
paid twice and add it back twice. Restoring the marketing gate without clearing the organic one
will fail loudly (by design) — but restoring *neither* would silently produce a mobile forecast
with **no paid treatment at all**, which is a plausible-looking number that no build ever
published.

## Numbers (mobile ALL MOBILE, 28d-MA)

The pre-swap build, verified from the parquet in this directory:

| | pre-headwind | post-headwind (`h`, mobile −27,162) |
|---|--:|--:|
| Summer trough (2026-08-16) | 16,932,979 | **16,929,292** |
| 2026-08-22 | 16,951,063 | 16,946,213 |
| **2026-12-15** | 17,891,894 | **17,864,732** |
| 2026-12-31 | 17,729,212 | 17,698,945 |

Dec-15 vs July delivered (17,923,869): **−59,137 (−0.33%)**.

## What is here

| file | note |
|---|---|
| `mozaic_daily_forecast.2026-07-28.gm-D.adj-m.parquet` | the build |
| `...adj-m.parquet.meta.json` | its sidecar (spec_sha1 `e2d88584…`) |
| `parameters.json` | the mobile config it was built with |
| `marketing.adjm-era.json` | **the `marketing.json` as it stood, with its date gate intact** |

The 838 MB `mozaic_objects.glean_mobile.2026-07-28.pkl` is **not** duplicated here. Unlike the
desktop s01 revert, the original build directory is not being overwritten — the new build goes to
a *new* directory — so the pickle remains at
`../mobile_uac_meta_2026-07-28/cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/`.
**Do not delete that directory while this revert target is live.**

## Why a revert might be wanted

Two open questions could send us back:

1. **The paid seam.** Under `p`, training rows carry *our* measurement of paid and forecast rows
   carry *marketing's* model of it. They disagree by **+36,674 (+2.48% of paid, ≈ +0.24% of
   total)** at the seam, and the build currently ships that step honestly rather than smoothing
   it. `research/mobile-organic/paid_seam_methods.ipynb` is the open decision.
2. **Assumption A1 — all Firefox iOS DAU is organic.** iOS carries no paid signal whatsoever
   (`paid_vs_organic_gclid` 100% NULL, 0 paid-attributed clients in 2026-07 out of 3.66M), but
   that measures *attribution*, not *spend*. If marketing confirms live iOS spend, the
   organic/paid partition is wrong for ~21.5% of mobile DAU. The total is unaffected either way,
   so this is an attribution problem, not a headline problem — it would not on its own justify a
   revert.

Neither is a reproducibility risk: `p` is deterministic and its inputs are pinned.

## How to revert

```bash
source .venv/bin/activate

# 1. Put the marketing date gate back, and remove the organic one.
cp data-official/2026-08/mobile_adjm_REVERT_2026-07-31/marketing.adjm-era.json \
   data-official/2026-08/marketing/marketing.json
#    then edit data-official/2026-08/organic/organic.json and set
#    "applies_to_forecast_start": null   (or delete the key)

# 2. Point the canonical notebook back at the adj-m build.
#    august_canonical_v2026-07-28.ipynb cell [setup]:
#      MOBILE_FORECAST_PATH -> mobile_uac_meta_2026-07-28/<slug>/...gm-D.adj-m.parquet
#    cell [load-parquets]:  require_state=["m"]   (not ["p"])
#    cell [mobile-dec15]:   UAC_META_MOBILE_DEC15 = 17_864_732

# 3. Re-execute the notebook and re-export the CSVs.

# 4. Verify.
```

**No model re-run is required** — the `adj-m` parquet was never overwritten.

**Verification target:** mobile Dec-15 comes back **17,864,732**, trough **16,929,292** on
2026-08-16, and Dec-15 vs July reads **−59,137**. If you get a different number, step 1 or 2 did
not take. In particular, if mobile Dec-15 lands *between* the two builds' values, check that
exactly one of the two specs has a date gate — a run with neither applies no paid treatment at
all.

Desktop is untouched by all of this and must show a **0 delta on all 365 curve days** in either
direction.
