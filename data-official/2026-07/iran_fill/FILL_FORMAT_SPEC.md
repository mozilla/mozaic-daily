# Iran counterfactual-fill — file format spec

**Status:** DRAFT for review (2026-06-30) · **Owner:** Brendan Wells
**Consumer:** the mozaic-forecasting ingestion code (to be written) · **Producer:** `scripts/generate_iran_fill.py`

This document is the **contract** between the Iran fill artifact and the mozaic-side ingestion
code. An implementer should be able to write the ingestion against this spec alone, without
reading the producer.

---

## 1. Purpose

Iran's internet shutdown collapsed native Firefox telemetry to near-zero from **2026-03-01 →
2026-05-25**; it fully recovered to pre-shutdown levels on **2026-05-26**. Fed raw into mozaic,
the 86-day hole corrupts Prophet (spurious changepoints/trend, broken reconciliation).

The fill provides **counterfactual IR values** — what Iran would have been with no shutdown —
for the gap window, at the exact granularity mozaic ingests. The values come from propagating the
mozaic model forward over the gap (train on clean pre-shutdown IR, forecast the gap). The
ingestion code substitutes these rows for the real (garbage) IR gap rows **before**
`populate_tiles()`.

## 2. Scope

- **Country:** `IR` only.
- **Data sources (3):** `glean_desktop`, `legacy_desktop`, `glean_mobile`.
- **Metrics (4):** `DAU`, `New Profiles`, `Existing Engagement DAU`, `Existing Engagement MAU`.
- **Granularity:** one row per (data_source, metric, date, population) — i.e. per mozaic tile.

## 3. Fill windows (inclusive) — per metric

| Metric | Fill window | Real data resumes |
|---|---|---|
| `DAU` | 2026-02-28 → 2026-05-25 | 2026-05-26 |
| `New Profiles` | 2026-02-28 → 2026-05-25 | 2026-05-26 |
| `Existing Engagement DAU` | 2026-02-28 → 2026-05-25 | 2026-05-26 |
| `Existing Engagement MAU` | **2026-02-28 → 2026-06-21** | **2026-06-22** |

**Why MAU differs:** MAU is a rolling-28-day metric. The last blackout day is 2026-05-25, so a
28-day window is free of blackout days only from **2026-06-22** (window 2026-05-26 … 2026-06-22).
Real MAU for 2026-05-26 … 2026-06-21 is contaminated (its window still contains blackout days) and
must be filled. The genuine 3-day recovery ramp (2026-05-26 … 28) is *real* signal and is **not**
overwritten — it lives in the post-2026-06-21 real data.

## 4. File layout

One parquet **per data source** (3 files), each a tidy long-by-metric table:

```
data-official/2026-07/iran_fill/
├── iran_fill.glean_desktop.parquet
├── iran_fill.legacy_desktop.parquet
├── iran_fill.glean_mobile.parquet
├── iran_fill.<source>.meta.json     # provenance sidecar per file (see §8)
└── FILL_FORMAT_SPEC.md              # this file
```

Per-source files keep each table **byte-compatible** with that source's `populate_tiles` input
(same segment columns), so ingestion is a plain concat/replace.

## 5. Schema

Common columns (all files):

| Column | Dtype | Notes |
|---|---|---|
| `metric` | string | one of the 4 metric strings in §2 (exact, case-sensitive) |
| `x` | date (`datetime64[ns]`, midnight) | the submission_date / first_seen_date; matches mozaic's `x` |
| `country` | string | always `"IR"` |
| *(segment booleans)* | bool | platform-specific, see below |
| `y` | float64 | counterfactual value (non-negative; never NaN inside the window) |

**Desktop segment columns** (`glean_desktop`, `legacy_desktop`): `modern_windows`, `winX`
**Mobile segment columns** (`glean_mobile`): `fenix_android`, `firefox_ios`, `focus_android`, `focus_ios`

The segment booleans encode the **population** exactly as `populate_tiles()` derives it
(`utils.py:52-57`): the population label is the concatenation of the True segment flags;
**all-False ⇒ population `"other"`**. Populations are single-label (the OS buckets are mutually
exclusive; an app row sets exactly one app flag), so **at most one segment flag is True per row.**

Population ⇄ segment-flag mapping:

| Population | Desktop flags | Mobile flags |
|---|---|---|
| `modern_windows` | modern_windows=T, winX=F | — |
| `winX` | modern_windows=F, winX=T | — |
| `fenix_android` | — | fenix_android=T (others F) |
| `firefox_ios` | — | firefox_ios=T (others F) |
| `focus_android` | — | focus_android=T (others F) |
| `focus_ios` | — | focus_ios=T (others F) |
| `other` | all F | all F |

## 6. Invariants (the producer asserts these; the consumer may rely on them)

1. `country == "IR"` for every row.
2. No NaN/null `y` inside the fill window; `y >= 0`.
3. Every (metric, population) present in the source's real pre-shutdown IR data is present for
   **every date** in that metric's fill window (dense daily coverage, no gaps).
4. At most one segment flag True per row (single-label populations).
5. Date coverage exactly matches §3 per metric — no rows outside the fill window.
6. **Sum check:** for any (metric, date), `sum(y over populations)` equals the fill's intended
   IR-ALL value for that (metric, date). (The producer harvests mozaic's own reconciled
   per-population splits, so this holds by construction; the consumer does **not** need an ALL row —
   ALL is recomputed by mozaic by summing tiles.)

## 7. How the ingestion should consume this (Approach A)

For each (data_source, metric) dataset the pipeline builds (IR no longer excluded from the query):

1. Drop real IR rows whose date falls inside this metric's fill window (§3).
2. Append the matching fill rows from `iran_fill.<data_source>.parquet` (filtered to `metric`).
3. Leave all non-IR rows and all out-of-window IR rows untouched.
4. Hand the combined frame to `populate_tiles()` as usual.

The fill rows carry the same columns as the queried frame, so step 2 is a column-aligned concat.

## 8. Provenance sidecar (`iran_fill.<source>.meta.json`)

Each file ships a sidecar recording: producer script + git commit, mozaic commit, `FORECAST_START`,
model config (July params) per platform, the fill windows applied, and the seam-scaling decision
(human go/no-go outcome: `no_scale` or `scale_factor` per metric × platform). This makes the
artifact's provenance and any applied scaling auditable.

## 9. Weekly amplitude (re-seasonalization)

The raw forward forecast damps the weekday→weekend swing (Prophet shrinks the weekly seasonal
amplitude — measured: the fill's peak/trough swing was ~46% of real IR's). After harvesting, the
producer **re-imposes the empirical day-of-week profile** measured from clean pre-shutdown IR
(window `2025-09-01` → shutdown), per `(metric, population)` with a pooled-IR-ALL fallback for
sparse populations:

```
y_corrected = y_smooth / forecast_DOW_factor[dow] * real_DOW_factor[dow]
```

then **rescales each (metric, population) to preserve its arithmetic mean** over the window — so the
weekly *shape/amplitude* matches real telemetry while the *level* (and the validated seam) is
unchanged. The smooth pre-correction path is retained in `_draft/iran_fill.<source>.smooth.parquet`.
Consumers don't need to do anything — the delivered `y` already carries realistic weekly amplitude.
The sidecar records `weekly_reseasonalized`, `dow_reference_window`, and `dow_granularity`.

## 10. Open sub-decision (flag for review)

- **Desktop source coverage:** spec currently produces both `glean_desktop` and `legacy_desktop`
  (the production mart carries both). If only one desktop source is canonical for the July
  delivery, we can drop the other.
