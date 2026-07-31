# research/autumn-decoupling

**Due-diligence exploration only. Read-only with respect to every forecast artifact.**

Question: can the August desktop forecast keep its higher summer (Aug-25 trough ~45.22M) while
bringing October and November back toward July's delivered curve?

Nothing here modifies `data-official/`. The August build is locked and shipping; this directory
exists to answer whether a decoupled alternative was ever available, not to produce one.

## What's here

| file | what it does |
|---|---|
| `curves.py` | Read-only loaders. Turns the four on-disk desktop builds into comparable 28d-MA curves, and renders either cycle's headwind ramp convention so display-layer effects can be separated from model effects. |
| `attribute_autumn.py` | Five-rung ladder decomposing the August-vs-July autumn gap into data refresh, LOL ceiling, headwind convention, s01 retune, and ceiling again. |
| `fetch_actuals.py` | Caches multi-year legacy-desktop DAU actuals to `actuals_desktop_dau.parquet`. Same source/filter as the canonical notebook's `[bq-actuals]`. |
| `seasonal_shape.py` | Tests whether history couples summer depth to autumn level, and compares the model's passthrough against the historical slope. |
| `LOG.md` | Append-only findings log, including dead ends. |
| `plots/` | Charts. |

## What's deliberately NOT here

- **Any reshaping of the `h` headwind.** Excluded from scope by the user on 2026-07-30: a
  compensating autumn headwind would work, and that is exactly why it is off the table — it is
  curve-fitting the deliverable rather than modelling it. `curves.py` renders the two *existing*
  conventions only, for attribution.
- **Holiday-parameter tuning.** Permanently excluded by project policy: local effects must not be
  used to move whole-season quantities.
- **Any rebuild of a historical artifact.** Locked builds are never re-run.

## Where new code goes

Cross-month, topic-anchored work, so it lives under `research/` per the hybrid rule in
`CLAUDE.md`. Add analysis scripts here; keep the plots in notebooks where they can be re-run.
If this exploration ever produces a candidate *build*, that build belongs in a scratch results
directory, not in `data-official/`.
