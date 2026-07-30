# `research/` — cross-cutting, ad-hoc analysis clusters

Topic-grouped notebooks, specs, and supporting data that don't belong to a single forecast cycle. Anything that spans months (April vs. June diagnostics, the Iran shutdown workaround, marketing-lift modeling, headwind exploration, etc.) lives here.

## Where new code goes

The split between `data-official/{YYYY-MM}/` and `research/{topic}/` is the project's hybrid rule:

- **Month-scoped artifact** (composite producer notebook, sanity check tied to one month's data, the month's adjusted CSV) → `data-official/{YYYY-MM}/`
- **Cross-month or topic-anchored work** (mechanism diagnostics, model explorations, validation against actuals over time) → `research/{topic}/`

Add a new topic cluster here when work spans more than one month or doesn't tie to a specific forecast cycle. Add an `_index.md` to it. Inside the cluster, organize by version (`v1-convolution/`, `v2-real-data/`) when one approach supersedes another so the lineage stays legible.

## Clusters (present on disk)

| Topic | What it covers |
|---|---|
| `marketing-lift/` | Fenix Android paid-marketing DAU lift; **v2 real-data (current)**. (v1 convolution retired — archived.) |
| `param-scans/` | Prophet param sensitivity exploration + `mobile-july/` grid search. Notebooks/tooling present; **the multi-GB sweep `results/` are archived to GCS** (regenerable). |
| `headwinds/` | Linear-ramp profile explorations for the `h` (headwinds) adjustment |
| `csv-vs-actuals/` | Validates exported forecast CSVs against actual DAU from BigQuery before release (per-cycle) |
| `ma-seam-turbulence/` | Diagnosis + backtest behind the `display_ma` seam work. Two fixes shipped: the v1 variance-matched transition (in the now-frozen `data-official/2026-06/export_canonical_curves.py`) and **Fix A**, the trend-estimator correction that moved the live implementation to `src/mozaic_daily/seam_ma.py` (test-locked in `tests/test_seam_ma.py`). See its `LOG.md` § Fix A. |

## Archived (GCS — pull back only for prior art)

Retired/superseded investigations were removed from the working tree at the July button-down and live
in `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/research-superseded/` (and in the
`july-forecast` branch history): `iran/` (shutdown workaround — superseded by the in-package
counterfactual fill), `marketing-lift/v1-convolution/`, `april-vs-june-mechanism/`,
`desktop-gap-decomp/`, `country-overrides/`.

## Conventions inherited from `data-official/`

Forecast artifacts produced inside `research/` clusters follow the same naming rules: `.raw.` / `.adj-{codes}.` markers, sidecar `<filename>.meta.json` with provenance. Use `mozaic_daily.adjustments.load_forecast()` to read them. See `data-official/_index.md` for the canonical statement of the rule.
