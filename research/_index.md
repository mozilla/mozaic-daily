# `research/` — cross-cutting, ad-hoc analysis clusters

Topic-grouped notebooks, specs, and supporting data that don't belong to a single forecast cycle. Anything that spans months (April vs. June diagnostics, the Iran shutdown workaround, marketing-lift modeling, headwind exploration, etc.) lives here.

## Where new code goes

The split between `data-official/{YYYY-MM}/` and `research/{topic}/` is the project's hybrid rule:

- **Month-scoped artifact** (composite producer notebook, sanity check tied to one month's data, the month's adjusted CSV) → `data-official/{YYYY-MM}/`
- **Cross-month or topic-anchored work** (mechanism diagnostics, model explorations, validation against actuals over time) → `research/{topic}/`

Add a new topic cluster here when work spans more than one month or doesn't tie to a specific forecast cycle. Add an `_index.md` to it. Inside the cluster, organize by version (`v1-convolution/`, `v2-real-data/`) when one approach supersedes another so the lineage stays legible.

## Clusters

| Topic | What it covers |
|---|---|
| `iran/` | Internet-shutdown workaround: spec, synthetic DAU methodology, partial-recovery model |
| `marketing-lift/` | Fenix Android paid-marketing DAU lift; v1 convolution (superseded) + v2 real-data (current) |
| `april-vs-june-mechanism/` | Why June forecast levels lower than April; threshold-matching, changepoint pinning investigations |
| `param-scans/` | Prophet `changepoint_prior_scale`, `recent_weeks`, `holiday_threshold` sensitivity exploration |
| `headwinds/` | Linear-ramp profile explorations for the `h` (headwinds) adjustment |
| `csv-vs-actuals/` | Validates exported forecast CSVs against actual DAU from BigQuery before release |

## Conventions inherited from `data-official/`

Forecast artifacts produced inside `research/` clusters follow the same naming rules: `.raw.` / `.adj-{codes}.` markers, sidecar `<filename>.meta.json` with provenance. Use `mozaic_daily.adjustments.load_forecast()` to read them. See `data-official/_index.md` for the canonical statement of the rule.
