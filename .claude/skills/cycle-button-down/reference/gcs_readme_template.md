# Mozaic Daily — <Month> <Year> Forecast Archive

Archived <YYYY-MM-DD> from `mozilla/mozaic-daily`, branch `<month>-forecast` at `<commit>`.
The branch holds every tracked file of the cycle; this prefix holds the gitignored artifacts
(pickles, parquets, raw pulls, zips, scan exhaust) that git does not.

## Context

<Two or three sentences: the cycle's forecast_start seam(s), what changed from the prior cycle
(model configs, adjustment codes added/retired, methodology swaps), and the canonical build
directories. Point at `data-official/<YYYY-MM>/_index.md` in the branch for the full story.>

## Cycle milestones

| forecast_start | What changed |
|---|---|
| YYYY-MM-DD | … |

## Directory layout

```
<month>-<year>/
├── README.md
├── data-official/<YYYY-MM>/        # mirrors the repo dir, incl. pkls + raw pulls + handoff zips
│   ├── <canonical desktop build>/
│   ├── <canonical mobile build>/
│   ├── <REVERT / baseline / superseded builds>/
│   └── …
├── param-scans/<search>/           # research/param-scans/<search>/ incl. per-probe pickles
├── research/<cluster>/             # stray research blobs, same relative path as in the repo
└── root_intermediates_<date>/      # repo-root mozaic_* files, if any
```

## File types

- `.parquet` — forecast outputs (`.raw.` / `.adj-*.` state markers) and raw BigQuery caches.
- `.pkl` — fitted Mozaic/Prophet objects, ~600–850 MB each. **First-class artifacts**: used to
  inspect Prophet internals and cross-check adjustments after the fact. Every probe pickle from
  the cycle's parameter searches is here.
- `.json` — sidecar metas, adjustment specs, `parameters.json` (also tracked in git).
- `.csv` / `.ipynb` / `.md` — tracked in git; present here only where they rode along in a
  directory copy.
- `.zip` — stakeholder handoff bundles.

## Verified on upload

| directory | files (local / remote) | bytes (local / remote) |
|---|---|---|
| data-official/<YYYY-MM> | … | … |
| param-scans/<search> | … | … |

## Key Dec-15 28d-MA numbers (published)

| Series | <prior cycle> | <this cycle> | Δ |
|---|---:|---:|---:|
| Desktop | | | |
| Mobile | | | |
| ALL | | | |

## Reproducing

`git checkout <month>-forecast`, pull the needed build directory back with
`gcloud storage cp -r gs://…/<dir> data-official/<YYYY-MM>/`, then follow that cycle's
`_index.md`. Load parquets only through `mozaic_daily.adjustments.load_forecast()`.
