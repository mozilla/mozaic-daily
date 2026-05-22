# prophet_decompose/

Decompose a desktop Mozaic DAU forecast pkl into its global Prophet components
(trend / weekly / yearly / holidays) for visual comparison across runs.

- `decompose.py` — CLI + library. Loads a `mozaic_objects.<source>.<date>.pkl`,
  iterates tiles, predicts via each tile's stored `_prophet_model`, converts
  log-growth tiles into linear-space contributions, sums across tiles, and
  writes a long parquet keyed by `(ds, label)`.

Not in this directory:
- The comparison notebook that consumes the parquets — lives at the repo root
  (`prophet_decomposition_april_vs_june.ipynb`).
- Mobile decomposition — currently desktop-only. Mobile pkls have been
  unreliable to load (see memory `project_mozaic_pkg_versions`).
