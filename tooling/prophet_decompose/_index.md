# prophet_decompose/

Pull the Prophet components (trend / weekly / yearly / holidays / residual) out of a Mozaic
forecast pkl for inspection and cross-run comparison.

- `decompose.py` — CLI + library. Loads a `mozaic_objects.<source>.<date>.pkl`,
  iterates tiles, predicts via each tile's stored `_prophet_model`, converts
  log-growth tiles into linear-space contributions, sums across tiles, and
  writes a long parquet keyed by `(ds, label)`. Desktop-only in practice.
- `plot_decomposition.py` — one stacked six-panel figure of the **top-level** Prophet fit
  (the aggregate model top-down reconciliation anchors on): actuals + fit, trend +
  changepoints, weekly seasonality as a full series (historical vs recent regime), the weekly
  *pattern* (one example Mon..Sun week from each regime, as % of trend level), yearly
  seasonality, and the training residual against holiday-detrended actuals. Handles both
  spaces: desktop's log(y+1) + multiplicative seasonality is back-transformed to DAU offsets;
  mobile's raw-DAU additive fit passes through. `prophet_recent_weeks` is read from the
  `parameters.json` beside the pkl (or `--recent-weeks`). Platform-agnostic.

  ```bash
  python3 tooling/prophet_decompose/plot_decomposition.py <build_dir>/mozaic_objects.legacy_desktop.<date>.pkl \
      --out data-official/<cycle>/plots/prophet_decomposition_desktop.png --title "..."
  ```

  Outputs so far: `data-official/2026-09/plots/prophet_decomposition_desktop.png` (September g01 desktop).
  The earlier mobile figure, `research/param-scans/mobile-july/plots/prophet_decomposition_mobile.png`,
  came from the archived July sensitivity notebook, not this script.

Not in this directory:
- The April-vs-June comparison notebooks that consumed `decompose.py`'s parquets were archived
  to GCS with the `april-vs-june-mechanism` research cluster
  (`gs://moz-data-science-brwells-bucket/mozaic-daily-archive/research-superseded/april-vs-june-mechanism/`).
