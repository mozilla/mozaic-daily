# `data-official/`

Versioned forecast outputs and the adjustment specs that derive composite numbers from them. Files here are referenced by leadership-facing analyses, so naming and provenance are load-bearing.

## Layout

```
data-official/
  adjustment_codes.yaml             # registry of adjustment codes used in filenames
  <cycle>/                          # e.g. 2026-04 — the per-cycle shape, kept for reference
    desktop_<config-slug>/
      mozaic_daily_forecast.<date>.ld-D.raw.parquet            # raw desktop forecast
      mozaic_daily_forecast.<date>.ld-D.raw.parquet.meta.json  # sidecar provenance
      mozaic_daily_forecast.<date>.ld-D.raw.plus_iran.parquet  # raw + synthetic Iran
      mozaic_daily_forecast.<date>.ld-D.raw.plus_iran.parquet.meta.json
      mozaic_parts.raw.legacy.desktop.DAU.parquet              # pre-forecast BQ aggregate
      mozaic_objects.legacy_desktop.<date>.pkl                 # fitted Mozaic dict
      parameters.json                                          # model config used
    mobile_<config-slug>/           # same pattern, gm-D suffix, glean_mobile
    adjustments/
      headwind.json                                            # linear_ramp spec
    comparisons/                                               # scratch param-scan runs
  2026-07/                          # July 2026 cycle (forecast_start 2026-07-06, both platforms)
    desktop_locked/                                            # LOCKED desktop, adj-lo (l+o overlays)
      mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet
    mobile_refresh_2026-07-06/<config>/                        # refreshed mobile, adj-m
      mozaic_daily_forecast.2026-07-06.gm-D.adj-m.parquet
    adjustments/headwind.json                                  # h spec
    launch_on_login/lol.json                                   # l spec (desktop tailwind)
    mozillaonline/mozillaonline.json                           # o spec (CN desktop migration)
    marketing/marketing.json                                   # m spec (mobile lift)
    iran_fill/                                                 # Iran counterfactual-fill specs
    csv/july_canonical_curves.csv                              # public-facing canonical export
    _index.md                                                  # START HERE for the cycle
```

**Working-tree scope:** cycles from the **last 3 calendar months** stay on disk (canonical + superseded
forecast parquets, sidecars, specs, CSVs, notebooks — but not fitted pickles, which are archived at each
button-down); older cycles and all large artifacts are archived to GCS
(`gs://moz-data-science-brwells-bucket/mozaic-daily-archive/{cycle}/`) and recoverable from that
cycle's branch history. See each cycle's `_index.md` "Present vs Archived" section.

**Cycle roll-forward.** On this branch the window has advanced for the **August 2026** cycle:

| cycle | state |
|---|---|
| `2026-08` | current — created on the `august-forecast` branch |
| `2026-07` | N-1, present in full. The prior-forecast comparison series for August comes from here |
| `2026-06` | N-2, **retained deliberately** — see below. Its large artifacts already went to GCS `june-2026/` during the July button-down; only ~10 MB of specs, canonical CSVs, and sidecars remain |
| `2026-04` | removed in an earlier roll-forward. GCS `april-2026/`, branch `april-forecasting` |
| `iran_synthetic/`, `march_brad_forecast.csv` | April-era leftovers, removed 2026-09-04 after verifying the copies in GCS `april-2026/data-official/` |

**Why 2026-06 is not pruned.** The rule is "current + N-1 in full", and June is N-2 — but its
remaining tracked files are *load-bearing for the July cycle*, so deleting them would break
reproducibility of the cycle we still depend on:

- `marketing/marketing_lift_model.real_data_v2.hybrid.2026-05-22.parquet` (+ its `.meta.json`) is
  what July's `marketing/build_lift.py` and the canonical `m` spec chain read.
- `mozillaonline/june_delivered_mo_tailwind.json` is read by July's canonical-review notebook and by
  `research/param-scans/aug22-retune/`.
- `csv/june_canonical_curves.csv` is the N-1 comparison series for
  `research/ma-seam-turbulence/backtest_seam.py` and `_archive/scripts/mobile_sensitivity.py` (archived
  2026-07-29 — it binds the frozen 2026-06 seam MA; see `_archive/_index.md`).
- `adjustments/headwind.json` is referenced across July. `export_canonical_curves.py` was the seam-MA home
  through the 2026-07 cycle; as of 2026-07-29 the live implementation is `src/mozaic_daily/seam_ma.py` and
  the 2026-06 copy is frozen (see `_archive/_index.md`).

Pruning it saves ~10 MB and costs a broken July. **Revisit when July becomes N-2** (i.e. at the
September roll-forward), at which point the August cycle should own its own copies of whatever it
still needs and June can go.

The prune is always done on this base branch, never on a cycle branch — cycle branches keep the
complete record of what that cycle shipped.

## Naming convention (load-bearing files only)

Every forecast artifact carries an **explicit adjustment-state marker**:

- `.raw.` — direct model output, no adjustments applied. *MUST* appear in filenames of fresh forecast outputs.
- `.adj-{codes}.` — one or more adjustments applied; codes sorted alphabetically (e.g. `.adj-h.`, `.adj-ht.`).

Combined with the Iran composition marker:

```
forecast.2026-05-13.ld-D.raw.parquet               # raw, no-Iran
forecast.2026-05-13.ld-D.raw.plus_iran.parquet     # raw, Iran added back
forecast.2026-05-13.ld-D.adj-h.plus_iran.parquet   # headwinds applied, Iran added
forecast.2026-05-17.gm-D.adj-m.parquet             # marketing-lift applied (retired from 2026-08)
forecast.2026-05-17.gm-D.adj-hm.parquet            # headwinds + marketing-lift
forecast.2026-07-28.gm-D.adj-p.parquet             # paid/organic split — the current mobile treatment
forecast.2026-07-28.ld-D.adj-lo.parquet            # launch-on-login + MozillaOnline (current desktop)
```

Note that `h` and `t` are **display-layer**: they are applied to the 28-day MA after mozaic and are
never baked into a parquet. So a current-cycle forecast parquet carries `adj-lo` (desktop) or `adj-p`
(mobile), and the `h`/`t` contribution exists only in the published CSVs and charts.

**Scratch files** under `comparisons/` and `param_scan_results/` are not held to this convention — they are all raw model output by directory convention. The scripts that produce them do not apply adjustments.

## Sidecar `.meta.json`

Every load-bearing file has a sibling `<filename>.meta.json` recording:

- `forecast_start_date`, `data_source`, `produced_by`, `produced_at`
- `model_config` — the `DesktopModelConfig` / `MobileModelConfig` used
- `adjustments_applied` — list of `{code, name, spec_file, spec_sha1}`, one per adjustment, in alphabetical code order
- `artifact_sha1` — content hash of the artifact
- `parent_file` + `parent_file_sha1` for derived files (e.g. composites)
- `mozaic_daily_commit` — git hash at the time of production
- `provenance: "regenerated"` or `"reconstructed"` for files written under this convention after the fact

The sidecar is canonical; the filename marker must match it. Loaders enforce this.

## Adjustment registry

`adjustment_codes.yaml` registers every adjustment code used in filenames. Each entry has:

- A one-letter `code`
- A `name` and `description`
- A `spec_glob` pointing at the JSON specs that drive the adjustment

Adding a new adjustment:

1. Pick an unused one-letter code, add it to `adjustment_codes.yaml`.
2. Register the applier. **Where depends on the applier style** (see
   `src/mozaic_daily/_index.md` for the full comparison):
   - *Composite post-forecast* (`h`, `t`) — `adjustments.py`. These need no per-code wiring at all:
     `load_adjustments_from_dir()` globs `*.json` and sums every spec it finds, so a display-layer
     adjustment goes live the moment its spec file lands in a cycle's `adjustments/` directory.
   - *Per-tile bidirectional* (`l`, `o`) — `adjustments.py`, reusing `load_overlay_spec()` and the
     generic subtract/add-back helpers. Each overlay needs a **distinct `sentinel_attr`** so several
     can stack on one training frame.
   - *Measured split* (`p`) — its own module pair, **not** `adjustments.py`: `organic_source.py`
     (producer) and `organic.py` (consumer).
3. Add unit tests. `tests/test_adjustments.py` for the first two styles; a dedicated module for the
   third (`tests/test_organic.py` is the reference).

**Codes are never unregistered.** A retired adjustment (currently `m`) must stay in
`adjustment_codes.yaml` so that historical artifacts carrying its marker keep loading and reproducing.
Retirement is done by clearing the spec's date gate, not by deleting the code.

## Producing files

All forecast artifacts should be produced via `src/mozaic_daily/adjustments.py`:

- `load_forecast(path)` — load + state-validation
- `apply_net_adjustment_to_series(...)` — apply adjustments to a 28-day MA
- `write_meta(path, ...)` — sidecar meta
- `build_adjustments_applied_list(...)` — populate the meta's adjustments list

Composite CSVs are regenerated by `scripts/regenerate_composites.py`, which is the canonical reproducer — diffing its output against on-disk files is the integrity check that the composite state is consistent with the underlying raw parquets.

## Migrating existing files

`scripts/verify_forecast_states.py` audits on-disk state and writes `tmp/inventory.csv`. `scripts/migrate_forecast_names.py` consumes that inventory to rename files and write reconstructed sidecar metas. Run both with `--dry-run` first.
