# `data-official/2026-08/adjustment_isolation/` — one-overlay-at-a-time desktop runs

Four desktop (`legacy_desktop` DAU) forecasts at the **same anchor (2026-07-28), same data, same locked
parameters**, differing *only* in which bidirectional overlays were active. Built to answer "which
adjustment causes which effect" — consumed by `../desktop_adjustment_ladder.ipynb`.

| run | overlays | artifact |
|---|---|---|
| A | none | `none/<slug>/mozaic_daily_forecast.2026-07-28.ld-D.raw.parquet` |
| B | `l` only | `l_only/<slug>/…ld-D.adj-l.parquet` |
| C | `o` only | `o_only/<slug>/…ld-D.adj-o.parquet` |
| D | `l` + `o` | *not here* — it is the canonical build, `../desktop_baseline_2026-07-28/` |

`<slug>` = `cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825` in all three.

These are **diagnostics, not deliverables.** They are pre-headwind (`h` is display-layer) and carry the
stale `o` curve, exactly like the canonical build they are compared against.

## Why separate runs are necessary

`l` and `o` are *per-tile bidirectional*: each is subtracted from `modern_windows` training rows **before**
mozaic and added back **after**. So they change what Prophet fits, not just the output level — you cannot
recover run A from run D by subtracting a curve. Measured non-additivity at Dec-15 is **−15,590**
(effect(l)+effect(o) = +598,095 but effect(l+o) = +582,505), i.e. the overlays are not independent.

## Reproducing

`--no-launch-on-login` / `--no-mozillaonline` were added to `scripts/run_param_scan.py` for this; they
suppress an overlay even when its spec matches `applies_to_forecast_start`, and leave the suppressed code
off the filename marker so each parquet's name states what is actually baked in. `--raw-cache-dir` at the
canonical slug dir reuses the fetched BQ aggregate, so none of these re-queried BigQuery.

```bash
source .venv/bin/activate
CACHE=data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825
python3 scripts/run_param_scan.py --forecast-start-date 2026-07-28 \
    --changepoint-prior-scale 0.08983 --changepoint-range 0.65 --n-changepoints 25 --recent-weeks 13 \
    --holiday-threshold -0.032 --holiday-max-radius 5 --holiday-min-radius 3 \
    --holiday-effect-floor -0.6 --raw-cache-dir "$CACHE" \
    --no-launch-on-login --no-mozillaonline \
    --results-dir data-official/2026-08/adjustment_isolation/none
```
Drop one `--no-*` flag for B or C. Logs: `logs/iso_{none,l,o}.log`.

**Present vs archived:** the `.parquet` forecasts, `mozaic_objects.*.pkl` and raw-parts parquets are
gitignored — sidecar `.meta.json` + `parameters.json` are tracked, so the runs stay legible without the
blobs. Regenerable in ~15 min each from the cached raw pull; archive to GCS at button-down or delete.

**Where new files go:** further single-overlay isolation runs for this cycle. A run that changes
*parameters* belongs in `research/param-scans/`, not here — this directory is defined by holding
parameters fixed.
