# Monthly Forecast Update

Produce a new cycle's canonical forecast the way the July and August 2026 cycles were produced. This is
the standard monthly refresh, not a parameter-tuning exercise. Rewritten 2026-09-04 against the August
cycle; the previous version described the retired synthetic-Iran workflow (April–June 2026) and is
superseded in full.

**Read first:** `data-official/<YYYY-MM>/_index.md` for the cycle being built (opened by
`/cycle-button-down` with the inherited to-do list), and `data-official/_index.md` for the retention
window and naming rules. The end-of-cycle procedure is a separate skill: `/cycle-button-down`.

## Constants

```
CYCLE        = <YYYY-MM>                              # e.g. 2026-09
PREV         = <YYYY-MM of the delivered prior cycle>  # e.g. 2026-08
DATA_DIR     = data-official/$CYCLE/
PREV_DIR     = data-official/$PREV/
SEAM         = <forecast_start_date, YYYY-MM-DD>       # training runs through SEAM - 1
PREV_SEAM    = <prior cycle's forecast_start_date>     # read from PREV_DIR/_index.md (August: 2026-08-02)
```

Every artifact this produces carries a `.raw.` / `.adj-<codes>.` state marker and a sidecar
`.meta.json`; load only through `mozaic_daily.adjustments.load_forecast()`. Never `pd.read_parquet` a
forecast directly.

## Ask the user, before any run

The five inputs below are decisions, not defaults. Present what the prior cycle used and wait for an
answer on each. A harness timeout is not an answer.

1. **Model parameters.** Show `PREV_DIR/desktop_*/<slug>/parameters.json` and `PREV_DIR/mobile_*/<slug>/parameters.json`
   (August: desktop **g01** `cps 0.1649, cpr 0.814, ncp 40, recent 17, sps 0.00825, regime multiplicative`;
   mobile `cps 0.035, cpr 0.725, ncp 25, recent 13, sps 0.1, regime auto, holiday_threshold −0.055`).
   Same as last cycle, or a new lock from a completed parameter search? Holiday knobs are never tuned
   (policy: local effects must not move whole-season KPIs); they stay at the recorded values.
2. **Which overlays carry forward, and which are rebuilt.** Per code:
   - `h` Win10 headwind (display layer, `adjustments/headwind.json`) — anchor value and whether the ramp
     start moves to the new seam (August convention: yes).
   - `t` mobile tailwind (display layer, `adjustments/tailwind.json`) — carry, resize, or drop. It is a
     discretionary number; its `notes` say so and any change must be recorded there.
   - `l` launch-on-login (baked into the desktop parquet, `launch_on_login/lol.json`) — ceiling is
     per-cycle; changing it needs a new curve from its producer **and** a model re-run.
   - `o` MozillaOnline (baked in, `mozillaonline/mozillaonline.json`) — August carried July's curve
     forward twice; the cycle index says rebuild.
   - `p` paid/organic split (baked into the mobile parquet, `organic/organic.json`) — the measured split
     is **always** rebuilt for the new training window (Step 2); ask only whether the paid curve it
     reads (`marketing/*.parquet`) is re-measured.
   - Any candidate overlay sitting unwired in `DATA_DIR` (September: `j` japan_bot, `i` india_excess) —
     wire or leave. Wiring is code work (register in `adjustment_codes.yaml`, distinct `sentinel_attr`,
     tests) and out of scope for this command.
   Change **one overlay per model run** so the Dec-15 delta stays attributable.
3. **Seam.** Today − 1 is the default. Confirm the data has landed (the pre-flight check will say).
4. **Whether the prior cycle's comparison curve is the published one.** It always should be:
   `PREV_DIR/csv/<prev-month>_canonical_curves.csv` is the N-1 series and its Dec-15 numbers are
   hardcoded into the notebook's reproduction check.
5. **Cycle-scoped scripts.** `DATA_DIR/STALE_REFERENCES_from_<prev-month>_button_down.md` lists the
   constants that still point at the prior cycle. Repoint them (or agree not to run them) before Step 5.

---

## Step 0 — Scaffold the cycle's spec directories

Copy each spec directory from `PREV_DIR` that carries forward and **re-gate** it. Overlay specs match
on exact string equality of `applies_to_forecast_start`; a run at a date no spec claims applies no
overlays and silently writes `.raw.` (the notebook's `require_state=` loads catch this later, but do
not rely on that).

```bash
for d in adjustments launch_on_login mozillaonline organic marketing; do
  mkdir -p data-official/$CYCLE/$d && cp -R data-official/$PREV/$d/. data-official/$CYCLE/$d/
done
grep -l applies_to_forecast_start data-official/$CYCLE/*/*.json     # each must be edited to "$SEAM"
```

- `adjustments/*.json` (`h`, `t`) have **no** gate — they are live by presence. `load_adjustments`
  globs the directory and sums every spec it finds. Edit `start_date` to `SEAM` per the user's answer;
  leave `anchor_date` at Dec-15. Delete `tailwind.json` here if the user dropped `t`.
- `marketing/marketing.json` keeps `applies_to_forecast_start: null` (retired code `m`); it stays only
  because `organic.json` reads its parquet as the paid level. Do not re-gate it — `main.py` raises if
  `m` and `p` both claim the seam.
- Each copied directory needs its `_index.md` updated to say what changed vs the prior cycle.

---

## Step 1 — Raw mobile pull (breaks the `p` circularity)

`build_fenix_organic_split.py` needs the cycle's raw mobile pull for its shredder-drift check, but the
mobile scan will not run until `organic.json` is gated to `SEAM`, which needs the split. Fetch the pull
alone first. Model-config independent; the scan reuses it via `--raw-cache-dir` with no re-query.

```bash
source .venv/bin/activate
python scripts/fetch_raw_pull.py --forecast-start-date $SEAM \
    --data-source glean_mobile --metric DAU \
    --output-dir data-official/$CYCLE/mobile_rawpull_$SEAM
```

Fails fast with a suggested `--forecast-start-date` if BigQuery has not landed `SEAM − 1` yet.

---

## Step 2 — Rebuild the measured paid/organic split (`p`)

```bash
python scripts/build_fenix_organic_split.py --forecast-start-date $SEAM --dry-run   # cost preview (~141 GB, ~$0.70)
python scripts/build_fenix_organic_split.py --forecast-start-date $SEAM \
    --production-raw data-official/$CYCLE/mobile_rawpull_$SEAM/mozaic_parts.raw.glean.mobile.DAU.parquet \
    --out-dir data-official/$CYCLE/organic
```

**Always pass `--production-raw`** — the shredder-drift check against the real level source is the only
thing that catches the mirror and production covering different Fenix populations. Four checks raise
on failure; do not proceed past a failure. Then point `organic/organic.json` `data_file` at the new
`fenix_paid_organic.$SEAM.parquet`, set `applies_to_forecast_start` to `SEAM`, and update `notes`.
The parquet is tracked (gitignore exception) because the mirror it comes from expires 2027-04-01.

---

## Step 3 — Run the two forecasts

`run_main.py` **cannot** reproduce a tuned build — it has no parameter flags. The two param-scan
runners are the real producers: each runs one forecast with an explicit config through
`main(model_configs=...)`, applies whatever overlays are gated to `SEAM`, and writes the parquet +
sidecar + `parameters.json` + fitted pickle into `<results-dir>/<slug>/`. Name the results dir by
config and seam (August: `desktop_g01_2026-08-02/`, `mobile_cpr0725_2026-08-02/`). Run both in the
background with logs under `logs/`; each takes 3–5 minutes plus the BigQuery pull; do not poll or
re-run mid-flight.

```bash
# Desktop (legacy_desktop DAU) — August's g01 values shown; substitute the confirmed lock
python scripts/run_param_scan.py --forecast-start-date $SEAM \
    --results-dir data-official/$CYCLE/desktop_<config>_$SEAM \
    --changepoint-prior-scale 0.1649 --changepoint-range 0.814 --n-changepoints 40 \
    --recent-weeks 17 --seasonality-prior-scale 0.00825 --seasonality-regime multiplicative \
    --holiday-threshold -0.032 --holiday-max-radius 5 --holiday-min-radius 3 --holiday-effect-floor -0.6 \
    > logs/<cycle>_desktop_$SEAM.log 2>&1

# Mobile (glean_mobile DAU) — reuses Step 1's pull
python scripts/run_mobile_param_scan.py --forecast-start-date $SEAM \
    --raw-cache-dir data-official/$CYCLE/mobile_rawpull_$SEAM \
    --results-dir data-official/$CYCLE/mobile_<config>_$SEAM \
    --changepoint-prior-scale 0.035 --changepoint-range 0.725 --n-changepoints 25 \
    --recent-weeks 13 --seasonality-prior-scale 0.1 \
    --holiday-threshold -0.055 --holiday-effect-floor -0.6 \
    > logs/<cycle>_mobile_$SEAM.log 2>&1
```

**Verify each output before continuing:**

- Filename carries the expected marker: desktop `.ld-D.adj-lo.parquet` (plus any newly wired desktop
  codes, alphabetical), mobile `.gm-D.adj-p.parquet`. A `.raw.` file means a spec was mis-gated.
- `load_forecast(path, require_state=[...])` succeeds.
- `parameters.json` matches the confirmed lock field by field.
- `python scripts/verify_training_rows_are_actuals.py <parquet>` exits 0 (the `p` add-back must leave
  training rows byte-identical to actuals; the notebook relies on this to skip a ~1 TB actuals query).
- Mobile only: `python scripts/mobile_app_breakdown.py` against the new build — `ALL MOBILE` equals the
  sum of the four apps.

The Iran 2026 counterfactual fill is applied automatically inside mozaic (`populate_tiles` receives
`data_source`); nothing to do. If the pipeline hangs on the BigQuery download, read the
`[BQ-WATCHDOG]` heartbeat lines (CLAUDE.md § Troubleshooting).

---

## Step 4 — Canonical notebook

Copy the prior cycle's producer notebook, keep its name pattern, never edit the prior copy:

```bash
cp data-official/$PREV/<prev-month>_canonical_v<PREV_SEAM>.ipynb \
   data-official/$CYCLE/<month>_canonical_v$SEAM.ipynb
```

Edit with `nb_cells.py` (`--file`, never heredoc). In the `setup` cell:

| constant | set to |
|---|---|
| `DESKTOP_FORECAST_PATH`, `MOBILE_FORECAST_PATH` | the Step 3 parquets |
| `PREV_FORECAST_DESKTOP_PATH`, `PREV_FORECAST_MOBILE_PATH` | `PREV_DIR`'s canonical parquets |
| `ADJUSTMENTS_DIR`, `PREV_ADJUSTMENTS_DIR` | `data-official/$CYCLE/adjustments`, `data-official/$PREV/adjustments` |
| `PLOTS_DIR` | `data-official/$CYCLE/plots` |
| `FORECAST_START`, `MOBILE_FORECAST_START`, `PREV_FORECAST_START` | `SEAM`, `SEAM`, `PREV_SEAM` |

Then, elsewhere in the notebook:

- **Config lock assertion** — replace the hardcoded prior lock with the confirmed one; keep the check
  that the four holiday knobs are at defaults.
- **Prior-curve reproduction** — replace the hardcoded prior Dec-15 values with `PREV_DIR`'s published
  numbers (August: desktop 48,703,443 / mobile 17,924,562 / ALL 66,628,005). The rebuilt prior curve
  must match within 1,000 DAU or the notebook aborts; this is what licenses quoting a delta.
- **`display_ma`** is imported from `mozaic_daily.seam_ma`, never from a cycle directory. The
  `data-official/2026-06/export_canonical_curves.py` copy is frozen.
- `[csv-export]` writes `csv/<month>_canonical_curves.csv` and `csv/<month>_dec15_summary.csv` and
  round-trips them; add the two filenames to `.gitignore`'s `!data-official/*/csv/` exceptions.
- Update the leading markdown cell: it is the cycle's narrative and is read by people.

Execute headless and check for errors:

```bash
jupyter nbconvert --to notebook --execute --inplace data-official/$CYCLE/<month>_canonical_v$SEAM.ipynb \
    > logs/<cycle>_notebook_exec.log 2>&1
```

---

## Step 5 — Derived exports and checks

```bash
python scripts/export_desktop_no_headwind_csv.py    # DESKTOP_ONLY.WIN10_HEADWIND_REMOVED twins (from published CSVs + both headwind.json)
python scripts/export_desktop_ex_ir_cn_csv.py       # DESKTOP_ONLY[.WIN10_HEADWIND_REMOVED].EX_IR_CN twins (reads the parquets)
python scripts/verify_forecast_states.py            # filename marker <-> sidecar consistency, writes tmp/inventory.csv
```

Both exporters are **cycle-scoped**: repoint `CSV_DIR`, `CURRENT_/PRIOR_ADJUSTMENTS_DIR`,
`FORECAST_START`, `PREV_FORECAST_START` first (Ask #5). Their `README.md` section in `csv/` must
travel with the files — the ex-IR/CN delta has the opposite sign to the published one.

Also update, or note as not done: `kpi_sheet/build_kpi_sheet_update.py` (the KPI workbook rows; August
was a `FUTURE` draft, not a promotion — read its `_index.md` before reusing), and the `handoff/` bundle
(`_index.md` there has the zip recipe).

---

## Step 6 — Report and record

Report to the user, from the CSVs (never from memory): Dec-15 28d-MA per platform with the prior-cycle
delta, the summer/near-horizon minimum and its date, and the seam step. State which overlays changed
and which carried forward. Then:

- `DATA_DIR/_index.md`: status line, "Current working set", attribution ledger of every change from the
  prior cycle, "How the current build was produced" with the exact commands run.
- Every new subdirectory gets an `_index.md`; every parquet has its sidecar.
- Commit on the cycle branch (`<month>-forecast`). Parquets are gitignored except the tracked
  overlay inputs and canonical CSVs.
- Sanity-run `pytest -q`. The suite writes a `mozaic_daily_forecast.<today>.gd-D.parquet` at the repo
  root; delete it.

The forecast is **not** delivered until the user says so; `h` and `t` edits after this point are spec
changes plus a notebook re-execution with no model re-run.

---

## Notes

- **Display layer vs baked in.** `h` and `t` are applied to the 28d MA after mozaic and live only in
  the CSVs and charts, so their Dec-15 effect is exactly their anchor. `l`, `o`, `p` (and any wired
  `j`/`i`) are baked into the parquet; changing them means a model re-run.
- **The paid seam step** (`p`'s measured-vs-marketing disagreement at the seam) is seam-dependent and
  must be re-measured after every refresh; derive the last-measured day from the seam, never hardcode.
- **`forecast-parameters/`** is the April–June provenance record and is no longer updated; each build
  directory's `parameters.json` + sidecar is the record now.
- **Pre-flight credentials**: `gcloud auth application-default print-access-token > /dev/null`. If it
  fails ask the user to run `! gcloud auth login`.
- **Retired and gone**: synthetic-Iran generation and add-back (`generate_iran_synthetic.py`,
  `add_iran_to_forecast.py`, `data-official/iran_synthetic/` — removed 2026-09-04, copies in GCS
  `april-2026/`), the `m` marketing-lift overlay (superseded by `p`), `*_composite_forecast.ipynb`
  notebooks and their `net_adjustments` helpers (superseded by the canonical notebook).
- **End of cycle**: `/cycle-button-down` locks the branch, archives to GCS with verification, prunes to
  the 3-month window, and opens the next cycle.
