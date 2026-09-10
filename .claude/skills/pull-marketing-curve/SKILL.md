---
name: pull-marketing-curve
description: Turn the marketing team's paid-DAU SQL query (the GMIO widget query with {{metric}} / {{country}} templates) into files on disk — the query output verbatim plus the daily paid-DAU curve that the mobile `p` split consumes — under data-official/{cycle}/marketing/, with a pull-date suffix and a PENDING_WIRING.md hand-off. Use when the user pastes or points at a marketing paid-DAU query, says "pull the marketing data", "re-pull the paid curve", "refresh the GMIO feed", or hands over a new ahe_gmio table suffix. Query-to-file only — it never edits organic.json, tests, the registry, or the model, and never overwrites an earlier pull.
disable-model-invocation: false
---

# Pull the marketing paid-DAU curve

The marketing team publishes paid mobile DAU as a **query**, not a file. This skill does the file
step for them: run the query once, keep what came back verbatim, and translate the weekly rows into
the daily paid-DAU level `p` stacks as delivered. The deliverable is **files under
`data-official/{cycle}/marketing/` plus a hand-off note**. Nothing the forecast reads changes.

The deterministic work is in `scripts/pull_paid_dau_curve.py`; logic in `mozaic_daily.paid_curve`
(pure) and `mozaic_daily.paid_curve_files` (writes). This skill supplies the conversation around it.

**Boundary with `/ingest-adjustment`.** That skill takes a *file* of *daily* rows and wires a
*registered adjustment code* (spec, registry, `_index.md` ledger, CLAUDE.md row, tests). This one
takes a *query* of *weekly* rows and produces an *input file* for the existing `p` code. They share no
files. Wiring the new curve into `organic.json` is a third, separate step that this skill only
describes in `PENDING_WIRING.md`.

**Rules.**

1. **Ask with question blocks; a harness timeout is never an answer.** Never pick a default on the
   user's behalf on anything below marked GATE.
2. **BigQuery only through `bq_query.py`** (dry-run validated, SELECT-only, billing-capped). The
   script does this; never run the SQL another way.
3. **Strict shape.** The query must return exactly `date, uac_actual, uac_forecast, uac_meta_actual,
   uac_meta_forecast` on consecutive ISO Mondays. Anything else halts; report it and stop. A new shape
   is a deliberate extension of `paid_curve.py`, not a per-run improvisation.
4. **Never overwrite.** Every output carries the pull date; the script refuses to clobber. Earlier
   pulls stay as siblings.
5. **Do not touch** `organic.json`, `tests/`, `adjustment_codes.yaml`, the cycle `_index.md`, or
   `CLAUDE.md`. Do not run the model.

---

## Phase 0 — Inputs (read-only)

Establish and show as a table for confirmation:

| name | how |
|---|---|
| `SQL` | the query. If pasted, save it under `./tmp/` first (the script copies it verbatim into `source_data/`). Note the feed table suffix (`ahe_gmio_..._YYYYMMDD`) |
| `CYCLE` | newest `data-official/YYYY-MM/`. Infer, then confirm |
| `SEAM` | `applies_to_forecast_start` in `data-official/$CYCLE/organic/organic.json`. Confirm |
| `PULL_DATE` | today, unless the user says otherwise |
| `VARIANT` | none for the point estimate; a slug (e.g. `ci90lo`) if the SQL header says it is another quantile. The user must name it |
| existing pulls | `ls data-official/$CYCLE/marketing/` — list every `marketing_lift_model.*` stem and which one `organic.json` points at |

Templates: `grep -o '{{[a-z_]*}}' $SQL | sort -u`. The widget query has `{{metric}}` and
`{{country}}`; the defaults are **Total Paid DAU / All** (what `p` needs: the total paid level,
world-wide). Any other template halts — the script raises and names it.

**GATE (question block) only if something is unusual:** a metric other than the default was asked
for, or the SQL has templates the script will not know.
Otherwise proceed.

---

## Phase 1 — Pull and build

```bash
source .venv/bin/activate
python scripts/pull_paid_dau_curve.py --sql $SQL --cycle $CYCLE --forecast-start $SEAM \
    [--pull-date YYYY-MM-DD] [--metric ...] [--country ...] [--variant SLUG]
```

**`--variant`** is for a query that is not the point estimate — the marketing team also publishes a
lower-bound twin (`ci_lo` = p5 of the 90% credible interval on forecast weeks, `_ci90_` feed tables).
Pass a slug such as `ci90lo`; it lands in every file name (`...gmio_uac_meta_total_ci90lo...`,
`paid_dau_curve.ci90lo...`), the meta's `model_name` / `variant`, the plot title and the hand-off
note, so the pull can never be mistaken for the point estimate. Omit it for the point estimate.

The script, in order: copies the template SQL; writes the resolved SQL with a provenance header;
runs it through `bq_query.py --machine`; writes the JSON result and a CSV **verbatim** under
`source_data/`; enforces the contract; composes one value per week (UAC+Meta where present, else UAC;
actual over forecast on the handoff week); places each value on its Monday, interpolates linearly to
daily, forward-fills to Dec 31; writes the parquet (`paid_dau_level_daily`, `paid_dau_level_ma` — the
level as delivered, no lift, no anchor), its CSV twin with an
`actuals`/`forecast` column, the meta, the three-sheet workbook, the plot, and `PENDING_WIRING.md`.

**If it halts** (shape, non-Monday rows, a gap week, handoff disagreement, actual after forecast,
unresolved template, an existing sibling for this pull date): report the one-line error and what the
user must do. Do not patch the data.

To rebuild from a saved result without re-querying: `--from-json source_data/<slug>.<stamp>.json`
with a new `--pull-date`.

---

## Phase 2 — Look at it

Open the plot with `code <path>` and name the path. Say in one sentence what you see: the actual /
forecast handoff, whether the values are total paid DAU at a plausible scale (~1.5–2M), a cliff or a plateau,
and where the seam falls relative to the last actual week.

Then compare against the pull `organic.json` currently points at, from the two metas' `key_values`:

| | wired pull | this pull |
|---|--:|--:|
| level at seam | | |
| level at Dec-15 | | |
| actuals through week of | | |

State the Dec-15 level change in one sentence. Because `p` stacks the level additively after
mozaic, that difference is the expected change in the published mobile Dec-15 from this input alone,
realised only after wiring and a rerun.

---

## Phase 3 — Report

Leave everything **uncommitted** and offer to commit. End with a self-contained report:

- what was pulled (feed tables, template params, pull date, weeks, actuals-through week);
- every file written, with the plot path;
- the comparison table from Phase 2;
- that `PENDING_WIRING.md` holds the hand-off and `organic.json` was **not** changed;
- a `result:` line: `result: paid curve pulled <pull_date> into data-official/<cycle>/marketing; wiring pending`.

---

## Reference: the contract the script enforces

| check | outcome |
|---|---|
| columns ≠ exactly the five widget columns | **halt** |
| a row not on an ISO Monday, or a skipped / duplicate week | **halt** |
| UAC (or UAC+Meta) actual ≠ forecast on the handoff week | **halt** |
| an actual week after a forecast week | **halt** |
| a week with no value in any column | **halt** |
| an output for this pull date already exists | **halt** |
| unresolved `{{template}}` | **halt** |
| `Attributed New Profiles` as metric | **halt** — a flow, not a paid-DAU level |
