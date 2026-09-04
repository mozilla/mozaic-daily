---
name: ingest-adjustment
description: Turn an external headwind or tailwind file (CSV, parquet, Excel, usually in ~/Downloads) into a registered mozaic-daily forecast adjustment — new or updating an existing one. Use when the user says "ingest this tailwind", "add this headwind", "import this curve", "update the MozillaOnline curve", or points at a delivered DAU file. Inventories existing adjustments, guesses and confirms the file's columns, builds the curve + spec + meta + registry entry through scripts/ingest_adjustment.py, and does the bookkeeping. Import only — it never reruns the model.
disable-model-invocation: false
---

# Ingest an adjustment

Import one delivered curve into the adjustment system. The deliverable is a **wired, documented
adjustment on disk**, ready for the cycle's model rerun that the user will do later, after other
imports and headwind edits. **Do not run the forecast. Do not run `verify_overlay.py`.** Print the
commands for both at the end.

The deterministic work is in `scripts/ingest_adjustment.py` (`inspect`, then `build`). This skill
supplies the conversation around it: what exists already, which family the curve belongs to, which
columns mean what, which sign, which countries, which letter — each confirmed before anything is
written — and the bookkeeping the script leaves to prose.

**Rules.**

1. **Ask real questions with question blocks; stop-and-confirm in text before any deletion.**
   A harness timeout on a question is never an answer. Never pick a default on the user's behalf.
2. **Plain words in every question.** Say "proportional to population" and "localized to specific
   countries", not `trailing_dau_share` / `fixed_country_shares`; say "subtracted before training and
   added back" or "added on top of the published curve", not "bidirectional" / "display-layer". Put the
   internal name in parentheses if at all.
3. **Weekly rows halt the whole skill.** Report the finding and stop; the user takes it from there.
4. **The original file is copied byte for byte** into the adjustment's `source_data/`. The script does
   this; never hand-edit the delivered file.
5. **Nothing is deleted unless it is already archived** — committed and pushed for tracked files, or
   present at the cycle's GCS prefix for gitignored ones — and the user has confirmed in text.

---

## Phase 0 — Inputs and inventory (read-only)

Establish, and show as a table for confirmation:

| name | how |
|---|---|
| `FILE` | the path the user gave. Do not copy it yet. `ls -la` it; if `.xlsx`, list sheet names |
| `CYCLE` | newest `data-official/YYYY-MM/` directory. **Infer, then confirm** |
| `SEAM` | the cycle's `forecast_start_date`. Read `applies_to_forecast_start` from any live spec in `data-official/$CYCLE/*/*.json`, else the cycle `_index.md`. Confirm |
| `BRANCH` | `git branch --show-current`; must be the cycle branch |

Then inventory what already exists, so "new vs update" is a grounded question:

```bash
python3 - <<'PY'
import yaml, json, glob
reg = yaml.safe_load(open("data-official/adjustment_codes.yaml"))["codes"]
for code, e in sorted(reg.items()):
    specs = sorted(glob.glob(e["spec_glob"]))
    live = [s for s in specs if json.load(open(s)).get("applies_to_forecast_start") or "adjustments/" in s]
    print(f"{code}  {e['name']:26s} {e['applier']:18s} specs={len(specs)}  newest={specs[-1] if specs else '-'}")
PY
ls data-official/$CYCLE/
```

Also list **unregistered handoffs** — directories under `data-official/$CYCLE/` holding a
`desktop_overlay` spec whose code is not in the registry (a producer may deliver a spec naming a
letter before anyone registers it; September 2026's `japan_bot` and `india_excess` arrived that way).
Their claimed letters count as taken when proposing a code.

Present one table: code, name, family in plain words, platform, live in this cycle?, spec path.

**GATE (question block):** *Is this file a new adjustment, or an update to one of these?* Options:
"New adjustment", one option per existing code that plausibly matches (by name/platform), "Other".

---

## Phase 1 — Family, platform, and what the number means

Ask in one question block (skip any the user already stated):

1. **How should this curve enter the forecast?** Three options, described plainly:
   - **Subtracted from training, then added back** (`per_tile_overlay`). The model learns history
     *without* the effect, so it does not extrapolate the effect on its own; the curve is then stacked
     back on as exactly its own value. Right when the effect has a hard start date. **Needs a model
     rerun.** References: launch-on-login `l`, MozillaOnline `o`.
   - **Added on top of the published 28-day curve** (`display_layer`, `daily_file`). Never enters the
     training frame; its Dec-15 effect is exactly the curve's own 28-day mean. **No rerun.** Right for
     effects that are not in telemetry at all (a planned campaign, a planning judgement). Reference:
     mobile tailwind `t`, which is the two-number version of this.
   - **Two numbers** (`display_layer`, `linear_ramp`): 0 at the seam to a value at Dec-15, flat after
     if `clamp_at_anchor: true`. Use when the user has an anchor number rather than a curve — **or when
     a delivered curve is to be reduced to its Dec-15 value** and applied with the cycle's standard ramp
     shape (the Win10 headwind, 2026-09: the producer's curve was anchored at the previous seam, and
     shifting it to this seam would have lost headwind; the fix was to read its Dec-15 value into a ramp
     from this seam). Still run Phase 2 on the file to validate it and copy it into `<name>/source_data/`
     with a value-read meta; skip the parquet.
2. **Platform and data source**: desktop (`legacy_desktop`) or mobile (`glean_mobile`). Mobile
   *subtract-and-add-back* is out of scope for now (that mechanism was retired for mobile paid in
   August 2026 because paid acquisition has no start date); if asked, say so and offer the
   display-layer path instead.
3. **Sign in one sentence**: "Positive numbers in this file mean the forecast should go *up*?" Most
   delivered files are positive even for headwinds — humans are imprecise about the contract. The
   inspection in Phase 2 will show the actual sign of the data; reconcile the two there.

---

## Phase 2 — Inspect the file and confirm the columns

```bash
source .venv/bin/activate
python scripts/ingest_adjustment.py inspect "$FILE" --forecast-start $SEAM --platform $PLATFORM --json \
    [--sheet NAME]
```

Read the JSON. **If any finding is `error`, stop.** Report each error in one sentence and what the
user must do (weekly rows → they analyse by hand; starts after the seam or ends before Dec 31 of the
forecast year → the producer must extend the file; actuals row after a forecast row → the producer
must fix the flag column, or the user can name the actuals-through date explicitly). Do not build.

Otherwise show a compact table of the guesses with their evidence and the first/last sample rows,
then a question block confirming, in one go:

- **date column**, **value column**, **actuals/forecast column** (or, if none: *"Through which date
  is this file measured telemetry rather than a model?"* → `--actuals-through`). If a 28-day-MA twin
  was found say it was checked against the daily column and will not be used.
- **sign**: state the data's sign (`sign_guess`) next to the user's Phase 1 answer. If they agree,
  `--sign 1`. If the file is positive but the effect is a headwind (or vice versa), `--sign -1`.
  Mixed sign: show the split and ask whether the file is right.
- **warnings** (`looks_like_moving_average`, `monotone_increasing`, `skipped_days`,
  `very_large_values`, `ma_twin_mismatch`): each as a yes/no "proceed anyway?" line. A moving average
  delivered as the daily series is a real mistake seen before; do not wave it through.
- **a file that IS a 28-day series** (the only value column is named like an MA, or the producer says
  so): that is legitimate for a display-layer curve. Confirm, and build with `--values-are-28d-ma` so
  it is used as-is rather than re-smoothed. A title line above the header is skipped automatically.
- **a curve anchored at an earlier seam** (it reads non-zero at this cycle's seam): ask whether the
  part accrued before the seam is already inside the Prophet base fit. If yes, build with
  `--rebase-to-seam` (0 at the seam, delivered series kept in the parquet, offset in the meta) and add
  a follow-up TODO to verify that assumption. If no, apply as delivered and record the seam step.

Mention the `hold_flat_tail` info line in words: "the file ends YYYY-MM-DD; the last 28-day mean,
about N DAU, is held flat to Dec 31 of next year so the component does not vanish on 1 January."

**The file's own actuals/forecast boundary does not have to match the forecast seam.** A producer's
measured data may end a few days before (or after) the seam; what matters is that the file has data of
some kind for the whole period the adjustment covers — from the seam (or earlier) through year end.
The inspector reports the offset as an info line (`file_boundary_before_seam` / `_after_seam`); repeat
it in words and move on. Do not ask the producer to re-cut the file to the seam.

---

## Phase 3 — Allocation (subtract-and-add-back only)

The curve is a world total; the pipeline splits it across country tiles. Ask:

- **Is this effect spread in proportion to where Firefox users are, or localized to specific
  countries?** Proportional → `--allocation trailing_dau_share` (each country's share of the segment's
  DAU over the last 28 days, like launch-on-login). Localized → `--allocation fixed_country_shares`
  and ask for the shares as `{"JP": 1.0}`-style fractions summing to 1 (a single-country effect is
  `{"XX": 1.0}`, like the Japan and India September handoffs).
- **Exclusions.** Default `--exclude IR` (Iran's curve is a counterfactual fill). Ask about **CN**
  separately and say why it matters: excluding CN from a China-based effect deletes it (MozillaOnline
  is ~93% CN). Excluded countries get none of the curve; the rest renormalize so the world total is
  preserved.
- **Segment.** Desktop defaults to `modern_windows`; only change it if the user says the effect lives
  elsewhere.

---

## Phase 4 — Code letter and name

Propose, then confirm in a question block:

- **Code**: the next unused lowercase letter after checking the registry **and** the unregistered
  handoff directories from Phase 0. Codes are permanent and never unregistered. For an *update*, the
  code is fixed already.
- **Name**: `snake_case`, the same word the directory and spec will carry (e.g. `japan_bot`).

---

## Phase 5 — Draft the prose, show it, and build without waiting

Draft the two texts below and **print them, then build immediately** — do not stop for edits. Both
land in JSON/YAML the user can change afterwards in seconds, and the user has asked not to be gated
on wording mid-ingest (2026-09-04). If they reply with edits later, apply them with `--replace` or a
direct edit of the spec/registry.

- the **registry description** (`--description`): what the effect is, who measured or modelled it,
  what is measured vs assumed, the family and why, in the register of the existing entries in
  `data-official/adjustment_codes.yaml`;
- the **spec notes** (`--notes`): one paragraph, cycle-dated.

Then build. New adjustment:

```bash
python scripts/ingest_adjustment.py build "$FILE" \
    --name $NAME --code $CODE --family $FAMILY --platform $PLATFORM --data-source $DATA_SOURCE \
    --forecast-start $SEAM --cycle $CYCLE \
    --date-column ... --value-column ... [--type-column ... | --actuals-through YYYY-MM-DD] [--ma-column ...] \
    [--sign -1] [--allocation ... --shares '{...}'] --exclude IR[,CN] \
    [--values-are-28d-ma] [--rebase-to-seam] \
    --description "..." --notes "..." [--sheet NAME]
```

For an existing code whose layout predates the `<name>/<name>.json` convention (e.g. `o` lives in
`mozillaonline/mozillaonline.json`, `h` in `adjustments/headwind.json`), the script follows the registered
`spec_glob`, so pass the **registry name** as `--name` and the files land where the dispatcher looks.
A display-layer code's spec goes to `adjustments/`; its curve, source and plot go to `<name>/`.

Update of an existing code: the same command **plus `--replace`** when the spec already exists in this
cycle's directory. A new cycle directory needs no `--replace`; the prior cycle's build stays frozen. The script moves the live spec,
data file and meta into `data-official/$CYCLE/${NAME}_REVERT_<today>/` with a `REVERT.md` and leaves
the registry alone. Tell the user the REVERT dir exists and that a revert is "move the files back and
rerun". (August's precedent: `data-official/2026-08/desktop_s01_REVERT_2026-07-29/`.)

Two-numbers path (no file): write `data-official/$CYCLE/adjustments/$NAME.json` as
`{"type":"linear_ramp","start_date":SEAM,"anchor_date":"<year>-12-15","desktop_dau":…,"mobile_dau":…,"notes":…}`
by hand, append the registry entry with `applier: display_layer` and
`spec_glob: "data-official/*/adjustments/$NAME.json"`, and create `data-official/$CYCLE/$NAME/_index.md`
as the rationale record (model on `data-official/2026-08/tailwind/_index.md`).

Read the build's JSON summary back to the user: files written, coverage table, Dec-15 28d-MA of the
curve, hold-flat value, whether the registry entry was new, and the `.gitignore` lines added.

**Open the shape plot** the build wrote (`files.plot`, under `<name>/plots/`) with `code <path>` and
name the path in the message. It shows the daily curve and its 28-day mean over measured / projected /
held stretches with the seam and Dec-15 marked. Look at it and say what you see in one sentence: a
wrong sign, a moving average passed off as daily, a cliff where the file ends, or a plateau that
starts before the seam are all visible here and nowhere else before the rerun. For a re-render
later: `python scripts/ingest_adjustment.py plot --name … --code … --cycle … --family … --platform …
--forecast-start …`.

---

## Phase 6 — Alternates and superseded files

If the adjustment directory holds other candidate curves (scenario parquets from a handoff, a
previous cycle's file, a REVERT dir from an earlier re-ingest this cycle), one curve stays on disk:

1. List each alternate with its size and whether it is **already archived**:
   `git log --oneline -1 -- <file>` and `git status --porcelain <file>` for tracked files (must be
   committed **and** pushed: `git branch -r --contains $(git log -1 --format=%H -- <file>)`), and
   `gcloud storage ls gs://moz-data-science-brwells-bucket/mozaic-daily-archive/<month>-<year>/…` for
   gitignored parquets.
2. Anything not archived: upload with `gcloud storage cp` to the cycle prefix and re-list to verify.
3. **Stop and confirm in text** with the exact list before `git rm` / `rm`. Never delete a `*_REVERT_*`
   dir while its cycle is live.
4. Record the decision (which scenario was chosen, where the losers live) in the adjustment's `_index.md`.

---

## Phase 7 — Bookkeeping

Do all of these; show what you wrote, but do not wait for approval of wording:

1. **Finish `data-official/$CYCLE/$NAME/_index.md`**: the "What it is" paragraph and the "measured vs
   assumed" table (the script left placeholders). Keep the coverage table it generated. Write it,
   don't gate on it — same rule as Phase 5.
2. **`data-official/$CYCLE/_index.md`**: add the directory to the *Expected layout* tree with a
   `# present` note; add an **attribution-ledger row** for the code marked **"pending rerun"** with the
   curve's Dec-15 28d-MA as the *expected* add-back for subtract-and-add-back codes (the realised effect
   is measured only after the rerun) or as the exact effect for display-layer codes; if the curve is a
   planning judgement rather than a measurement, add it to the *"Read this before quoting the
   headline"* table.
3. **`CLAUDE.md`**: a row in the adjustment-codes table (§ Forecast Artifact Naming Convention),
   written like the existing rows.
4. **Tests**: extend `tests/test_overlays.py::TestCommittedRegistryAndSpecs` with an assertion that
   `resolve_overlays(SEAM)` includes the new code (subtract-and-add-back only). Run `pytest tests/ -q`.
5. **Canonical notebook check (display-layer only).** If `data-official/$CYCLE/*_canonical_*.ipynb`
   exists, it must import `render_adjustment` / `load_adjustments_from_dir` from
   `mozaic_daily.adjustments` rather than define its own copy — a local copy silently renders a
   `daily_file` spec as zero. `grep -n "def render_adjustment\|load_adjustments_from_dir" <notebook>`;
   if it defines its own, **stop and tell the user**. Also note that `scripts/regenerate_composites.py`
   hard-codes `["h"]` as the display-layer codes and will need the new code added before it can
   reproduce the cycle's composite CSV.
6. **Scope check**: `git status --short`. Everything touched should be under `data-official/$CYCLE/$NAME/`,
   `data-official/$CYCLE/adjustments/` (display layer), `data-official/$CYCLE/_index.md`,
   `data-official/adjustment_codes.yaml`, `.gitignore`, `CLAUDE.md`, `tests/test_overlays.py`. Anything
   else is a mistake to explain.

---

## Phase 8 — Report

Leave the changes **uncommitted** and offer to commit. End with a self-contained report:

- what was ingested (code, name, family in plain words, platform, sign, allocation, exclusions);
- coverage: delivered range, actuals-through, hold-flat value and date, Dec-15 28d-MA of the curve;
- every file written, the REVERT dir if any, alternates archived/deleted if any;
- the two commands the user will run later, **not now**:

```bash
# rerun (subtract-and-add-back codes only; one overlay change per run keeps the Dec-15 delta interpretable)
python scripts/run_main.py --data-sources $DATA_SOURCE --metrics DAU --forecast-start-date $SEAM --output-dir <build dir>
# then verify the pass-through
python scripts/verify_overlay.py --code $CODE --cycle $CYCLE --raw-cache-dir <build dir>
```

Then a `result:` line: `result: <code> <name> ingested into data-official/<cycle> as <family>; rerun pending`.

---

## Reference: the contract the inspector enforces

From `templates/tailwind/TAILWIND_CSV_FORMAT.md` plus the 2026-09-04 decisions:

| check | outcome |
|---|---|
| rows are weekly or irregular | **halt** |
| first date after the seam | **halt** — the curve must cover the cycle from its first forecast day |
| file's actuals end before/after the seam | info — allowed; only coverage matters |
| last date before Dec 31 of the forecast year | **halt** |
| last date between Dec 31 this year and Dec 31 next year | info — held flat at the final 28-day mean |
| rows past Dec 31 next year | info — dropped |
| an actuals row after a forecast row | **halt** |
| no actuals/forecast column | ask for the actuals-through date |
| skipped days inside the range | warning — linearly interpolated |
| day-to-day change under 3% of level | warning — looks like a moving average |
| never decreases | warning — looks cumulative |
| mixed signs | warning |
| 28-day-MA twin present | checked against the daily column, never used |
