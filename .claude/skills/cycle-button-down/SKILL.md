---
name: cycle-button-down
description: Lock, archive, prune, and roll forward a mozaic-daily forecast cycle. Use at the end of a cycle ("button down August", "archive the July forecast", "roll forward to September") — commits the cycle branch intact, archives every large artifact to GCS with verification, prunes the working tree to the 3-month retention window on the clean-slate branch, flags stale references, and opens the next cycle's branch.
disable-model-invocation: false
---

# Forecast-cycle button-down

End-of-cycle procedure for `mozaic-daily`. It reproduces what was done by hand at the
June→July (2026-07-07/08) and July→August (2026-07-28) transitions — see
`reference/history.md` for the commit-level record and the two things that were skipped
back then and must not be skipped again.

**Three rules override everything below.**

1. **Nothing is deleted until its archive copy is verified** (object count and bytes). The
   GCS archive is the storage medium for historical forecasts; the disk is a 3-month cache.
2. **The cycle branch is never pruned.** It is committed and pushed intact as the complete
   record. All removals happen on `clean-slate`.
3. **Fitted-model pickles (`mozaic_objects.*.pkl`) are first-class artifacts**, including
   per-probe scan pickles. They were used in August to cross-check the Win10 headwind against
   an independent approach. Archive every one regardless of upload cost.

Every phase ends at a **GATE**: report what will happen and wait for the user's explicit
go-ahead. A harness timeout on a question is not a go-ahead. If a step fails, fix it or stop
at the gate; never skip a step to keep moving.

Use `gcloud storage` for every transfer. `gsutil` multiprocessing crashes on this Mac
(`-m`, `rsync`, and sliced downloads of >100 MB objects all hit it). If `gsutil` must be
used, always prefix `-o "GSUtil:parallel_process_count=1"`.

---

## Inputs to establish first

| name | how to get it |
|---|---|
| `CYCLE` | the cycle being closed, `YYYY-MM` (e.g. `2026-08`) |
| `CYCLE_BRANCH` | its branch, `<month>-forecast` (e.g. `august-forecast`) |
| `NEXT` / `NEXT_BRANCH` | `YYYY-MM` and `<month>-forecast` for the cycle being opened |
| `PREFIX` | GCS prefix `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/<month>-<year>/` |
| `RETAIN` | cycle months within the last **3 calendar months** of today, inclusive of `CYCLE` |
| `PRE_WORK` | paths already on disk that belong to `NEXT`, typically `data-official/<NEXT>/` |

Retention on 2026-09-04: keep `2026-06`, `2026-07`, `2026-08`. Everything under
`data-official/` dated earlier (and undated April-era leftovers such as `iran_synthetic/`,
`march_brad_forecast.csv`) is a **check-and-delete** candidate.

---

## Phase 0 — Preconditions and inventory (read-only)

```bash
git branch --show-current                      # must be CYCLE_BRANCH
git status --porcelain | wc -l                 # uncommitted work to be committed in Phase 1
git rev-list --count origin/$CYCLE_BRANCH..HEAD
gcloud auth list                               # gcloud auth login (not just ADC) is required
gcloud storage ls gs://moz-data-science-brwells-bucket/mozaic-daily-archive/
du -sh data-official/* research/* tmp logs _archive 2>/dev/null | sort -h
find . -type l -not -path './.venv/*'          # symlinks: some scan dirs symlink the shared raw pull
```

**Count files with `find <dir> \( -type f -o -type l \)`.** Uploads resolve symlinks into full copies,
so the remote object count equals local files *plus* links; a plain `-type f` count will look one
short per symlink. Likewise `stat` on a link reports the link length, so local byte sums undershoot —
compare counts and pickle size lists, not raw byte totals (see Phase 2).

**Run `pytest -q` before taking the inventory, not after.** `docker/test_docker.py` writes a real
`mozaic_daily_forecast.<today>.gd-D.parquet` at the repo root; in August it appeared mid-upload and
landed in the archive as a stray. Run the suite, then inventory, then upload.

Build the inventory as three lists and show it to the user:

- **Archive set** — everything large or gitignored that belongs to `CYCLE`:
  `data-official/$CYCLE/` in full (parquets, pkls, raw pulls, zips, handoff staging), every
  `research/param-scans/<search>/` opened this cycle (probe parquets, sidecars **and pickles**),
  stray pickles elsewhere under `research/` (e.g. `headwinds/*/refit_*/`,
  `forecast-vs-summer-actuals/data/pkl/`), any repo-root `mozaic_*.pkl|parquet`, and `tmp/`
  contents worth keeping (usually none — say so explicitly).
- **Check-and-delete set** — cycle dirs and leftovers older than `RETAIN`, plus retired
  research clusters. For each: which GCS prefix should already hold it.
- **Stale-reference set** — produced in Phase 3 from the two lists above.

Classify uncommitted changes: anything under `PRE_WORK` is **excluded** from the cycle
commit and carried as untracked files across branch switches to `NEXT_BRANCH` (Phase 5).

**GATE 0.** Present the three lists, sizes, and the retention window.

---

## Phase 1 — Lock the cycle branch

Goal: `CYCLE_BRANCH` holds the complete, documented, green record.

1. **Doc currency.** `data-official/$CYCLE/_index.md` must open with a "current usable
   working set" block naming the canonical desktop and mobile parquets, the published CSVs,
   every wired spec (`adjustments/`, `launch_on_login/`, `mozillaonline/`, `organic/`, …),
   and the producer notebook. Every subdirectory needs an `_index.md` (or README). Add a
   **"Present vs Archived"** section stating what will stay on disk and what goes to `PREFIX`
   — write it now, before the move, so the branch record already says where things went.
2. **Tests.** `source .venv/bin/activate && pytest -q`. Tests left red by deliberate modeling
   changes are updated to match (July did four); regressions are fixed. Also
   `pytest _archive/tests/ -q` to confirm frozen behaviour still holds.
3. **Commit** everything except `PRE_WORK`:
   ```bash
   git add -A
   git reset -q -- data-official/$NEXT        # keep pre-work untracked
   git status --short | grep -v "^??"          # review; nothing from NEXT may appear
   git commit -m "<Month> cycle button-down: lock canonical, doc currency, green suite"
   ```
4. **Push the branch only** — no PR, no merge to `main` (no cycle branch has ever been merged).

**GATE 1.** Show the commit(s) and ask before `git push origin $CYCLE_BRANCH`.

---

## Phase 2 — Archive to GCS and verify

Layout under `PREFIX` mirrors the tree (see `reference/history.md` for the live inventory):

```
<month>-<year>/
  README.md                          # REQUIRED — July skipped it; use reference/gcs_readme_template.md
  data-official/<CYCLE>/             # whole cycle dir, including pkls and gitignored blobs
  param-scans/<search>/              # each research/param-scans/<search>/ opened this cycle
  research/<cluster>/                # any other research blobs (stray pickles) — same relative path
  root_intermediates_<date>/         # repo-root mozaic_* files, if any
research-superseded/<cluster>/       # retired research clusters (shared prefix, not per cycle)
```

Upload with `gcloud storage cp -r` (parallel, no macOS bug). August's 78 GB took **34 minutes**
end to end. Put the sequence in a small script under `tmp/` and launch it detached — the Bash tool's
background jobs are killed at the 10-minute timeout, and macOS has no `setsid`:

```bash
nohup tmp/archive_upload.sh >/dev/null 2>&1 & disown
```

Have the script append `START` / `DONE` / `FAILED` lines per directory to `tmp/archive_upload.log`
and watch it with a `Monitor` on `tail -f … | grep --line-buffered 'DONE|FAILED|FINISHED'`.

```bash
gcloud storage cp -r data-official/$CYCLE  $PREFIX/data-official/
gcloud storage cp -r research/param-scans/<search>  $PREFIX/param-scans/
gcloud storage cp README.md $PREFIX/README.md
```

Symlinks: `gcloud storage cp` follows them by default, which duplicates the shared raw pull
into each scan dir. That is acceptable (a few hundred MB) and safer than a missing file;
note it in the README. If a link is dangling, fix or remove it first.

**Verification is mandatory and mechanical.** For each uploaded directory:

```bash
# local
find <dir> -type f | wc -l ; du -sk <dir>
# remote
gcloud storage ls -r $PREFIX/<mirror>/** | grep -v '/$' | wc -l ; gcloud storage du -s $PREFIX/<mirror>
```

Counts must match exactly (local counted with `-type f -o -type l`). Bytes match exactly for
directories without symlinks; where they differ, `join` the per-file size lists and confirm every
differing file is a symlink whose link length was replaced by the target's size. Then compare the
**sorted list of all pickle sizes**, local vs remote (`md5` of each list) — identical lists mean every
first-class artifact landed at full size. Record the numbers in the cycle `_index.md` "Present vs
Archived" section and in the prefix `README.md`.

**Also verify the check-and-delete set** (Phase 0) exists in its prior prefix with the same
procedure. If anything older than `RETAIN` is *not* in GCS, upload it now before it can be
deleted; do not assume an earlier button-down covered it.

**GATE 2.** Show the verification table (local count/bytes vs remote, per directory).

---

## Phase 3 — Stale-reference scan (report only)

For every path in the archive set that will leave the disk and every path in the
check-and-delete set, search the live tree:

```bash
grep -rIn -e '<path fragment>' --include='*.py' --include='*.ipynb' --include='*.md' \
     --include='*.json' --include='*.yaml' --include='*.toml' \
     src scripts tests research data-official docker _archive CLAUDE.md .claude
```

Also list every script CLAUDE.md marks **cycle-scoped** (`mobile_scoring.py`,
`export_desktop_no_headwind_csv.py`, `export_desktop_ex_ir_cn_csv.py`, `mobile_app_breakdown.py`,
`score_near_horizon.py`, `run_aug_trough_gradient.py`) and their hardcoded `FORECAST_START` /
target constants.

Qualify ambiguous fragments with the cycle (`2026-08/desktop_locked`, not `desktop_locked` — the
prior cycle has a directory of the same name that is *retained*). Write the result to
`data-official/<NEXT>/STALE_REFERENCES_from_<month>_button_down.md` so it travels to the new branch as
the repointing to-do list, with two tables: the cycle-scoped script constants, and file (lines) per
referenced path with its post-prune status (archived / deleted / retained). **Do not edit any of
these files.** Updating them is a separate task for a
human; this skill only flags candidates. Two known standing dependencies to state every time:

- `_archive/` and `research/ma-seam-turbulence/` import the frozen
  `data-official/2026-06/export_canonical_curves.py`. Pruning 2026-06 breaks them.
- `data-official/2026-06/` still holds the `real_data_v2` marketing parquet and
  `june_delivered_mo_tailwind.json` that later cycles read.

**GATE 3.** Present the table.

---

## Phase 4 — Prune on `clean-slate`

`clean-slate` is the standing base branch: it carries all tooling, none of the exhaust. Each
cycle it must absorb the cycle branch's tooling improvements, so **fast-forward it** to the
cycle branch tip and prune from there (the cycle branch descends from it, so this is always a
fast-forward; if it is not, stop and ask).

```bash
git checkout clean-slate
git merge --ff-only $CYCLE_BRANCH
```

Untracked `PRE_WORK` files ride along through the checkout — confirm with `git status`.

Then, per item and **only after GATE 2 verification**:

- **Tracked files older than `RETAIN`** (whole cycle dirs, retired research clusters):
  `git rm -r <path>`. Preserved in `$CYCLE_BRANCH` history and in GCS.
- **Gitignored blobs** in retained cycle dirs (pkls, raw pulls, zips, handoff staging,
  superseded builds, `adjustment_isolation/` runs, `*_backup_*`): `rm -rf` when the directory
  is fully gitignored; `git clean -Xfdn <dir>` (dry run) then `git clean -Xfd <dir>` when it
  mixes tracked sidecars with blobs, so notebooks and `.meta.json` survive.
- **Keep every forecast and raw-pull `.parquet` in the closing cycle's `data-official/` dir** —
  canonical, superseded, baseline and REVERT builds alike (June precedent; August's 36 came to
  ~25 MB) — plus every `.meta.json` / `parameters.json`, README, RESTORE/REVERT doc, and the
  KPI-sheet and source-data CSVs. These are what the cycle's notebooks, scan scripts and
  diagnostics read, so keeping them turns the stale-reference list from "broken" into "fine until
  repointed" at no disk cost. What leaves is the pickles, zips, handoff staging dirs, `_backup_*`
  snapshots and `.DS_Store`. Build the list from `git clean -Xdn data-official/$CYCLE` filtered to
  `\.pkl$|\.zip$|_handoff/$|_backup_.*/csv/$|\.DS_Store$`, save it to `tmp/`, review, then delete.
- **Scan exhaust** (July precedent — the opposite rule): `git clean -Xfd research/param-scans`
  removes probe pickles **and** probe parquets and regenerable report HTML; the conclusions live in
  tracked scores CSVs, `FINDINGS.md`, sidecars and notebooks, which stay.
- **Never run `git clean -X` unscoped** — it would list `.venv/`. Always pass a path.
- `tmp/`: empty it. `logs/`: prune to the retained window.

Confirm, for a sample of removed tracked paths:
`git ls-tree -r $CYCLE_BRANCH --name-only -- <path> | head` is non-empty and the same on
`clean-slate` is empty.

**Present-vs-Archived doc pass.** Every surviving `_index.md` that lost content states what
is on disk vs what moved, with the `gs://` path and the verified counts. Update
`research/_index.md` (cluster table + "Archived" list), `research/param-scans/_index.md`,
`data-official/_index.md` (cycle-window table), and the CLAUDE.md line listing research topics
"currently on disk". Re-run `pytest -q`. Reconcile the "Present vs Archived" section written in Phase 1 with what was
actually kept (the keep rule above may differ from what was planned). Commit on `clean-slate` in two
commits as before (prune; doc pass) and push.

**GATE 4a** before the first deletion. **GATE 4b** before the push.

---

## Phase 5 — Roll forward to `NEXT`

1. `git checkout -b $NEXT_BRANCH clean-slate`.
2. Scaffold `data-official/$NEXT/_index.md`: status "empty cycle, `<CYCLE>` remains
   authoritative until this branch produces output", expected layout, state-marker and sidecar
   requirements, what is inherited (prior `TODO_factors`-style decisions, carried-forward
   tooling and the cycle-scoped constants from Phase 3 that need repointing). If `PRE_WORK`
   already exists, index what is there and whether it is wired (August's `japan_bot` and
   `india_excess` handoffs were explicitly **not** wired).
3. Update the cycle-window table in `data-official/_index.md` (`NEXT` current, `CYCLE` N-1, …).
4. `git add data-official/$NEXT` and commit the pre-work plus scaffold: "Open the <Month> <year>
   forecast cycle".
5. Push the branch.

**GATE 5** before the push. Finish with a report: the three branches and their tips at origin,
`PREFIX` contents with verified sizes, the local tree size before/after, the stale-reference
table, and the retention window now in force.

---

## Known traps (each one happened)

- July's GCS prefix has no `README.md`. Write it in Phase 2, from the template.
- `gsutil rsync` and large `gsutil cp` downloads multiprocess without `-m` and hang or crash.
- Broken symlinks abort a transfer; scan dirs symlink the shared raw pull.
- 2026-06 was deferred from pruning twice because later cycles read its files; check the
  stale-reference table, not the retention rule alone, before removing a retained-but-old dir.
- The August REVERT directory says "do not delete while August is live". Once September is the
  live cycle it becomes gitignored-blob exhaust with a tracked README — archive, then prune.
- Next-cycle pre-work landing in the closing cycle's commit. Exclude `PRE_WORK` in Phase 1.
- The test suite writing a forecast parquet at the repo root mid-button-down (August 2026).
- A Bash-tool background upload dying at the 10-minute tool timeout; `setsid` absent on macOS.
- Fragment-only greps (`desktop_locked`) matching the retained prior cycle's directory of the same name.
