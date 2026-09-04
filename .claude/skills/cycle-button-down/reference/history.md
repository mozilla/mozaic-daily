# Button-down history and archive inventory

What each past transition actually did, so the skill's steps can be checked against the record.
Commit hashes are on the branches named; `git show --stat <hash>` reproduces the detail.

## June → July (2026-07-07 / 08), branches `july-forecast` → `clean-slate`

| commit | branch | what |
|---|---|---|
| `953c492` | july-forecast | Button-up: lock desktop model, regenerate canonical notebook, export CSVs, repoint docs |
| `5cc084e` | july-forecast | "Phase A" doc currency: "current usable working set" callout, missing subdir `_index.md`s, June marked as N-1, **tests updated to match deliberate July behaviour** (282 passed) |
| `c4346ed` | clean-slate | Prune: `git rm` of `data-official/2026-04/` and retired research (`iran/`, `marketing-lift/v1-convolution/`, `april-vs-june-mechanism/`, `desktop-gap-decomp/`); gitignored blobs removed from disk separately |
| `b456e12` | clean-slate | "Phase F" Present-vs-Archived doc pass; 61 GB param-scan blobs archived (verified 37.18 + 23.9 GiB) then `rm -rf results/` and `git clean -X mobile-july`. Tree 110 GB → 1.1 GB |

GCS: `june-2026/` (pkls + zips of the June cycle), `july-2026/data-official/2026-07/`,
`july-2026/param-scans/`, `july-2026/root_intermediates_2026-06-29/`, `research-superseded/`.
**No `july-2026/README.md` was written.**

## July → August (2026-07-28), `july-forecast` → `clean-slate` → `august-forecast`

| commit | branch | what |
|---|---|---|
| `31508f7` | july-forecast | Late July work (Aug-trough retune) landed after the prune |
| `547fad6` | clean-slate | Cherry-picked the reusable half of `31508f7` (tooling + findings, not per-probe exhaust) — the mechanism this skill replaces with a fast-forward merge |
| `850adf4` | clean-slate | Cycle-window table rolled to August; recorded **why 2026-06 was not pruned** (load-bearing files for July) |
| `14875c3` | august-forecast | "Open the August 2026 forecast cycle": scaffold `_index.md` only |

Mid-cycle, out of tree: `august-2026-model-handoff/` (1.4 GB, 2026-08-25) and
`april-2026-model-handoff/` (1.6 GB) — handoff bundles for a colleague, not cycle archives.

## Archive inventory as of 2026-09-04

```
gs://moz-data-science-brwells-bucket/mozaic-daily-archive/
  april-2026/                  30.97 GiB   README.md present
  june-2026/                   10.65 GiB   README.md present
  july-2026/                  105.55 GiB   NO README
    data-official/2026-07/
    param-scans/{aug22-retune,mobile-july,results}/   (101 GiB)
    root_intermediates_2026-06-29/
  research-superseded/         20.7 MiB    {april-vs-june-mechanism,country-overrides,desktop-gap-decomp,iran,ma-seam-turbulence,marketing-lift}/
  april-2026-model-handoff/     1.58 GiB
  august-2026-model-handoff/    1.36 GiB
```

Project `moz-fx-data-bq-data-science`. Requires `gcloud auth login`.

## Local state at the start of the August button-down (2026-09-04)

- `august-forecast` 5 commits ahead of origin, 93 uncommitted paths (58 modified tracked, ~35
  untracked incl. new scripts/tests, `adjustments/tailwind.json`, `data-official/2026-09/`
  pre-work from another agent: `japan_bot/`, `india_excess/`, neither wired).
- Tree 80 GB: `research/param-scans/` 67 GB (summer-trough-v2 40 GB / 68 pkls, mobile-aug 26 GB
  / 33 pkls, aug25-gap 1.7 GB / 390 probes), `data-official/2026-08/` 9.4 GB (five desktop and
  five mobile ~600–800 MB builds, `adjustment_isolation/` 1.8 GB, `marketing/` 802 MB),
  `research/headwinds/aug-post-seam-retune/refit_2026-08-17/` 582 MB pkl,
  `research/forecast-vs-summer-actuals/data/pkl/` 631 MB pkl (a July pkl pulled back from GCS),
  `tmp/` 804 MB.
- `data-official/2026-07/` 25 MB and `2026-06/` 67 MB: canonical parquets + specs only, pkls
  already in GCS.
- `main` is at `6e13c2d` (February); no cycle branch has ever been merged into it.

## August → September (2026-09-04), first run of this skill

| commit | branch | what |
|---|---|---|
| `cb9b33f` | august-forecast | Button-down: 234 files, five build-dir `_index.md`s, Present vs Archived, skill added; 435 + 12 tests green; pushed |
| (Phase 4) | clean-slate | Fast-forwarded to `cb9b33f`; prune + doc-pass commits (see `git log clean-slate`) |
| (Phase 5) | september-forecast | Opened off clean-slate with the pre-work + stale-reference report |

GCS `august-2026/`: 79.0 GiB, README present. Verified 210/210, 426/426, 230/230, 2,003/2,003, 13/13,
9/9 objects; 119 pickles with identical size lists. Upload took 34 min via a detached `nohup` script.
Prune: tree 80 GB → 1.2 GB. Kept all 36 August forecast/raw-pull parquets (June precedent), deleted
11 pickles + zip + staging + backup; `git clean -Xfd` on the three scan dirs (69 GB) and two research
blob dirs; removed `iran_synthetic/` (tracked) and `march_brad_forecast.csv` after confirming
`april-2026/` copies. Stale-reference report: 129 refs in 46 code files →
`data-official/2026-09/STALE_REFERENCES_from_august_button_down.md`.

## Decisions recorded for this skill (2026-09-04, user)

- Retention: artifacts from the last **3 months** stay on disk "to answer questions and refer
  to"; older forecasts live only in GCS after a verified check-and-delete.
- `clean-slate` must absorb every tooling improvement of the closing cycle branch.
- Next-cycle pre-work is never committed on the closing cycle's branch.
- **Probe pickles are first-class** (used in August to compare the Win10 headwind against an
  alternate approach). Always archive before deleting, whatever the upload cost.
- Stale references to archived paths are **flagged to a human**, not edited by this skill.
- Keep the closing cycle's forecast/raw-pull parquets on disk (they are small); only pickles and
  bundles leave (GATE 4a, August).
- Roll-forward additions (user, 2026-09-04, after the first run left `2026-09/` nearly empty):
  copy the closing cycle's canonical notebook as the template, named without a seam; copy every
  registered adjustment (incl. `p`) forward unmodified with a **seam warning in `notes`, not a
  block**; audit script references and **edit the unambiguous cycle-scoped constants**, flag the
  ambiguous ones. Raw pulls and the `p` split rebuild stay in the monthly update — the skill does
  not pull data.
