# s01 — REVERT TARGET (not an archive)

> ## ⛔ DO NOT FOLLOW STEP 2 AS WRITTEN — the coupling it assumes was broken on 2026-08-03
>
> This kit was written on 2026-07-30, when the g01 retune and a +25,000 headwind attenuation were
> applied **as one unit**, so reverting meant undoing both. **Two things have happened since, and both
> invalidate the headwind half of these instructions:**
>
> 1. **2026-08-02 data refresh.** The canonical desktop build is no longer this build's successor —
>    it is `../desktop_g01_2026-08-02/`, trained through 2026-08-01 at the 2026-08-02 seam. Reverting
>    the *config* now means rebuilding s01 **at the new seam**, not restoring the 07-28 parquet.
> 2. **2026-08-03 headwind reversal.** The anchor was moved −1,220,000 → **−1,295,000**, independently
>    of the model config and for an unrelated reason (giving back 75,000 of the refresh's +100,840).
>    So the anchor is now *below* the −1,245,000 this kit tells you to restore. **Following step 2
>    would attenuate the headwind by 50,000 while reverting a model change — the opposite of the
>    compensation it was written to preserve, and it would publish a number no build has produced.**
>
> **What to do instead:** treat step 1 (the config) as the only revertable item here, rebuild s01 at
> the live seam with `run_param_scan.py`, and **leave `adjustments/headwind.json` alone** unless there
> is a separate, stated reason to move it. The numbers below remain a correct record of the
> 2026-07-30 comparison and nothing else. See `../adjustments/_index.md` § attenuation history.

**This directory exists so the August desktop forecast can be put back the way it was.** It is not
a historical curiosity; a revert was considered a real possibility at the time of the swap
(2026-07-30) and nothing here should be deleted while the August cycle is live.

It holds the **complete** August canonical desktop build as it stood from 2026-07-29 to 2026-07-30,
plus a snapshot of the headwind spec that went with it.

## What was replaced, and why

On 2026-07-30 the desktop model was retuned from **s01** to **g01** to close part of the gap between
the August and July curves at the Aug-25 trough. Two things changed together, and **reverting means
undoing both**:

| # | change | from | to |
|---|---|---|---|
| 1 | desktop model config | **s01** (`cps 0.1849, cpr 0.734, ncp 35, recent 17`) | **g01** (`cps 0.1649, cpr 0.814, ncp 40, recent 17`) |
| 2 | Win10 headwind desktop anchor | **−1,245,000** | **−1,220,000** |

The headwind moved because the config change alone dropped Dec-15 by 31,357; +25,000 of headwind
attenuation was applied to absorb most (not all) of it. **The +25,000 is part of this revertable
change, not an independent decision** — putting s01 back without also restoring −1,245,000 would
leave Dec-15 25,000 above where s01 originally published.

## Numbers (desktop ALL, 28d-MA, post-headwind)

| | s01 @ −1,245,000 (this build) | g01 @ −1,220,000 (current canonical) | delta |
|---|--:|--:|--:|
| Aug-25 trough | 45,223,249 | 45,041,389 | **−181,860** |
| Dec-15 | 48,703,960 | 48,697,603 | **−6,357** |
| seam kink (model-only) | −9,554 | −16,549 | −6,996 (**1.73×**) |

Aug-25 is quoted at each build's own headwind. The pure config effect there is **−186,860**
(45,223,249 → 45,036,389, both measured at −1,245,000).

**The +25,000 headwind attenuation does not add back +25,000 at Aug-25 — it adds +5,000.** `h` is a
linear ramp from the seam (2026-07-28) to the Dec-15 anchor, 140 days; Aug-25 is 28 days in, so only
20% of the anchor value has accrued. The ramp reads −249,000 at Aug-25 under −1,245,000 and −244,000
under −1,220,000. Hence net −181,860, not −161,860.

The same asymmetry is why this trade works at all: the headwind buys back 25,000 at Dec-15 while
giving up only 5,000 of the trough movement it was meant to preserve.

## Why a revert might be wanted

**g01 is an isolated optimum.** It was the single deepest cell of a 243-cell factorial, and **all
seven of its measured one-step neighbours are 52,092–165,860 shallower** at Aug-25. It is fully
deterministic and reproduces exactly, so this is not a reproducibility risk — but the depth sits on
a needle. Any future data refresh, package upgrade, or re-tune that shifts an effective parameter
slightly will likely lose most of the 9.52% gap closure while keeping the 1.73× seam-kink cost.

s01, by contrast, sits on the flat top of its own neighbourhood and carries a 1.00× kink.

Full evidence: `../../../research/param-scans/aug25-gap/LOG.md`, figures under that directory's
`plots/`.

## How to revert

```bash
cd /Users/brendanwells/work/mozaic-daily

# 1. Restore the build
cp data-official/2026-08/desktop_s01_REVERT_2026-07-29/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet \
   data-official/2026-08/desktop_s01_REVERT_2026-07-29/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet.meta.json \
   data-official/2026-08/desktop_s01_REVERT_2026-07-29/parameters.json \
   data-official/2026-08/desktop_s01_REVERT_2026-07-29/mozaic_objects.legacy_desktop.2026-07-28.pkl \
   data-official/2026-08/desktop_locked/

# 2. Restore the headwind anchor (-1,220,000 -> -1,245,000)
cp data-official/2026-08/desktop_s01_REVERT_2026-07-29/headwind.s01-era.json \
   data-official/2026-08/adjustments/headwind.json

# 3. Repoint the notebook's config lock back to s01, then re-execute all cells
#    (the lock asserts the sidecar field-by-field and WILL abort on the wrong config)
```

Then update `../_index.md` (status line + attribution ledger) and `desktop_locked/README.md`, and
drop the two g01 ledger rows. `README_as_canonical.md` in this directory is the exact README s01
carried while it was canonical — restore it as `desktop_locked/README.md`.

**The raw BigQuery pull is shared and unchanged**, so a revert needs no re-query and no model re-run:
every artifact here was produced from the same
`desktop_baseline_2026-07-28/cps0.08983_.../mozaic_parts.raw.legacy.desktop.DAU.parquet`.

## Contents

| file | what |
|---|---|
| `mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` (+ `.meta.json`) | the s01 forecast, `l`(200K)+`o` baked in, pre-headwind |
| `parameters.json` | the s01 config, field-for-field |
| `mozaic_objects.legacy_desktop.2026-07-28.pkl` | fitted mozaic objects (634MB, gitignored — GCS archive if pruned) |
| `headwind.s01-era.json` | the headwind spec as it stood, desktop anchor **−1,245,000** |
| `README_as_canonical.md` | the README s01 carried while canonical |
| `REVERT.md` | this file |

## Do not

- **Do not delete this directory** while August is the live cycle.
- **Do not re-run** these artifacts to "refresh" them. Historical builds are locked — published
  deltas were quoted against them and a chain whose links move cannot be audited.
- **Do not revert the config without the headwind**, or vice versa. They were changed as one unit.
