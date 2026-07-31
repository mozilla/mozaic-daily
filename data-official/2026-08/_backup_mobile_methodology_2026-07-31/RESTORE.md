# RESTORE — August 2026 mobile marketing methodology

Snapshot taken **2026-07-31**, immediately before replacing the mobile marketing-lift
methodology. Everything needed to put the cycle back exactly as it was is either in this
directory or still in place at its original path. **Nothing was deleted.**

## What changed, and what it was before

| | BEFORE (this backup) | AFTER (current) |
|---|---|---|
| method | June-anchored outlook deltas: `L_july(d) = L_june(d) + (Jul CSV − Jun CSV)`, carried into August byte-identical | Anchor-and-subtract on the UAC + Meta query: `lift(d) = paid_dau(d) − paid_dau(2026-03-30)` |
| curve file | `marketing_lift_model.total.2026-06-29.parquet` | `marketing_lift_model.uac_meta_total.2026-07-28.parquet` |
| Dec-15 lift | 778,880 | 637,227 |
| mobile parquet | `mobile_baseline_2026-07-28/cps0.035_.../…gm-D.adj-m.parquet` | `mobile_uac_meta_2026-07-28/cps0.035_.../…gm-D.adj-m.parquet` |

Both mobile parquets remain on disk. The BEFORE build was never overwritten, so a restore is a
path swap plus a notebook rerun — **no model re-run is required**.

## Restore procedure

Run from the repo root. Steps 1–3 are the restore; step 4 verifies it.

### 1. Restore the marketing spec

```bash
cp data-official/2026-08/_backup_mobile_methodology_2026-07-31/marketing.json \
   data-official/2026-08/marketing/marketing.json
```

Verify it points back at July's curve:

```bash
python3 -c "import json;s=json.load(open('data-official/2026-08/marketing/marketing.json'));print(s['data_file'])"
# expect: marketing_lift_model.total.2026-06-29.parquet
```

### 2. Restore the canonical notebook

This is the simplest and safest option — it reverts `MOBILE_FORECAST_PATH`, the `m`-overlay
commentary, and the `[mobile-dec15]` pin and assertion in one move:

```bash
cp data-official/2026-08/_backup_mobile_methodology_2026-07-31/august_canonical_v2026-07-28.ipynb \
   data-official/2026-08/august_canonical_v2026-07-28.ipynb
```

<details>
<summary>Alternative: hand-edit instead of overwriting (if the notebook has since gained unrelated changes you want to keep)</summary>

Three edits, all in cells named in `[brackets]`:

- **`[setup]`** — set `MOBILE_FORECAST_PATH` back to:
  ```
  data-official/2026-08/mobile_baseline_2026-07-28/
  cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/
  mozaic_daily_forecast.2026-07-28.gm-D.adj-m.parquet
  ```
  and restore the overlay note to `m (marketing lift) -- byte-identical carry-forward from July. STALE. Mobile only.`
- **`[mobile-dec15]`** — set `AUG_BASELINE_MOBILE_DEC15 = 17_924_607` and restore the
  `abs(mobile_drift) < 1` assertion with its original "desktop-only overlay change" wording.
- **`[dec15-summary]`** — restore the printed header line describing `o` and `m` as
  "unchanged stale carry-forwards from July".

</details>

### 3. Restore the published outputs

The notebook regenerates these, so this step only matters if you want the old files back
*without* rerunning:

```bash
BK=data-official/2026-08/_backup_mobile_methodology_2026-07-31
cp $BK/csv/*.csv    data-official/2026-08/csv/
cp $BK/plots/*.png  data-official/2026-08/plots/
```

### 4. Verify

Rerun the notebook top to bottom. The restore is correct when **all three** hold:

- `[load-parquets]` succeeds with `require_state=["m"]`
- `[mobile-dec15]` passes its assertions, and mobile Dec-15 reads **17,924,607**
- `[dec15-summary]` reports **Mobile +738** against July delivered, and **ALL 66,628,567**

If mobile Dec-15 comes back 17,891,894 instead, step 1 or 2 did not take — the notebook is
still pointed at the new build.

## Integrity

`MANIFEST.txt` carries SHA1s for every backed-up file plus the two curve parquets, and records
the git HEAD (`f5a5c6f`) and branch (`august-forecast`) at snapshot time. Check with:

```bash
cd data-official/2026-08/_backup_mobile_methodology_2026-07-31 && shasum -c <(grep '^[0-9a-f]' MANIFEST.txt | sed 's|  data-official|  ../../../data-official|')
```

## Not backed up, and why

- **The old mobile forecast parquet** — still at its original path, never touched. Duplicating
  1.3 MB into a backup would be redundant; its SHA1 is in `MANIFEST.txt` so you can confirm it
  has not moved.
- **Desktop artifacts** — this change is mobile-only. Desktop plots are in the backup solely
  because a notebook rerun rewrites every figure, so they are there to compare against, not
  because desktop inputs changed.
- **`data-official/2026-08/_index.md`** — its headline table still quotes the BEFORE mobile
  numbers (Mobile 17,924,607, ALL 66,628,567). That makes it correct for the restored state and
  **stale for the current state**; see the note at the end of the swap report.
