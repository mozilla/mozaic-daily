# `adjustment_combinatorics/` — every subset of the October desktop adjustments, and what each one added

Built 2026-10-06 (Brendan's go-ahead, D-014) at the canonical 2026-10-05 seam and g01 config: all 32 subsets of the
five droppable desktop overlays (`e` existing-users launch at login, `i` India excess, `j` Japan bot, `l` new-users
launch at login, `o` MozillaOnline), 31 real desktop runs cached under `../adjustment_ladder/<codes>.<key>/` plus the
canonical `.adj-eijlo.` build adopted as the all-in run. `h` (−1,017,277) is applied to every row at assembly time.
**Mobile pair added later on 2026-10-06** once the paid curve was re-pulled: `p` = the canonical
`../mobile_cpr0725_paid1006_2026-10-05/` build, `raw` = its `--no-organic-split` twin `../mobile_raw_2026-10-05/`
(a total-DAU fit with no paid treatment, Dec-15 28d-MA 17,790,480). Both currency checks pass against the canonical
sidecars.

| file | what |
|---|---|
| `index.html` | the planning report: every combination's canonical desktop chart and Dec-15 vs September's 49,332,443, the all-in build and the targets |
| `dec15_by_combination.csv` | the table behind it |
| `adjustment_subsets.csv`, `adjustment_effects.csv`, `adjustment_curves_28ma.csv`, `adjustment_dec15_by_country.csv`, `adjustment_effects.meta.json` | the adjustment-effects record (tracked) |
| `adjustment_effects_vs_2026-09.csv` | per-code change since September, split into curve-moved and pass-through-moved |
| `combinatorics_manifest.json` | run list, keys, sha1s; `export_adjustment_effects.py --check-current` verified it matches the canonical desktop sidecar |

## Dec-15 28d-MA, `h` applied, from `adjustment_effects.csv`

| code | nominal | single (added to raw) | marginal (removed from all-in) | Shapley | pass-through |
|---|--:|--:|--:|--:|--:|
| `e` | 736,935 | 747,691 | 711,194 | **713,619** | 0.97 |
| `i` | 50,994 | −23,336 | 52,533 | 4,022 | 0.08 |
| `j` | 43,813 | 27,937 | −8,548 | 8,726 | 0.20 |
| `l` | 200,000 | 7,223 | 18,392 | 9,455 | 0.05 |
| `o` | 668,839 | −123,816 | −67,642 | **−117,465** | −0.18 |
| `h` | −1,017,277 | exact | exact | −1,017,277 | 1.00 |
| `p` (mobile) | 1,968,021 | 332,771 | 332,771 | **332,771** | 0.17 |
| `t`, `u` (mobile) | +299,000 / −27,162 | exact | exact | exact | 1.00 |

Raw model + `h` (every overlay dropped): **49,571,434**, already +238,991 above September delivered. All-in: 50,189,791
(+857,348). The five overlays together add +618,357, of which `e` is +713,619 and the other four net **−95,000**:
`o` now *costs* 117K at Dec-15 (its curve is subtracted from training and added back, and with the stale 2026-08-31
curve the subtraction steepens the trend Prophet fits by more than the add-back returns), `l`, `i`, `j` pass through
at 5–20%. Dropping `o` alone would publish 50,257,433. No combination was selected; October ships all five pending the
re-exports.

For `p` the "raw" run is a total-DAU fit that already contains paid, so 332,771 is marketing's paid level minus the paid
Prophet implies when it fits total DAU, not absorption: 1,968,021 − 332,771 = 1,635,250 of implied paid at Dec-15.

## Change since September (`adjustment_effects_vs_2026-09.csv`, Shapley view)

Every carried curve is byte-identical to September's, so all movement is pass-through: `o` −72,628 (−44,837 → −117,465),
`l` −51,555 (61,009 → 9,455), `j` −6,821, `i` +12,607. `e` is new (+713,619). Mobile `p` +96,168 (236,603 → 332,771):
+21,370 from the curve moving (nominal 1,804,995 → 1,968,021 at September's pass-through) and +74,798 from pass-through
moving (the total-DAU fit's implied paid grew less than measured paid did). The headline desktop delta vs September
(+857,348) therefore decomposes as `e` +713,619, raw-model refresh +238,991 − September's raw-to-all-in gap, and the
carried overlays losing ~118K of realized effect between them.
