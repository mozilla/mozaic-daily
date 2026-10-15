# iOS-rate cap on organic Fenix MAU (India, Indonesia) — what it is and how to turn it off

**Status: PROVISIONAL (Brendan, 2026-10-15).** "Yes, go ahead with that. Document it well, because we may need to roll this
back depending on what marketing DS says." The question is with the marketing data-science team; this note is written so
the rollback is one flag.

## What

A **data correction** to the training rows of the 2027 ORGANIC mobile MAU forecast, for **India and Indonesia only**, from
**2026-04-01**:

```
cap_level(t)  = organic_fenix(2026-04-01) × iOS_ma28(t) / iOS_ma28(2026-04-01)      # the country's own Firefox iOS, trailing 28-day mean
capped(t)     = min(organic_fenix(t), cap_level(t))
removed(t)    = organic_fenix(t) − capped(t)   ≥ 0, treated as paid, NOT added back anywhere
```

Nothing else changes: other countries, other apps, forecast rows, the paid/organic split itself, Iran's fill, the model
config. The removed MAU is not forecast and not handed to anyone; marketing forecasts paid, and this excess is being
treated as paid.

Numbers on the last training day (2026-10-09), from `ios_rate_cap.ID_IN.2026-04-01.meta.json`:

| | organic Fenix MAU | removed | organic growth since 2026-04-01 | iOS 28d ratio | first capped day |
|---|---|---|---|---|---|
| India | 3,812,610 | **396,385** | +30.1% | 1.166 | 2026-04-03 (binding continuously from late July) |
| Indonesia | 650,910 | **43,696** | +22.6% | 1.144 | 2026-04-02 |
| total | | **440,081** | = 1.32% of world organic Fenix, 0.93% of world organic mobile MAU | | |

The correlation page quoted 486,413 for the same idea with the two raw endpoints; the 28-day-smoothed ratio gives 440,081
because iOS sits a little below its own 28-day mean on 2026-10-09. The two numbers are the same finding.

## Why

`../paid_organic_correlation.html` section D (plots `../plots/paid_vs_excess_over_ios.png`,
`../plots/paid_organic_in_id_vs_peers.png`). Over the high spend period (April–October 2026) organic Fenix MAU grew
**+15 points above the same country's Firefox iOS in India and +7 in Indonesia** as the paid share of Fenix MAU climbed past
30% (India 25% → 55%); Brazil and Mexico at 45–50% paid share show no such lift (Brazil is −11). The paid definition
requires a Play Store install with a gclid, and India's campaign installs often arrive by sideload or other stores, so the
likely mechanism is **paid installs booked as organic** rather than organic growth. Left in, the series hands the model a
slope that is partly spend.

Why iOS as the yardstick: it is the same country, the same months and the same market, and it carries no paid acquisition
at all, so it removes the "marketing spends where mobile grows" explanation.

## In the forecast: the excess is carried FLAT (Brendan, 2026-10-15: "keep it flat, but document that choice")

The removed series is the **inauthentic organic** component. It is in neither of the two forecasts that make up the total: our organic
model trained on the capped (authentic) series, and the marketing DS plan counts only Play-Store-attributed installs. So the composition
(`scripts/compose_mobile_total_mau.py`) carries it as its own column, `organic_inauthentic_<population>`: the removed series over history,
then the last value (India 396,385, Indonesia 43,696; 440,081 together) **held flat** through 2027-12-31, in its own country. Total =
authentic organic + inauthentic organic + paid, and history rows still equal the raw actuals exactly.

Why flat: there is no model for it and no information about how it will move; flat is the same no-information choice as the desktop
Linux bot component. The alternatives considered on 2026-10-15 were (a) scale it with each country's paid plan (it is ~14% of India's
measured paid, ~6% of Indonesia's; would take India's from 396K to ~640K by December 2027 and the world total up ~0.3M), on the
reading that it is campaign installs arriving outside the Play Store; (b) drop it (`--cap-policy drop`), which opens a 0.9% hole at the
seam in India. Either is a recompose into a new directory, no model run.

## What carries it

| build | cap | notes |
|---|---|---|
| `probes_mobile_mau/<slug>/` (round 1, six probes) | **off** | uncorrected organic; the comparison beside the corrected runs |
| `probes_mobile_mau/round2_ioscap/<slug>/` (round 2, six probes) | **on** | sidecar `extra.ios_rate_cap` + `parameters.json.ios_rate_cap` name the file |
| `probes_mobile_mau/round3_ioscap/<slug>/` (round 3, eleven probes) | **on** | same; `cps0.0065_cpr0.87` is the chosen organic build |
| `mobile_organic_mau_2026-10-10/` (the chosen organic build, copy) | **on** | the organic half of the deliverable |
| `mobile_total_mau_2026-10-10/` + `handoff/glean2027_mobile_mau_handoff/` | **on**, excess held flat | the deliverable; the `organic_inauthentic_` columns are this correction's removed series |

Every build that carries it says so in two places: `parameters.json` → `"ios_rate_cap": "<path>"` and the parquet sidecar
`extra.ios_rate_cap` (countries, from date, removed per country on the last day, cap file). A build whose sidecar has
`"ios_rate_cap": null` did not train on it.

## How to turn it off (rollback)

1. Rerun the chosen build **without** `--ios-rate-cap`:
   ```bash
   python scripts/run_mobile_mau_forecast.py --forecast-start-date 2026-10-10 \
       --raw-dir data-official/2027-glean-draft/raw_glean_mobile_countryxapp_2026-10-09 \
       --iran-fill data-official/2027-glean-draft/iran_fill_mobile/iran_fill.glean_mobile.MAU.parquet \
       --organic-split data-official/2027-glean-draft/organic_mau/fenix_paid_organic_mau.2026-10-09.parquet \
       --results-dir <new dir> --changepoint-range <cpr> --changepoint-prior-scale <cps>
   ```
   (for the probes: `GRIDS["round2"]["ios_rate_cap"] = False` in `scripts/run_mobile_mau_probes.py`, or add a grid.)
2. Re-export the curves / handoff from the new build and move the LOCK.md row to it.
3. Leave this directory and the capped builds on disk (historical builds are never deleted); note the reversal here and in
   PROCEDURE.html §16.

There is nothing to add back: the correction never produced a curve that was added to a forecast, so turning it off is
exactly "train on the uncorrected organic series".

## How to change it

Different countries, date or smoothing: rerun the builder and pass the new file.
```bash
python scripts/build_ios_rate_cap.py --countries IN ID --from-date 2026-04-01 [--smoothing-days 28]
```
The runner refuses a cap file whose `organic_mau` column does not match the organic frame it is given (a different split or
raw pull means the cap must be rebuilt), and refuses to apply a cap twice.

## Files

- `ios_rate_cap.ID_IN.2026-04-01.parquet` — the series (`submission_date, country, organic_mau, ios_mau, ios_ratio_smoothed, cap_level, capped_mau, removed_mau`)
- `ios_rate_cap.ID_IN.2026-04-01.meta.json` — inputs with sha1s, the numbers above, the evidence pointer
- `ios_rate_cap.json` — the spec (countries, from date, smoothing, data file + sha1, runner flag)
- `plots/ios_rate_cap.ID_IN.2026-04-01.png` — organic vs capped vs the iOS-rate path per country, full range + zoom
- code: `src/mozaic_daily/ios_rate_cap.py` (pure), `scripts/build_ios_rate_cap.py` (writer), `scripts/run_mobile_mau_forecast.py --ios-rate-cap` (applier), `tests/test_ios_rate_cap.py`

## Known rough edges

- In the first weeks after 2026-04-01 the cap binds briefly in both countries (Indonesia's iOS dipped in April) and removes
  up to ~10K; that is the rule working as written, not the attribution effect, which only becomes material from June.
- The cap holds the two countries exactly at their iOS growth. If some of the excess was real organic growth, the corrected
  series is low by that amount; if some of the bleed started before April (India's paid share was already 25%), it is high.
  Neither is measurable from here; the marketing DS read is the next input.
