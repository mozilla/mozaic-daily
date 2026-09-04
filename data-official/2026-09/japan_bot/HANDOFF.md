# Handoff: adjustment `j` — `japan_bot`

**Read this before touching the files here.** They were produced by a different agent
working in a different repository, they are **not wired into the pipeline**, and two of the
things they encode contradict assumptions you may reasonably hold.

* **Producer**: `~/work/product-data-science-core/scratch/brwells/regional-story/forecast/`
* **Evidence page**: that project's `site/forecast.html` — every number below is on it
* **Reasoning**: that project's `DECISIONS.md`, D76–D90
* **Data edge**: 2026-08-30 · **Cycle**: 2026-09 · **Code**: `j` · **Source**: `legacy_desktop`

---

## 1. What the component is

Japan's reported desktop DAU carries a population of automated clients that arrived from
late June 2026. This is the curve that lets the forecast subtract them before training and
add them back afterwards, exactly like `l` and `o`.

**It is a masking effect, not an improvement.** Japan has two separate inflows on top of a
falling real user base: this one, and the MozillaOnline migration you already model as `o`.
Underneath both, ordinary Japanese users on ordinary Firefox are down. Any narrative that
reports Japan improving without saying so is wrong.

**No double-count with `o`.** The flagged population is 95.1% ESR; the MozillaOnline
migration is entirely non-ESR (`zh` × ESR moved −23 DAU/day year-on-year). `o` already
allocates 0.98% of its curve to JP, and that is a different population. This was checked,
not assumed.

## 2. Which file to use

| file | use |
|---|---|
| `japan_bot.json` | the spec. Points at **MIDDLE**. |
| `japan_bot.{scenario}.2026-08-30.parquet` | **what the pipeline loads** |
| `japan_bot.{scenario}.2026-08-30.csv` | human reading only — `load_lift_series` cannot read it |
| `japan_bot.all_scenarios.2026-08-30.csv` | all three side by side |
| `japan_bot.{scenario}.2026-08-30.meta.json` | provenance, fit statistics, caveats |
| `_index.md` | short summary for a human |

The parquet carries the date as its **index**, not a column — `load_lift_series` does
`df[value_column]` then reads `.index`, so a date column would silently give a RangeIndex
and every lookup would miss. Columns match `l` and `o`: `japan_bot_dau_daily` +
`japan_bot_dau_ma`, plus the MAU pair and a `source` column marking measured vs projected.

## 3. Wiring it — seven touch points in `main.py`, plus the registry

`main.py` hand-wires every adjustment code; there is no generic registry path. **Copy the
`o` (MozillaOnline) pattern, not the `l` one** — `j` uses fixed country shares, and `l`
computes them from a trailing DAU window.

1. `_find_japan_bot_spec_for_forecast()` — mirror `_find_mozillaonline_spec_for_forecast`
   (~line 392); glob `data-official/*/japan_bot/japan_bot.json`.
2. `_apply_japan_bot_pre_mozaic()` — mirror `_apply_mozillaonline_pre_mozaic` (~line 419).
   Use `fixed_country_shares_from_spec`, and a **distinct** `sentinel_attr`
   (`"japan_bot_subtracted"`). Two overlays on one training frame need distinct sentinels
   or the idempotency guard misfires.
3. `process_data_source(...)` — add `japan_bot_spec_path` (~line 471) and its docstring
   entry (~line 506).
4. Pre-mozaic call (~line 575), gated on `data_source == DataSource.LEGACY_DESKTOP`.
5. Add-back via `add_lift_to_forecast` (~line 668), `population_value` taken from
   `spec["allocation"]["flag_column"]`, before the format function.
6. The wrapper at ~line 697/746 that threads the path through.
7. `main()` resolves it at ~line 897 and passes it at ~line 928.

Then `data-official/adjustment_codes.yaml`, and a case in `tests/test_adjustments.py`.

**The code must stay a single letter.** `parse_state_from_path` splits the filename marker
into characters, so `adj-hjb` parses to `['b','h','j']` against meta `['h','jb']` and
`load_forecast` raises `State drift` on every artifact. Verified by running it.

## 4. Scenarios — the plateau is a planning choice, not an estimate

| scenario | plateau DAU/day | % of Japan | reaches | Dec-15 DAU (28ma) | Dec-15 MAU (28ma) |
|---|--:|--:|---|--:|--:|
| Low | 33,551 | 3.09% | at the edge | 33,551 | 139,094 |
| **Middle** (shipped) | 67,101 | 6.19% | 2026-11-28 | 67,094 | 277,108 |
| High | 108,423 | 10.00% | 2027-02-17 | 82,617 | 326,265 |

No inflection has been observed, so the ceiling is **not identifiable from the data**. All
three ramp at the same measured rate (501 DAU/day since onset) and differ only in where they
stop. Switching scenario is a one-line `data_file` edit **plus a model re-run** — the curve
is subtracted from training rows, so a spec-only change moves nothing downstream.

Note where Low sits: if arrivals merely stopped *rising* and held at their current rate, the
component would settle around 55,900. Low is a mild step *down* from what has already
happened, not a no-growth case.

## 5. Four things that will bite you

**The history is raw daily and deliberately unsmoothed.** `l` smooths its curve with a
7-day mean; do not copy that here. `l`'s curve is a noisy *estimate* of a smooth effect.
This one is a direct count — the fleet genuinely contributed 24,559 client-days on 22 August
and 45,689 on 28 August. Because the overlay is bidirectional and `subtract_lift_from_training`
runs before the model, subtracting a smoothed curve would leave a synthetic ±10k oscillation
in the training frame that is not Japanese users.

**The curve is zero before 2026-06-24 by design.** The file spans 2026-04-01 → 2027-12-31,
but everything before the detected onset is deliberately zeroed. `excess = flagged - baseline`
is clipped at zero, so without the gate the positive half of the baseline's own wobble
survives and the negative half does not — 48 non-zero days and 12,027 client-days of
rectified noise, all inside the baseline's sd of 265. The onset date itself is corroborated:
three independent "no precedent" rules (3σ of the excess, level above its 23-month maximum,
28-day ratio above its 23-month maximum) land within five days of each other.

**The curve runs to 2027-12-31, held flat. Do not truncate it.** `add_lift_to_forecast`
zero-fills absent dates, so a curve ending 2026-12-31 drops the entire component on 1
January. `o` and `l` both hold flat for the same reason.

**`sample_id` prunes on `desktop_active_users`; `country` does not.** 1,762 GB → 198 GB at
10%. This contradicts the widely-held note that the table prunes on neither. It is why the
source pull costs $2.64 rather than $50 — if you regenerate anything from this table, keep
the `sample_id` filter.

**Do not treat the source cohort file as a level.** It is a 20% client sample. Shape comes
from it; levels come from the full-population signals file. Same split the mobile
paid/organic component uses ("share from the mirror, level from production").

## 6. What is measured and what is assumed

Honest accounting, because the two are easy to conflate:

| quantity | status |
|---|---|
| daily excess, 2026-06-24 → 2026-08-30 | **measured** — this is what history ships |
| arrivals, through 2026-08-03 | **measured** (counted cohorts) |
| per-cohort activity kernel, through 2026-08-03 | **measured** |
| sample scale (6.0) | calibration, one constant |
| arrivals, 2026-08-04 → edge | inverted from measured DAU |
| tail kernel scale (4.94 active days) | **solved from MAU** |
| tail kernel shape | borrowed from the last measurable cohort |
| the plateau | **an assumption. Yours to choose.** |

Fit against raw daily values: **DAU WAPE 3.3%, r 0.9972**, 10 of 10 turning points at zero
lag. **MAU WAPE 7.9%, r 0.991**. DAU carries one free parameter, MAU twenty-eight — they are
not comparable on error alone.

**MAU is an input here, not a validation.** DAU is structurally blind to the tail kernel:
the tail arrival series is inverted *from* DAU, so any kernel value reproduces DAU to 0.3%
because arrivals simply absorb it. Only a client count can break the tie. Do not describe
MAU as an out-of-sample check — an earlier version of this work did, and it was wrong.

## 7. Two assumptions with a shelf life

**The most recent four weeks of arrivals can never be measured.** A cohort's behaviour is
unknowable until it has lived 28 days. Every refresh inherits this; it is not a gap to fix.

**The classifier's `age ≤ 28d` clause can only see automation younger than four weeks.** It
is safe *today* because the fleet re-provisions rather than ages — with the clause dropped,
the 0-6d band went 781 → 28,457 between May and August while 28-180d went 4,532 → 5,122. If
that middle band starts rising, the component is silently under-counted. The producer's page
plots it as a standing monitor.

## 8. Regenerating

Do not hand-edit these files. From the producer project:

```bash
python3 tooling/pull_bot_cohort_activity.py --dry-run   # ~423 GB, ~$2.64
python3 tooling/pull_bot_cohort_activity.py
python3 tooling/build_forecast_site.py
python3 diagnostics/check_forecast.py                   # must be green before shipping
python3 -c "from forecast import deliver; deliver.write()"
```

`diagnostics/check_forecast.py` (73 checks) guards the specific mistakes this component made
on the way here: a kernel denominator that produced survival ratios above 1, a stationary
kernel where the data is not stationary, a curve that stops before the horizon, smoothed
history, and prose that reverts to calling MAU an out-of-sample check.
