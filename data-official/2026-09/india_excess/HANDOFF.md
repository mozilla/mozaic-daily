# Handoff: adjustment `i` — `india_excess`

**Read this before touching the files here.** They were produced by a different agent
working in a different repository, they are **not wired into the pipeline**, and the thing
they encode is a *measured gap with a hypothesised cause*, not an attribution.

* **Producer**: `~/work/product-data-science-core/scratch/brwells/regional-story/india_forecast/`
* **Evidence page**: that project's `site/india_forecast.html` — every number below is on it
* **Reasoning**: that project's `DECISIONS.md`, D92–D96
* **Data edge**: 2026-08-29 · **Cycle**: 2026-09 · **Code**: `i` · **Source**: `legacy_desktop`

---

## 1. What the component is

India's desktop DAU has run above what a typical year would have done since late May 2026.
This is the curve that lets the forecast subtract that gap before training and add it back
afterwards, exactly like `l`, `o` and `j`. "Typical" is the 2022–2025 mean of each year's
rebased 28-day curve, by calendar day — the same norm every page on the regional site uses.

**The cause is a hypothesis.** A change in the Indian university calendar is the leading
candidate and is not established. The regional project's earlier reading (D70–D74) put 2026
inside India's historical range on measures that remove level and trend, and attributed most
of India's year-on-year channel improvement to the arithmetic of a decaying ESR population.
This component rests on Brendan's decision (2026-09-01) that a twelve-week run above the
four-year *maximum* is a real-world effect to carry forward, not noise to revert. Report it
as "India above typical", never as an education effect.

**It is already net of `l`.** Launch-on-login allocates its lift by trailing `modern_windows`
DAU share; India's share is **5.72%** (from the August cycle's own locked legacy training
frame), i.e. **11,450 DAU/day** at the edge, 21% of the gross gap. That is subtracted here so
the two overlays do not remove the same DAU twice. **Do not net it again.** `o` allocates
India nothing (ROW 1%); `j` is Japan only.

## 2. Which file to use

| file | use |
|---|---|
| `india_excess.json` | the spec. Points at **SETTLE**. |
| `india_excess.{scenario}.2026-08-29.parquet` | **what the pipeline loads** |
| `india_excess.{scenario}.2026-08-29.csv` | human reading only — `load_lift_series` cannot read it |
| `india_excess.all_scenarios.2026-08-29.csv` | all five side by side |
| `india_excess.{scenario}.2026-08-29.meta.json` | provenance, onset rule and alternatives, every bridged day, fitted half-life, caveats |
| `_index.md` | short summary for a human |

The parquet carries the date as its **index**, not a column — `load_lift_series` does
`df[value_column]` then reads `.index`. Columns: `india_excess_dau_daily`,
`india_excess_dau_ma` (trailing 28), `source` (`pre-onset` / `measured` / `projected`).

## 3. Wiring it — registry entry only (updated 2026-09-04)

The seven `main.py` touch points this section used to list are gone: `src/mozaic_daily/overlays.py`
now dispatches every code registered with `applier: per_tile_overlay`, finds its spec by
`spec_glob`, gates on `applies_to_forecast_start`, applies it to `applies_to_data_source`, and
derives a distinct sentinel (`india_excess_subtracted`) from the registry `name`. So:

1. `data-official/adjustment_codes.yaml` — add `i` with `applier: per_tile_overlay`,
   `spec_glob: "data-official/*/india_excess/india_excess.json"`.
2. Nothing in `main.py`. Fixed country shares are chosen by the spec's `allocation.key`.
3. `tests/test_overlays.py::TestCommittedRegistryAndSpecs` — pin that the September seam resolves to
   the intended set.
4. A model re-run.

Single letter only — `parse_state_from_path` splits the marker into characters.

## 4. Scenarios — which path persists is a planning choice, judged at 15 Dec 2026

Net excess, trailing 28-day mean, DAU/day:

| scenario | Dec-15 2026 | Jun-15 2027 | Dec-15 2027 | assumes |
|---|--:|--:|--:|---|
| Hold · the peak excess is carried flat | 57,155 | 57,155 | 57,155 | whatever produced this keeps producing it at the largest level yet seen (28-day peak, 27 Jun) |
| Proportional · a constant share of the typical level | 41,945 | 41,099 | 41,945 | excess stays 1.58% of India's typical DAU; follows the seasonal calendar, returns next summer |
| Linger · decays over a year | 33,317 | 20,115 | 12,110 | excess relaxes toward zero with a **250-day half-life — a planning constant**, the midpoint of the menu at the decision date |
| **Settle · decays slowly** (shipped) | 20,946 | 5,156 | 1,260 | excess relaxes toward zero with a **90-day half-life — a planning constant** (≈ one semester), not an estimate |
| Fade · decays fast | 1,161 | 1 | 0 | reverts at the rate India's past deviations from typical did: **18-day half-life, fitted** (AR(1) on monthly leave-one-out blocks, φ 0.31/month, 44 pairs) |

All five start from the same net level at the edge, **43,286/day**. Settle at 60 / 120 days
reads 14,592 / 25,105 on Dec-15 2026. Switching scenario is a one-line `data_file` edit
**plus a model re-run** — the curve is subtracted from training rows.

**What history says (evidence.py; it narrows, it does not decide).** The summer's own
increment over 2025 is 92% clients older than 28 days and 73% clients active 10+ days a
month, with no new-profile surge and a DAU/MAU rise three times anything in the norm years:
existing users more often, not a cohort, so `fade`'s churn logic does not fit the mechanism.
The one positive-summer norm year (2023) held about two thirds of its late-August excess
through November and was gone by 15 December; the 2026 excess itself halved once in five
weeks and recovered — both against `hold`. History leans to the `settle` end of the middle;
`linger` is the case for a driver running the whole semester, `proportional` for a permanent
change in use. The 2020/21 shocks are secondary (depressions that recovered in ~2 months,
anchored on distorted springs).

## 5. Four things that will bite you

**Holidays.** Every year was bridged before the gap was measured — mozaic's own India
calendar days that dipped past mozaic's `holiday_threshold` (−3.2%), *plus* any India-only
day below −10% of its weekday-matched neighbours while the world ex-India was not down.
The second rule exists because mozaic's calendar has **no Diwali, Dussehra, Makar Sankranti
or Ambedkar Jayanti**, and its Islamic dates run a day early; a cross-year comparison is only
holiday-neutral if every year is treated alike. **This changes nothing about what mozaic
sees**: the delivered curve is `actual × ratio`, so every dip stays in the training frame
exactly as it is today. It changed the answer a lot, though — the unbridged late-August gap
reads +4.75 index points, the bridged one +2.32 (2026's Saturday Independence Day alone was
worth ~1.7). Never quote an unbridged gap as the component.

**The curve is a trailing-28-day statistic.** The daily value on day d carries the gap *as
of the window ending on d*, so a step arrives over four weeks — the same lag the KPI is
quoted with — and the curve's own 28-day mean is the four-week mean of the gap, not the gap
on the last day.

**The `l` share is the August cycle's.** The September run recomputes India's share on
fresher training data; it moves by tenths of a point. If `l` is re-pointed at a different
allocation, regenerate this.

**The curve runs to 2027-12-31.** `add_lift_to_forecast` zero-fills absent dates. `fade`
reaches zero on its own; the others are held on their path.

## 6. What is measured and what is assumed

| quantity | status |
|---|---|
| the typical curve (2022–2025) | **measured** |
| the 2026 gap through 2026-08-29 | **measured** — this is what ships as history |
| onset 2026-05-22 | **detected**: first 14-day run above the norm years' maximum after the anchor window (Apr 16); a 7-day sustain and a 1σ rule land on 22 and 24 May; 2σ never fires |
| holiday and dip bridging | **rule**, symmetric across years, every day listed in the meta |
| India's share of `l` | **computed** from the pipeline's own frame |
| `fade` half-life (18 d) | **fitted from history** |
| `settle` half-life (90 d) | **planning constant** |
| which path persists | **an assumption. Yours to choose.** |
| the cause | **a hypothesis** |

## 7. Regenerating

Do not hand-edit these files. From the producer project (`~/.pyenv/versions/3.9.24/bin/python3`,
`MPLCONFIGDIR=./tmp/mpl`):

```bash
python3 tooling/refresh.py --dry-run                     # if the data edge should move
python3 tooling/build_india_forecast_site.py
python3 diagnostics/check_india_forecast.py              # 117 checks, must be green
python3 -c "from india_forecast import deliver; deliver.write()"
```
